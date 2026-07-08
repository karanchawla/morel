use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use morel::{merge, Graph, Replay, Stop, Stream, Time};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

const RUN_STEPS: u64 = 10_000;
const WARMUP_STEPS: u64 = 10_000;
const TIMER_PRESSURE_STEPS: u64 = 128;

fn counter(g: &Graph) -> Stream<i64> {
    let mut n = 0i64;
    g.ticker(Duration::from_nanos(1)).map(move |()| {
        n += 1;
        n
    })
}

fn build_pipeline() -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let last = Rc::new(Cell::new(0i64));
    let last_sink = last.clone();
    counter(&g)
        .map(|x| x * 2)
        .filter(|x| x % 3 != 0)
        .map(|x| x + 1)
        .scan(0i64, |acc, x| *acc += x)
        .sink(move |x, _| last_sink.set(x));
    (g, last)
}

/// Build and run a small replay pipeline.
fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    for cycles in [1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(cycles));
        group.bench_with_input(BenchmarkId::new("morel", cycles), &cycles, |b, &cycles| {
            b.iter_batched(
                build_pipeline,
                |(g, last)| {
                    g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(cycles))))
                        .unwrap();
                    black_box(last.get());
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(
            BenchmarkId::new("raw_loop", cycles),
            &cycles,
            |b, &cycles| {
                b.iter(|| {
                    let mut n = 0i64;
                    let mut acc = 0i64;
                    for _ in 0..black_box(cycles) {
                        n += 1;
                        let x = n * 2;
                        if x % 3 != 0 {
                            acc += x + 1;
                        }
                    }
                    black_box(acc);
                });
            },
        );
    }
    group.finish();
}

fn build_map_chain(depth: usize) -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let mut current = counter(&g);
    for _ in 0..depth {
        current = current.map(|x| x + 1);
    }

    let last = Rc::new(Cell::new(0i64));
    let last_sink = last.clone();
    current.sink(move |x, _| last_sink.set(x));
    (g, last)
}

/// Cost of a single source flowing through N maps.
fn bench_map_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_chain");
    for depth in [1usize, 5, 10, 20, 50] {
        group.throughput(Throughput::Elements(RUN_STEPS));
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched(
                || build_map_chain(depth),
                |(g, last)| {
                    g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(RUN_STEPS))))
                        .unwrap();
                    black_box(last.get());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn build_fanout(width: usize) -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let src = counter(&g);
    let last = Rc::new(Cell::new(0i64));
    for i in 0..width {
        let last_sink = last.clone();
        src.map(move |x| x + i as i64)
            .sink(move |x, _| last_sink.set(x));
    }
    (g, last)
}

/// Cost of one source feeding N independent branches.
fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout");
    for width in [2usize, 5, 10, 20, 50] {
        group.throughput(Throughput::Elements(RUN_STEPS * width as u64));
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, &width| {
            b.iter_batched(
                || build_fanout(width),
                |(g, last)| {
                    g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(RUN_STEPS))))
                        .unwrap();
                    black_box(last.get());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Per-step cost on a graph that has already been built and started.
fn bench_step_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("step_overhead");

    let scenarios: &[(&str, usize, usize)] =
        &[("node", 0, 0), ("10x10", 10, 10), ("100x100", 100, 100)];
    for &(name, width, depth) in scenarios {
        group.bench_function(name, |b| {
            let g = Graph::new();
            let src = counter(&g);
            if width > 0 {
                let branches: Vec<Stream<i64>> = (0..width)
                    .map(|_| {
                        let mut s = src.clone();
                        for _ in 0..depth {
                            s = s.map(std::hint::black_box);
                        }
                        s
                    })
                    .collect();
                let refs: Vec<&Stream<i64>> = branches.iter().collect();
                merge(&refs).sink(|_, _| {});
            } else {
                src.sink(|_, _| {});
            }
            g.begin(Replay::from(Time::EPOCH));
            b.iter(|| {
                black_box(g.step());
            });
            g.end().unwrap();
        });
    }
    group.finish();
}

fn build_step_latency_pipeline() -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let last = Rc::new(Cell::new(0i64));
    let last_sink = last.clone();

    g.ticker(Duration::from_nanos(1))
        .map(|()| 1i64)
        .scan(0i64, |acc, value| *acc += value)
        .map(|value| value.wrapping_mul(3))
        .filter(|value| value % 2 != 0)
        .sink(move |value, _| last_sink.set(value));

    (g, last)
}

/// Steady-state cost of one engine step on a warmed graph-internal pipeline.
fn bench_step_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("step_latency");
    group.throughput(Throughput::Elements(1));

    group.bench_function("pipeline", |b| {
        let (g, last) = build_step_latency_pipeline();
        g.begin(Replay::from(Time::EPOCH).stop(Stop::Never));

        for _ in 0..WARMUP_STEPS {
            black_box(g.step());
        }

        b.iter(|| {
            black_box(g.step());
        });

        black_box(last.get());
        g.end().unwrap();
    });

    group.finish();
}

/// Cost to wire a graph with N branches.
fn bench_graph_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_build");
    for node_count in [1usize, 10, 50, 100] {
        group.throughput(Throughput::Elements(2 * node_count as u64 + 2));
        group.bench_with_input(
            BenchmarkId::from_parameter(node_count),
            &node_count,
            |b, &node_count| {
                b.iter(|| {
                    let g = Graph::new();
                    let src = counter(&g);
                    for i in 0..node_count {
                        src.map(move |x| x + i as i64).sink(|_, _| {});
                    }
                    black_box(g.len());
                });
            },
        );
    }
    group.finish();
}

fn build_sparse_activation(dormant: usize) -> Graph {
    let g = Graph::new();

    let cold = g.ticker(Duration::from_secs(10_000));
    for i in 0..dormant {
        cold.map(move |()| i as i64).sink(|_, _| {});
    }

    counter(&g).map(|x| x + 1).map(|x| x * 2).sink(|_, _| {});
    g
}

fn bench_sparse_activation(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_activation");
    for dormant in [0usize, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(dormant),
            &dormant,
            |b, &dormant| {
                let g = build_sparse_activation(dormant);
                g.begin(Replay::from(Time::EPOCH).stop(Stop::Never));
                black_box(g.step());
                b.iter(|| {
                    black_box(g.step());
                });
                g.end().unwrap();
            },
        );
    }
    group.finish();
}

fn build_timer_pressure(timers: usize) -> Graph {
    let g = Graph::new();
    for i in 0..timers {
        g.ticker(Duration::from_nanos(i as u64 + 1)).sink(|_, _| {});
    }
    g
}

fn bench_timer_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer_pressure");
    for timers in [10usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(TIMER_PRESSURE_STEPS));
        group.bench_with_input(
            BenchmarkId::from_parameter(timers),
            &timers,
            |b, &timers| {
                b.iter_batched(
                    || {
                        let g = build_timer_pressure(timers);
                        g.begin(Replay::from(Time::EPOCH).stop(Stop::Never));
                        black_box(g.step());
                        g
                    },
                    |g| {
                        for _ in 0..black_box(TIMER_PRESSURE_STEPS) {
                            black_box(g.step());
                        }
                        g.end().unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn build_payload_i64() -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let last = Rc::new(Cell::new(0i64));
    let last_sink = last.clone();
    counter(&g)
        .map(|x| x)
        .map(|x| x)
        .map(|x| x)
        .sink(move |x, _| last_sink.set(x));
    (g, last)
}

fn build_payload_bytes() -> (Graph, Rc<Cell<[u8; 64]>>) {
    let g = Graph::new();
    let last = Rc::new(Cell::new([0u8; 64]));
    let last_sink = last.clone();
    g.ticker(Duration::from_nanos(1))
        .map(|()| [42u8; 64])
        .map(|x| x)
        .map(|x| x)
        .map(|x| x)
        .sink(move |x, _| last_sink.set(x));
    (g, last)
}

fn build_payload_string() -> (Graph, Rc<Cell<usize>>) {
    let g = Graph::new();
    let last_len = Rc::new(Cell::new(0usize));
    let last_sink = last_len.clone();
    let payload = String::from("morel payload");
    g.ticker(Duration::from_nanos(1))
        .map(move |()| payload.clone())
        .map(|x| x)
        .map(|x| x)
        .map(|x| x)
        .sink(move |x, _| last_sink.set(x.len()));
    (g, last_len)
}

fn bench_payload_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_size");
    group.throughput(Throughput::Elements(RUN_STEPS));

    group.bench_function("i64", |b| {
        b.iter_batched(
            build_payload_i64,
            |(g, last)| {
                g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(RUN_STEPS))))
                    .unwrap();
                black_box(last.get());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("bytes_64", |b| {
        b.iter_batched(
            build_payload_bytes,
            |(g, last)| {
                g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(RUN_STEPS))))
                    .unwrap();
                black_box(last.get());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("owned_string_clone", |b| {
        b.iter_batched(
            build_payload_string,
            |(g, last_len)| {
                g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(black_box(RUN_STEPS))))
                    .unwrap();
                black_box(last_len.get());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pipeline,
    bench_map_chain,
    bench_fanout,
    bench_step_overhead,
    bench_step_latency,
    bench_graph_build,
    bench_sparse_activation,
    bench_timer_pressure,
    bench_payload_size,
);
criterion_main!(benches);
