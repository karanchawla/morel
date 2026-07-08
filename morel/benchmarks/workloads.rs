use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use morel::{gather, merge, Graph, Replay, Time};
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

const WORKLOAD_EVENTS: u64 = 20_000;
const WORD_EVENTS: u64 = WORKLOAD_EVENTS;
const MAP_FILTER_EVENTS: u64 = WORKLOAD_EVENTS;
const WINDOW_EVENTS: u64 = WORKLOAD_EVENTS;
const JOIN_EVENTS: u64 = WORKLOAD_EVENTS;
const FANOUT_EVENTS: u64 = WORKLOAD_EVENTS;

#[derive(Clone, Copy)]
struct Reading {
    partition: u64,
    value: u64,
}

fn ns(nanos: u64) -> Time {
    Time::from_nanos(nanos)
}

fn build_word_count() -> (Graph, Rc<Cell<u64>>) {
    let g = Graph::new();
    let corpus = [
        "morel streams count words",
        "streams process events",
        "stateful event aggregation",
        "count words with scan",
        "morel replay streams",
        "short text events",
        "scan maintains counts",
        "event streams aggregate text",
    ];
    let events = (0..WORD_EVENTS as usize).map(move |i| {
        let at = ns(i as u64);
        (at, corpus[i % corpus.len()])
    });

    let last_total = Rc::new(Cell::new(0u64));
    let last_total_sink = last_total.clone();
    g.replay_from_iter(events)
        .map(|line| {
            line.split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .scan(HashMap::<String, u64>::new(), |counts, words| {
            for word in words {
                *counts.entry(word).or_insert(0) += 1;
            }
        })
        .sink(move |counts, _| {
            last_total_sink.set(counts.values().sum());
        });

    (g, last_total)
}

fn build_map_filter_scan() -> (Graph, Rc<Cell<u64>>) {
    let g = Graph::new();
    let events = (0..MAP_FILTER_EVENTS).map(|seq| (ns(seq * 1_000), seq));

    let last_total = Rc::new(Cell::new(0u64));
    let last_total_sink = last_total.clone();
    g.replay_from_iter(events)
        .map(|value| (value * 3) + 7)
        .filter(|value| value % 5 != 0)
        .scan(0u64, |total, value| *total = total.wrapping_add(value))
        .sink(move |value, _| last_total_sink.set(value));

    (g, last_total)
}

fn build_windowed_count() -> (Graph, Rc<Cell<u64>>) {
    let g = Graph::new();
    let readings = (0..WINDOW_EVENTS).map(|seq| {
        (
            ns(seq * 1_000_000),
            Reading {
                partition: seq % 6,
                value: 1 + (seq % 10),
            },
        )
    });

    let values = g
        .replay_from_iter(readings)
        .map(|reading| reading.value + reading.partition);
    let tumbling_count = values
        .window_tumbling(Duration::from_millis(10))
        .map_batch(|batch| batch.len() as u64);
    let sliding_sum = values
        .window_sliding(Duration::from_millis(30), Duration::from_millis(10))
        .map_batch(|batch| batch.iter().sum::<u64>());

    let last_window = Rc::new(Cell::new(0u64));
    let last_window_sink = last_window.clone();
    merge(&[&tumbling_count, &sliding_sum]).sink(move |value, _| last_window_sink.set(value));

    (g, last_window)
}

fn build_latest_join() -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let events = (0..JOIN_EVENTS).map(|seq| (ns(seq * 1_000), (seq as i64 % 97) - 32));
    let controls = (0..JOIN_EVENTS / 4).map(|seq| (ns(seq * 4_000), 2 + (seq as i64 % 5)));
    let ticks = (0..JOIN_EVENTS / 2).map(|seq| (ns((seq * 2 + 1) * 1_000), seq));

    let values = g.replay_from_iter(events);
    let factors = g.replay_from_iter(controls);
    let trigger = g.replay_from_iter(ticks);

    let last_joined = Rc::new(Cell::new(0i64));
    let last_joined_sink = last_joined.clone();
    values
        .with_latest(&factors, |value, factor| value * factor)
        .sample(&trigger)
        .sink(move |value, _| last_joined_sink.set(value));

    (g, last_joined)
}

fn build_fanout_gather() -> (Graph, Rc<Cell<i64>>) {
    let g = Graph::new();
    let source = g.replay_from_iter((0..FANOUT_EVENTS).map(|seq| (ns(seq * 10), seq as i64)));
    let doubled = source.map(|value| value * 2);
    let offset = source.map(|value| value + 17);
    let parity = source.map(|value| value & 1);
    let branch_refs = [&doubled, &offset, &parity];

    let last_sum = Rc::new(Cell::new(0i64));
    let last_sum_sink = last_sum.clone();
    gather(&branch_refs)
        .map(|values| values.into_iter().sum::<i64>())
        .sink(move |value, _| last_sum_sink.set(value));

    (g, last_sum)
}

fn bench_word_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("word_count");
    group.throughput(Throughput::Elements(WORD_EVENTS));
    group.bench_function("morel", |b| {
        b.iter_batched(
            build_word_count,
            |(g, last_total)| {
                g.run(Replay::from(Time::EPOCH)).unwrap();
                black_box(last_total.get());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_map_filter_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_filter_scan");
    group.throughput(Throughput::Elements(MAP_FILTER_EVENTS));
    group.bench_function("morel", |b| {
        b.iter_batched(
            build_map_filter_scan,
            |(g, last_total)| {
                g.run(Replay::from(Time::EPOCH)).unwrap();
                black_box(last_total.get());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_windowed_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("windowed_count");
    group.throughput(Throughput::Elements(WINDOW_EVENTS));
    group.bench_function("morel", |b| {
        b.iter_batched(
            build_windowed_count,
            |(g, last_window)| {
                g.run(Replay::from(Time::EPOCH)).unwrap();
                black_box(last_window.get());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_latest_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("latest_join");
    group.throughput(Throughput::Elements(JOIN_EVENTS));
    group.bench_function("morel", |b| {
        b.iter_batched(
            build_latest_join,
            |(g, last_joined)| {
                g.run(Replay::from(Time::EPOCH)).unwrap();
                black_box(last_joined.get());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_fanout_gather(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_gather");
    group.throughput(Throughput::Elements(FANOUT_EVENTS));
    group.bench_function("morel", |b| {
        b.iter_batched(
            build_fanout_gather,
            |(g, last_sum)| {
                g.run(Replay::from(Time::EPOCH)).unwrap();
                black_box(last_sum.get());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_map_filter_scan,
    bench_word_count,
    bench_windowed_count,
    bench_latest_join,
    bench_fanout_gather,
);
criterion_main!(benches);
