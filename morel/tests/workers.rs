use morel::{
    producer, source_worker, worker, Ctx, Graph, Input, Live, Operator, Output, ProducerClosed,
    Replay, Stream, Time,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

struct Scheduled {
    values: Vec<(i64, Time)>,
    index: usize,
    out: Output<i64>,
}

impl Operator for Scheduled {
    fn on_start(&mut self, cx: &mut Ctx) {
        if let Some((_, at)) = self.values.first() {
            cx.at(*at);
        }
    }

    fn step(&mut self, cx: &mut Ctx) {
        let (value, _) = self.values[self.index];
        self.index += 1;
        if let Some((_, at)) = self.values.get(self.index) {
            cx.at(*at);
        }
        self.out.set(value);
    }
}

fn scheduled(g: &Graph, values: &[(i64, u64)]) -> Stream<i64> {
    let values = values
        .iter()
        .map(|(v, ms)| (*v, Time::EPOCH + Duration::from_millis(*ms)))
        .collect();
    g.add(move |w| Scheduled {
        values,
        index: 0,
        out: w.output(),
    })
}

fn collect_flat(s: &Stream<Vec<i64>>) -> Rc<RefCell<Vec<(i64, Time)>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen2 = seen.clone();
    s.sink(move |burst, at| {
        for value in burst {
            seen2.borrow_mut().push((value, at));
        }
    });
    seen
}

struct DoubleFireAtSameTime {
    input: Input<Vec<i64>>,
    out: Output<i64>,
    pending_second: Option<i64>,
}

impl Operator for DoubleFireAtSameTime {
    fn step(&mut self, cx: &mut Ctx) {
        if self.input.fired() {
            let first = self.input.get().into_iter().next().unwrap();
            self.pending_second = Some(first * 100);
            cx.at(cx.now());
            self.out.set(first * 10);
        } else if let Some(second) = self.pending_second.take() {
            self.out.set(second);
        }
    }
}

#[test]
fn replay_worker_matches_single_threaded_equivalent() {
    fn run_worker() -> Vec<(i64, Time)> {
        let g = Graph::new();
        let src = scheduled(&g, &[(1, 10), (2, 20), (3, 30)]);
        let out = worker(&src, |_child, input| {
            input.map(|burst| burst.into_iter().next().unwrap() * 10)
        });
        let seen = collect_flat(&out);
        g.run(Replay::from(Time::EPOCH)).unwrap();
        let collected = seen.borrow().clone();
        collected
    }

    fn run_inline() -> Vec<(i64, Time)> {
        let g = Graph::new();
        let src = scheduled(&g, &[(1, 10), (2, 20), (3, 30)]);
        let out = src.map(|value| value * 10).map(|value| vec![value]);
        let seen = collect_flat(&out);
        g.run(Replay::from(Time::EPOCH)).unwrap();
        let collected = seen.borrow().clone();
        collected
    }

    assert_eq!(run_worker(), run_inline());
    assert_eq!(run_worker(), run_worker());
}

#[test]
fn replay_worker_sparse_child_output_uses_same_time_watermarks() {
    let g = Graph::new();
    let src = scheduled(&g, &[(1, 10), (2, 20), (3, 30), (4, 40)]);
    let out = worker(&src, |_child, input| {
        input
            .map(|burst| burst.into_iter().next().unwrap())
            .filter(|value| value % 2 == 0)
            .map(|value| value * 10)
    });
    let seen = collect_flat(&out);

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(
        *seen.borrow(),
        vec![
            (20, Time::EPOCH + Duration::from_millis(20)),
            (40, Time::EPOCH + Duration::from_millis(40)),
        ]
    );
}

#[test]
fn replay_worker_slow_child_is_still_deterministic() {
    let g = Graph::new();
    let src = scheduled(&g, &[(1, 10), (2, 20), (3, 30)]);
    let out = worker(&src, |_child, input| {
        input.map(|burst| {
            thread::sleep(Duration::from_millis(20));
            burst.into_iter().next().unwrap() * 10
        })
    });
    let seen = collect_flat(&out);

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(
        seen.borrow().iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
}

#[test]
fn replay_worker_rejects_delayed_child_output() {
    let g = Graph::new();
    let src = scheduled(&g, &[(1, 10), (2, 20)]);
    let out = worker(&src, |_child, input| {
        input
            .map(|burst| burst.into_iter().next().unwrap())
            .delay(Duration::from_millis(1))
    });
    let _seen = collect_flat(&out);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err
        .to_string()
        .contains("paced replay packet arrived after its lockstep instant"));
}

#[test]
fn replay_worker_rejects_multiple_child_return_fires_at_one_time() {
    let g = Graph::new();
    let src = scheduled(&g, &[(1, 10), (2, 20)]);
    let out = worker(&src, |child, input| {
        child.add(|w| DoubleFireAtSameTime {
            input: w.on(&input),
            out: w.output(),
            pending_second: None,
        })
    });
    let _seen = collect_flat(&out);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(
        err.to_string()
            .contains("replay packet time is behind receiver time"),
        "{err}"
    );
}

#[test]
fn live_worker_delivers_values() {
    let g = Graph::new();
    let mut n = 0i64;
    let src = g
        .ticker(Duration::from_millis(20))
        .map(move |()| {
            n += 1;
            n
        })
        .take(3);
    let out = worker(&src, |_child, input| {
        input.map(|burst| {
            thread::sleep(Duration::from_millis(2));
            burst.into_iter().next().unwrap() * 10
        })
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(morel::Stop::After(Duration::from_millis(160))))
        .unwrap();

    assert_eq!(
        seen.borrow().iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
}

#[test]
fn live_worker_sparse_child_output_shuts_down_cleanly() {
    let g = Graph::new();
    let mut n = 0i64;
    let src = g
        .ticker(Duration::from_millis(5))
        .map(move |()| {
            n += 1;
            n
        })
        .take(4);
    let out = worker(&src, |_child, input| {
        input
            .map(|burst| burst.into_iter().next().unwrap())
            .filter(|value| value % 2 == 0)
            .map(|value| value * 10)
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(morel::Stop::After(Duration::from_millis(120))))
        .unwrap();

    assert_eq!(
        seen.borrow().iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![20, 40]
    );
}

#[test]
fn live_source_worker_idle_child_shuts_down_cleanly() {
    let g = Graph::new();
    let out = source_worker(&g, |child| {
        child
            .ticker(Duration::from_secs(60))
            .filter(|()| false)
            .map(|()| 1i64)
    });
    let seen = collect_flat(&out);

    g.run(Live::new().stop(morel::Stop::After(Duration::from_millis(30))))
        .unwrap();

    assert!(seen.borrow().is_empty());
}

#[test]
fn replay_source_worker_matches_inline_source() {
    fn run_source_worker() -> Vec<(i64, Time)> {
        let g = Graph::new();
        let out = source_worker(&g, |child| scheduled(child, &[(5, 10), (6, 20)]));
        let seen = collect_flat(&out);
        g.run(Replay::from(Time::EPOCH).stop(morel::Stop::After(Duration::from_millis(30))))
            .unwrap();
        let collected = seen.borrow().clone();
        collected
    }

    fn run_inline() -> Vec<(i64, Time)> {
        let g = Graph::new();
        let out = scheduled(&g, &[(5, 10), (6, 20)]).map(|v| vec![v]);
        let seen = collect_flat(&out);
        g.run(Replay::from(Time::EPOCH).stop(morel::Stop::After(Duration::from_millis(30))))
            .unwrap();
        let collected = seen.borrow().clone();
        collected
    }

    assert_eq!(run_source_worker(), run_inline());
}

#[test]
fn replay_source_worker_requires_finite_parent_horizon() {
    let g = Graph::new();
    let out = source_worker(&g, |child| {
        child
            .ticker(Duration::from_secs(60))
            .filter(|()| false)
            .map(|()| 1i64)
    });
    let _seen = collect_flat(&out);

    let err = g
        .run(Replay::from(Time::EPOCH).stop(morel::Stop::Steps(1)))
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("source worker replay requires finite parent horizon"));
}

#[test]
fn external_producer_live_delivers_values() {
    let g = Graph::new();
    let out = producer(&g, |p| {
        for value in [1i64, 2, 3] {
            p.send(value).unwrap();
        }
    });
    let seen = collect_flat(&out);

    g.run(morel::Live::new().stop(morel::Stop::After(Duration::from_millis(80))))
        .unwrap();

    assert_eq!(
        seen.borrow().iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn external_producer_replay_fails_before_spawning() {
    let g = Graph::new();
    let out = producer(&g, |_p| panic!("producer should not start in replay"));
    let _seen = collect_flat(&out);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err.to_string().contains("external producer is live-only"));
}

#[test]
fn producer_send_reports_closed_after_graph_stops() {
    let g = Graph::new();
    let out = producer(&g, |p| {
        std::thread::sleep(Duration::from_millis(50));
        let result = p.send(1i64);
        assert!(matches!(result, Err(ProducerClosed)));
    });
    let _seen = collect_flat(&out);

    g.run(morel::Live::new().stop(morel::Stop::After(Duration::from_millis(10))))
        .unwrap();
}

#[test]
fn child_panic_surfaces_as_parent_node_error() {
    let g = Graph::new();
    let src = scheduled(&g, &[(1, 10)]);
    let out = worker(&src, |_child, input| {
        input.map(|_burst| -> i64 { panic!("child exploded") })
    });
    let _seen = collect_flat(&out);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err.to_string().contains("child exploded"), "{err}");
}

#[test]
fn producer_is_closed_flips_after_graph_stops() {
    let (probe_tx, probe_rx) = std::sync::mpsc::channel();
    let g = Graph::new();
    let out = producer::<i64, _>(&g, move |p| {
        while !p.is_closed() {
            thread::sleep(Duration::from_millis(1));
        }
        probe_tx.send(()).unwrap();
    });
    let _seen = collect_flat(&out);

    g.run(morel::Live::new().stop(morel::Stop::After(Duration::from_millis(15))))
        .unwrap();

    probe_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("producer saw is_closed");
}
