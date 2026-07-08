use morel::core::{Graph, Replay, Stop, Time};
use morel::ops::merge;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

/// Build enough topology to exercise replay ordering and timer behavior.
fn run_once() -> Vec<(i64, u64)> {
    let g = Graph::new();
    let mut na = 0i64;
    let a = g.ticker(Duration::from_millis(7)).map(move |()| {
        na += 1;
        na
    });
    let mut nb = 0i64;
    let b = g.ticker(Duration::from_millis(13)).map(move |()| {
        nb += 100;
        nb
    });
    let left = a.map(|x| x * 2);
    let right = a.map(|x| x + 1000);
    let diamond = left.with(&right, |l, r| l + r);
    let merged = merge(&[&diamond, &b]);
    let thinned = merged.throttle(Duration::from_millis(10));
    let summed = thinned.scan(0i64, |acc, v| *acc += v);

    let seen = Rc::new(RefCell::new(Vec::new()));
    let s2 = seen.clone();
    summed.sink(move |v, t| s2.borrow_mut().push((v, t.as_nanos())));

    g.run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(500))))
        .unwrap();

    // The sink owns the other `Rc`; after dropping the graph, this should be
    // the only remaining handle.
    drop(g);
    Rc::try_unwrap(seen).unwrap().into_inner()
}

#[test]
fn replay_is_bit_identical_across_runs() {
    let first = run_once();
    assert!(!first.is_empty(), "graph must actually produce output");
    for _ in 0..5 {
        assert_eq!(run_once(), first, "replay must be deterministic");
    }
}
