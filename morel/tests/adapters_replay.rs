use morel::{Graph, Live, Replay, Stop, Stream, Time};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn collect_timed<T: Clone + 'static>(s: &Stream<T>) -> Rc<RefCell<Vec<(T, Time)>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen2 = seen.clone();
    s.sink(move |v, at| seen2.borrow_mut().push((v, at)));
    seen
}

#[test]
fn replay_from_iter_emits_each_item_at_its_time() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(20), 2), (ms(35), 3)]);
    let seen = collect_timed(&src);

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(*seen.borrow(), vec![(1, ms(10)), (2, ms(20)), (3, ms(35))]);
}

#[test]
fn equal_timestamps_fire_as_ordered_sub_instant_steps() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(10), 2), (ms(10), 3), (ms(20), 4)]);
    let seen = collect_timed(&src);

    let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(
        *seen.borrow(),
        vec![(1, ms(10)), (2, ms(10)), (3, ms(10)), (4, ms(20))]
    );
    assert_eq!(summary.steps, 4, "each item is its own engine step");
}

#[test]
fn empty_input_ends_the_run_immediately() {
    let g = Graph::new();
    let src = g.replay_from_iter(Vec::<(Time, i64)>::new());
    let seen = collect_timed(&src);

    let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

    assert!(seen.borrow().is_empty());
    assert_eq!(summary.steps, 0);
}

#[test]
fn live_run_with_replay_source_fails() {
    let g = Graph::new();
    let _src = g.replay_from_iter(vec![(ms(10), 1i64)]);

    let err = g
        .run(Live::new().stop(Stop::After(Duration::from_millis(10))))
        .unwrap_err();

    assert!(err.to_string().contains("replay source used in a live run"));
}

#[test]
fn item_before_run_start_fails() {
    let g = Graph::new();
    let _src = g.replay_from_iter(vec![(ms(10), 1i64)]);

    let err = g.run(Replay::from(ms(50))).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("is behind the run"), "{message}");
    assert!(message.contains("0.010000000"), "{message}");
    assert!(message.contains("0.050000000"), "{message}");
}

#[test]
fn decreasing_times_fail_at_the_offending_item() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(20), 1i64), (ms(10), 2)]);
    let seen = collect_timed(&src);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert_eq!(
        *seen.borrow(),
        vec![(1, ms(20))],
        "valid prefix still emits"
    );
    let message = err.to_string();
    assert!(message.contains("is behind the run"), "{message}");
    assert!(message.contains("0.010000000"), "{message}");
    assert!(message.contains("0.020000000"), "{message}");
}
