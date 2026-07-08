use morel::{merge, producer, Graph, Live, Recording, Replay, Stop, Stream, Time};
use std::thread;
use std::time::Duration;

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

/// Diamond with a throttled branch. Throttle keeps no timers of its own, so
/// its output is a pure function of input times -- replayable.
fn topology(boundary: &Stream<Vec<i64>>) -> Stream<i64> {
    let per_burst = boundary.map(|burst| burst.iter().sum::<i64>());
    let doubled = per_burst.map(|x| x * 2);
    let throttled = per_burst
        .throttle(Duration::from_millis(25))
        .map(|x| x + 100);
    merge(&[&throttled, &doubled])
}

#[test]
fn record_captures_time_value_pairs() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(25), 2)]);
    let log = Recording::new();
    src.record(&log);

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(log.take(), vec![(ms(10), 1), (ms(25), 2)]);
}

#[test]
fn record_emits_unit_per_fire() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(20), 2)]);
    let log = Recording::new();
    let count = src.record(&log).count();

    g.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(count.peek(), Some(2));
}

#[test]
fn replay_from_log_reproduces_a_recording() {
    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(5), 10i64), (ms(5), 20), (ms(30), 30)]);
    let log = Recording::new();
    src.record(&log);
    g.run(Replay::from(Time::EPOCH)).unwrap();

    let g2 = Graph::new();
    let replayed = g2.replay_from_log(log.take());
    let log2 = Recording::new();
    replayed.record(&log2);
    g2.run(Replay::from(Time::EPOCH)).unwrap();

    assert_eq!(log2.take(), vec![(ms(5), 10), (ms(5), 20), (ms(30), 30)]);
}

#[test]
fn live_record_then_replay_is_bit_identical() {
    // Live: an external producer feeds the graph. Record what actually
    // crossed the boundary (whichever bursts wake coalescing produced) and
    // what the topology emitted.
    let g = Graph::new();
    let feed = producer(&g, |p| {
        for v in 1..=6i64 {
            if p.send(v).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    let boundary = Recording::new();
    feed.record(&boundary);
    let live_out = Recording::new();
    topology(&feed).record(&live_out);
    let summary = g
        .run(Live::new().stop(Stop::After(Duration::from_millis(120))))
        .unwrap();

    // Replay the boundary log through the same topology from the live start.
    let g2 = Graph::new();
    let feed2 = g2.replay_from_log(boundary.take());
    let replay_out = Recording::new();
    topology(&feed2).record(&replay_out);
    g2.run(Replay::from(summary.started_at)).unwrap();

    let live_result = live_out.take();
    assert!(
        !live_result.is_empty(),
        "live run must have produced output"
    );
    assert!(
        live_result.iter().any(|(_, value)| *value >= 100),
        "live run must include throttled branch output"
    );
    assert_eq!(live_result, replay_out.take());
}
