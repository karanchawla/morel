//! Demonstrates stream fan-in, sampling, gating, gathering, and pair splitting.

use morel::{gather, merge, Graph, Replay, Stop, Time};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct FanInControlOutput {
    pub with_just_prefix: Vec<(u64, i64)>,
    pub with_latest: Vec<(u64, (i64, i64))>,
    pub gated: Vec<(u64, i64)>,
    pub sampled: Vec<(u64, i64)>,
    pub merged: Vec<(u64, (&'static str, i64))>,
    pub gathered: Vec<(u64, Vec<i64>)>,
    pub unzipped_left: Vec<(u64, String)>,
    pub unzipped_right: Vec<(u64, i64)>,
}

pub fn run() -> Result<FanInControlOutput, morel::Error> {
    let graph = Graph::new();

    let fast = counter_after_start(&graph, 10);
    let slow = counter_after_start(&graph, 25);
    let medium = counter_after_start(&graph, 15);
    let start_counter = counter_from_start(&graph, 10);

    let with_just_prefix = fast
        .with(&graph.just(100), |value, base| value + base)
        .take(3)
        .history();
    let with_latest = fast.with_latest(&slow, |a, b| (a, b)).history();

    let open = graph.replay_from_iter([(ms(15), false), (ms(25), true), (ms(65), false)]);
    let gated = start_counter.gate(&open).history();

    let heartbeat = graph.ticker(Duration::from_millis(20));
    let sampled = fast.sample(&heartbeat).history();

    let fast_labeled = fast.map(|value| ("fast", value));
    let slow_labeled = slow.map(|value| ("slow", value));
    // Merge breaks same-step ties by source order, so fast wins at shared instants such as 50ms.
    let merged = merge(&[&fast_labeled, &slow_labeled]).history();

    let gathered = gather(&[&fast, &medium, &slow]).history();

    let pairs = graph.replay_from_iter([
        (ms(0), ("a".to_string(), 1)),
        (ms(20), ("b".to_string(), 2)),
    ]);
    let (left, right) = pairs.unzip();
    let unzipped_left = left.history();
    let unzipped_right = right.history();

    graph.run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(90))))?;

    Ok(FanInControlOutput {
        with_just_prefix: time_history(
            with_just_prefix
                .peek()
                .expect("with just prefix should emit during replay"),
        ),
        with_latest: time_history(
            with_latest
                .peek()
                .expect("with_latest should emit during replay"),
        ),
        gated: time_history(
            gated
                .peek()
                .expect("gated values should emit during replay"),
        ),
        sampled: time_history(
            sampled
                .peek()
                .expect("sampled values should emit during replay"),
        ),
        merged: time_history(
            merged
                .peek()
                .expect("merged values should emit during replay"),
        ),
        gathered: time_history(
            gathered
                .peek()
                .expect("gathered values should emit during replay"),
        ),
        unzipped_left: time_history(
            unzipped_left
                .peek()
                .expect("left unzip should emit during replay"),
        ),
        unzipped_right: time_history(
            unzipped_right
                .peek()
                .expect("right unzip should emit during replay"),
        ),
    })
}

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn counter_after_start(graph: &Graph, period_ms: u64) -> morel::Stream<i64> {
    let mut n = 0i64;
    let mut first = true;
    graph
        .ticker(Duration::from_millis(period_ms))
        .filter(move |_: &()| !std::mem::take(&mut first))
        .map(move |()| {
            n += 1;
            n
        })
}

fn counter_from_start(graph: &Graph, period_ms: u64) -> morel::Stream<i64> {
    let mut n = 0i64;
    graph
        .ticker(Duration::from_millis(period_ms))
        .map(move |()| {
            n += 1;
            n
        })
}

fn time_history<T>(values: Vec<(Time, T)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("with_just_prefix={:?}", output.with_just_prefix);
    println!("with_latest={:?}", output.with_latest);
    println!("gated={:?}", output.gated);
    println!("sampled={:?}", output.sampled);
    println!("merged={:?}", output.merged);
    println!("gathered={:?}", output.gathered);
    println!("unzipped_left={:?}", output.unzipped_left);
    println!("unzipped_right={:?}", output.unzipped_right);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fan_in_and_control_operators_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.with_just_prefix,
            vec![(10_000_000, 101), (20_000_000, 102), (30_000_000, 103)]
        );
        assert_eq!(
            output.gated,
            vec![
                (30_000_000, 4),
                (40_000_000, 5),
                (50_000_000, 6),
                (60_000_000, 7),
            ]
        );
        assert_eq!(
            output.with_latest,
            vec![
                (30_000_000, (3, 1)),
                (40_000_000, (4, 1)),
                (50_000_000, (5, 2)),
                (60_000_000, (6, 2)),
                (70_000_000, (7, 2)),
                (80_000_000, (8, 3)),
                (90_000_000, (9, 3)),
            ]
        );
        assert_eq!(
            output.sampled,
            vec![
                (20_000_000, 2),
                (40_000_000, 4),
                (60_000_000, 6),
                (80_000_000, 8),
            ]
        );
        assert_eq!(
            output.merged,
            vec![
                (10_000_000, ("fast", 1)),
                (20_000_000, ("fast", 2)),
                (25_000_000, ("slow", 1)),
                (30_000_000, ("fast", 3)),
                (40_000_000, ("fast", 4)),
                (50_000_000, ("fast", 5)),
                (60_000_000, ("fast", 6)),
                (70_000_000, ("fast", 7)),
                (75_000_000, ("slow", 3)),
                (80_000_000, ("fast", 8)),
                (90_000_000, ("fast", 9)),
            ]
        );
        assert_eq!(
            output.gathered,
            vec![
                (25_000_000, vec![2, 1, 1]),
                (30_000_000, vec![3, 2, 1]),
                (40_000_000, vec![4, 2, 1]),
                (45_000_000, vec![4, 3, 1]),
                (50_000_000, vec![5, 3, 2]),
                (60_000_000, vec![6, 4, 2]),
                (70_000_000, vec![7, 4, 2]),
                (75_000_000, vec![7, 5, 3]),
                (80_000_000, vec![8, 5, 3]),
                (90_000_000, vec![9, 6, 3]),
            ]
        );
        assert_eq!(
            output.unzipped_left,
            vec![(0, "a".to_string()), (20_000_000, "b".to_string())]
        );
        assert_eq!(output.unzipped_right, vec![(0, 1), (20_000_000, 2)]);
    }
}
