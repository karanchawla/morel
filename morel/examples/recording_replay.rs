//! Shows how to record a stream boundary and replay that log through a fresh graph.

use morel::{Graph, Recording, Replay, Time};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct RecordingReplayOutput {
    pub boundary_log: Vec<(u64, Vec<i64>)>,
    pub first_output: Vec<(u64, i64)>,
    pub replayed_output: Vec<(u64, i64)>,
}

pub fn run() -> Result<RecordingReplayOutput, morel::Error> {
    let first_graph = Graph::new();
    let input = first_graph.replay_from_iter([
        (ms(0), vec![1, 2]),
        (ms(10), vec![3]),
        (ms(40), vec![4, 5]),
    ]);
    let boundary = Recording::new();
    let _record_boundary = input.record(&boundary);
    let first_output = topology(&input).history();

    first_graph.run(Replay::from(Time::EPOCH))?;

    let boundary_log = boundary.take();
    let first_output = first_output
        .peek()
        .expect("topology output should emit during first replay");

    let replay_graph = Graph::new();
    let replayed_input = replay_graph.replay_from_log(boundary_log.clone());
    let replayed_output = topology(&replayed_input).history();

    replay_graph.run(Replay::from(Time::EPOCH))?;

    Ok(RecordingReplayOutput {
        boundary_log: time_history(boundary_log),
        first_output: time_history(first_output),
        replayed_output: time_history(
            replayed_output
                .peek()
                .expect("topology output should emit during replayed run"),
        ),
    })
}

fn topology(input: &morel::Stream<Vec<i64>>) -> morel::Stream<i64> {
    let per_burst = input.map(|burst| burst.iter().sum::<i64>());
    let doubled = per_burst.map(|value| value * 2);
    let throttled = per_burst
        .throttle(Duration::from_millis(25))
        .map(|value| value + 100);
    morel::merge(&[&throttled, &doubled])
}

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn time_history<T>(values: Vec<(Time, T)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("boundary_log={:?}", output.boundary_log);
    println!("first_output={:?}", output.first_output);
    println!("replayed_output={:?}", output.replayed_output);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_replays_through_the_same_topology() {
        let output = run().unwrap();

        assert_eq!(
            output.boundary_log,
            vec![
                (0, vec![1, 2]),
                (10_000_000, vec![3]),
                (40_000_000, vec![4, 5]),
            ]
        );
        assert_eq!(output.first_output, output.replayed_output);
        // `merge` breaks same-step ties by the first listed source, so the
        // doubled values at 0ms and 40ms are deterministic but not emitted.
        assert_eq!(
            output.first_output,
            vec![(0, 103), (10_000_000, 6), (40_000_000, 109)]
        );
    }
}
