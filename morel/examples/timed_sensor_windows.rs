//! Demonstrates replayed sensor readings with Morel's time-aware operators.

use morel::{Graph, Replay, Stop, Time};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct TimedSensorWindowsOutput {
    pub buffered_averages: Vec<(u64, f64)>,
    pub tumbling_averages: Vec<(u64, f64)>,
    pub sliding_latest: Vec<(u64, f64)>,
    pub delayed_times: Vec<(u64, (u64, f64))>,
    pub throttled: Vec<(u64, f64)>,
    pub debounced: Vec<(u64, f64)>,
}

pub fn run() -> Result<TimedSensorWindowsOutput, morel::Error> {
    let graph = Graph::new();
    let readings = graph.replay_from_iter([
        (ms(0), 21.5),
        (ms(10), 21.7),
        (ms(20), 22.4),
        (ms(35), 25.1),
        (ms(45), 24.9),
        (ms(80), 22.0),
        (ms(90), 21.8),
    ]);

    let buffered_averages = readings.buffer(3).map_batch(avg).history();
    let tumbling_averages = readings
        .window_tumbling(Duration::from_millis(30))
        .map_batch(avg)
        .history();
    let sliding_latest = readings
        .window_sliding(Duration::from_millis(40), Duration::from_millis(20))
        .collapse()
        .history();
    let delayed_times = readings
        .delay(Duration::from_millis(5))
        .timestamp()
        .take(3)
        .history();
    let throttled = readings.throttle(Duration::from_millis(25)).history();
    let debounced = readings.debounce(Duration::from_millis(20)).history();

    graph.run(Replay::from(Time::EPOCH).stop(Stop::At(ms(120))))?;

    Ok(TimedSensorWindowsOutput {
        buffered_averages: time_history(
            buffered_averages
                .peek()
                .expect("buffered averages should emit during replay"),
        ),
        tumbling_averages: time_history(
            tumbling_averages
                .peek()
                .expect("tumbling averages should emit during replay"),
        ),
        sliding_latest: time_history(
            sliding_latest
                .peek()
                .expect("sliding latest values should emit during replay"),
        ),
        delayed_times: nested_time_history(
            delayed_times
                .peek()
                .expect("delayed timestamps should emit during replay"),
        ),
        throttled: time_history(
            throttled
                .peek()
                .expect("throttled readings should emit during replay"),
        ),
        debounced: time_history(
            debounced
                .peek()
                .expect("debounced readings should emit during replay"),
        ),
    })
}

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn avg(values: &[f64]) -> f64 {
    let sum: f64 = values.iter().sum();
    ((sum / values.len() as f64) * 100.0).round() / 100.0
}

fn time_history<T>(values: Vec<(Time, T)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect()
}

fn nested_time_history(values: Vec<(Time, (Time, f64))>) -> Vec<(u64, (u64, f64))> {
    values
        .into_iter()
        .map(|(history_time, (event_time, value))| {
            (history_time.as_nanos(), (event_time.as_nanos(), value))
        })
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("buffered_averages={:?}", output.buffered_averages);
    println!("tumbling_averages={:?}", output.tumbling_averages);
    println!("sliding_latest={:?}", output.sliding_latest);
    println!("delayed_times={:?}", output.delayed_times);
    println!("throttled={:?}", output.throttled);
    println!("debounced={:?}", output.debounced);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_aware_sensor_operators_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.buffered_averages,
            vec![(20_000_000, 21.87), (80_000_000, 24.0), (120_000_000, 21.8),]
        );
        assert_eq!(
            output.throttled,
            vec![(0, 21.5), (35_000_000, 25.1), (80_000_000, 22.0)]
        );
        assert_eq!(
            output.tumbling_averages,
            vec![
                (30_000_000, 21.87),
                (60_000_000, 25.0),
                (90_000_000, 22.0),
                (120_000_000, 21.8),
            ]
        );
        assert_eq!(
            output.sliding_latest,
            vec![
                (20_000_000, 22.4),
                (40_000_000, 25.1),
                (60_000_000, 24.9),
                (80_000_000, 22.0),
                (100_000_000, 21.8),
                (120_000_000, 21.8),
            ]
        );
        assert_eq!(
            output.delayed_times,
            vec![
                (5_000_000, (5_000_000, 21.5)),
                (15_000_000, (15_000_000, 21.7)),
                (25_000_000, (25_000_000, 22.4)),
            ]
        );
        assert_eq!(
            output.debounced,
            vec![(65_000_000, 24.9), (110_000_000, 21.8)]
        );
    }
}
