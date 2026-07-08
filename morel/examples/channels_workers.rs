//! Groups cross-graph channels, workers, source workers, and live producers.

use morel::{
    channel, producer, source_worker, worker, Capacity, Graph, Live, OnClose, Replay, Stop, Time,
};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelsWorkersOutput {
    pub channel: Vec<(u64, Vec<String>)>,
    pub worker: Vec<(u64, i64)>,
    pub source_worker: Vec<(u64, String)>,
    pub live_values: Vec<String>,
}

pub fn run() -> Result<ChannelsWorkersOutput, morel::Error> {
    Ok(ChannelsWorkersOutput {
        channel: replay_channel_round_trip()?,
        worker: worker_pipeline()?,
        source_worker: source_worker_pipeline()?,
        live_values: live_producer_demo()?,
    })
}

fn replay_channel_round_trip() -> Result<Vec<(u64, Vec<String>)>, morel::Error> {
    let graph = Graph::new();
    let source = graph.replay_from_iter([
        (ms(0), "a".to_string()),
        (ms(10), "b".to_string()),
        (ms(20), "c".to_string()),
    ]);
    let (tx, rx) = channel(Capacity::Unbounded);
    let _send = tx.attach(&source);
    let received = rx.into_stream_paced(&source, OnClose::Continue).history();

    graph.run(Replay::from(Time::EPOCH))?;

    Ok(time_history(
        received
            .peek()
            .expect("channel history should emit during replay"),
    ))
}

fn worker_pipeline() -> Result<Vec<(u64, i64)>, morel::Error> {
    let graph = Graph::new();
    let source = graph.replay_from_iter([(ms(0), 10i64), (ms(10), 20), (ms(20), 30), (ms(30), 40)]);
    let returned = worker(&source, |child, input| {
        let _keep_child = child;
        input.map(|values| values.into_iter().sum::<i64>()).sum()
    })
    .history();

    graph.run(Replay::from(Time::EPOCH))?;

    Ok(flatten_single_value_bursts(
        returned
            .peek()
            .expect("worker history should emit during replay"),
    ))
}

fn source_worker_pipeline() -> Result<Vec<(u64, String)>, morel::Error> {
    let graph = Graph::new();
    let returned = source_worker(&graph, |child| {
        child.replay_from_iter([
            (ms(0), vec!["left-0".to_string(), "right".to_string()]),
            (ms(10), vec!["left-10".to_string()]),
        ])
    })
    .history();

    graph.run(Replay::from(Time::EPOCH).stop(Stop::At(ms(10))))?;

    Ok(flatten_nested_bursts(
        returned
            .peek()
            .expect("source worker history should emit during replay"),
    ))
}

fn live_producer_demo() -> Result<Vec<String>, morel::Error> {
    let graph = Graph::new();
    let live = producer(&graph, |p| {
        p.send("live-a".to_string()).ok();
        p.send("live-b".to_string()).ok();
    })
    .history();

    graph.run(Live::new().stop(Stop::After(Duration::from_millis(100))))?;

    Ok(live
        .peek()
        .expect("live producer history should emit during live run")
        .into_iter()
        .flat_map(|(_time, burst)| burst)
        .collect())
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

fn flatten_single_value_bursts(values: Vec<(Time, Vec<i64>)>) -> Vec<(u64, i64)> {
    values
        .into_iter()
        .flat_map(|(time, burst)| burst.into_iter().map(move |value| (time.as_nanos(), value)))
        .collect()
}

fn flatten_nested_bursts<T>(values: Vec<(Time, Vec<Vec<T>>)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .flat_map(|(time, bursts)| {
            bursts
                .into_iter()
                .flatten()
                .map(move |value| (time.as_nanos(), value))
        })
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("channel={:?}", output.channel);
    println!("worker={:?}", output.worker);
    println!("source_worker={:?}", output.source_worker);
    println!("live_values={:?}", output.live_values);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_graph_examples_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.channel,
            vec![
                (0, vec!["a".to_string()]),
                (10_000_000, vec!["b".to_string()]),
                (20_000_000, vec!["c".to_string()]),
            ]
        );
        assert_eq!(
            output.worker,
            vec![
                (0, 10),
                (10_000_000, 30),
                (20_000_000, 60),
                (30_000_000, 100)
            ]
        );
        assert_eq!(
            output.source_worker,
            vec![
                (0, "left-0".to_string()),
                (0, "right".to_string()),
                (10_000_000, "left-10".to_string()),
            ]
        );
        assert_eq!(
            output.live_values,
            vec!["live-a".to_string(), "live-b".to_string()]
        );
    }
}
