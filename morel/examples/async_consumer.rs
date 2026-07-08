//! Shows how an async consumer receives replay values and exposes tick history.

use futures_util::StreamExt;
use morel::{Graph, Replay, Time};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct AsyncConsumerOutput {
    pub seen: Vec<(u64, i64)>,
    pub consumer_ticks: Vec<(u64, ())>,
}

pub fn run() -> Result<AsyncConsumerOutput, Box<dyn std::error::Error>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_consumer = Arc::clone(&seen);

    let graph = Graph::new();
    let source = graph.replay_from_iter([(ms(10), 1i64), (ms(20), 2), (ms(20), 3)]);
    let consumer_ticks = source
        .consume_async(move |_params, mut input| async move {
            while let Some((time, value)) = input.next().await {
                seen_for_consumer
                    .lock()
                    .expect("async consumer output mutex should not be poisoned")
                    .push((time.as_nanos(), value));
            }
            Ok::<(), std::convert::Infallible>(())
        })
        .history();

    graph.run(Replay::from(Time::EPOCH))?;

    let seen = {
        seen.lock()
            .expect("async consumer output mutex should not be poisoned")
            .clone()
    };
    let consumer_ticks = consumer_ticks
        .peek()
        .expect("async consumer tick history should emit during replay")
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect();

    Ok(AsyncConsumerOutput {
        seen,
        consumer_ticks,
    })
}

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("seen={:?}", output.seen);
    println!("consumer_ticks={:?}", output.consumer_ticks);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_consumer_collects_values_and_tick_history() {
        let output = run().unwrap();

        assert_eq!(
            output.seen,
            vec![(10_000_000, 1), (20_000_000, 2), (20_000_000, 3)]
        );
        assert_eq!(
            output.consumer_ticks,
            vec![(10_000_000, ()), (20_000_000, ()), (20_000_000, ())]
        );
    }
}
