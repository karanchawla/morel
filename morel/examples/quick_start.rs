//! A minimal replay pipeline that turns price ticks into threshold signals.

use morel::{Graph, Replay, Time};

#[derive(Clone, Debug, PartialEq)]
pub struct PriceSignal {
    pub price: f64,
    pub above_100: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuickStartOutput {
    pub steps: u64,
    pub signals: Vec<(u64, PriceSignal)>,
}

pub fn run() -> Result<QuickStartOutput, morel::Error> {
    let graph = Graph::new();
    let prices = graph.replay_from_iter(
        [(0, 99.5), (10, 101.25), (20, 98.75), (30, 104.0)]
            .map(|(nanos, price)| (Time::from_nanos(nanos), price)),
    );

    let signals = prices
        .map(|price| PriceSignal {
            price,
            above_100: price > 100.0,
        })
        .filter(|signal| signal.above_100);
    let history = signals.history();

    let summary = graph.run(Replay::from(Time::EPOCH))?;
    let signals = history
        .peek()
        .expect("threshold signal history should emit during replay")
        .into_iter()
        .map(|(time, signal)| (time.as_nanos(), signal))
        .collect();

    Ok(QuickStartOutput {
        steps: summary.steps,
        signals,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("steps={}", output.steps);
    for (nanos, signal) in output.signals {
        println!(
            "{nanos}ns price={:.2} above_100={}",
            signal.price, signal.above_100
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_only_prices_above_threshold() {
        assert_eq!(
            run().unwrap(),
            QuickStartOutput {
                steps: 4,
                signals: vec![
                    (
                        10,
                        PriceSignal {
                            price: 101.25,
                            above_100: true,
                        },
                    ),
                    (
                        30,
                        PriceSignal {
                            price: 104.0,
                            above_100: true,
                        },
                    ),
                ],
            }
        );
    }
}
