//! Demonstrates custom Morel operators, wiring inputs, timers, and final flushes.

use morel::{Ctx, Graph, Input, Operator, Output, Replay, Stop, Time};
use std::time::Duration;

pub struct Difference {
    current: Input<i64>,
    baseline: Input<i64>,
    out: Output<i64>,
}

impl Operator for Difference {
    fn step(&mut self, _ctx: &mut Ctx) {
        if let Some(baseline) = self.baseline.peek() {
            self.out.set(self.current.get() - baseline);
        }
    }
}

pub struct TimerSource {
    next: i64,
    out: Output<i64>,
}

impl Operator for TimerSource {
    fn on_start(&mut self, ctx: &mut Ctx) {
        ctx.at(ctx.now());
        ctx.every(Duration::from_millis(10));
    }

    fn step(&mut self, ctx: &mut Ctx) {
        self.out.set(self.next);
        if self.next == 3 {
            ctx.stop();
        }
        self.next += 1;
    }
}

pub struct FlushOnStop {
    input: Input<i64>,
    pending: Vec<i64>,
    out: Output<Vec<i64>>,
}

impl Operator for FlushOnStop {
    fn step(&mut self, ctx: &mut Ctx) {
        if ctx.is_final() {
            self.out.set(std::mem::take(&mut self.pending));
        } else if self.input.fired() {
            self.pending.push(self.input.get());
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CustomOperatorOutput {
    pub differences: Vec<(u64, i64)>,
    pub timer_values: Vec<(u64, i64)>,
    pub flushed: Option<Vec<i64>>,
    pub checked_timer_last: Option<i64>,
}

pub fn run() -> Result<CustomOperatorOutput, morel::Error> {
    let replay_graph = Graph::new();
    let baseline = replay_graph.replay_from_iter([(ms(0), 10)]);
    let current = replay_graph.replay_from_iter([(ms(10), 15), (ms(20), 20), (ms(30), 22)]);
    let differences = current
        .wire(|w| Difference {
            current: w.on(&current),
            baseline: w.watch(&baseline),
            out: w.output(),
        })
        .history();

    let flush_input = replay_graph.replay_from_iter([(ms(0), 1), (ms(10), 2), (ms(20), 3)]);
    let flushed = flush_input.wire(|w| {
        w.finalize();
        FlushOnStop {
            input: w.on(&flush_input),
            pending: Vec::new(),
            out: w.output(),
        }
    });

    replay_graph.run(Replay::from(Time::EPOCH).stop(Stop::At(ms(40))))?;

    let timer_graph = Graph::new();
    let timer_values = timer_graph
        .add::<i64, _>(|w| TimerSource {
            next: 1,
            out: w.output(),
        })
        .history();
    timer_graph.run(Replay::from(Time::EPOCH))?;

    let checked_graph = Graph::new();
    let checked_timer = checked_graph
        .try_add::<i64, _, &'static str>(|w| {
            Ok(TimerSource {
                next: 1,
                out: w.output(),
            })
        })
        .expect("checked timer build should succeed");
    checked_graph.run(Replay::from(Time::EPOCH))?;

    Ok(CustomOperatorOutput {
        differences: time_history(
            differences
                .peek()
                .expect("differences should emit during replay"),
        ),
        timer_values: time_history(
            timer_values
                .peek()
                .expect("timer values should emit before timer stop"),
        ),
        flushed: Some(
            flushed
                .peek()
                .expect("flush-on-stop should emit during graph finalization"),
        ),
        checked_timer_last: Some(
            checked_timer
                .peek()
                .expect("checked timer should emit before stopping the graph"),
        ),
    })
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

    println!("differences={:?}", output.differences);
    println!("timer_values={:?}", output.timer_values);
    println!("flushed={:?}", output.flushed);
    println!("checked_timer_last={:?}", output.checked_timer_last);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_operators_emit_stable_output() {
        let output = run().unwrap();

        assert_eq!(
            output.differences,
            vec![(10_000_000, 5), (20_000_000, 10), (30_000_000, 12)]
        );
        assert_eq!(
            output.timer_values,
            vec![(0, 1), (10_000_000, 2), (20_000_000, 3)]
        );
        assert_eq!(output.flushed, Some(vec![1, 2, 3]));
        assert_eq!(output.checked_timer_last, Some(3));
    }
}
