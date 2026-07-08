use std::time::Duration;

use crate::core::graph::{Ctx, Graph, Operator, Stream};
use crate::core::port::Output;

struct Just<T: Clone + 'static> {
    value: T,
    out: Output<T>,
}

impl<T: Clone + 'static> Operator for Just<T> {
    fn on_start(&mut self, cx: &mut Ctx) {
        cx.at(cx.now());
    }

    fn step(&mut self, _cx: &mut Ctx) {
        self.out.set(self.value.clone());
    }
}

struct Ticker {
    period: Duration,
    out: Output<()>,
}

impl Operator for Ticker {
    fn on_start(&mut self, cx: &mut Ctx) {
        cx.at(cx.now());
        cx.every(self.period);
    }

    fn step(&mut self, _cx: &mut Ctx) {
        self.out.set(());
    }
}

impl Graph {
    pub fn just<T: Clone + 'static>(&self, value: T) -> Stream<T> {
        self.add(|w| Just {
            value,
            out: w.output(),
        })
    }

    pub fn ticker(&self, period: Duration) -> Stream<()> {
        self.add(|w| Ticker {
            period,
            out: w.output(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{Graph, Replay, Stop, Time};
    use std::time::Duration;

    #[test]
    fn just_fires_once_at_start() {
        let g = Graph::new();
        let c = g.just(42);
        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();
        assert_eq!(c.peek(), Some(42));
        assert_eq!(summary.steps, 1);
    }

    #[test]
    fn just_peek_is_none_before_running() {
        let g = Graph::new();
        let c = g.just(42);
        assert_eq!(c.peek(), None);
        g.run(Replay::from(Time::EPOCH)).unwrap();
        assert_eq!(c.peek(), Some(42));
    }

    #[test]
    fn ticker_fires_at_start_then_every_period_anchored() {
        let g = Graph::new();
        let t = g.ticker(Duration::from_millis(100));
        let summary = g
            .run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(450))))
            .unwrap();
        assert_eq!(summary.steps, 5);
        assert_eq!(t.peek(), Some(()));
        assert_eq!(summary.ended_at, Time::EPOCH + Duration::from_millis(400));
    }
}
