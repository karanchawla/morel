use std::collections::VecDeque;
use std::time::Duration;

use crate::core::graph::{Ctx, Operator, Stream};
use crate::core::port::{Input, Output};
use crate::core::time::Time;

struct Delay<T> {
    input: Input<T>,
    out: Output<T>,
    d: Duration,
    queue: VecDeque<(Time, T)>,
}

impl<T> Operator for Delay<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        if self.input.fired() {
            if self.d.is_zero() {
                self.out.set(self.input.get());
                return;
            }

            let due = cx.now() + self.d;
            self.queue.push_back((due, self.input.get()));
            cx.at(due);
        }

        while self.queue.front().is_some_and(|(due, _)| *due <= cx.now()) {
            let (_, value) = self.queue.pop_front().unwrap();
            self.out.set(value);
        }
    }
}

struct Throttle<T> {
    input: Input<T>,
    out: Output<T>,
    interval: Duration,
    last_emit: Option<Time>,
}

impl<T> Operator for Throttle<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();
        if self
            .last_emit
            .is_none_or(|last| now >= last + self.interval)
        {
            self.last_emit = Some(now);
            self.out.set(self.input.get());
        }
    }
}

struct Debounce<T> {
    input: Input<T>,
    out: Output<T>,
    quiet: Duration,
    pending: Option<T>,
    emit_at: Option<Time>,
}

impl<T> Operator for Debounce<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();

        if self.input.fired() {
            let at = now + self.quiet;
            self.pending = Some(self.input.get());
            self.emit_at = Some(at);
            cx.at(at);
        }

        if self.emit_at.is_some_and(|at| now >= at) {
            if let Some(value) = self.pending.take() {
                self.emit_at = None;
                self.out.set(value);
            }
        }
    }
}

impl<T> Stream<T>
where
    T: Clone + 'static,
{
    /// Re-emit each value after `d`, preserving order.
    pub fn delay(&self, d: Duration) -> Stream<T> {
        self.wire(|w| Delay {
            input: w.on(self),
            out: w.output(),
            d,
            queue: VecDeque::new(),
        })
    }

    /// Leading-edge rate limit.
    pub fn throttle(&self, interval: Duration) -> Stream<T> {
        self.wire(|w| Throttle {
            input: w.on(self),
            out: w.output(),
            interval,
            last_emit: None,
        })
    }

    /// Emit the latest value after the source has been quiet for `quiet`.
    pub fn debounce(&self, quiet: Duration) -> Stream<T> {
        self.wire(|w| Debounce {
            input: w.on(self),
            out: w.output(),
            quiet,
            pending: None,
            emit_at: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{Graph, Replay, Stop, Time};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    fn counter_ms(g: &Graph, period_ms: u64) -> crate::core::Stream<i64> {
        let mut n = 0i64;
        g.ticker(Duration::from_millis(period_ms)).map(move |()| {
            n += 1;
            n
        })
    }

    fn collect_timed<T: Clone + 'static>(
        s: &crate::core::Stream<T>,
    ) -> Rc<RefCell<Vec<(T, Time)>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let s2 = seen.clone();
        s.sink(move |v, t| s2.borrow_mut().push((v, t)));
        seen
    }

    fn run_ms(g: &Graph, ms: u64) {
        g.run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(ms))))
            .unwrap();
    }

    fn ms(n: u64) -> Time {
        Time::EPOCH + Duration::from_millis(n)
    }

    #[test]
    fn delay_shifts_emission_times() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 100).take(3).delay(Duration::from_millis(50)));
        run_ms(&g, 300);
        assert_eq!(
            *seen.borrow(),
            vec![(1, ms(50)), (2, ms(150)), (3, ms(250))]
        );
    }

    #[test]
    fn zero_delay_passes_through_same_step() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 100).take(2).delay(Duration::ZERO));
        run_ms(&g, 150);
        assert_eq!(*seen.borrow(), vec![(1, ms(0)), (2, ms(100))]);
    }

    #[test]
    fn delay_buffers_multiple_in_flight() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 10).delay(Duration::from_millis(100)));
        run_ms(&g, 150);
        let values: Vec<i64> = seen.borrow().iter().map(|(v, _)| *v).collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(seen.borrow()[0].1, ms(100));
    }

    #[test]
    fn throttle_leading_edge() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 10).throttle(Duration::from_millis(50)));
        run_ms(&g, 200);
        let values: Vec<i64> = seen.borrow().iter().map(|(v, _)| *v).collect();
        assert_eq!(values, vec![1, 6, 11, 16, 21]);
    }

    #[test]
    fn throttle_passes_slow_sources_through() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 100).throttle(Duration::from_millis(50)));
        run_ms(&g, 300);
        let values: Vec<i64> = seen.borrow().iter().map(|(v, _)| *v).collect();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn debounce_emits_after_quiet_period() {
        let g = Graph::new();
        let seen = collect_timed(
            &counter_ms(&g, 10)
                .take(3)
                .debounce(Duration::from_millis(30)),
        );
        run_ms(&g, 100);
        assert_eq!(*seen.borrow(), vec![(3, ms(50))]);
    }

    #[test]
    fn debounce_never_fires_while_source_is_busy() {
        let g = Graph::new();
        let seen = collect_timed(&counter_ms(&g, 10).debounce(Duration::from_millis(50)));
        run_ms(&g, 100);
        assert!(seen.borrow().is_empty());
    }
}
