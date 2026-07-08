use std::ops::{Add, Sub};

use crate::core::graph::{Ctx, Operator, Stream};
use crate::core::port::{Input, Output};
use crate::core::time::Time;

/// Lossy-but-total conversion to `f64` for numeric streams.
///
/// Integer values beyond 2^53 can lose precision when converted to `f64`.
pub trait ToF64 {
    fn to_f64(&self) -> f64;
}

struct Scan<T, U, F> {
    input: Input<T>,
    out: Output<U>,
    init: U,
    f: F,
}

impl<T, U, F> Operator for Scan<T, U, F>
where
    T: Clone + 'static,
    U: Clone + 'static,
    F: FnMut(&mut U, T) + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        let Self { out, init, f, .. } = self;
        out.update(|| init.clone(), |acc| f(acc, value));
    }
}

struct Sum<T> {
    input: Input<T>,
    out: Output<T>,
    acc: Option<T>,
}

impl<T> Operator for Sum<T>
where
    T: Clone + Add<Output = T> + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        let acc = match self.acc.take() {
            Some(acc) => acc + value,
            None => value,
        };
        self.acc = Some(acc.clone());
        self.out.set(acc);
    }
}

struct Reduce<T, F> {
    input: Input<T>,
    out: Output<T>,
    acc: Option<T>,
    f: F,
}

impl<T, F> Operator for Reduce<T, F>
where
    T: Clone + 'static,
    F: FnMut(T, T) -> T + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        let acc = match self.acc.take() {
            Some(acc) => (self.f)(acc, value),
            None => value,
        };
        self.acc = Some(acc.clone());
        self.out.set(acc);
    }
}

struct Delta<T> {
    input: Input<T>,
    out: Output<T>,
    prev: Option<T>,
}

impl<T> Operator for Delta<T>
where
    T: Clone + Sub<Output = T> + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        if let Some(prev) = self.prev.replace(value.clone()) {
            self.out.set(value - prev);
        }
    }
}

struct Mean<T> {
    input: Input<T>,
    out: Output<f64>,
    count: u64,
    mean: f64,
}

impl<T> Operator for Mean<T>
where
    T: Clone + ToF64 + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get().to_f64();
        self.count += 1;
        self.mean += (value - self.mean) / self.count as f64;
        self.out.set(self.mean);
    }
}

struct History<T> {
    input: Input<T>,
    out: Output<Vec<(Time, T)>>,
}

impl<T> Operator for History<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();
        let value = self.input.get();
        self.out.update(Vec::new, |log| log.push((now, value)));
    }
}

macro_rules! impl_to_f64 {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ToF64 for $ty {
                fn to_f64(&self) -> f64 {
                    *self as f64
                }
            }
        )*
    };
}

impl_to_f64!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize, f32, f64,);

impl<T> Stream<T>
where
    T: Clone + 'static,
{
    /// Maintain state with `f(&mut state, value)` and emit the updated state.
    pub fn scan<U: Clone + 'static>(
        &self,
        init: U,
        f: impl FnMut(&mut U, T) + 'static,
    ) -> Stream<U> {
        self.wire(|w| Scan {
            input: w.on(self),
            out: w.output(),
            init,
            f,
        })
    }

    /// Running reduction seeded by the first value.
    pub fn reduce(&self, f: impl FnMut(T, T) -> T + 'static) -> Stream<T> {
        self.wire(|w| Reduce {
            input: w.on(self),
            out: w.output(),
            acc: None,
            f,
        })
    }

    /// Running sum seeded by the first value.
    pub fn sum(&self) -> Stream<T>
    where
        T: Add<Output = T>,
    {
        self.wire(|w| Sum {
            input: w.on(self),
            out: w.output(),
            acc: None,
        })
    }

    /// Difference from the previous value.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let values = g.replay_from_iter(vec![(Time::EPOCH, 3), (Time::EPOCH, 8)]);
    /// let delta = values.delta();
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(delta.peek(), Some(5));
    /// ```
    pub fn delta(&self) -> Stream<T>
    where
        T: Sub<Output = T>,
    {
        self.wire(|w| Delta {
            input: w.on(self),
            out: w.output(),
            prev: None,
        })
    }

    /// Running arithmetic mean using an incremental update.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let values = g.replay_from_iter(vec![(Time::EPOCH, 2i64), (Time::EPOCH, 4)]);
    /// let mean = values.mean();
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(mean.peek(), Some(3.0));
    /// ```
    pub fn mean(&self) -> Stream<f64>
    where
        T: ToF64,
    {
        self.wire(|w| Mean {
            input: w.on(self),
            out: w.output(),
            count: 0,
            mean: 0.0,
        })
    }

    /// Count this stream's fires.
    pub fn count(&self) -> Stream<u64> {
        self.scan(0u64, |acc, _| *acc += 1)
    }

    /// Accumulate all values seen so far.
    pub fn accumulate(&self) -> Stream<Vec<T>> {
        self.scan(Vec::new(), |acc, value| acc.push(value))
    }

    /// Accumulate all values with the engine time at which each value fired.
    ///
    /// This grows without bound and every downstream [`Stream::peek`] or
    /// [`Input::get`](crate::core::Input::get) clones the whole vector. Treat it
    /// as a test workhorse or diagnostic helper, not as a hot-path operator.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let values = g.replay_from_iter(vec![(Time::EPOCH, 1), (Time::EPOCH, 2)]);
    /// let history = values.history();
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(history.peek(), Some(vec![(Time::EPOCH, 1), (Time::EPOCH, 2)]));
    /// ```
    pub fn history(&self) -> Stream<Vec<(Time, T)>> {
        self.wire(|w| History {
            input: w.on(self),
            out: w.output(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{Graph, Replay, Stop, Time};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    fn counter(g: &Graph) -> crate::core::Stream<i64> {
        let mut n = 0i64;
        g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            n
        })
    }

    fn collect<T: Clone + 'static>(s: &crate::core::Stream<T>) -> Rc<RefCell<Vec<T>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = seen.clone();
        s.sink(move |v, _t| seen2.borrow_mut().push(v));
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
    fn scan_emits_running_state() {
        let g = Graph::new();
        let sums = counter(&g).scan(0i64, |acc, v| *acc += v);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let s2 = seen.clone();
        sums.sink(move |v, _| s2.borrow_mut().push(v));
        run_ms(&g, 40);
        assert_eq!(*seen.borrow(), vec![1, 3, 6, 10, 15]);
    }

    #[test]
    fn reduce_seeds_with_first_value() {
        let g = Graph::new();
        let sums = counter(&g).reduce(|acc, x| acc + x);
        let seen = collect(&sums);

        run_ms(&g, 30);

        assert_eq!(*seen.borrow(), vec![1, 3, 6, 10]);
    }

    #[test]
    fn sum_needs_no_identity() {
        let g = Graph::new();
        let sums = counter(&g).sum();
        run_ms(&g, 40);
        assert_eq!(sums.peek(), Some(15));
    }

    #[test]
    fn delta_is_silent_on_first_fire() {
        let g = Graph::new();
        let values = [3, 7, 12, 20];
        let deltas = counter(&g).map(move |x| values[(x - 1) as usize]).delta();
        let seen = collect(&deltas);

        run_ms(&g, 30);

        assert_eq!(*seen.borrow(), vec![4, 5, 8]);
    }

    #[test]
    fn mean_runs_incrementally_over_integers() {
        let g = Graph::new();
        let means = counter(&g).mean();
        let seen = collect(&means);

        run_ms(&g, 30);

        assert_eq!(*seen.borrow(), vec![1.0, 1.5, 2.0, 2.5]);
    }

    #[test]
    fn mean_works_on_floats() {
        let g = Graph::new();
        let means = counter(&g).map(|x| x as f64 / 2.0).mean();
        let seen = collect(&means);

        run_ms(&g, 30);

        assert_eq!(*seen.borrow(), vec![0.5, 0.75, 1.0, 1.25]);
    }

    #[test]
    fn count_counts_fires() {
        let g = Graph::new();
        let n = counter(&g).filter(|x| x % 2 == 0).count();
        run_ms(&g, 50);
        assert_eq!(n.peek(), Some(3));
    }

    #[test]
    fn accumulate_collects_history() {
        let g = Graph::new();
        let all = counter(&g).take(3).accumulate();
        run_ms(&g, 100);
        assert_eq!(all.peek(), Some(vec![1, 2, 3]));
    }

    #[test]
    fn history_accumulates_timestamped_values() {
        let g = Graph::new();
        let history = counter(&g).history();
        let fires = history.count();

        run_ms(&g, 30);

        assert_eq!(
            history.peek(),
            Some(vec![(ms(0), 1), (ms(10), 2), (ms(20), 3), (ms(30), 4)])
        );
        assert_eq!(fires.peek(), Some(4));
    }
}
