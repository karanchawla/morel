use crate::core::graph::{Ctx, Operator, Stream};
use crate::core::port::{Input, Output};
use crate::core::time::Time;

struct Map<T, U, F> {
    input: Input<T>,
    out: Output<U>,
    f: F,
}

impl<T, U, F> Operator for Map<T, U, F>
where
    T: Clone + 'static,
    U: 'static,
    F: FnMut(T) -> U + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        self.out.set((self.f)(self.input.get()));
    }
}

struct Filter<T, F> {
    input: Input<T>,
    out: Output<T>,
    pred: F,
}

impl<T, F> Operator for Filter<T, F>
where
    T: Clone + 'static,
    F: FnMut(&T) -> bool + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        if (self.pred)(&value) {
            self.out.set(value);
        }
    }
}

struct FilterMap<T, U, F> {
    input: Input<T>,
    out: Output<U>,
    f: F,
}

impl<T, U, F> Operator for FilterMap<T, U, F>
where
    T: Clone + 'static,
    U: 'static,
    F: FnMut(T) -> Option<U> + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let Some(value) = (self.f)(self.input.get()) {
            self.out.set(value);
        }
    }
}

struct TryMap<T, U, F> {
    input: Input<T>,
    out: Output<U>,
    f: F,
}

impl<T, U, E, F> Operator for TryMap<T, U, F>
where
    T: Clone + 'static,
    U: 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    F: FnMut(T) -> Result<U, E> + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        match (self.f)(self.input.get()) {
            Ok(value) => self.out.set(value),
            Err(err) => cx.fail(err),
        }
    }
}

struct Distinct<T> {
    input: Input<T>,
    out: Output<T>,
    last: Option<T>,
}

impl<T> Operator for Distinct<T>
where
    T: Clone + PartialEq + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        let value = self.input.get();
        if self.last.as_ref() != Some(&value) {
            self.last = Some(value.clone());
            self.out.set(value);
        }
    }
}

struct Take<T> {
    input: Input<T>,
    out: Output<T>,
    remaining: u64,
}

impl<T> Operator for Take<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.out.set(self.input.get());
        }
    }
}

struct Inspect<T, F> {
    input: Input<T>,
    out: Output<T>,
    f: F,
}

impl<T, F> Operator for Inspect<T, F>
where
    T: Clone + 'static,
    F: FnMut(&T, Time) + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let value = self.input.get();
        (self.f)(&value, cx.now());
        self.out.set(value);
    }
}

struct Timestamp<T> {
    input: Input<T>,
    out: Output<(Time, T)>,
}

impl<T> Operator for Timestamp<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        self.out.set((cx.now(), self.input.get()));
    }
}

struct Sink<T, F> {
    input: Input<T>,
    out: Output<()>,
    f: F,
}

impl<T, F> Operator for Sink<T, F>
where
    T: Clone + 'static,
    F: FnMut(T, Time) + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        (self.f)(self.input.get(), cx.now());
        self.out.set(());
    }
}

struct TrySink<T, F> {
    input: Input<T>,
    out: Output<()>,
    f: F,
}

impl<T, E, F> Operator for TrySink<T, F>
where
    T: Clone + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    F: FnMut(T, Time) -> Result<(), E> + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        match (self.f)(self.input.get(), cx.now()) {
            Ok(()) => self.out.set(()),
            Err(err) => cx.fail(err),
        }
    }
}

impl<T> Stream<T>
where
    T: Clone + 'static,
{
    /// Apply `f` to each value.
    pub fn map<U: 'static>(&self, f: impl FnMut(T) -> U + 'static) -> Stream<U> {
        self.wire(|w| Map {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }

    /// Keep values that satisfy `pred`.
    pub fn filter(&self, pred: impl FnMut(&T) -> bool + 'static) -> Stream<T> {
        self.wire(|w| Filter {
            input: w.on(self),
            out: w.output(),
            pred,
        })
    }

    /// Apply `f` to each value and emit only returned `Some` values.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let values = g.replay_from_iter(vec![(Time::EPOCH, 1), (Time::EPOCH, 2)]);
    /// let evens = values.filter_map(|x| (x % 2 == 0).then_some(x));
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(evens.peek(), Some(2));
    /// ```
    pub fn filter_map<U: 'static>(&self, f: impl FnMut(T) -> Option<U> + 'static) -> Stream<U> {
        self.wire(|w| FilterMap {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }

    /// Apply fallible `f` to each value, failing the run on the first error.
    ///
    /// Error values must be `Send + Sync` so the same operator can be used
    /// inside worker child graphs without losing the concrete error type.
    pub fn try_map<U, E>(&self, f: impl FnMut(T) -> Result<U, E> + 'static) -> Stream<U>
    where
        U: 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        self.wire(|w| TryMap {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }

    /// Drop consecutive duplicate values.
    pub fn distinct(&self) -> Stream<T>
    where
        T: PartialEq,
    {
        self.wire(|w| Distinct {
            input: w.on(self),
            out: w.output(),
            last: None,
        })
    }

    /// Keep at most the first `n` values.
    pub fn take(&self, n: u64) -> Stream<T> {
        self.wire(|w| Take {
            input: w.on(self),
            out: w.output(),
            remaining: n,
        })
    }

    /// Observe each value by reference and forward it unchanged.
    ///
    /// This follows [`Iterator::inspect`]: it is intended for side-effectful
    /// observation of a stream without changing the values. A common idiom is
    /// to call `inspect(|value, time| log::debug!(...))` while debugging a
    /// pipeline.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// let g = Graph::new();
    /// let seen = Rc::new(RefCell::new(Vec::new()));
    /// let seen2 = seen.clone();
    /// let values = g.replay_from_iter(vec![(Time::EPOCH, 7)]);
    /// let inspected = values.inspect(move |value, _time| seen2.borrow_mut().push(*value));
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(inspected.peek(), Some(7));
    /// assert_eq!(*seen.borrow(), vec![7]);
    /// ```
    pub fn inspect(&self, f: impl FnMut(&T, Time) + 'static) -> Stream<T> {
        self.wire(|w| Inspect {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }

    /// Pair each value with the engine time at which it fired.
    pub fn timestamp(&self) -> Stream<(Time, T)> {
        self.wire(|w| Timestamp {
            input: w.on(self),
            out: w.output(),
        })
    }

    /// Run `f` for each value with the engine time at which it fired.
    pub fn sink(&self, f: impl FnMut(T, Time) + 'static) -> Stream<()> {
        self.wire(|w| Sink {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }

    /// Run fallible `f` for each value, failing the run on the first error.
    ///
    /// Error values must be `Send + Sync` so the same operator can be used
    /// inside worker child graphs without losing the concrete error type.
    pub fn try_sink<E>(&self, f: impl FnMut(T, Time) -> Result<(), E> + 'static) -> Stream<()>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        self.wire(|w| TrySink {
            input: w.on(self),
            out: w.output(),
            f,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{Graph, Replay, Time};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    /// Shared source for operator tests: 1, 2, 3, ... every 10 ms.
    pub(crate) fn counter(g: &Graph) -> crate::core::Stream<i64> {
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
        g.run(Replay::from(Time::EPOCH).stop(crate::core::Stop::After(Duration::from_millis(ms))))
            .unwrap();
    }

    fn ms(n: u64) -> Time {
        Time::EPOCH + Duration::from_millis(n)
    }

    #[test]
    fn map_transforms_each_value() {
        let g = Graph::new();
        let seen = collect(&counter(&g).map(|x| x * 2));
        run_ms(&g, 30);
        assert_eq!(*seen.borrow(), vec![2, 4, 6, 8]);
    }

    #[test]
    fn map_changes_type() {
        let g = Graph::new();
        let seen = collect(&counter(&g).map(|x| x.to_string()));
        run_ms(&g, 10);
        assert_eq!(*seen.borrow(), vec!["1".to_string(), "2".to_string()]);
    }

    #[test]
    fn filter_drops_values() {
        let g = Graph::new();
        let seen = collect(&counter(&g).filter(|x| x % 2 == 0));
        run_ms(&g, 50);
        assert_eq!(*seen.borrow(), vec![2, 4, 6]);
    }

    #[test]
    fn filter_map_emits_some_drops_none() {
        #[derive(Clone, PartialEq, Debug)]
        struct NoDefault(i64);

        let g = Graph::new();
        let evens = counter(&g).filter_map(|x| if x % 2 == 0 { Some(NoDefault(x)) } else { None });
        let seen = collect(&evens);

        run_ms(&g, 50);

        assert_eq!(
            *seen.borrow(),
            vec![NoDefault(2), NoDefault(4), NoDefault(6)]
        );
    }

    #[test]
    fn try_map_error_fails_run_and_prefix_still_emits() {
        let g = Graph::new();
        let seen = collect(&counter(&g).try_map(|x| {
            if x < 3 {
                Ok(x * 10)
            } else {
                Err("bad value 3")
            }
        }));

        let err = g
            .run(
                Replay::from(Time::EPOCH)
                    .stop(crate::core::Stop::After(Duration::from_millis(100))),
            )
            .unwrap_err();

        assert!(format!("{err}").contains("bad value 3"));
        assert_eq!(*seen.borrow(), vec![10, 20]);
    }

    #[test]
    fn try_map_accepts_string_and_concrete_errors() {
        let g = Graph::new();

        let _string_error = counter(&g).try_map(Ok::<_, String>);
        let _io_error = counter(&g).try_map(Ok::<_, std::io::Error>);
    }

    #[test]
    fn try_sink_error_fails_run() {
        let g = Graph::new();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = seen.clone();

        let sink = counter(&g).try_sink(move |x, _time| {
            if x < 3 {
                seen2.borrow_mut().push(x);
                Ok(())
            } else {
                Err("bad value 3")
            }
        });
        let sink_seen = collect(&sink);

        let err = g
            .run(
                Replay::from(Time::EPOCH)
                    .stop(crate::core::Stop::After(Duration::from_millis(100))),
            )
            .unwrap_err();

        assert!(format!("{err}").contains("bad value 3"));
        assert_eq!(*seen.borrow(), vec![1, 2]);
        assert_eq!(*sink_seen.borrow(), vec![(), ()]);
    }

    #[test]
    fn distinct_suppresses_repeats_but_emits_first_value() {
        let g = Graph::new();
        let seen = collect(&counter(&g).map(|x| (x + 1) / 2).distinct());
        run_ms(&g, 40);
        assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    }

    #[test]
    fn take_limits_emissions() {
        let g = Graph::new();
        let seen = collect(&counter(&g).take(3));
        run_ms(&g, 100);
        assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    }

    #[test]
    fn sink_sees_engine_time() {
        let g = Graph::new();
        let times = Rc::new(RefCell::new(Vec::new()));
        let t2 = times.clone();
        counter(&g).sink(move |_v, t| t2.borrow_mut().push(t));
        run_ms(&g, 20);
        assert_eq!(
            *times.borrow(),
            vec![
                Time::EPOCH,
                Time::EPOCH + Duration::from_millis(10),
                Time::EPOCH + Duration::from_millis(20),
            ]
        );
    }

    #[test]
    fn inspect_observes_by_reference_and_forwards() {
        let g = Graph::new();
        let observed = Rc::new(RefCell::new(Vec::new()));
        let observed2 = observed.clone();
        let inspected = counter(&g).inspect(move |value, time| {
            observed2.borrow_mut().push((*value, time));
        });
        let seen = collect(&inspected);

        run_ms(&g, 30);

        assert_eq!(
            *observed.borrow(),
            vec![(1, ms(0)), (2, ms(10)), (3, ms(20)), (4, ms(30))]
        );
        assert_eq!(*seen.borrow(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn timestamp_pairs_values_with_engine_time() {
        let g = Graph::new();
        let seen = collect(&counter(&g).timestamp());

        run_ms(&g, 30);

        assert_eq!(
            *seen.borrow(),
            vec![(ms(0), 1), (ms(10), 2), (ms(20), 3), (ms(30), 4)]
        );
    }

    #[test]
    fn chained_pipeline() {
        let g = Graph::new();
        let seen = collect(&counter(&g).map(|x| x * 2).filter(|x| *x > 5).map(|x| x + 1));
        run_ms(&g, 50);
        assert_eq!(*seen.borrow(), vec![7, 9, 11, 13]);
    }
}
