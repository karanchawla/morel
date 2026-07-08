use crate::core::graph::{Ctx, Operator, Stream};
use crate::core::port::{Input, Output};

struct With<T, U, V, F> {
    a: Input<T>,
    b: Input<U>,
    out: Output<V>,
    f: F,
}

impl<T, U, V, F> Operator for With<T, U, V, F>
where
    T: Clone + 'static,
    U: Clone + 'static,
    V: 'static,
    F: FnMut(T, U) -> V + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let (Some(a), Some(b)) = (self.a.peek(), self.b.peek()) {
            self.out.set((self.f)(a, b));
        }
    }
}

struct Merge<T> {
    inputs: Vec<Input<T>>,
    out: Output<T>,
}

impl<T> Operator for Merge<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        for input in &self.inputs {
            if input.fired() {
                self.out.set(input.get());
                return;
            }
        }
    }
}

struct Sample<T> {
    source: Input<T>,
    out: Output<T>,
}

impl<T> Operator for Sample<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let Some(value) = self.source.peek() {
            self.out.set(value);
        }
    }
}

struct WithLatest<T, U, V, F> {
    input: Input<T>,
    other: Input<U>,
    out: Output<V>,
    f: F,
}

impl<T, U, V, F> Operator for WithLatest<T, U, V, F>
where
    T: Clone + 'static,
    U: Clone + 'static,
    V: 'static,
    F: FnMut(T, U) -> V + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let Some(other) = self.other.peek() {
            self.out.set((self.f)(self.input.get(), other));
        }
    }
}

struct Gate<T> {
    input: Input<T>,
    open: Input<bool>,
    out: Output<T>,
}

impl<T> Operator for Gate<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if self.open.peek() == Some(true) {
            self.out.set(self.input.get());
        }
    }
}

struct Gather<T> {
    inputs: Vec<Input<T>>,
    out: Output<Vec<T>>,
}

impl<T> Operator for Gather<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if self.inputs.iter().all(Input::has_value) {
            self.out.set(
                self.inputs
                    .iter()
                    .map(|input| input.peek().expect("gather input checked with has_value"))
                    .collect(),
            );
        }
    }
}

struct UnzipLeft<A, B> {
    input: Input<(A, B)>,
    out: Output<A>,
}

impl<A, B> Operator for UnzipLeft<A, B>
where
    A: Clone + 'static,
    B: 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let Some(pair) = self.input.borrow() {
            self.out.set(pair.0.clone());
        }
    }
}

struct UnzipRight<A, B> {
    input: Input<(A, B)>,
    out: Output<B>,
}

impl<A, B> Operator for UnzipRight<A, B>
where
    A: 'static,
    B: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if let Some(pair) = self.input.borrow() {
            self.out.set(pair.1.clone());
        }
    }
}

impl<T> Stream<T>
where
    T: Clone + 'static,
{
    /// Combine the latest values whenever either stream fires.
    pub fn with<U: Clone + 'static, V: 'static>(
        &self,
        other: &Stream<U>,
        f: impl FnMut(T, U) -> V + 'static,
    ) -> Stream<V> {
        self.wire(|w| With {
            a: w.on(self),
            b: w.on(other),
            out: w.output(),
            f,
        })
    }

    /// Combine this stream with the latest value from `other` when this stream fires.
    pub fn with_latest<U: Clone + 'static, V: 'static>(
        &self,
        other: &Stream<U>,
        f: impl FnMut(T, U) -> V + 'static,
    ) -> Stream<V> {
        self.wire(|w| WithLatest {
            input: w.on(self),
            other: w.watch(other),
            out: w.output(),
            f,
        })
    }

    /// Pass values through only while `open` most recently emitted `true`.
    pub fn gate(&self, open: &Stream<bool>) -> Stream<T> {
        self.wire(|w| Gate {
            input: w.on(self),
            open: w.watch(open),
            out: w.output(),
        })
    }

    /// Emit this stream's latest value whenever `trigger` fires.
    pub fn sample<U>(&self, trigger: &Stream<U>) -> Stream<T> {
        self.wire(|w| {
            w.on(trigger);
            Sample {
                source: w.watch(self),
                out: w.output(),
            }
        })
    }
}

impl<A, B> Stream<(A, B)>
where
    A: Clone + 'static,
    B: Clone + 'static,
{
    /// Project a stream of pairs into one stream per member.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let pairs = g.replay_from_iter(vec![(Time::EPOCH, (1, "one"))]);
    /// let (left, right) = pairs.unzip();
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(left.peek(), Some(1));
    /// assert_eq!(right.peek(), Some("one"));
    /// ```
    pub fn unzip(&self) -> (Stream<A>, Stream<B>) {
        let left = self.wire(|w| UnzipLeft {
            input: w.on(self),
            out: w.output(),
        });
        let right = self.wire(|w| UnzipRight {
            input: w.on(self),
            out: w.output(),
        });
        (left, right)
    }
}

/// Merge streams, using the first source in the slice to break same-step ties.
pub fn merge<T: Clone + 'static>(sources: &[&Stream<T>]) -> Stream<T> {
    let first = sources.first().expect("merge requires at least one source");
    first.wire(|w| Merge {
        inputs: sources.iter().map(|source| w.on(source)).collect(),
        out: w.output(),
    })
}

/// Gather the latest values from all sources in slice order.
pub fn gather<T: Clone + 'static>(sources: &[&Stream<T>]) -> Stream<Vec<T>> {
    let first = sources
        .first()
        .expect("gather requires at least one source");
    first.wire(|w| Gather {
        inputs: sources.iter().map(|source| w.on(source)).collect(),
        out: w.output(),
    })
}

#[cfg(test)]
mod tests {
    use super::{gather, merge};
    use crate::core::{Graph, Replay, Stop, Time};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    fn collect<T: Clone + 'static>(s: &crate::core::Stream<T>) -> Rc<RefCell<Vec<T>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let s2 = seen.clone();
        s.sink(move |v, _| s2.borrow_mut().push(v));
        seen
    }

    fn run_ms(g: &Graph, ms: u64) {
        g.run(Replay::from(Time::EPOCH).stop(Stop::After(Duration::from_millis(ms))))
            .unwrap();
    }

    fn ms(n: u64) -> Time {
        Time::EPOCH + Duration::from_millis(n)
    }

    fn collect_timed<T: Clone + 'static>(
        s: &crate::core::Stream<T>,
    ) -> Rc<RefCell<Vec<(T, Time)>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let s2 = seen.clone();
        s.sink(move |v, t| s2.borrow_mut().push((v, t)));
        seen
    }

    fn counter_ms_after_start(g: &Graph, period_ms: u64) -> crate::core::Stream<i64> {
        let mut n = 0i64;
        let mut first = true;
        g.ticker(Duration::from_millis(period_ms))
            .filter(move |_: &()| !std::mem::take(&mut first))
            .map(move |()| {
                n += 1;
                n
            })
    }

    #[test]
    fn with_combines_latest_once_both_present() {
        let g = Graph::new();
        let mut n = 0i64;
        let a = g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            n
        });
        let b = g.just(100i64);
        let seen = collect(&a.with(&b, |x, y| x + y));
        run_ms(&g, 20);
        assert_eq!(*seen.borrow(), vec![101, 102, 103]);
    }

    #[test]
    fn with_is_silent_until_both_have_values() {
        let g = Graph::new();
        let mut n = 0i64;
        let a = g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            n
        });
        let b = g
            .ticker(Duration::from_millis(25))
            .filter({
                let mut first = true;
                move |_: &()| !std::mem::take(&mut first)
            })
            .map(|()| 1000i64);
        let seen = collect(&a.with(&b, |x, y| x + y));
        run_ms(&g, 30);
        assert_eq!(*seen.borrow(), vec![1003, 1004]);
    }

    #[test]
    fn merge_interleaves_and_first_listed_wins_ties() {
        let g = Graph::new();
        let mut na = 0i64;
        let a = g.ticker(Duration::from_millis(20)).map(move |()| {
            na += 10;
            na
        });
        let mut nb = 0i64;
        let b = g.ticker(Duration::from_millis(30)).map(move |()| {
            nb += 100;
            nb
        });
        let seen = collect(&merge(&[&a, &b]));
        run_ms(&g, 60);
        assert_eq!(*seen.borrow(), vec![10, 20, 200, 30, 40]);
    }

    #[test]
    fn sample_reads_source_on_trigger() {
        let g = Graph::new();
        let mut n = 0i64;
        let fast = g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            n
        });
        let slow = g.ticker(Duration::from_millis(20));
        let seen = collect(&fast.sample(&slow));
        run_ms(&g, 40);
        // At tied times the source updates before the sampler because it has a
        // lower node id.
        assert_eq!(*seen.borrow(), vec![1, 3, 5]);
    }

    #[test]
    fn sample_is_silent_until_source_produces() {
        let g = Graph::new();
        let never = g.ticker(Duration::from_millis(1000)).map(|()| 7i64).take(0);
        let trigger = g.ticker(Duration::from_millis(10));
        let seen = collect(&never.sample(&trigger));
        run_ms(&g, 50);
        assert!(seen.borrow().is_empty());
    }

    #[test]
    fn with_latest_fires_on_self_only() {
        let g = Graph::new();
        let mut fast_n = 0i64;
        let fast = g.ticker(Duration::from_millis(10)).map(move |()| {
            fast_n += 1;
            fast_n
        });
        let slow = counter_ms_after_start(&g, 25);
        let seen = collect_timed(&fast.with_latest(&slow, |a, b| (a, b)));

        run_ms(&g, 100);

        assert_eq!(
            *seen.borrow(),
            vec![
                ((4, 1), ms(30)),
                ((5, 1), ms(40)),
                ((6, 2), ms(50)),
                ((7, 2), ms(60)),
                ((8, 2), ms(70)),
                ((9, 3), ms(80)),
                ((10, 3), ms(90)),
                ((11, 4), ms(100)),
            ]
        );
    }

    #[test]
    fn gate_passes_only_while_open() {
        let g = Graph::new();
        let mut n = 0i64;
        let source = g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            n
        });
        let open = g.replay_from_iter(vec![(ms(15), false), (ms(25), true), (ms(65), false)]);
        let seen = collect_timed(&source.gate(&open));

        run_ms(&g, 90);

        assert_eq!(
            *seen.borrow(),
            vec![(4, ms(30)), (5, ms(40)), (6, ms(50)), (7, ms(60))]
        );
    }

    #[test]
    fn gather_snapshots_in_slice_order() {
        let g = Graph::new();
        let a = counter_ms_after_start(&g, 10);
        let b = counter_ms_after_start(&g, 15);
        let c = counter_ms_after_start(&g, 20);
        let seen = collect_timed(&gather(&[&b, &c, &a]));

        run_ms(&g, 60);

        assert_eq!(
            *seen.borrow(),
            vec![
                (vec![1, 1, 2], ms(20)),
                (vec![2, 1, 3], ms(30)),
                (vec![2, 2, 4], ms(40)),
                (vec![3, 2, 4], ms(45)),
                (vec![3, 2, 5], ms(50)),
                (vec![4, 3, 6], ms(60)),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "gather requires at least one source")]
    fn gather_panics_on_empty_slice() {
        let sources: [&crate::core::Stream<i64>; 0] = [];
        let _ = gather(&sources);
    }

    #[test]
    fn unzip_projects_both_members_in_step() {
        let g = Graph::new();
        let mut n = 0i64;
        let tuples = g.ticker(Duration::from_millis(10)).map(move |()| {
            n += 1;
            (n, format!("v{n}"))
        });
        let (left, right) = tuples.unzip();
        let seen_left = collect_timed(&left);
        let seen_right = collect_timed(&right);

        run_ms(&g, 20);

        assert_eq!(
            *seen_left.borrow(),
            vec![(1, ms(0)), (2, ms(10)), (3, ms(20))]
        );
        assert_eq!(
            *seen_right.borrow(),
            vec![
                ("v1".to_string(), ms(0)),
                ("v2".to_string(), ms(10)),
                ("v3".to_string(), ms(20)),
            ]
        );
    }

    #[test]
    fn unzip_clones_members_not_tuples() {
        struct CloneProbe {
            label: &'static str,
            clones: Rc<RefCell<Vec<&'static str>>>,
        }

        impl Clone for CloneProbe {
            fn clone(&self) -> Self {
                self.clones.borrow_mut().push(self.label);
                Self {
                    label: self.label,
                    clones: self.clones.clone(),
                }
            }
        }

        let g = Graph::new();
        let clones = Rc::new(RefCell::new(Vec::new()));
        let tuples = g.replay_from_iter(vec![(
            ms(10),
            (
                CloneProbe {
                    label: "left",
                    clones: clones.clone(),
                },
                CloneProbe {
                    label: "right",
                    clones: clones.clone(),
                },
            ),
        )]);
        let (_left, _right) = tuples.unzip();

        clones.borrow_mut().clear();

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*clones.borrow(), vec!["left", "right"]);
    }
}
