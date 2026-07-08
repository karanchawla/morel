use std::time::Duration;

use crate::core::graph::{Ctx, Operator, Stream};
use crate::core::port::{Input, Output};
use crate::core::time::Time;

fn duration_nanos(d: Duration) -> u128 {
    d.as_nanos()
}

fn next_boundary_after(start: Time, now: Time, period: Duration) -> Time {
    let period = duration_nanos(period);
    let elapsed = u128::from(now.as_nanos().saturating_sub(start.as_nanos()));
    let offset = ((elapsed / period) + 1).saturating_mul(period);
    let nanos = u128::from(start.as_nanos()).saturating_add(offset);
    Time::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

fn exclusive_lower_bound(start: Time, at: Time, size: Duration) -> Option<Time> {
    let elapsed = u128::from(at.as_nanos().saturating_sub(start.as_nanos()));
    if elapsed >= duration_nanos(size) {
        Some(at.saturating_sub(size))
    } else {
        None
    }
}

struct Buffer<T> {
    input: Input<T>,
    out: Output<Vec<T>>,
    capacity: usize,
    pending: Vec<T>,
}

impl<T> Operator for Buffer<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        if self.input.fired() {
            self.pending.push(self.input.get());
        }

        if self.pending.len() >= self.capacity || (cx.is_final() && !self.pending.is_empty()) {
            self.out.set(std::mem::take(&mut self.pending));
        }
    }
}

struct WindowTumbling<T> {
    input: Input<T>,
    out: Output<Vec<T>>,
    size: Duration,
    window_end: Option<Time>,
    pending: Vec<T>,
}

impl<T> Operator for WindowTumbling<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();
        let input_fired = self.input.fired();

        if let Some(end) = self.window_end {
            if now >= end {
                let batch = std::mem::take(&mut self.pending);
                self.window_end = None;

                if input_fired {
                    self.pending.push(self.input.get());
                    let next = next_boundary_after(cx.started_at(), now, self.size);
                    self.window_end = Some(next);
                    cx.at(next);
                }
                if !batch.is_empty() {
                    self.out.set(batch);
                }
                return;
            }
        }

        if input_fired {
            self.pending.push(self.input.get());
            if self.window_end.is_none() {
                let end = next_boundary_after(cx.started_at(), now, self.size);
                self.window_end = Some(end);
                cx.at(end);
            }
        }
    }
}

struct WindowSliding<T> {
    input: Input<T>,
    out: Output<Vec<T>>,
    size: Duration,
    slide: Duration,
    next_emit: Option<Time>,
    entries: Vec<(Time, T)>,
}

impl<T> Operator for WindowSliding<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, cx: &mut Ctx) {
        let now = cx.now();

        if self.input.fired() {
            self.entries.push((now, self.input.get()));
            if self.next_emit.is_none() {
                let next = next_boundary_after(cx.started_at(), now, self.slide);
                self.next_emit = Some(next);
                cx.at(next);
            }
        }

        if let Some(cutoff) = exclusive_lower_bound(cx.started_at(), now, self.size) {
            self.entries.retain(|(at, _)| *at > cutoff);
        }

        if let Some(at) = self.next_emit {
            if now >= at {
                let batch: Vec<T> = self
                    .entries
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect();
                let next = at + self.slide;
                let has_future_non_empty =
                    if let Some(cutoff) = exclusive_lower_bound(cx.started_at(), next, self.size) {
                        self.entries.iter().any(|(entry_at, _)| *entry_at > cutoff)
                    } else {
                        !self.entries.is_empty()
                    };

                if has_future_non_empty {
                    self.next_emit = Some(next);
                    cx.at(next);
                } else {
                    self.next_emit = None;
                }

                if !batch.is_empty() {
                    self.out.set(batch);
                }
            }
        }
    }
}

struct Collapse<T> {
    input: Input<Vec<T>>,
    out: Output<T>,
}

impl<T> Operator for Collapse<T>
where
    T: Clone + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if !self.input.fired() {
            return;
        }

        let value = self.input.borrow().and_then(|batch| batch.last().cloned());
        if let Some(value) = value {
            self.out.set(value);
        }
    }
}

struct MapBatch<T, U, F> {
    input: Input<Vec<T>>,
    out: Output<U>,
    f: F,
}

impl<T, U, F> Operator for MapBatch<T, U, F>
where
    T: 'static,
    U: 'static,
    F: FnMut(&[T]) -> U + 'static,
{
    fn step(&mut self, _cx: &mut Ctx) {
        if !self.input.fired() {
            return;
        }

        let value = match self.input.borrow() {
            Some(batch) => (self.f)(&batch[..]),
            None => return,
        };
        self.out.set(value);
    }
}

impl<T> Stream<T>
where
    T: Clone + 'static,
{
    /// Group values into fixed-size batches, flushing any remainder at shutdown.
    pub fn buffer(&self, capacity: usize) -> Stream<Vec<T>> {
        assert!(capacity > 0, "buffer capacity must be greater than zero");
        self.wire(|w| {
            w.finalize();
            Buffer {
                input: w.on(self),
                out: w.output(),
                capacity,
                pending: Vec::with_capacity(capacity),
            }
        })
    }

    /// Group values into non-overlapping time windows.
    pub fn window_tumbling(&self, size: Duration) -> Stream<Vec<T>> {
        assert!(!size.is_zero(), "window size must be greater than zero");
        self.wire(|w| WindowTumbling {
            input: w.on(self),
            out: w.output(),
            size,
            window_end: None,
            pending: Vec::new(),
        })
    }

    /// Group values into overlapping windows emitted every `slide`.
    pub fn window_sliding(&self, size: Duration, slide: Duration) -> Stream<Vec<T>> {
        assert!(!size.is_zero(), "window size must be greater than zero");
        assert!(!slide.is_zero(), "window slide must be greater than zero");
        self.wire(|w| WindowSliding {
            input: w.on(self),
            out: w.output(),
            size,
            slide,
            next_emit: None,
            entries: Vec::new(),
        })
    }
}

impl<T> Stream<Vec<T>>
where
    T: Clone + 'static,
{
    /// Emit the last value of each non-empty batch.
    ///
    /// ```
    /// use morel::{Graph, Replay, Time};
    /// let g = Graph::new();
    /// let batches = g.replay_from_iter(vec![(Time::EPOCH, vec![1, 2])]);
    /// let latest = batches.collapse();
    /// g.run(Replay::from(Time::EPOCH)).unwrap();
    /// assert_eq!(latest.peek(), Some(2));
    /// ```
    pub fn collapse(&self) -> Stream<T> {
        self.wire(|w| Collapse {
            input: w.on(self),
            out: w.output(),
        })
    }

    /// Transform each batch by borrowing it as a slice.
    pub fn map_batch<U: 'static>(&self, f: impl FnMut(&[T]) -> U + 'static) -> Stream<U> {
        self.wire(|w| MapBatch {
            input: w.on(self),
            out: w.output(),
            f,
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

    #[test]
    fn collapse_emits_last_of_batch_and_skips_empties() {
        let g = Graph::new();
        let src = g.replay_from_iter(vec![
            (ms(10), vec![1i64, 2]),
            (ms(20), vec![]),
            (ms(30), vec![3]),
        ]);
        let collapsed = src.collapse();
        let seen = collect(&collapsed);
        let counts = collect(&collapsed.count());

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*seen.borrow(), vec![2, 3]);
        assert_eq!(counts.borrow().last().copied(), Some(2));
    }

    #[test]
    fn map_batch_runs_on_every_batch_including_empty() {
        let g = Graph::new();
        let src = g.replay_from_iter(vec![
            (ms(10), vec![1i64, 2]),
            (ms(20), vec![]),
            (ms(30), vec![3]),
        ]);
        let seen = collect(&src.map_batch(|batch| batch.len()));

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*seen.borrow(), vec![2, 0, 1]);
    }

    #[test]
    fn map_batch_does_not_clone_the_batch() {
        let g = Graph::new();
        let retained = Rc::new(7i64);
        let src = g.replay_from_iter(vec![(ms(10), vec![retained.clone()])]);
        let seen = collect(&src.map_batch(|batch| Rc::strong_count(&batch[0])));

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*seen.borrow(), vec![2]);
        assert_eq!(Rc::strong_count(&retained), 2);
    }

    #[test]
    fn buffer_emits_full_batches_and_flushes_remainder() {
        let g = Graph::new();
        let seen = collect(&counter_ms(&g, 100).buffer(3));
        run_ms(&g, 650);
        assert_eq!(*seen.borrow(), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[test]
    fn buffer_flushes_even_when_never_full() {
        let g = Graph::new();
        let seen = collect(&counter_ms(&g, 100).buffer(10));
        run_ms(&g, 350);
        assert_eq!(*seen.borrow(), vec![vec![1, 2, 3, 4]]);
    }

    #[test]
    fn tumbling_window_batches_by_time() {
        let g = Graph::new();
        let seen = collect(&counter_ms(&g, 20).window_tumbling(Duration::from_millis(100)));
        run_ms(&g, 250);
        // Tumbling windows are half-open: [start, end).
        assert_eq!(
            (*seen.borrow())[..2].to_vec(),
            vec![vec![1, 2, 3, 4, 5], vec![6, 7, 8, 9, 10]]
        );
    }

    #[test]
    fn sliding_window_overlaps() {
        let g = Graph::new();
        let seen = collect(
            &counter_ms(&g, 20)
                .window_sliding(Duration::from_millis(100), Duration::from_millis(50)),
        );
        run_ms(&g, 160);
        let first = seen.borrow()[0].clone();
        assert_eq!(first, vec![1, 2, 3]);
        assert!(seen.borrow().len() >= 2);
    }

    #[test]
    fn sliding_window_lower_boundary_is_exclusive() {
        let g = Graph::new();
        let seen = collect(
            &counter_ms(&g, 50)
                .window_sliding(Duration::from_millis(100), Duration::from_millis(50)),
        );
        run_ms(&g, 100);
        // Sliding windows keep the upper bound inclusive and lower bound exclusive.
        assert_eq!(*seen.borrow(), vec![vec![1, 2], vec![2, 3]]);
    }

    #[test]
    fn finite_tumbling_window_stops_when_idle_after_flush() {
        let g = Graph::new();
        let seen = collect(&g.just(1i64).window_tumbling(Duration::from_millis(10)));

        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*seen.borrow(), vec![vec![1]]);
        assert_eq!(summary.ended_at, Time::EPOCH + Duration::from_millis(10));
    }

    #[test]
    fn finite_sliding_window_stops_when_idle_after_flush() {
        let g = Graph::new();
        let seen = collect(
            &g.just(1i64)
                .window_sliding(Duration::from_millis(100), Duration::from_millis(50)),
        );

        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*seen.borrow(), vec![vec![1]]);
        assert_eq!(summary.ended_at, Time::EPOCH + Duration::from_millis(50));
    }

    #[test]
    fn empty_windows_do_not_emit() {
        let g = Graph::new();
        let seen = collect(&counter_ms(&g, 1000).window_tumbling(Duration::from_millis(10)));
        run_ms(&g, 100);
        assert_eq!(seen.borrow().len(), 1);
    }
}
