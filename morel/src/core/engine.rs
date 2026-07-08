use std::cmp::{Ordering, Reverse};
use std::rc::Rc;
use std::time::Duration;

use crate::core::graph::{Ctx, GraphCore, NodeId};
use crate::core::time::Time;

const WORD_BITS: usize = u64::BITS as usize;

impl GraphCore {
    pub(crate) fn mark(&self, id: NodeId) {
        debug_assert!(id.0 < self.nodes.borrow().len());
        let word = id.0 / WORD_BITS;
        let bit = id.0 % WORD_BITS;
        let mut pending = self.pending.borrow_mut();
        if pending.len() <= word {
            pending.resize(word + 1, 0);
        }
        pending[word] |= 1u64 << bit;
    }

    pub(crate) fn push_timer(&self, at: Time, node: NodeId, repeat: Option<Duration>) {
        if let Some(period) = repeat {
            assert!(
                !period.is_zero(),
                "repeating timers must have a non-zero period"
            );
        }
        let seq = self.timer_seq.get();
        self.timer_seq
            .set(seq.checked_add(1).expect("timer sequence exhausted"));
        self.timers.borrow_mut().push(Reverse(TimerEntry {
            at,
            seq,
            node,
            repeat,
        }));
    }

    pub(crate) fn fire_due_timers(&self, now: Time) -> bool {
        let mut fired = false;

        loop {
            let Some(entry) = self.timers.borrow().peek().map(|entry| entry.0) else {
                break;
            };
            if entry.at > now {
                break;
            }

            self.timers.borrow_mut().pop();
            self.mark(entry.node);
            fired = true;

            if let Some(period) = entry.repeat {
                self.push_timer(entry.at + period, entry.node, Some(period));
            }
        }

        fired
    }

    pub(crate) fn advance_to_next_timer(&self) -> bool {
        let Some(next) = self.next_timer_at() else {
            return false;
        };

        self.clock.set(next);
        self.fire_due_timers(next)
    }

    pub(crate) fn next_timer_at(&self) -> Option<Time> {
        self.timers.borrow().peek().map(|entry| entry.0.at)
    }
}

impl Ctx<'_> {
    pub fn now(&self) -> Time {
        self.core.clock.get()
    }

    pub fn started_at(&self) -> Time {
        self.core.started_at.get()
    }

    pub fn elapsed(&self) -> Duration {
        self.now() - self.started_at()
    }

    pub fn is_final(&self) -> bool {
        self.core.is_final.get()
    }

    pub fn is_live(&self) -> bool {
        self.core.mode.get() == crate::core::run::Mode::Live
    }

    pub fn at(&mut self, at: Time) {
        self.core.push_timer(at, self.node, None);
    }

    pub fn after(&mut self, delay: Duration) {
        self.at(self.now() + delay);
    }

    pub fn every(&mut self, period: Duration) {
        assert!(
            !period.is_zero(),
            "repeating timers must have a non-zero period"
        );
        self.core
            .push_timer(self.now() + period, self.node, Some(period));
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TimerEntry {
    pub(crate) at: Time,
    pub(crate) seq: u64,
    pub(crate) node: NodeId,
    pub(crate) repeat: Option<Duration>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

pub(crate) fn run_step(core: &Rc<GraphCore>) -> bool {
    let nodes = core.nodes.borrow();
    let mut pending = core.pending.borrow_mut();
    let mut fired_scratch = core.fired_scratch.borrow_mut();
    let mut any_ran = false;

    fired_scratch.clear();

    for word_index in 0..pending.len() {
        loop {
            let word = pending[word_index];
            if word == 0 {
                break;
            }

            let bit = word.trailing_zeros() as usize;
            let mask = 1u64 << bit;
            pending[word_index] &= !mask;

            let id = NodeId(word_index * WORD_BITS + bit);
            if id.0 >= nodes.len() {
                continue;
            }

            any_ran = true;
            let node = &nodes[id.0];
            let mut cx = Ctx { core, node: id };
            node.op.borrow_mut().step(&mut cx);

            if node.fired.get() {
                fired_scratch.push(id);
                for &downstream in &node.downstream {
                    let downstream_word = downstream.0 / WORD_BITS;
                    let downstream_bit = downstream.0 % WORD_BITS;
                    pending[downstream_word] |= 1u64 << downstream_bit;
                }
            }
        }
    }

    for &id in fired_scratch.iter() {
        nodes[id.0].fired.set(false);
    }

    any_ran
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::{Ctx, Graph, Operator};
    use crate::core::port::{Input, Output};
    use crate::core::time::Time;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    struct Emit {
        value: i32,
        out: Output<i32>,
    }
    impl Operator for Emit {
        fn step(&mut self, _cx: &mut Ctx) {
            self.out.set(self.value);
        }
    }

    struct Add {
        a: Input<i32>,
        b: Input<i32>,
        out: Output<i32>,
        steps: Rc<RefCell<u32>>,
    }
    impl Operator for Add {
        fn step(&mut self, _cx: &mut Ctx) {
            *self.steps.borrow_mut() += 1;
            if let (Some(a), Some(b)) = (self.a.peek(), self.b.peek()) {
                self.out.set(a + b);
            }
        }
    }

    #[test]
    fn cascade_completes_in_one_run_step() {
        let g = Graph::new();
        let src = g.add::<i32, _>(|w| Emit {
            value: 5,
            out: w.output(),
        });
        let double = src.map_for_test(&g, |x| x * 2);
        let plus_one = double.map_for_test(&g, |x| x + 1);
        g.core.mark(src.id);
        assert!(run_step(&g.core));
        assert_eq!(plus_one.peek(), Some(11));
    }

    #[test]
    fn diamond_steps_join_exactly_once() {
        let g = Graph::new();
        let steps = Rc::new(RefCell::new(0u32));
        let src = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        let left = src.map_for_test(&g, |x| x + 10);
        let right = src.map_for_test(&g, |x| x + 20);
        let join = g.add::<i32, _>(|w| Add {
            a: w.on(&left),
            b: w.on(&right),
            out: w.output(),
            steps: steps.clone(),
        });
        g.core.mark(src.id);
        run_step(&g.core);
        assert_eq!(
            *steps.borrow(),
            1,
            "join must step once, not once per input"
        );
        assert_eq!(join.peek(), Some(11 + 21));
    }

    #[test]
    fn fired_flags_are_cleared_after_step() {
        let g = Graph::new();
        let src = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        g.core.mark(src.id);
        run_step(&g.core);
        assert!(!g.core.nodes.borrow()[0].fired.get());
    }

    #[test]
    fn unmarked_nodes_do_not_run() {
        let g = Graph::new();
        let _a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        let b = g.add::<i32, _>(|w| Emit {
            value: 2,
            out: w.output(),
        });
        g.core.mark(b.id);
        run_step(&g.core);
        assert_eq!(_a.peek(), None);
        assert_eq!(b.peek(), Some(2));
    }

    #[test]
    fn empty_pending_returns_false() {
        let g = Graph::new();
        let _a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        assert!(!run_step(&g.core));
    }

    #[test]
    fn fired_scratch_is_preallocated_for_all_nodes() {
        let g = Graph::new();
        let _a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        let _b = g.add::<i32, _>(|w| Emit {
            value: 2,
            out: w.output(),
        });
        assert!(g.core.fired_scratch.borrow().capacity() >= g.len());
    }

    #[test]
    fn run_step_does_not_own_running_flag() {
        struct CheckRunning {
            expected: bool,
            out: Output<i32>,
        }
        impl Operator for CheckRunning {
            fn step(&mut self, cx: &mut Ctx) {
                assert_eq!(cx.core.running.get(), self.expected);
                self.out.set(1);
            }
        }

        let idle = Graph::new();
        let idle_node = idle.add::<i32, _>(|w| CheckRunning {
            expected: false,
            out: w.output(),
        });
        idle.core.mark(idle_node.id);
        run_step(&idle.core);
        assert!(!idle.core.running.get());

        let active = Graph::new();
        let active_node = active.add::<i32, _>(|w| CheckRunning {
            expected: true,
            out: w.output(),
        });
        active.core.running.set(true);
        active.core.mark(active_node.id);
        run_step(&active.core);
        assert!(active.core.running.get());
    }

    #[test]
    fn non_firing_node_does_not_propagate() {
        struct Silent {
            out: Output<i32>,
        }
        impl Operator for Silent {
            fn step(&mut self, _cx: &mut Ctx) {
                let _ = &self.out;
            }
        }
        let g = Graph::new();
        let silent = g.add::<i32, _>(|w| Silent { out: w.output() });
        let down = silent.map_for_test(&g, |x| x);
        g.core.mark(silent.id);
        run_step(&g.core);
        assert_eq!(down.peek(), None);
    }

    struct TestMap<F> {
        input: Input<i32>,
        out: Output<i32>,
        f: F,
    }
    impl<F: FnMut(i32) -> i32 + 'static> Operator for TestMap<F> {
        fn step(&mut self, _cx: &mut Ctx) {
            self.out.set((self.f)(self.input.get()));
        }
    }
    impl crate::core::graph::Stream<i32> {
        fn map_for_test(
            &self,
            g: &Graph,
            f: impl FnMut(i32) -> i32 + 'static,
        ) -> crate::core::graph::Stream<i32> {
            g.add(|w| TestMap {
                input: w.on(self),
                out: w.output(),
                f,
            })
        }
    }

    struct SelfTicker {
        period: Duration,
        count: i32,
        out: Output<i32>,
    }

    impl Operator for SelfTicker {
        fn step(&mut self, cx: &mut Ctx) {
            self.count += 1;
            self.out.set(self.count);
            cx.after(self.period);
        }
    }

    #[test]
    fn timers_fire_in_time_then_seq_order() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        let b = g.add::<i32, _>(|w| Emit {
            value: 2,
            out: w.output(),
        });
        g.core.push_timer(Time::from_nanos(200), b.id, None);
        g.core.push_timer(Time::from_nanos(100), a.id, None);

        assert!(g.core.advance_to_next_timer());
        assert_eq!(g.core.clock.get(), Time::from_nanos(100));
        run_step(&g.core);
        assert_eq!(a.peek(), Some(1));
        assert_eq!(b.peek(), None);

        assert!(g.core.advance_to_next_timer());
        assert_eq!(g.core.clock.get(), Time::from_nanos(200));
        run_step(&g.core);
        assert_eq!(b.peek(), Some(2));

        assert!(!g.core.advance_to_next_timer());
    }

    #[test]
    fn same_instant_timers_drain_together() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        let b = g.add::<i32, _>(|w| Emit {
            value: 2,
            out: w.output(),
        });
        g.core.push_timer(Time::from_nanos(100), a.id, None);
        g.core.push_timer(Time::from_nanos(100), b.id, None);

        g.core.advance_to_next_timer();
        run_step(&g.core);
        assert_eq!(a.peek(), Some(1));
        assert_eq!(b.peek(), Some(2));
        assert!(!g.core.advance_to_next_timer());
    }

    #[test]
    fn ctx_after_reschedules_relative_to_virtual_clock() {
        let g = Graph::new();
        let t = g.add::<i32, _>(|w| SelfTicker {
            period: Duration::from_nanos(10),
            count: 0,
            out: w.output(),
        });
        g.core.clock.set(Time::EPOCH);
        g.core.push_timer(Time::EPOCH, t.id, None);

        for _ in 0..3 {
            assert!(g.core.advance_to_next_timer());
            run_step(&g.core);
        }
        assert_eq!(t.peek(), Some(3));
        assert_eq!(g.core.clock.get(), Time::from_nanos(20));
    }

    #[test]
    fn repeating_timer_is_anchored() {
        struct CountOnly {
            count: i32,
            out: Output<i32>,
        }
        impl Operator for CountOnly {
            fn step(&mut self, _cx: &mut Ctx) {
                self.count += 1;
                self.out.set(self.count);
            }
        }
        let g = Graph::new();
        let n = g.add::<i32, _>(|w| CountOnly {
            count: 0,
            out: w.output(),
        });
        g.core
            .push_timer(Time::from_nanos(10), n.id, Some(Duration::from_nanos(10)));

        for expected_clock in [10u64, 20, 30] {
            g.core.advance_to_next_timer();
            assert_eq!(g.core.clock.get(), Time::from_nanos(expected_clock));
            run_step(&g.core);
        }
        assert_eq!(n.peek(), Some(3));
    }

    #[test]
    fn next_timer_at_peeks_without_popping() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Emit {
            value: 1,
            out: w.output(),
        });
        assert_eq!(g.core.next_timer_at(), None);
        g.core.push_timer(Time::from_nanos(50), a.id, None);
        assert_eq!(g.core.next_timer_at(), Some(Time::from_nanos(50)));
        assert_eq!(g.core.next_timer_at(), Some(Time::from_nanos(50)));
    }

    #[test]
    #[should_panic(expected = "repeating timers must have a non-zero period")]
    fn ctx_every_zero_period_panics() {
        struct EveryZero {
            out: Output<i32>,
        }
        impl Operator for EveryZero {
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(1);
                cx.every(Duration::ZERO);
            }
        }

        let g = Graph::new();
        let n = g.add::<i32, _>(|w| EveryZero { out: w.output() });
        g.core.mark(n.id);
        run_step(&g.core);
    }
}
