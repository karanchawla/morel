use std::fmt;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam::channel::RecvTimeoutError;

use crate::core::engine::run_step;
use crate::core::graph::{Ctx, Graph, NodeId};
use crate::core::time::Time;

/// Stop condition for a run.
#[derive(Clone, Copy, Debug)]
pub enum Stop {
    /// Stop when no timers remain.
    Idle,
    /// Stop after processing timers at this instant.
    At(Time),
    /// Stop after this much engine time has elapsed.
    After(Duration),
    /// Stop after this many non-idle engine steps.
    Steps(u64),
    /// Run until [`Ctx::stop`] or [`Ctx::fail`].
    Never,
}

/// Deterministic run driven by virtual time.
pub struct Replay {
    start: Time,
    stop: Stop,
}

impl Replay {
    pub fn from(start: Time) -> Self {
        Self {
            start,
            stop: Stop::Idle,
        }
    }

    pub fn stop(mut self, stop: Stop) -> Self {
        self.stop = stop;
        self
    }
}

/// Wall-clock run driven by timers and external wakes.
pub struct Live {
    stop: Stop,
}

impl Live {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { stop: Stop::Never }
    }

    pub fn stop(mut self, stop: Stop) -> Self {
        self.stop = stop;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Replay,
    Live,
}

#[doc(hidden)]
pub enum RunConfig {
    Replay { start: Time, stop: Stop },
    Live { stop: Stop },
}

pub trait RunSpec {
    #[doc(hidden)]
    fn config(self) -> RunConfig;
}

impl RunSpec for Replay {
    fn config(self) -> RunConfig {
        RunConfig::Replay {
            start: self.start,
            stop: self.stop,
        }
    }
}

impl RunSpec for Live {
    fn config(self) -> RunConfig {
        RunConfig::Live { stop: self.stop }
    }
}

/// Summary returned by a completed run.
#[derive(Debug)]
pub struct Summary {
    pub steps: u64,
    pub started_at: Time,
    pub ended_at: Time,
}

#[derive(Debug)]
pub enum Error {
    /// An operator failed the run via [`Ctx::fail`].
    Node {
        node: NodeId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Node { node, source } => write!(f, "node {node:?} failed: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Node { source, .. } => {
                Some(source.as_ref() as &(dyn std::error::Error + 'static))
            }
        }
    }
}

pub(crate) enum StopRequest {
    Clean,
    Failed {
        node: NodeId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Ctx<'_> {
    /// Request a clean shutdown after the current engine step.
    pub fn stop(&mut self) {
        let mut stopping = self.core.stopping.borrow_mut();
        if stopping.is_none() {
            *stopping = Some(StopRequest::Clean);
        }
    }

    /// Fail the run and attribute the error to this node.
    ///
    /// The first failure wins; later failures in the same run are dropped as
    /// consequences of the first. A failure still overrides a clean stop.
    /// Errors must be `Send + Sync` so failures can be propagated through
    /// worker child graphs and other cross-thread adapters without flattening
    /// their concrete type.
    pub fn fail(&mut self, source: impl Into<Box<dyn std::error::Error + Send + Sync>>) {
        let mut stopping = self.core.stopping.borrow_mut();
        if matches!(*stopping, Some(StopRequest::Failed { .. })) {
            return;
        }
        *stopping = Some(StopRequest::Failed {
            node: self.node,
            source: source.into(),
        });
    }
}

impl Graph {
    /// Run the graph to completion.
    pub fn run(&self, spec: impl RunSpec) -> Result<Summary, Error> {
        self.begin(spec);
        match self.core.mode.get() {
            Mode::Replay => self.drive_replay(),
            Mode::Live => self.drive_live(),
        }
        self.end()
    }

    /// Prepare a run for manual stepping.
    pub fn begin(&self, spec: impl RunSpec) {
        let core = &self.core;
        assert!(!core.running.get(), "graph is already running");

        let (mode, start, stop) = match spec.config() {
            RunConfig::Replay { start, stop } => (Mode::Replay, start, stop),
            RunConfig::Live { stop } => (Mode::Live, Time::wall_now(), stop),
        };

        core.running.set(true);
        core.mode.set(mode);
        core.is_final.set(false);
        core.steps.set(0);
        *core.stopping.borrow_mut() = None;
        core.clock.set(start);
        core.started_at.set(start);
        core.timers.borrow_mut().clear();
        core.timer_seq.set(0);
        while core.wake_rx.try_recv().is_ok() {}
        core.live.store(mode == Mode::Live, Ordering::Release);

        let (end_at, end_steps, stop_on_idle) = match stop {
            Stop::Idle => (Time::MAX, u64::MAX, true),
            Stop::At(t) => (t, u64::MAX, false),
            Stop::After(d) => (start + d, u64::MAX, false),
            Stop::Steps(n) => (Time::MAX, n, false),
            Stop::Never => (Time::MAX, u64::MAX, false),
        };
        core.end_at.set(end_at);
        core.end_steps.set(end_steps);
        core.stop_on_idle.set(stop_on_idle);

        {
            let need = core.nodes.borrow().len().div_ceil(u64::BITS as usize);
            let mut pending = core.pending.borrow_mut();
            pending.resize(need, 0);
            pending.fill(0);
        }

        let n = core.nodes.borrow().len();
        for i in 0..n {
            let op = { core.nodes.borrow()[i].op.clone() };
            let mut cx = Ctx {
                core,
                node: NodeId(i),
            };
            op.borrow_mut().on_start(&mut cx);
        }

        for entry in core.nodes.borrow().iter() {
            entry.fired.set(false);
        }
    }

    /// Advance replay to the next timer and run all work due at that time.
    pub fn step(&self) -> bool {
        let core = &self.core;
        if core.stopping.borrow().is_some() || core.steps.get() >= core.end_steps.get() {
            return false;
        }

        match core.next_timer_at() {
            None => return false,
            Some(t) if t > core.end_at.get() => return false,
            Some(_) => {}
        }

        if !core.advance_to_next_timer() {
            return false;
        }
        if run_step(core) {
            core.steps.set(core.steps.get() + 1);
        }
        true
    }

    /// Finish a run and call each operator's shutdown hook.
    pub fn end(&self) -> Result<Summary, Error> {
        let core = &self.core;
        assert!(core.running.get(), "graph is not running");
        let failed = matches!(*core.stopping.borrow(), Some(StopRequest::Failed { .. }));

        if !failed {
            let finalize_nodes: Vec<NodeId> = core
                .nodes
                .borrow()
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.finalize)
                .map(|(i, _)| NodeId(i))
                .collect();

            if !finalize_nodes.is_empty() {
                core.is_final.set(true);
                for id in finalize_nodes {
                    core.mark(id);
                }
                if run_step(core) {
                    core.steps.set(core.steps.get() + 1);
                }
            }
        }

        core.live.store(false, Ordering::Release);

        let n = core.nodes.borrow().len();
        for i in 0..n {
            let op = { core.nodes.borrow()[i].op.clone() };
            let mut cx = Ctx {
                core,
                node: NodeId(i),
            };
            op.borrow_mut().on_stop(&mut cx);
        }

        core.running.set(false);
        core.is_final.set(false);

        match core.stopping.borrow_mut().take() {
            Some(StopRequest::Failed { node, source }) => Err(Error::Node { node, source }),
            _ => Ok(Summary {
                steps: core.steps.get(),
                started_at: core.started_at.get(),
                ended_at: core.clock.get(),
            }),
        }
    }

    fn drive_replay(&self) {
        loop {
            if !self.step() {
                break;
            }
        }
    }

    fn drive_live(&self) {
        let core = &self.core;

        loop {
            if core.stopping.borrow().is_some() || core.steps.get() >= core.end_steps.get() {
                break;
            }

            let now = Time::wall_now();
            if now >= core.end_at.get() {
                break;
            }

            let mut progressed = false;

            while let Ok(id) = core.wake_rx.try_recv() {
                core.mark(id);
                progressed = true;
            }
            if core.fire_due_timers(now) {
                progressed = true;
            }

            if progressed {
                core.clock.set(now);
                if run_step(core) {
                    core.steps.set(core.steps.get() + 1);
                }
                continue;
            }

            let next_timer = core.next_timer_at();
            if core.stop_on_idle.get() && next_timer.is_none() {
                break;
            }

            let deadline = next_timer.map_or(core.end_at.get(), |timer_at| {
                timer_at.min(core.end_at.get())
            });
            let now = Time::wall_now();
            if deadline <= now {
                continue;
            }

            if deadline == Time::MAX {
                match core.wake_rx.recv() {
                    Ok(id) => {
                        let mut wakes = vec![id];
                        while let Ok(id) = core.wake_rx.try_recv() {
                            wakes.push(id);
                        }
                        let now = Time::wall_now();
                        if now >= core.end_at.get() {
                            break;
                        }
                        for id in wakes {
                            core.mark(id);
                        }
                        core.clock.set(now);
                        if run_step(core) {
                            core.steps.set(core.steps.get() + 1);
                        }
                    }
                    Err(_) => unreachable!("core holds a wake sender"),
                }
            } else {
                match core.wake_rx.recv_timeout(deadline - now) {
                    Ok(id) => {
                        let mut wakes = vec![id];
                        while let Ok(id) = core.wake_rx.try_recv() {
                            wakes.push(id);
                        }
                        let now = Time::wall_now();
                        if now >= core.end_at.get() {
                            break;
                        }
                        for id in wakes {
                            core.mark(id);
                        }
                        core.clock.set(now);
                        if run_step(core) {
                            core.steps.set(core.steps.get() + 1);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        unreachable!("core holds a wake sender")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::{Ctx, Graph, Operator};
    use crate::core::port::{Input, Output};
    use crate::core::time::Time;
    use std::cell::RefCell;
    use std::error::Error as _;
    use std::rc::Rc;
    use std::time::Duration;

    /// Timer-only source used by replay tests.
    struct Scheduled {
        values: Vec<(i32, Time)>,
        index: usize,
        out: Output<i32>,
    }

    impl Operator for Scheduled {
        fn on_start(&mut self, cx: &mut Ctx) {
            if let Some(&(_, at)) = self.values.first() {
                cx.at(at);
            }
        }
        fn step(&mut self, cx: &mut Ctx) {
            let (v, _) = self.values[self.index];
            self.index += 1;
            if let Some(&(_, at)) = self.values.get(self.index) {
                cx.at(at);
            }
            self.out.set(v);
        }
    }

    fn scheduled(g: &Graph, values: Vec<(i32, Time)>) -> crate::core::graph::Stream<i32> {
        g.add(move |w| Scheduled {
            values,
            index: 0,
            out: w.output(),
        })
    }

    /// Records each observed value with the engine time.
    struct Recorder {
        input: Input<i32>,
        out: Output<()>,
        seen: Rc<RefCell<Vec<(i32, Time)>>>,
    }

    impl Operator for Recorder {
        fn step(&mut self, cx: &mut Ctx) {
            self.seen.borrow_mut().push((self.input.get(), cx.now()));
            self.out.set(());
        }
    }

    fn recorder(g: &Graph, src: &crate::core::graph::Stream<i32>) -> Rc<RefCell<Vec<(i32, Time)>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = seen.clone();
        let _sink: crate::core::graph::Stream<()> = g.add(|w| Recorder {
            input: w.on(src),
            out: w.output(),
            seen: seen2,
        });
        seen
    }

    #[test]
    fn replay_teleports_time_and_stops_when_idle() {
        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![
                (1, Time::from_nanos(100)),
                (2, Time::from_nanos(200)),
                (3, Time::from_nanos(1_000_000_000)),
            ],
        );
        let seen = recorder(&g, &src);

        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(
            *seen.borrow(),
            vec![
                (1, Time::from_nanos(100)),
                (2, Time::from_nanos(200)),
                (3, Time::from_nanos(1_000_000_000)),
            ]
        );
        assert_eq!(summary.steps, 3);
        assert_eq!(summary.ended_at, Time::from_nanos(1_000_000_000));
    }

    #[test]
    fn stop_steps_is_exact() {
        let g = Graph::new();
        let src = scheduled(
            &g,
            (1..=5)
                .map(|i| (i, Time::from_nanos(i as u64 * 100)))
                .collect(),
        );
        let seen = recorder(&g, &src);

        g.run(Replay::from(Time::EPOCH).stop(Stop::Steps(3)))
            .unwrap();

        assert_eq!(seen.borrow().len(), 3);
    }

    #[test]
    fn stop_at_is_inclusive_and_skips_later_timers() {
        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![
                (1, Time::from_nanos(100)),
                (2, Time::from_nanos(500)),
                (3, Time::from_nanos(501)),
            ],
        );
        let seen = recorder(&g, &src);

        g.run(Replay::from(Time::EPOCH).stop(Stop::At(Time::from_nanos(500))))
            .unwrap();

        assert_eq!(seen.borrow().len(), 2);
    }

    #[test]
    fn stop_after_is_relative_to_start() {
        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![
                (1, Time::from_nanos(1_100)),
                (2, Time::from_nanos(1_400)),
                (3, Time::from_nanos(1_600)),
            ],
        );
        let seen = recorder(&g, &src);

        g.run(Replay::from(Time::from_nanos(1_000)).stop(Stop::After(Duration::from_nanos(500))))
            .unwrap();

        assert_eq!(seen.borrow().len(), 2);
    }

    #[test]
    fn lifecycle_order_start_step_stop() {
        let events = Rc::new(RefCell::new(Vec::<&'static str>::new()));
        struct Lifecycle {
            events: Rc<RefCell<Vec<&'static str>>>,
            out: Output<()>,
        }
        impl Operator for Lifecycle {
            fn on_start(&mut self, cx: &mut Ctx) {
                self.events.borrow_mut().push("start");
                cx.at(cx.now());
            }
            fn step(&mut self, _cx: &mut Ctx) {
                self.events.borrow_mut().push("step");
                self.out.set(());
            }
            fn on_stop(&mut self, _cx: &mut Ctx) {
                self.events.borrow_mut().push("stop");
            }
        }
        let g = Graph::new();
        let ev = events.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Lifecycle {
            events: ev,
            out: w.output(),
        });

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(*events.borrow(), vec!["start", "step", "stop"]);
    }

    #[test]
    fn finalize_runs_one_last_step_with_is_final() {
        struct Flusher {
            input: Input<i32>,
            out: Output<Vec<i32>>,
            pending: Vec<i32>,
        }
        impl Operator for Flusher {
            fn step(&mut self, cx: &mut Ctx) {
                if self.input.fired() {
                    self.pending.push(self.input.get());
                }
                if cx.is_final() && !self.pending.is_empty() {
                    self.out.set(std::mem::take(&mut self.pending));
                }
            }
        }
        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![(1, Time::from_nanos(100)), (2, Time::from_nanos(200))],
        );
        let flushed = g.add::<Vec<i32>, _>(|w| {
            w.finalize();
            Flusher {
                input: w.on(&src),
                out: w.output(),
                pending: Vec::new(),
            }
        });

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(flushed.peek(), Some(vec![1, 2]));
    }

    #[test]
    fn ctx_stop_ends_run_cleanly() {
        struct StopAfterTwo {
            count: u32,
            out: Output<u32>,
        }
        impl Operator for StopAfterTwo {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.count += 1;
                self.out.set(self.count);
                if self.count == 2 {
                    cx.stop();
                } else {
                    cx.after(Duration::from_nanos(10));
                }
            }
        }
        let g = Graph::new();
        let n = g.add::<u32, _>(|w| StopAfterTwo {
            count: 0,
            out: w.output(),
        });

        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(n.peek(), Some(2));
        assert_eq!(summary.steps, 2);
    }

    #[test]
    fn ctx_fail_surfaces_as_error() {
        struct Failing {
            out: Output<()>,
        }
        impl Operator for Failing {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.fail("bad tick");
            }
        }
        let g = Graph::new();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Failing { out: w.output() });

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert!(err.to_string().contains("bad tick"));
    }

    #[test]
    fn first_failure_wins_over_later_failures() {
        struct FailWith {
            message: &'static str,
            out: Output<()>,
        }
        impl Operator for FailWith {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.fail(self.message);
            }
        }
        let g = Graph::new();
        let _a: crate::core::graph::Stream<()> = g.add(|w| FailWith {
            message: "first",
            out: w.output(),
        });
        let _b: crate::core::graph::Stream<()> = g.add(|w| FailWith {
            message: "second",
            out: w.output(),
        });

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert!(err.to_string().contains("first"), "{err}");
    }

    #[test]
    fn failure_overrides_clean_stop_request() {
        struct StopThenFail {
            out: Output<()>,
        }
        impl Operator for StopThenFail {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.stop();
                cx.fail("real problem");
            }
        }
        let g = Graph::new();
        let _n: crate::core::graph::Stream<()> = g.add(|w| StopThenFail { out: w.output() });

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert!(err.to_string().contains("real problem"), "{err}");
    }

    #[test]
    fn ctx_fail_keeps_first_error_through_shutdown() {
        struct StepFailing {
            out: Output<()>,
        }
        impl Operator for StepFailing {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.fail("first failure");
            }
        }

        struct StopFailing {
            out: Output<()>,
        }
        impl Operator for StopFailing {
            fn step(&mut self, _cx: &mut Ctx) {}

            fn on_stop(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.fail("shutdown failure");
            }
        }

        let g = Graph::new();
        let _primary: crate::core::graph::Stream<()> = g.add(|w| StepFailing { out: w.output() });
        let _shutdown: crate::core::graph::Stream<()> = g.add(|w| StopFailing { out: w.output() });

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();
        let text = err.to_string();

        assert!(text.contains("first failure"), "{text}");
        assert!(!text.contains("shutdown failure"), "{text}");
    }

    #[test]
    fn node_error_exposes_wrapped_source() {
        struct Failing {
            out: Output<()>,
        }
        impl Operator for Failing {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
            }
            fn step(&mut self, cx: &mut Ctx) {
                self.out.set(());
                cx.fail("underlying failure");
            }
        }
        let g = Graph::new();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Failing { out: w.output() });

        let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

        assert_eq!(err.source().unwrap().to_string(), "underlying failure");
    }

    #[test]
    fn clean_ctx_stop_still_runs_finalizers() {
        struct StopAfterTwo {
            input: Input<i32>,
            count: u32,
            out: Output<()>,
        }
        impl Operator for StopAfterTwo {
            fn step(&mut self, cx: &mut Ctx) {
                let _ = self.input.get();
                self.count += 1;
                self.out.set(());
                if self.count == 2 {
                    cx.stop();
                }
            }
        }

        struct Flusher {
            input: Input<i32>,
            out: Output<Vec<i32>>,
            pending: Vec<i32>,
        }
        impl Operator for Flusher {
            fn step(&mut self, cx: &mut Ctx) {
                if self.input.fired() {
                    self.pending.push(self.input.get());
                }
                if cx.is_final() && !self.pending.is_empty() {
                    self.out.set(std::mem::take(&mut self.pending));
                }
            }
        }

        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![
                (1, Time::from_nanos(100)),
                (2, Time::from_nanos(200)),
                (3, Time::from_nanos(300)),
            ],
        );
        let flushed = g.add::<Vec<i32>, _>(|w| {
            w.finalize();
            Flusher {
                input: w.on(&src),
                out: w.output(),
                pending: Vec::new(),
            }
        });
        let _stopper: crate::core::graph::Stream<()> = g.add(|w| StopAfterTwo {
            input: w.on(&src),
            count: 0,
            out: w.output(),
        });

        g.run(Replay::from(Time::EPOCH)).unwrap();

        assert_eq!(flushed.peek(), Some(vec![1, 2]));
    }

    #[test]
    fn begin_clears_leftover_timers_from_previous_run() {
        struct LeavesFutureTimer {
            starts: u32,
            count: Rc<RefCell<u32>>,
            out: Output<u32>,
        }
        impl Operator for LeavesFutureTimer {
            fn on_start(&mut self, cx: &mut Ctx) {
                if self.starts == 0 {
                    cx.at(cx.now());
                }
                self.starts += 1;
            }
            fn step(&mut self, cx: &mut Ctx) {
                let mut count = self.count.borrow_mut();
                *count += 1;
                self.out.set(*count);
                cx.at(Time::from_nanos(1_000));
            }
        }

        let g = Graph::new();
        let count = Rc::new(RefCell::new(0));
        let count2 = count.clone();
        let _n = g.add::<u32, _>(|w| LeavesFutureTimer {
            starts: 0,
            count: count2,
            out: w.output(),
        });

        let first = g
            .run(Replay::from(Time::EPOCH).stop(Stop::Steps(1)))
            .unwrap();
        let second = g
            .run(Replay::from(Time::EPOCH).stop(Stop::Steps(1)))
            .unwrap();

        assert_eq!(first.steps, 1);
        assert_eq!(second.steps, 0);
        assert_eq!(*count.borrow(), 1);
    }

    #[test]
    #[should_panic(expected = "graph is not running")]
    fn end_without_begin_panics() {
        let g = Graph::new();
        let _ = g.end();
    }

    #[test]
    #[should_panic(expected = "graph is not running")]
    fn end_twice_panics() {
        let g = Graph::new();
        g.begin(Replay::from(Time::EPOCH));
        g.end().unwrap();
        let _ = g.end();
    }

    #[test]
    fn empty_graph_finishes_immediately() {
        let g = Graph::new();
        let summary = g.run(Replay::from(Time::EPOCH)).unwrap();
        assert_eq!(summary.steps, 0);
    }

    #[test]
    fn stepping_api_drives_replay_manually() {
        let g = Graph::new();
        let src = scheduled(
            &g,
            vec![(1, Time::from_nanos(100)), (2, Time::from_nanos(200))],
        );
        let seen = recorder(&g, &src);

        g.begin(Replay::from(Time::EPOCH));
        assert!(g.step());
        assert_eq!(seen.borrow().len(), 1);
        assert!(g.step());
        assert!(!g.step());
        g.end().unwrap();

        assert_eq!(seen.borrow().len(), 2);
    }

    #[test]
    #[should_panic(expected = "cannot add nodes while the graph is running")]
    fn adding_during_run_panics() {
        let g = Graph::new();
        let src = scheduled(&g, vec![(1, Time::from_nanos(100))]);
        let _seen = recorder(&g, &src);
        g.begin(Replay::from(Time::EPOCH));
        let _late = scheduled(&g, vec![(9, Time::from_nanos(900))]);
    }
}
