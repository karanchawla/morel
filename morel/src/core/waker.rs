use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam::channel::Sender;

use crate::core::graph::{Ctx, NodeId};

/// Cross-thread handle for scheduling a node during a live run.
#[derive(Clone)]
pub struct Waker {
    node: NodeId,
    tx: Sender<NodeId>,
    live: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeError {
    /// The graph is not currently running in live mode.
    NotLive,
    /// The graph was dropped.
    GraphGone,
}

impl Waker {
    /// Schedule the node associated with this waker.
    pub fn wake(&self) -> Result<(), WakeError> {
        if !self.live.load(Ordering::Acquire) {
            return Err(WakeError::NotLive);
        }
        self.tx.send(self.node).map_err(|_| WakeError::GraphGone)
    }
}

impl Ctx<'_> {
    /// Return a handle that can schedule this node from another thread.
    pub fn waker(&self) -> Waker {
        Waker {
            node: self.node,
            tx: self.core.wake_tx.clone(),
            live: self.core.live.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::{Ctx, Graph, Operator};
    use crate::core::port::Output;
    use crate::core::run::{Live, Stop};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    struct Wakeable {
        waker_tx: mpsc::Sender<Waker>,
        steps: Arc<AtomicU32>,
        out: Output<()>,
    }

    impl Operator for Wakeable {
        fn on_start(&mut self, cx: &mut Ctx) {
            let _ = self.waker_tx.send(cx.waker());
        }

        fn step(&mut self, _cx: &mut Ctx) {
            self.steps.fetch_add(1, Ordering::SeqCst);
            self.out.set(());
        }
    }

    #[test]
    fn wake_before_any_run_errors_not_live() {
        let g = Graph::new();
        let (tx, rx) = mpsc::channel();
        let steps = Arc::new(AtomicU32::new(0));
        let s2 = steps.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Wakeable {
            waker_tx: tx,
            steps: s2,
            out: w.output(),
        });

        // `on_start` is the normal place to hand a waker to outside code.
        g.begin(Live::new().stop(Stop::After(Duration::from_millis(1))));
        let waker = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        g.end().unwrap();

        assert_eq!(waker.wake(), Err(WakeError::NotLive));
    }

    #[test]
    fn wakes_step_the_node_during_live_run() {
        let g = Graph::new();
        let (tx, rx) = mpsc::channel();
        let steps = Arc::new(AtomicU32::new(0));
        let s2 = steps.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Wakeable {
            waker_tx: tx,
            steps: s2,
            out: w.output(),
        });

        let handle = thread::spawn(move || {
            let waker = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            for _ in 0..5 {
                thread::sleep(Duration::from_millis(20));
                waker.wake().expect("wake during live run");
            }
        });

        g.run(Live::new().stop(Stop::After(Duration::from_millis(200))))
            .unwrap();
        handle.join().unwrap();

        assert_eq!(steps.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn rapid_wakes_batch_into_fewer_steps() {
        let g = Graph::new();
        let (tx, rx) = mpsc::channel();
        let steps = Arc::new(AtomicU32::new(0));
        let s2 = steps.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| Wakeable {
            waker_tx: tx,
            steps: s2,
            out: w.output(),
        });

        let handle = thread::spawn(move || {
            let waker = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            thread::sleep(Duration::from_millis(20));
            for _ in 0..10 {
                waker.wake().unwrap();
            }
            thread::sleep(Duration::from_millis(50));
            for _ in 0..10 {
                waker.wake().unwrap();
            }
        });

        g.run(Live::new().stop(Stop::After(Duration::from_millis(150))))
            .unwrap();
        handle.join().unwrap();

        let n = steps.load(Ordering::SeqCst);
        assert!(n >= 2, "at least one step per burst, got {n}");
        assert!(n < 10, "rapid wakes should batch, got {n}");
    }

    #[test]
    fn live_timers_fire_and_stop_after_duration() {
        struct Tick {
            out: Output<u32>,
            count: u32,
        }
        impl Operator for Tick {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
                cx.every(Duration::from_millis(20));
            }

            fn step(&mut self, _cx: &mut Ctx) {
                self.count += 1;
                self.out.set(self.count);
            }
        }
        let g = Graph::new();
        let n = g.add::<u32, _>(|w| Tick {
            out: w.output(),
            count: 0,
        });

        let start = std::time::Instant::now();
        g.run(Live::new().stop(Stop::After(Duration::from_millis(100))))
            .unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(90),
            "stopped early: {elapsed:?}"
        );
        assert!(
            elapsed <= Duration::from_millis(200),
            "stopped late: {elapsed:?}"
        );
        let ticks = n.peek().unwrap();
        assert!(
            (4..=7).contains(&ticks),
            "expected ~5-6 ticks at 20ms over 100ms, got {ticks}"
        );
    }

    #[test]
    fn live_steps_stop_is_exact() {
        struct Tick {
            out: Output<u32>,
            count: u32,
        }
        impl Operator for Tick {
            fn on_start(&mut self, cx: &mut Ctx) {
                cx.at(cx.now());
                cx.every(Duration::from_millis(5));
            }

            fn step(&mut self, _cx: &mut Ctx) {
                self.count += 1;
                self.out.set(self.count);
            }
        }
        for expected in [1u32, 3, 5] {
            let g = Graph::new();
            let n = g.add::<u32, _>(|w| Tick {
                out: w.output(),
                count: 0,
            });
            g.run(Live::new().stop(Stop::Steps(expected as u64)))
                .unwrap();
            assert_eq!(n.peek(), Some(expected));
        }
    }

    #[test]
    fn late_wake_after_stop_after_deadline_does_not_step() {
        struct LateWake {
            steps: Arc<AtomicU32>,
            out: Output<()>,
        }

        impl Operator for LateWake {
            fn on_start(&mut self, cx: &mut Ctx) {
                thread::sleep(Duration::from_millis(20));
                cx.waker().wake().unwrap();
            }

            fn step(&mut self, _cx: &mut Ctx) {
                self.steps.fetch_add(1, Ordering::SeqCst);
                self.out.set(());
            }
        }

        let g = Graph::new();
        let steps = Arc::new(AtomicU32::new(0));
        let s2 = steps.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| LateWake {
            steps: s2,
            out: w.output(),
        });

        g.run(Live::new().stop(Stop::After(Duration::from_millis(1))))
            .unwrap();

        assert_eq!(steps.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn waker_rejects_during_on_stop() {
        struct StopWake {
            result: Arc<std::sync::Mutex<Option<Result<(), WakeError>>>>,
            waker: Option<Waker>,
            out: Output<()>,
        }

        impl Operator for StopWake {
            fn on_start(&mut self, cx: &mut Ctx) {
                self.waker = Some(cx.waker());
            }

            fn on_stop(&mut self, _cx: &mut Ctx) {
                let result = self.waker.as_ref().unwrap().wake();
                *self.result.lock().unwrap() = Some(result);
            }

            fn step(&mut self, _cx: &mut Ctx) {
                self.out.set(());
            }
        }

        let g = Graph::new();
        let result = Arc::new(std::sync::Mutex::new(None));
        let r2 = result.clone();
        let _n: crate::core::graph::Stream<()> = g.add(|w| StopWake {
            result: r2,
            waker: None,
            out: w.output(),
        });

        g.run(Live::new().stop(Stop::After(Duration::from_millis(1))))
            .unwrap();

        assert_eq!(*result.lock().unwrap(), Some(Err(WakeError::NotLive)));
    }
}
