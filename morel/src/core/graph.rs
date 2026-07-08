use std::any::Any;
use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::convert::Infallible;
use std::rc::{Rc, Weak};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender};

use crate::core::engine::TimerEntry;
use crate::core::port::{FireFlag, Input, Output, SharedValue};
use crate::core::run::{Mode, StopRequest};
use crate::core::time::Time;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub(crate) usize);

/// A graph node.
///
/// Implement this trait for custom operators and register them with
/// [`Graph::add`]. A node propagates only when it writes to its [`Output`].
pub trait Operator: 'static {
    /// Run the node once.
    fn step(&mut self, cx: &mut Ctx);

    /// Called before the first step. Schedule initial timers here; writes made
    /// during startup are not propagated.
    fn on_start(&mut self, _cx: &mut Ctx) {}

    /// Called after the run has finished.
    fn on_stop(&mut self, _cx: &mut Ctx) {}
}

/// Engine context for the node currently running.
pub struct Ctx<'g> {
    pub(crate) core: &'g Rc<GraphCore>,
    pub(crate) node: NodeId,
}

pub(crate) struct NodeEntry {
    pub(crate) op: Rc<RefCell<dyn Operator>>,
    pub(crate) fired: FireFlag,
    pub(crate) downstream: Vec<NodeId>,
    pub(crate) finalize: bool,
}

pub(crate) struct GraphCore {
    pub(crate) nodes: RefCell<Vec<NodeEntry>>,
    pub(crate) pending: RefCell<Vec<u64>>,
    pub(crate) fired_scratch: RefCell<Vec<NodeId>>,
    pub(crate) timers: RefCell<BinaryHeap<Reverse<TimerEntry>>>,
    pub(crate) timer_seq: Cell<u64>,
    pub(crate) clock: Cell<Time>,
    pub(crate) started_at: Cell<Time>,
    pub(crate) is_final: Cell<bool>,
    pub(crate) running: Cell<bool>,
    pub(crate) mode: Cell<Mode>,
    pub(crate) end_at: Cell<Time>,
    pub(crate) end_steps: Cell<u64>,
    pub(crate) stop_on_idle: Cell<bool>,
    pub(crate) steps: Cell<u64>,
    pub(crate) stopping: RefCell<Option<StopRequest>>,
    pub(crate) wake_tx: Sender<NodeId>,
    pub(crate) wake_rx: Receiver<NodeId>,
    pub(crate) live: Arc<AtomicBool>,
    #[cfg(feature = "async-io")]
    // TODO: decide whether worker/source_worker child graphs should be able to
    // inherit this graph-local runtime explicitly, instead of always starting
    // with a fresh runtime cell.
    pub(crate) async_runtime: std::cell::OnceCell<std::sync::Arc<tokio::runtime::Runtime>>,
}

impl GraphCore {
    pub(crate) fn new() -> Rc<Self> {
        let (wake_tx, wake_rx) = crossbeam::channel::unbounded();
        Rc::new(Self {
            nodes: RefCell::new(Vec::new()),
            pending: RefCell::new(Vec::new()),
            fired_scratch: RefCell::new(Vec::new()),
            timers: RefCell::new(BinaryHeap::new()),
            timer_seq: Cell::new(0),
            clock: Cell::new(Time::EPOCH),
            started_at: Cell::new(Time::EPOCH),
            is_final: Cell::new(false),
            running: Cell::new(false),
            mode: Cell::new(Mode::Replay),
            end_at: Cell::new(Time::MAX),
            end_steps: Cell::new(u64::MAX),
            stop_on_idle: Cell::new(true),
            steps: Cell::new(0),
            stopping: RefCell::new(None),
            wake_tx,
            wake_rx,
            live: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "async-io")]
            async_runtime: std::cell::OnceCell::new(),
        })
    }
}

struct ErasedOutput {
    value: Box<dyn Any>,
    fired: FireFlag,
}

/// Declares a node's inputs, output, and shutdown behavior during construction.
pub struct Wire<'g> {
    core: &'g Rc<GraphCore>,
    triggers: Vec<NodeId>,
    finalize: bool,
    output: Option<ErasedOutput>,
}

impl Wire<'_> {
    /// Read `upstream` and step this node whenever `upstream` fires.
    pub fn on<T>(&mut self, upstream: &Stream<T>) -> Input<T> {
        self.check_same_graph(upstream);
        self.triggers.push(upstream.id);
        Input {
            value: upstream.value.clone(),
            fired: upstream.fired.clone(),
        }
    }

    /// Read `upstream` without making it a trigger for this node.
    pub fn watch<T>(&mut self, upstream: &Stream<T>) -> Input<T> {
        self.check_same_graph(upstream);
        Input {
            value: upstream.value.clone(),
            fired: upstream.fired.clone(),
        }
    }

    /// Create the node's output port.
    ///
    /// `T` must match the `Stream<T>` returned by [`Graph::add`].
    pub fn output<T: 'static>(&mut self) -> Output<T> {
        assert!(self.output.is_none(), "Wire::output called more than once");
        let value: SharedValue<T> = Rc::new(RefCell::new(None));
        let fired: FireFlag = Rc::new(Cell::new(false));
        self.output = Some(ErasedOutput {
            value: Box::new(value.clone()),
            fired: fired.clone(),
        });
        Output { value, fired }
    }

    /// Run this node during shutdown so it can flush buffered state.
    pub fn finalize(&mut self) {
        self.finalize = true;
    }

    fn check_same_graph<T>(&self, upstream: &Stream<T>) {
        assert!(
            Weak::as_ptr(&upstream.graph) == Rc::as_ptr(self.core),
            "streams belong to different graphs"
        );
    }
}

/// Typed handle to a node's latest output.
pub struct Stream<T> {
    pub(crate) graph: Weak<GraphCore>,
    pub(crate) id: NodeId,
    pub(crate) value: SharedValue<T>,
    pub(crate) fired: FireFlag,
}

impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            id: self.id,
            value: self.value.clone(),
            fired: self.fired.clone(),
        }
    }
}

impl<T> Stream<T> {
    pub(crate) fn core(&self) -> Rc<GraphCore> {
        self.graph.upgrade().expect("graph was dropped")
    }

    /// Register a new operator in this stream's graph.
    pub fn wire<U, Op>(&self, build: impl FnOnce(&mut Wire) -> Op) -> Stream<U>
    where
        U: 'static,
        Op: Operator,
    {
        add_node(&self.core(), build)
    }

    /// Fallibly register a new operator in this stream's graph.
    ///
    /// If the builder returns an error, no node or downstream edges are added.
    pub fn try_wire<U, Op, E>(
        &self,
        build: impl FnOnce(&mut Wire) -> Result<Op, E>,
    ) -> Result<Stream<U>, E>
    where
        U: 'static,
        Op: Operator,
    {
        add_node_result(&self.core(), build)
    }
}

impl<T: Clone> Stream<T> {
    /// Return the latest output value, if the stream has produced one.
    pub fn peek(&self) -> Option<T> {
        self.value.borrow().clone()
    }
}

/// Owns a stream graph.
pub struct Graph {
    pub(crate) core: Rc<GraphCore>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            core: GraphCore::new(),
        }
    }

    /// Register a custom operator.
    pub fn add<T, Op>(&self, build: impl FnOnce(&mut Wire) -> Op) -> Stream<T>
    where
        T: 'static,
        Op: Operator,
    {
        add_node(&self.core, build)
    }

    /// Fallibly register a custom operator.
    ///
    /// If the builder returns an error, no node or downstream edges are added.
    pub fn try_add<T, Op, E>(
        &self,
        build: impl FnOnce(&mut Wire) -> Result<Op, E>,
    ) -> Result<Stream<T>, E>
    where
        T: 'static,
        Op: Operator,
    {
        add_node_result(&self.core, build)
    }

    pub fn len(&self) -> usize {
        self.core.nodes.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn add_node<T, Op>(
    core: &Rc<GraphCore>,
    build: impl FnOnce(&mut Wire) -> Op,
) -> Stream<T>
where
    T: 'static,
    Op: Operator,
{
    match add_node_result(core, |wire| Ok::<Op, Infallible>(build(wire))) {
        Ok(stream) => stream,
        Err(err) => match err {},
    }
}

pub(crate) fn add_node_result<T, Op, E>(
    core: &Rc<GraphCore>,
    build: impl FnOnce(&mut Wire) -> Result<Op, E>,
) -> Result<Stream<T>, E>
where
    T: 'static,
    Op: Operator,
{
    assert!(
        !core.running.get(),
        "cannot add nodes while the graph is running"
    );
    let mut wire = Wire {
        core,
        triggers: Vec::new(),
        finalize: false,
        output: None,
    };
    let op = build(&mut wire)?;
    let out = wire
        .output
        .expect("wiring must create the node's output port");
    let value = *out
        .value
        .downcast::<SharedValue<T>>()
        .expect("Wire::output type does not match the Stream type returned by add");
    let fired = out.fired;
    let id = NodeId(core.nodes.borrow().len());
    {
        let mut nodes = core.nodes.borrow_mut();
        for &trigger in &wire.triggers {
            nodes[trigger.0].downstream.push(id);
        }
        nodes.push(NodeEntry {
            op: Rc::new(RefCell::new(op)),
            fired: fired.clone(),
            downstream: Vec::new(),
            finalize: wire.finalize,
        });
    }
    core.pending
        .borrow_mut()
        .resize((id.0 / u64::BITS as usize) + 1, 0);
    {
        let node_count = id.0 + 1;
        let mut fired_scratch = core.fired_scratch.borrow_mut();
        if fired_scratch.capacity() < node_count {
            fired_scratch.reserve_exact(node_count);
        }
    }
    Ok(Stream {
        graph: Rc::downgrade(core),
        id,
        value,
        fired,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::port::{Input, Output};

    struct Noop {
        out: Output<i32>,
    }

    impl Operator for Noop {
        fn step(&mut self, _cx: &mut Ctx) {
            self.out.set(0);
        }
    }

    struct Follows {
        input: Input<i32>,
        out: Output<i32>,
    }

    impl Operator for Follows {
        fn step(&mut self, _cx: &mut Ctx) {
            self.out.set(self.input.get());
        }
    }

    #[test]
    fn ids_are_sequential_insertion_order() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });
        let b = g.add::<i32, _>(|w| Noop { out: w.output() });
        assert_eq!(a.id, NodeId(0));
        assert_eq!(b.id, NodeId(1));
        assert_eq!(g.len(), 2);
    }

    #[test]
    fn on_records_trigger_edge_watch_does_not() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });
        let _b = g.add::<i32, _>(|w| Follows {
            input: w.on(&a),
            out: w.output(),
        });
        let _c = g.add::<i32, _>(|w| Follows {
            input: w.watch(&a),
            out: w.output(),
        });
        let nodes = g.core.nodes.borrow();
        assert_eq!(nodes[0].downstream, vec![NodeId(1)]);
        assert!(nodes[1].downstream.is_empty());
    }

    #[test]
    #[should_panic(expected = "output called more than once")]
    fn double_output_panics() {
        let g = Graph::new();
        g.add::<i32, _>(|w| {
            let _first: Output<i32> = w.output();
            Noop { out: w.output() }
        });
    }

    #[test]
    #[should_panic(expected = "wiring must create the node's output port")]
    fn missing_output_panics() {
        struct NoOutput;
        impl Operator for NoOutput {
            fn step(&mut self, _cx: &mut Ctx) {}
        }
        let g = Graph::new();
        g.add::<i32, _>(|_w| NoOutput);
    }

    #[test]
    #[should_panic(expected = "output type does not match")]
    fn output_type_mismatch_panics() {
        let g = Graph::new();
        // This catches mismatches between the declared stream type and the
        // output slot created by the builder.
        g.add::<String, _>(|w| Noop { out: w.output() });
    }

    #[test]
    #[should_panic(expected = "streams belong to different graphs")]
    fn cross_graph_wiring_panics() {
        let g1 = Graph::new();
        let g2 = Graph::new();
        let a = g1.add::<i32, _>(|w| Noop { out: w.output() });
        g2.add::<i32, _>(|w| Follows {
            input: w.on(&a),
            out: w.output(),
        });
    }

    #[test]
    #[should_panic(expected = "graph was dropped")]
    fn handle_outliving_graph_panics_on_use() {
        let a = {
            let g = Graph::new();
            g.add::<i32, _>(|w| Noop { out: w.output() })
        };
        a.core();
    }

    #[test]
    fn peek_is_none_before_any_fire() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });
        assert_eq!(a.peek(), None);
    }

    #[test]
    fn finalize_is_recorded() {
        let g = Graph::new();
        let _a = g.add::<i32, _>(|w| {
            w.finalize();
            Noop { out: w.output() }
        });
        assert!(g.core.nodes.borrow()[0].finalize);
    }

    #[test]
    #[should_panic(expected = "cannot add nodes while the graph is running")]
    fn adding_while_running_panics() {
        let g = Graph::new();
        g.core.running.set(true);
        g.add::<i32, _>(|w| Noop { out: w.output() });
    }

    #[test]
    fn wire_enables_graphless_component_functions() {
        // Component builders should not need access to the owning `Graph`.
        fn follow(input: &Stream<i32>) -> Stream<i32> {
            input.wire(|w| Follows {
                input: w.on(input),
                out: w.output(),
            })
        }
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });
        let b = follow(&a);
        assert_eq!(b.id, NodeId(1));
        assert_eq!(g.core.nodes.borrow()[0].downstream, vec![NodeId(1)]);
    }

    #[test]
    fn try_add_error_does_not_append_node_or_edges() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });

        let err = match g.try_add::<i32, _, _>(|w| {
            let _input = w.on(&a);
            Err::<Noop, _>("wire failed")
        }) {
            Ok(_) => panic!("builder should fail"),
            Err(err) => err,
        };

        assert_eq!(err, "wire failed");
        assert_eq!(g.len(), 1);
        assert!(g.core.nodes.borrow()[0].downstream.is_empty());
    }

    #[test]
    fn try_wire_error_does_not_append_node_or_edges() {
        let g = Graph::new();
        let a = g.add::<i32, _>(|w| Noop { out: w.output() });

        let err = match a.try_wire::<i32, _, _>(|w| {
            let _input = w.on(&a);
            Err::<Noop, _>("stream wire failed")
        }) {
            Ok(_) => panic!("builder should fail"),
            Err(err) => err,
        };

        assert_eq!(err, "stream wire failed");
        assert_eq!(g.len(), 1);
        assert!(g.core.nodes.borrow()[0].downstream.is_empty());
    }
}
