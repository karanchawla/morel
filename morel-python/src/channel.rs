use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle, ThreadId};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyIterator};

use crate::error::callback_error;
use crate::graph::{duration_from_seconds, py_log_items, PyGraph, PySummary};
use crate::ops::{batch, combine, stateful, stateless};
use crate::recording::PyRecording;
use crate::stream::{PyStream, PyStreamOwner};
use crate::value::{
    positive_duration_nanos, py_history_value, py_list_value, py_none_value, PyValue,
};

static NEXT_CHILD_ID: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    static CHILD_GRAPHS: RefCell<HashMap<usize, usize>> = RefCell::new(HashMap::new());
    static CHILD_STREAMS: RefCell<HashMap<usize, morel::Stream<PyValue>>> = RefCell::new(HashMap::new());
}

#[pyclass(name = "Capacity")]
#[derive(Clone, Copy)]
pub(crate) struct PyCapacity {
    capacity: morel::Capacity,
}

#[pymethods]
impl PyCapacity {
    #[staticmethod]
    fn unbounded() -> Self {
        Self {
            capacity: morel::Capacity::Unbounded,
        }
    }

    #[staticmethod]
    fn bounded(n: usize) -> PyResult<Self> {
        if n == 0 {
            return Err(PyValueError::new_err("capacity must be greater than 0"));
        }
        Ok(Self {
            capacity: morel::Capacity::Bounded(n),
        })
    }
}

#[pyclass(name = "OnClose")]
#[derive(Clone, Copy)]
pub(crate) struct PyOnClose {
    on_close: morel::OnClose,
}

#[pymethods]
impl PyOnClose {
    #[staticmethod]
    fn stop() -> Self {
        Self {
            on_close: morel::OnClose::Stop,
        }
    }

    #[staticmethod]
    fn continue_() -> Self {
        Self {
            on_close: morel::OnClose::Continue,
        }
    }
}

#[pyclass(unsendable, name = "ChannelTx")]
pub(crate) struct PyChannelTx {
    tx: RefCell<Option<morel::ChannelTx<PyValue>>>,
}

#[pymethods]
impl PyChannelTx {
    fn attach(&self, py: Python<'_>, source: PyRef<'_, PyStream>) -> PyResult<PyStream> {
        source.ensure_can_add_nodes(py)?;
        let tx = self.take_tx()?;
        let stream = tx
            .attach(&source.stream)
            .map(|()| Python::attach(py_none_value));
        Ok(PyStream::wrap(stream, source.owner.clone_ref(py)))
    }

    fn attach_with_heartbeat(
        &self,
        py: Python<'_>,
        source: PyRef<'_, PyStream>,
        heartbeat: PyRef<'_, PyStream>,
    ) -> PyResult<PyStream> {
        source.ensure_can_add_nodes(py)?;
        source.ensure_same_owner(&heartbeat)?;
        let tx = self.take_tx()?;
        let stream = tx
            .attach_with_heartbeat(&source.stream, &heartbeat.stream)
            .map(|()| Python::attach(py_none_value));
        Ok(PyStream::wrap(stream, source.owner.clone_ref(py)))
    }
}

impl PyChannelTx {
    fn take_tx(&self) -> PyResult<morel::ChannelTx<PyValue>> {
        self.tx
            .borrow_mut()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("channel transmitter already attached"))
    }
}

#[pyclass(unsendable, name = "ChannelRx")]
pub(crate) struct PyChannelRx {
    rx: RefCell<Option<morel::ChannelRx<PyValue>>>,
}

#[pymethods]
impl PyChannelRx {
    #[allow(clippy::wrong_self_convention)]
    fn into_stream(
        &self,
        py: Python<'_>,
        graph: PyRef<'_, PyGraph>,
        on_close: PyRef<'_, PyOnClose>,
    ) -> PyResult<PyStream> {
        graph.ensure_can_add_nodes()?;
        let owner = Py::from(graph);
        let rx = self.take_rx()?;
        let stream = rx
            .into_stream(owner.borrow(py).graph(), on_close.on_close)
            .try_map(py_batch_value);
        Ok(PyStream::wrap(stream, owner))
    }

    #[allow(clippy::wrong_self_convention)]
    fn into_stream_paced(
        &self,
        py: Python<'_>,
        pace: PyRef<'_, PyStream>,
        on_close: PyRef<'_, PyOnClose>,
    ) -> PyResult<PyStream> {
        pace.ensure_can_add_nodes(py)?;
        let rx = self.take_rx()?;
        let stream = rx
            .into_stream_paced(&pace.stream, on_close.on_close)
            .try_map(py_batch_value);
        Ok(PyStream::wrap(stream, pace.owner.clone_ref(py)))
    }
}

impl PyChannelRx {
    fn take_rx(&self) -> PyResult<morel::ChannelRx<PyValue>> {
        self.rx
            .borrow_mut()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("channel receiver already materialized"))
    }
}

#[pyfunction]
pub(crate) fn channel(capacity: PyRef<'_, PyCapacity>) -> (PyChannelTx, PyChannelRx) {
    let (tx, rx) = morel::channel(capacity.capacity);
    (
        PyChannelTx {
            tx: RefCell::new(Some(tx)),
        },
        PyChannelRx {
            rx: RefCell::new(Some(rx)),
        },
    )
}

#[pyclass(name = "Producer")]
pub(crate) struct PyProducer {
    tx: mpsc::Sender<ProducerMessage>,
    waker: morel::Waker,
    closed: Arc<AtomicBool>,
}

#[pymethods]
impl PyProducer {
    fn send(&self, value: Py<PyAny>) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("producer channel is closed"));
        }
        self.tx
            .send(ProducerMessage::Value(PyValue::new(value)))
            .map_err(|_| PyRuntimeError::new_err("producer channel is closed"))?;
        let _ = self.waker.wake();
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

enum ProducerMessage {
    Value(PyValue),
    Error(String),
    Closed,
}

struct PyProducerOp {
    produce: Option<Py<PyAny>>,
    rx: mpsc::Receiver<ProducerMessage>,
    tx: mpsc::Sender<ProducerMessage>,
    out: morel::Output<Vec<PyValue>>,
    closed: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    scratch: Vec<PyValue>,
}

impl morel::Operator for PyProducerOp {
    fn on_start(&mut self, cx: &mut morel::Ctx) {
        if !cx.is_live() {
            cx.fail(io::Error::other("external producer is live-only"));
            return;
        }
        cx.at(cx.now());
        let Some(produce) = self.produce.take() else {
            return;
        };
        let producer = PyProducer {
            tx: self.tx.clone(),
            waker: cx.waker(),
            closed: self.closed.clone(),
        };
        let completion_tx = self.tx.clone();
        let completion_waker = cx.waker();
        self.join = Some(thread::spawn(move || {
            let result = Python::attach(|py| -> PyResult<()> {
                let py_producer = Py::new(py, producer)?;
                produce.bind(py).call1((py_producer.bind(py),))?;
                Ok(())
            });
            if let Err(err) = result {
                let _ = completion_tx.send(ProducerMessage::Error(err.to_string()));
                let _ = completion_waker.wake();
            }
            let _ = completion_tx.send(ProducerMessage::Closed);
            let _ = completion_waker.wake();
        }));
    }

    fn step(&mut self, cx: &mut morel::Ctx) {
        loop {
            match self.rx.try_recv() {
                Ok(ProducerMessage::Value(value)) => self.scratch.push(value),
                Ok(ProducerMessage::Error(message)) => {
                    cx.fail(io::Error::other(message));
                    return;
                }
                Ok(ProducerMessage::Closed) => {
                    self.closed.store(true, Ordering::Release);
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.closed.store(true, Ordering::Release);
                    break;
                }
            }
        }

        if !self.scratch.is_empty() {
            let scratch = &mut self.scratch;
            self.out.update(Vec::new, |burst| {
                burst.clear();
                burst.append(scratch);
            });
        }
    }

    fn on_stop(&mut self, _cx: &mut morel::Ctx) {
        self.closed.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[pyfunction]
pub(crate) fn producer(
    py: Python<'_>,
    graph: PyRef<'_, PyGraph>,
    produce: Py<PyAny>,
) -> PyResult<PyStream> {
    graph.ensure_can_add_nodes()?;
    graph.mark_requires_detached_run();
    let owner = Py::from(graph);
    let (tx, rx) = mpsc::channel();
    let closed = Arc::new(AtomicBool::new(false));
    let stream = owner.borrow(py).graph().add(|w| PyProducerOp {
        produce: Some(produce),
        rx,
        tx,
        out: w.output(),
        closed,
        join: None,
        scratch: Vec::new(),
    });
    let stream = stream.try_map(py_batch_value);
    Ok(PyStream::wrap(stream, owner))
}

#[derive(Clone)]
struct ChildState {
    graph_id: usize,
    active: Arc<AtomicBool>,
    thread_id: ThreadId,
    stream_ids: Arc<Mutex<Vec<usize>>>,
}

impl ChildState {
    fn new(graph: &morel::Graph) -> Self {
        let graph_id = NEXT_CHILD_ID.fetch_add(1, Ordering::Relaxed);
        CHILD_GRAPHS.with(|graphs| {
            graphs
                .borrow_mut()
                .insert(graph_id, graph as *const morel::Graph as usize);
        });
        Self {
            graph_id,
            active: Arc::new(AtomicBool::new(true)),
            thread_id: thread::current().id(),
            stream_ids: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        CHILD_GRAPHS.with(|graphs| {
            graphs.borrow_mut().remove(&self.graph_id);
        });
        let stream_ids = self
            .stream_ids
            .lock()
            .expect("child stream id mutex poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        CHILD_STREAMS.with(|streams| {
            let mut streams = streams.borrow_mut();
            for stream_id in stream_ids {
                streams.remove(&stream_id);
            }
        });
    }

    fn ensure_active_on_owner_thread(&self) -> PyResult<()> {
        if !self.active.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("ChildGraph is no longer active"));
        }
        if thread::current().id() != self.thread_id {
            return Err(PyRuntimeError::new_err(
                "ChildGraph may only be used during its build callback",
            ));
        }
        Ok(())
    }

    fn with_graph<T>(&self, f: impl FnOnce(&morel::Graph) -> T) -> PyResult<T> {
        self.ensure_active_on_owner_thread()?;
        CHILD_GRAPHS.with(|graphs| {
            let graph_ptr = graphs
                .borrow()
                .get(&self.graph_id)
                .copied()
                .ok_or_else(|| PyRuntimeError::new_err("ChildGraph is no longer active"))?;
            // SAFETY: child graph pointers are registered only in the child
            // build thread while the core build callback is active. Access is
            // limited to that same thread and removed before the callback
            // returns.
            let graph = unsafe { &*(graph_ptr as *const morel::Graph) };
            Ok(f(graph))
        })
    }

    fn register_stream(&self, stream: morel::Stream<PyValue>) -> PyChildStream {
        let stream_id = NEXT_CHILD_ID.fetch_add(1, Ordering::Relaxed);
        CHILD_STREAMS.with(|streams| {
            streams.borrow_mut().insert(stream_id, stream);
        });
        self.stream_ids
            .lock()
            .expect("child stream id mutex poisoned")
            .push(stream_id);
        PyChildStream {
            stream_id,
            state: self.clone(),
        }
    }

    fn stream(&self, stream_id: usize) -> PyResult<morel::Stream<PyValue>> {
        self.ensure_active_on_owner_thread()?;
        CHILD_STREAMS.with(|streams| {
            streams
                .borrow()
                .get(&stream_id)
                .cloned()
                .ok_or_else(|| PyRuntimeError::new_err("ChildGraph is no longer active"))
        })
    }
}

struct ChildStateGuard {
    state: ChildState,
}

impl ChildStateGuard {
    fn new(graph: &morel::Graph) -> Self {
        Self {
            state: ChildState::new(graph),
        }
    }

    fn state(&self) -> &ChildState {
        &self.state
    }
}

impl Drop for ChildStateGuard {
    fn drop(&mut self) {
        self.state.deactivate();
    }
}

#[pyclass(name = "ChildGraph")]
pub(crate) struct PyChildGraph {
    state: ChildState,
}

#[pymethods]
impl PyChildGraph {
    fn len(&self) -> PyResult<usize> {
        self.state.with_graph(|graph| graph.len())
    }

    fn is_empty(&self) -> PyResult<bool> {
        self.state.with_graph(|graph| graph.is_empty())
    }

    fn add(&self, py: Python<'_>, operator: Py<PyAny>) -> PyResult<PyChildStream> {
        self.state.ensure_active_on_owner_thread()?;
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        let stream_owner = PyStreamOwner::from(owner.clone_ref(py));
        let stream = self.state.with_graph(|graph| {
            graph.try_add(|wire| {
                crate::custom::build_custom_operator(
                    py,
                    wire,
                    stream_owner.clone_ref(py),
                    operator.clone_ref(py),
                )
            })
        })??;
        Ok(self.state.register_stream(stream))
    }

    fn just(&self, value: Py<PyAny>) -> PyResult<PyChildStream> {
        let stream = self
            .state
            .with_graph(|graph| graph.just(PyValue::new(value)))?;
        Ok(self.state.register_stream(stream))
    }

    #[pyo3(signature = (period_seconds=None, period_nanos=None))]
    fn ticker(
        &self,
        period_seconds: Option<f64>,
        period_nanos: Option<u64>,
    ) -> PyResult<PyChildStream> {
        self.state.ensure_active_on_owner_thread()?;
        let period = match (period_seconds, period_nanos) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "provide exactly one of period_seconds or period_nanos",
                ));
            }
            (None, None) => {
                return Err(PyValueError::new_err(
                    "provide exactly one of period_seconds or period_nanos",
                ));
            }
            (Some(seconds), None) => duration_from_seconds(seconds, "period_seconds")?,
            (None, Some(nanos)) => positive_duration_nanos(nanos, "period_nanos")?,
        };
        let stream = self.state.with_graph(|graph| {
            graph.add(|w| crate::graph::PyTicker {
                period,
                out: w.output(),
            })
        })?;
        Ok(self.state.register_stream(stream))
    }

    fn replay_from_iter(&self, items: &Bound<'_, PyAny>) -> PyResult<PyChildStream> {
        self.state.ensure_active_on_owner_thread()?;
        let mut converted = Vec::new();
        for item in PyIterator::from_object(items)? {
            let (nanos, value): (u64, Py<PyAny>) = item?.extract()?;
            converted.push((morel::Time::from_nanos(nanos), PyValue::new(value)));
        }
        let stream = self
            .state
            .with_graph(|graph| graph.replay_from_iter(converted))?;
        Ok(self.state.register_stream(stream))
    }

    fn replay_from_log(&self, log: &Bound<'_, PyAny>) -> PyResult<PyChildStream> {
        self.state.ensure_active_on_owner_thread()?;
        let converted = py_log_items(log)?;
        let stream = self
            .state
            .with_graph(|graph| graph.replay_from_log(converted))?;
        Ok(self.state.register_stream(stream))
    }

    fn replay_from_csv(
        &self,
        py: Python<'_>,
        path: PathBuf,
        parse: Py<PyAny>,
    ) -> PyResult<PyChildStream> {
        self.state.ensure_active_on_owner_thread()?;
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        let stream = self.state.with_graph(|graph| {
            crate::csv::replay_from_csv_on_graph(graph, owner.clone_ref(py), path, parse)
        })??;
        Ok(self.state.register_stream(stream.stream))
    }
}

impl PyChildGraph {
    fn new(state: ChildState) -> Self {
        Self { state }
    }

    pub(crate) fn graph_id(&self) -> usize {
        self.state.graph_id
    }

    pub(crate) fn ensure_can_add_nodes(&self) -> PyResult<()> {
        self.state.ensure_active_on_owner_thread()
    }
}

#[pyclass(name = "ChildStream")]
pub(crate) struct PyChildStream {
    stream_id: usize,
    state: ChildState,
}

#[pymethods]
impl PyChildStream {
    fn peek(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.stream()?.peek() {
            Some(value) => Ok(value.bind(py).clone().unbind()),
            None => Ok(py.None()),
        }
    }

    fn run(&self, _py: Python<'_>, _spec: &Bound<'_, PyAny>) -> PyResult<PySummary> {
        Err(PyRuntimeError::new_err(
            "child streams cannot run their graph directly",
        ))
    }

    fn wire(&self, py: Python<'_>, operator: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| {
            crate::custom::stream_wire(stream, py, operator)
        })
    }

    fn history(&self) -> PyResult<PyChildStream> {
        let stream = self.state.stream(self.stream_id)?;
        let history = stream.history().try_map(|history| {
            Python::attach(|py| py_history_value(py, history).map_err(callback_error))
        });
        Ok(self.state.register_stream(history))
    }

    fn record(&self, recording: PyRef<'_, PyRecording>) -> PyResult<PyChildStream> {
        let stream = self.state.stream(self.stream_id)?;
        let recorded = stream
            .record(&recording.recording)
            .map(|()| Python::attach(py_none_value));
        Ok(self.state.register_stream(recorded))
    }

    fn count(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::count(stream, py))
    }

    fn accumulate(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::accumulate(stream, py))
    }

    fn timestamp(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::timestamp(stream, py))
    }

    fn scan(&self, py: Python<'_>, init: Py<PyAny>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::scan(stream, py, init, func))
    }

    fn reduce(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::reduce(stream, py, func))
    }

    fn sum(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::sum(stream, py))
    }

    fn delta(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::delta(stream, py))
    }

    fn mean(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateful::mean(stream, py))
    }

    #[pyo3(signature = (*, delay_nanos))]
    fn delay(&self, py: Python<'_>, delay_nanos: u64) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::delay(stream, py, delay_nanos))
    }

    #[pyo3(signature = (*, interval_nanos))]
    fn throttle(&self, py: Python<'_>, interval_nanos: u64) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::throttle(stream, py, interval_nanos))
    }

    #[pyo3(signature = (*, quiet_nanos))]
    fn debounce(&self, py: Python<'_>, quiet_nanos: u64) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::debounce(stream, py, quiet_nanos))
    }

    fn buffer(&self, py: Python<'_>, capacity: usize) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::buffer(stream, py, capacity))
    }

    #[pyo3(signature = (*, size_nanos))]
    fn window_tumbling(&self, py: Python<'_>, size_nanos: u64) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::window_tumbling(stream, py, size_nanos))
    }

    #[pyo3(signature = (*, size_nanos, slide_nanos))]
    fn window_sliding(
        &self,
        py: Python<'_>,
        size_nanos: u64,
        slide_nanos: u64,
    ) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| {
            batch::window_sliding(stream, py, size_nanos, slide_nanos)
        })
    }

    fn collapse(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::collapse(stream, py))
    }

    fn map_batch(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| batch::map_batch(stream, py, func))
    }

    fn with_(
        &self,
        py: Python<'_>,
        other: PyRef<'_, PyChildStream>,
        func: Py<PyAny>,
    ) -> PyResult<PyChildStream> {
        let (left, right) = self.stream_pair(py, &other)?;
        self.register_result(combine::with_(&left, py, &right, func)?)
    }

    fn with_latest(
        &self,
        py: Python<'_>,
        other: PyRef<'_, PyChildStream>,
        func: Py<PyAny>,
    ) -> PyResult<PyChildStream> {
        let (left, right) = self.stream_pair(py, &other)?;
        self.register_result(combine::with_latest(&left, py, &right, func)?)
    }

    fn gate(&self, py: Python<'_>, open: PyRef<'_, PyChildStream>) -> PyResult<PyChildStream> {
        let (input, open) = self.stream_pair(py, &open)?;
        self.register_result(combine::gate(&input, py, &open)?)
    }

    fn sample(&self, py: Python<'_>, trigger: PyRef<'_, PyChildStream>) -> PyResult<PyChildStream> {
        let (input, trigger) = self.stream_pair(py, &trigger)?;
        self.register_result(combine::sample(&input, py, &trigger)?)
    }

    fn unzip(&self, py: Python<'_>) -> PyResult<(PyChildStream, PyChildStream)> {
        self.with_stream_multi(py, |stream| {
            let (left, right) = combine::unzip(stream, py)?;
            Ok((left.stream, right.stream))
        })
    }

    fn map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::map(stream, py, func))
    }

    fn try_map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::map(stream, py, func))
    }

    fn filter(&self, py: Python<'_>, pred: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::filter(stream, py, pred))
    }

    fn filter_map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::filter_map(stream, py, func))
    }

    fn distinct(&self, py: Python<'_>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::distinct(stream, py))
    }

    fn take(&self, py: Python<'_>, n: u64) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::take(stream, py, n))
    }

    fn inspect(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::inspect(stream, py, func))
    }

    fn sink(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::sink(stream, py, func))
    }

    fn try_sink(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| stateless::sink(stream, py, func))
    }

    #[cfg(feature = "async-io")]
    fn consume_async(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<PyChildStream> {
        self.with_stream(py, |stream| {
            crate::async_io::consume_async(stream, py, callback)
        })
    }
}

impl PyChildStream {
    pub(crate) fn stream(&self) -> PyResult<morel::Stream<PyValue>> {
        self.state.stream(self.stream_id)
    }

    pub(crate) fn graph_id(&self) -> usize {
        self.state.graph_id
    }

    pub(crate) fn to_py_stream(&self, py: Python<'_>) -> PyResult<PyStream> {
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        Ok(PyStream::wrap(self.stream()?, owner))
    }

    fn with_stream(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&PyStream) -> PyResult<PyStream>,
    ) -> PyResult<PyChildStream> {
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        let stream = PyStream::wrap(self.stream()?, owner);
        self.register_result(f(&stream)?)
    }

    fn with_stream_multi(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&PyStream) -> PyResult<(morel::Stream<PyValue>, morel::Stream<PyValue>)>,
    ) -> PyResult<(PyChildStream, PyChildStream)> {
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        let stream = PyStream::wrap(self.stream()?, owner);
        let (left, right) = f(&stream)?;
        Ok((
            self.state.register_stream(left),
            self.state.register_stream(right),
        ))
    }

    fn stream_pair(&self, py: Python<'_>, other: &PyChildStream) -> PyResult<(PyStream, PyStream)> {
        self.ensure_same_child_graph(other)?;
        let owner = Py::new(py, PyChildGraph::new(self.state.clone()))?;
        Ok((
            PyStream::wrap(self.stream()?, owner.clone_ref(py)),
            PyStream::wrap(other.stream()?, owner),
        ))
    }

    fn ensure_same_child_graph(&self, other: &PyChildStream) -> PyResult<()> {
        self.state.ensure_active_on_owner_thread()?;
        other.state.ensure_active_on_owner_thread()?;
        if self.state.graph_id == other.state.graph_id {
            Ok(())
        } else {
            Err(PyValueError::new_err(
                "streams must belong to the same graph",
            ))
        }
    }

    fn register_result(&self, stream: PyStream) -> PyResult<PyChildStream> {
        Ok(self.state.register_stream(stream.stream))
    }
}

#[pyfunction]
pub(crate) fn worker(
    py: Python<'_>,
    input: PyRef<'_, PyStream>,
    build: Py<PyAny>,
) -> PyResult<PyStream> {
    input.ensure_can_add_nodes(py)?;
    input.owner.mark_requires_detached_run(py);
    let owner = input.owner.clone_ref(py);
    let stream = morel::worker(
        &input.stream,
        move |child, child_input| match build_worker_stream(child, child_input, &build) {
            Ok(stream) => stream,
            Err(message) => callback_failure_stream(child, message),
        },
    )
    .try_map(py_batch_value);
    Ok(PyStream::wrap(stream, owner))
}

#[pyfunction]
pub(crate) fn source_worker(
    py: Python<'_>,
    graph: PyRef<'_, PyGraph>,
    build: Py<PyAny>,
) -> PyResult<PyStream> {
    graph.ensure_can_add_nodes()?;
    graph.mark_requires_detached_run();
    let owner = Py::from(graph);
    let stream =
        morel::source_worker(
            owner.borrow(py).graph(),
            move |child| match build_source_worker_stream(child, &build) {
                Ok(stream) => stream,
                Err(message) => callback_failure_stream(child, message),
            },
        )
        .try_map(py_batch_value);
    Ok(PyStream::wrap(stream, owner))
}

fn build_worker_stream(
    child: &morel::Graph,
    child_input: morel::Stream<Vec<PyValue>>,
    build: &Py<PyAny>,
) -> Result<morel::Stream<PyValue>, Box<dyn Error + Send + Sync>> {
    let guard = ChildStateGuard::new(child);
    let state = guard.state().clone();
    Python::attach(|py| {
        let py_child = Py::new(py, PyChildGraph::new(state.clone()))?;
        let result = (|| {
            let child_input = child_input.try_map(py_batch_value);
            let py_child_input = Py::new(py, state.register_stream(child_input))?;
            let returned = build
                .bind(py)
                .call1((py_child.bind(py), py_child_input.bind(py)))?;
            extract_child_stream(&returned)
        })();
        result
    })
    .map_err(callback_error)
}

fn build_source_worker_stream(
    child: &morel::Graph,
    build: &Py<PyAny>,
) -> Result<morel::Stream<PyValue>, Box<dyn Error + Send + Sync>> {
    let guard = ChildStateGuard::new(child);
    let state = guard.state().clone();
    Python::attach(|py| {
        let py_child = Py::new(py, PyChildGraph::new(state.clone()))?;
        let result = (|| {
            let returned = build.bind(py).call1((py_child.bind(py),))?;
            extract_child_stream(&returned)
        })();
        result
    })
    .map_err(callback_error)
}

fn extract_child_stream(returned: &Bound<'_, PyAny>) -> PyResult<morel::Stream<PyValue>> {
    if let Ok(stream) = returned.extract::<PyRef<'_, PyChildStream>>() {
        return stream.stream();
    }

    if let Ok(stream) = returned.extract::<PyRef<'_, PyStream>>() {
        if matches!(stream.owner, PyStreamOwner::Child(_)) {
            return Ok(stream.stream.clone());
        }
    }

    Err(PyValueError::new_err(
        "worker build must return a stream from the child graph",
    ))
}

fn callback_failure_stream(
    child: &morel::Graph,
    error: Box<dyn Error + Send + Sync>,
) -> morel::Stream<PyValue> {
    let mut error = Some(error);
    child.just(Python::attach(py_none_value)).try_map(
        move |_| -> Result<PyValue, Box<dyn std::error::Error + Send + Sync>> {
            Err(error.take().unwrap_or_else(|| {
                Box::new(io::Error::other("callback failed")) as Box<dyn Error + Send + Sync>
            }))
        },
    )
}

fn py_batch_value(
    values: Vec<PyValue>,
) -> Result<PyValue, Box<dyn std::error::Error + Send + Sync>> {
    Python::attach(|py| py_list_value(py, values).map_err(callback_error))
}
