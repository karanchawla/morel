use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::channel::PyChildGraph;
use crate::error::callback_error;
use crate::graph::{PyGraph, PyRunSpec, PySummary};
use crate::ops::{batch, combine, stateful, stateless};
use crate::recording::PyRecording;
use crate::value::{py_history_value, PyValue};

pub(crate) enum PyStreamOwner {
    Graph(Py<PyGraph>),
    Child(Py<PyChildGraph>),
}

impl PyStreamOwner {
    pub(crate) fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            Self::Graph(graph) => Self::Graph(graph.clone_ref(py)),
            Self::Child(graph) => Self::Child(graph.clone_ref(py)),
        }
    }

    pub(crate) fn ensure_can_add_nodes(&self, py: Python<'_>) -> PyResult<()> {
        match self {
            Self::Graph(graph) => graph.borrow(py).ensure_can_add_nodes(),
            Self::Child(graph) => graph.borrow(py).ensure_can_add_nodes(),
        }
    }

    pub(crate) fn run_spec(&self, py: Python<'_>, spec: PyRunSpec) -> PyResult<PySummary> {
        match self {
            Self::Graph(graph) => graph.borrow(py).run_spec(py, spec),
            Self::Child(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "child streams cannot run their graph directly",
            )),
        }
    }

    pub(crate) fn mark_requires_detached_run(&self, py: Python<'_>) {
        if let Self::Graph(graph) = self {
            graph.borrow(py).mark_requires_detached_run();
        }
    }

    pub(crate) fn same_graph(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Graph(left), Self::Graph(right)) => left.as_ptr() == right.as_ptr(),
            (Self::Child(left), Self::Child(right)) => {
                Python::attach(|py| left.borrow(py).graph_id() == right.borrow(py).graph_id())
            }
            _ => false,
        }
    }
}

impl From<Py<PyGraph>> for PyStreamOwner {
    fn from(value: Py<PyGraph>) -> Self {
        Self::Graph(value)
    }
}

impl From<Py<PyChildGraph>> for PyStreamOwner {
    fn from(value: Py<PyChildGraph>) -> Self {
        Self::Child(value)
    }
}

#[pyclass(unsendable, name = "Stream")]
pub(crate) struct PyStream {
    pub(crate) stream: morel::Stream<PyValue>,
    pub(crate) owner: PyStreamOwner,
}

#[pymethods]
impl PyStream {
    fn peek(&self, py: Python<'_>) -> Py<PyAny> {
        match self.stream.peek() {
            Some(value) => value.bind(py).clone().unbind(),
            None => py.None(),
        }
    }

    fn run(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<PySummary> {
        let spec = PyRunSpec::extract(spec)?;
        self.owner.run_spec(py, spec)
    }

    fn wire(&self, py: Python<'_>, operator: Py<PyAny>) -> PyResult<PyStream> {
        crate::custom::stream_wire(self, py, operator)
    }

    fn history(&self, py: Python<'_>) -> PyResult<PyStream> {
        self.owner.ensure_can_add_nodes(py)?;
        let stream = self.stream.history().try_map(|history| {
            Python::attach(|py| py_history_value(py, history).map_err(callback_error))
        });
        Ok(PyStream::wrap(stream, self.owner.clone_ref(py)))
    }

    fn record(&self, py: Python<'_>, recording: PyRef<'_, PyRecording>) -> PyResult<PyStream> {
        self.owner.ensure_can_add_nodes(py)?;
        let stream = self
            .stream
            .record(&recording.recording)
            .map(|()| Python::attach(crate::value::py_none_value));
        Ok(PyStream::wrap(stream, self.owner.clone_ref(py)))
    }

    fn count(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::count(self, py)
    }

    fn accumulate(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::accumulate(self, py)
    }

    fn timestamp(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::timestamp(self, py)
    }

    fn scan(&self, py: Python<'_>, init: Py<PyAny>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateful::scan(self, py, init, func)
    }

    fn reduce(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateful::reduce(self, py, func)
    }

    fn sum(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::sum(self, py)
    }

    fn delta(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::delta(self, py)
    }

    fn mean(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateful::mean(self, py)
    }

    #[pyo3(signature = (*, delay_nanos))]
    fn delay(&self, py: Python<'_>, delay_nanos: u64) -> PyResult<PyStream> {
        batch::delay(self, py, delay_nanos)
    }

    #[pyo3(signature = (*, interval_nanos))]
    fn throttle(&self, py: Python<'_>, interval_nanos: u64) -> PyResult<PyStream> {
        batch::throttle(self, py, interval_nanos)
    }

    #[pyo3(signature = (*, quiet_nanos))]
    fn debounce(&self, py: Python<'_>, quiet_nanos: u64) -> PyResult<PyStream> {
        batch::debounce(self, py, quiet_nanos)
    }

    fn buffer(&self, py: Python<'_>, capacity: usize) -> PyResult<PyStream> {
        batch::buffer(self, py, capacity)
    }

    #[pyo3(signature = (*, size_nanos))]
    fn window_tumbling(&self, py: Python<'_>, size_nanos: u64) -> PyResult<PyStream> {
        batch::window_tumbling(self, py, size_nanos)
    }

    #[pyo3(signature = (*, size_nanos, slide_nanos))]
    fn window_sliding(
        &self,
        py: Python<'_>,
        size_nanos: u64,
        slide_nanos: u64,
    ) -> PyResult<PyStream> {
        batch::window_sliding(self, py, size_nanos, slide_nanos)
    }

    fn collapse(&self, py: Python<'_>) -> PyResult<PyStream> {
        batch::collapse(self, py)
    }

    fn map_batch(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        batch::map_batch(self, py, func)
    }

    fn with_(
        &self,
        py: Python<'_>,
        other: PyRef<'_, PyStream>,
        func: Py<PyAny>,
    ) -> PyResult<PyStream> {
        combine::with_(self, py, &other, func)
    }

    fn with_latest(
        &self,
        py: Python<'_>,
        other: PyRef<'_, PyStream>,
        func: Py<PyAny>,
    ) -> PyResult<PyStream> {
        combine::with_latest(self, py, &other, func)
    }

    fn gate(&self, py: Python<'_>, open: PyRef<'_, PyStream>) -> PyResult<PyStream> {
        combine::gate(self, py, &open)
    }

    fn sample(&self, py: Python<'_>, trigger: PyRef<'_, PyStream>) -> PyResult<PyStream> {
        combine::sample(self, py, &trigger)
    }

    fn unzip(&self, py: Python<'_>) -> PyResult<(PyStream, PyStream)> {
        combine::unzip(self, py)
    }

    fn map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::map(self, py, func)
    }

    fn try_map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::map(self, py, func)
    }

    fn filter(&self, py: Python<'_>, pred: Py<PyAny>) -> PyResult<PyStream> {
        stateless::filter(self, py, pred)
    }

    fn filter_map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::filter_map(self, py, func)
    }

    fn distinct(&self, py: Python<'_>) -> PyResult<PyStream> {
        stateless::distinct(self, py)
    }

    fn take(&self, py: Python<'_>, n: u64) -> PyResult<PyStream> {
        stateless::take(self, py, n)
    }

    fn inspect(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::inspect(self, py, func)
    }

    fn sink(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::sink(self, py, func)
    }

    fn try_sink(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
        stateless::sink(self, py, func)
    }

    #[cfg(feature = "async-io")]
    fn consume_async(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<PyStream> {
        crate::async_io::consume_async(self, py, callback)
    }
}

impl PyStream {
    pub(crate) fn wrap(stream: morel::Stream<PyValue>, owner: impl Into<PyStreamOwner>) -> Self {
        Self {
            stream,
            owner: owner.into(),
        }
    }

    pub(crate) fn ensure_can_add_nodes(&self, py: Python<'_>) -> PyResult<()> {
        self.owner.ensure_can_add_nodes(py)
    }

    pub(crate) fn ensure_same_owner(&self, other: &Self) -> PyResult<()> {
        if self.owner.same_graph(&other.owner) {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyValueError::new_err(
                "streams must belong to the same graph",
            ))
        }
    }
}
