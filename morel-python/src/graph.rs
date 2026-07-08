use std::cell::Cell;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyIterator};

use crate::error::morel_error_to_pyerr;
use crate::stream::PyStream;
use crate::value::{positive_duration_nanos, py_none_value, PyValue};

pub(crate) struct PyTicker {
    pub(crate) period: Duration,
    pub(crate) out: morel::Output<PyValue>,
}

impl morel::Operator for PyTicker {
    fn on_start(&mut self, cx: &mut morel::Ctx) {
        cx.at(cx.now());
        cx.every(self.period);
    }

    fn step(&mut self, _cx: &mut morel::Ctx) {
        self.out.set(Python::attach(py_none_value));
    }
}

#[pyclass(unsendable, name = "Graph")]
pub(crate) struct PyGraph {
    graph: morel::Graph,
    running: Cell<bool>,
    requires_detached_run: Cell<bool>,
}

#[pyclass(name = "Summary")]
pub(crate) struct PySummary {
    #[pyo3(get)]
    steps: u64,
    #[pyo3(get)]
    started_at: u64,
    #[pyo3(get)]
    ended_at: u64,
}

impl From<morel::Summary> for PySummary {
    fn from(summary: morel::Summary) -> Self {
        Self {
            steps: summary.steps,
            started_at: summary.started_at.as_nanos(),
            ended_at: summary.ended_at.as_nanos(),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PyRunSpec {
    Replay {
        start: morel::Time,
        stop: morel::Stop,
    },
    Live {
        stop: morel::Stop,
    },
}

impl PyRunSpec {
    pub(crate) fn extract(spec: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(replay) = spec.extract::<PyRef<'_, PyReplay>>() {
            return Ok(Self::Replay {
                start: replay.start,
                stop: replay.stop,
            });
        }
        if let Ok(live) = spec.extract::<PyRef<'_, PyLive>>() {
            return Ok(Self::Live { stop: live.stop });
        }
        Err(PyTypeError::new_err("expected Replay or Live run spec"))
    }
}

#[pyclass(name = "Replay")]
pub(crate) struct PyReplay {
    start: morel::Time,
    stop: morel::Stop,
}

#[pymethods]
impl PyReplay {
    #[staticmethod]
    fn from_nanos(start: u64) -> Self {
        Self {
            start: morel::Time::from_nanos(start),
            stop: morel::Stop::Idle,
        }
    }

    fn stop(&self, stop: PyRef<'_, PyStop>) -> Self {
        Self {
            start: self.start,
            stop: stop.stop,
        }
    }
}

#[pyclass(name = "Live")]
pub(crate) struct PyLive {
    stop: morel::Stop,
}

#[pymethods]
impl PyLive {
    #[new]
    fn new() -> Self {
        Self {
            stop: morel::Stop::Never,
        }
    }

    fn stop(&self, stop: PyRef<'_, PyStop>) -> Self {
        Self { stop: stop.stop }
    }
}

#[pyclass(name = "Stop")]
pub(crate) struct PyStop {
    stop: morel::Stop,
}

#[pymethods]
impl PyStop {
    #[staticmethod]
    fn idle() -> Self {
        Self {
            stop: morel::Stop::Idle,
        }
    }

    #[staticmethod]
    fn at_nanos(nanos: u64) -> Self {
        Self {
            stop: morel::Stop::At(morel::Time::from_nanos(nanos)),
        }
    }

    #[staticmethod]
    fn after_nanos(nanos: u64) -> PyResult<Self> {
        Ok(Self {
            stop: morel::Stop::After(positive_duration_nanos(nanos, "stop")?),
        })
    }

    #[staticmethod]
    fn after_seconds(seconds: f64) -> PyResult<Self> {
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "stop seconds must be finite and greater than 0",
            ));
        }
        let duration = Duration::try_from_secs_f64(seconds)
            .map_err(|_| PyValueError::new_err("stop seconds are out of range"))?;
        if duration.is_zero() {
            return Err(PyValueError::new_err(
                "stop seconds must be at least one nanosecond",
            ));
        }
        Ok(Self {
            stop: morel::Stop::After(duration),
        })
    }

    #[staticmethod]
    fn steps(steps: u64) -> Self {
        Self {
            stop: morel::Stop::Steps(steps),
        }
    }

    #[staticmethod]
    fn never() -> Self {
        Self {
            stop: morel::Stop::Never,
        }
    }
}

#[pymethods]
impl PyGraph {
    #[new]
    fn new() -> Self {
        Self {
            graph: morel::Graph::new(),
            running: Cell::new(false),
            requires_detached_run: Cell::new(false),
        }
    }

    fn len(&self) -> usize {
        self.graph.len()
    }

    fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    fn add(slf: PyRef<'_, Self>, py: Python<'_>, operator: Py<PyAny>) -> PyResult<PyStream> {
        crate::custom::graph_add(slf, py, operator)
    }

    fn just(slf: PyRef<'_, Self>, value: Py<PyAny>) -> PyResult<PyStream> {
        if slf.running.get() {
            return Err(PyRuntimeError::new_err(
                "cannot add nodes while graph is running",
            ));
        }
        let stream = slf.graph.just(PyValue::new(value));
        Ok(PyStream::wrap(stream, Py::from(slf)))
    }

    #[pyo3(signature = (period_seconds=None, period_nanos=None))]
    fn ticker(
        slf: PyRef<'_, Self>,
        period_seconds: Option<f64>,
        period_nanos: Option<u64>,
    ) -> PyResult<PyStream> {
        if slf.running.get() {
            return Err(PyRuntimeError::new_err(
                "cannot add nodes while graph is running",
            ));
        }
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

        let stream = slf.graph.add(|w| PyTicker {
            period,
            out: w.output(),
        });
        Ok(PyStream::wrap(stream, Py::from(slf)))
    }

    fn replay_from_iter(slf: PyRef<'_, Self>, items: &Bound<'_, PyAny>) -> PyResult<PyStream> {
        if slf.running.get() {
            return Err(PyRuntimeError::new_err(
                "cannot add nodes while graph is running",
            ));
        }
        let mut converted = Vec::new();
        for item in PyIterator::from_object(items)? {
            let (nanos, value): (u64, Py<PyAny>) = item?.extract()?;
            converted.push((morel::Time::from_nanos(nanos), PyValue::new(value)));
        }

        let stream = slf.graph.replay_from_iter(converted);
        Ok(PyStream::wrap(stream, Py::from(slf)))
    }

    fn replay_from_log(slf: PyRef<'_, Self>, log: &Bound<'_, PyAny>) -> PyResult<PyStream> {
        if slf.running.get() {
            return Err(PyRuntimeError::new_err(
                "cannot add nodes while graph is running",
            ));
        }
        let converted = py_log_items(log)?;
        let stream = slf.graph.replay_from_log(converted);
        Ok(PyStream::wrap(stream, Py::from(slf)))
    }

    fn replay_from_csv(
        slf: PyRef<'_, Self>,
        path: std::path::PathBuf,
        parse: Py<PyAny>,
    ) -> PyResult<PyStream> {
        crate::csv::replay_from_csv(slf, path, parse)
    }

    fn run(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<PySummary> {
        if self.running.get() {
            return Err(PyRuntimeError::new_err("graph is already running"));
        }
        self.run_spec(py, PyRunSpec::extract(spec)?)
    }

    fn begin(&self, spec: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.running.get() {
            return Err(PyRuntimeError::new_err("graph is already running"));
        }
        self.begin_spec(PyRunSpec::extract(spec)?);
        self.running.set(true);
        Ok(())
    }

    fn step(&self, py: Python<'_>) -> PyResult<bool> {
        if !self.running.get() {
            return Err(PyRuntimeError::new_err("graph is not running"));
        }
        if self.requires_detached_run.get() {
            Ok(detached_step(py, &self.graph))
        } else {
            Ok(self.graph.step())
        }
    }

    fn end(&self, py: Python<'_>) -> PyResult<PySummary> {
        if !self.running.get() {
            return Err(PyRuntimeError::new_err("graph is not running"));
        }
        let result = if self.requires_detached_run.get() {
            detached_end(py, &self.graph)
                .map(PySummary::from)
                .map_err(morel_error_to_pyerr)
        } else {
            self.graph
                .end()
                .map(PySummary::from)
                .map_err(morel_error_to_pyerr)
        };
        self.running.set(false);
        result
    }
}

impl PyGraph {
    pub(crate) fn ensure_can_add_nodes(&self) -> PyResult<()> {
        if self.running.get() {
            return Err(PyRuntimeError::new_err(
                "cannot add nodes while graph is running",
            ));
        }
        Ok(())
    }

    pub(crate) fn graph(&self) -> &morel::Graph {
        &self.graph
    }

    pub(crate) fn mark_requires_detached_run(&self) {
        self.requires_detached_run.set(true);
    }

    pub(crate) fn run_spec(&self, py: Python<'_>, spec: PyRunSpec) -> PyResult<PySummary> {
        if self.running.get() {
            return Err(PyRuntimeError::new_err("graph is already running"));
        }
        self.running.set(true);
        let result = if self.requires_detached_run.get() {
            detached_run(py, &self.graph, spec)
                .map(PySummary::from)
                .map_err(morel_error_to_pyerr)
        } else {
            match spec {
                PyRunSpec::Replay { start, stop } => {
                    self.graph.run(morel::Replay::from(start).stop(stop))
                }
                PyRunSpec::Live { stop } => self.graph.run(morel::Live::new().stop(stop)),
            }
            .map(PySummary::from)
            .map_err(morel_error_to_pyerr)
        };
        self.running.set(false);
        result
    }

    fn begin_spec(&self, spec: PyRunSpec) {
        match spec {
            PyRunSpec::Replay { start, stop } => {
                self.graph.begin(morel::Replay::from(start).stop(stop));
            }
            PyRunSpec::Live { stop } => {
                self.graph.begin(morel::Live::new().stop(stop));
            }
        }
    }
}

fn detached_run(
    py: Python<'_>,
    graph: &morel::Graph,
    spec: PyRunSpec,
) -> Result<morel::Summary, morel::Error> {
    let graph_ptr = graph as *const morel::Graph as usize;
    let mut result = None;
    let result_ptr = &mut result as *mut Option<Result<morel::Summary, morel::Error>> as usize;
    py.detach(move || {
        // SAFETY: `Python::detach` runs this closure synchronously on the same
        // OS thread while releasing the GIL. The graph reference outlives this
        // call, and no other Python method can access it until the call returns.
        let graph = unsafe { &*(graph_ptr as *const morel::Graph) };
        let run_result = match spec {
            PyRunSpec::Replay { start, stop } => graph.run(morel::Replay::from(start).stop(stop)),
            PyRunSpec::Live { stop } => graph.run(morel::Live::new().stop(stop)),
        };
        // SAFETY: `result_ptr` points to a stack slot in this function. The
        // detached closure executes synchronously on the same thread and
        // returns before the stack slot is read.
        unsafe {
            *(result_ptr as *mut Option<Result<morel::Summary, morel::Error>>) = Some(run_result);
        }
    });
    result.expect("detached run result must be set")
}

fn detached_end(py: Python<'_>, graph: &morel::Graph) -> Result<morel::Summary, morel::Error> {
    let graph_ptr = graph as *const morel::Graph as usize;
    let mut result = None;
    let result_ptr = &mut result as *mut Option<Result<morel::Summary, morel::Error>> as usize;
    py.detach(move || {
        // SAFETY: see `detached_run`.
        let graph = unsafe { &*(graph_ptr as *const morel::Graph) };
        let end_result = graph.end();
        // SAFETY: see `detached_run`.
        unsafe {
            *(result_ptr as *mut Option<Result<morel::Summary, morel::Error>>) = Some(end_result);
        }
    });
    result.expect("detached end result must be set")
}

fn detached_step(py: Python<'_>, graph: &morel::Graph) -> bool {
    let graph_ptr = graph as *const morel::Graph as usize;
    py.detach(move || {
        // SAFETY: see `detached_run`.
        let graph = unsafe { &*(graph_ptr as *const morel::Graph) };
        graph.step()
    })
}

pub(crate) fn py_log_items(items: &Bound<'_, PyAny>) -> PyResult<Vec<(morel::Time, PyValue)>> {
    let mut converted = Vec::new();
    for item in PyIterator::from_object(items)? {
        let (nanos, value): (u64, Py<PyAny>) = item?.extract()?;
        converted.push((morel::Time::from_nanos(nanos), PyValue::new(value)));
    }
    Ok(converted)
}

pub(crate) fn duration_from_seconds(seconds: f64, name: &str) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be finite and greater than 0"
        )));
    }
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err(format!("{name} is out of range")))?;
    if duration.is_zero() {
        return Err(PyValueError::new_err(format!(
            "{name} must be at least one nanosecond"
        )));
    }
    Ok(duration)
}
