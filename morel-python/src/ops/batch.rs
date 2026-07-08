use std::error::Error;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PySequence, PySequenceMethods};

use crate::error::callback_error;
use crate::stream::PyStream;
use crate::value::{positive_duration_nanos, py_list_value, PyValue};

struct PyCollapse {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
}

impl morel::Operator for PyCollapse {
    fn step(&mut self, cx: &mut morel::Ctx) {
        if !self.input.fired() {
            return;
        }

        let value = self.input.get();
        match Python::attach(|py| sequence_last(py, &value)) {
            Ok(Some(value)) => self.out.set(value),
            Ok(None) => {}
            Err(err) => cx.fail(err),
        }
    }
}

struct PyMapBatch {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyMapBatch {
    fn step(&mut self, cx: &mut morel::Ctx) {
        if !self.input.fired() {
            return;
        }

        let value = self.input.get();
        match Python::attach(|py| call_batch(py, &self.func, &value)) {
            Ok(value) => self.out.set(value),
            Err(err) => cx.fail(err),
        }
    }
}

pub(crate) fn delay(stream: &PyStream, py: Python<'_>, delay_nanos: u64) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    Ok(PyStream::wrap(
        stream.stream.delay(Duration::from_nanos(delay_nanos)),
        stream.owner.clone_ref(py),
    ))
}

pub(crate) fn throttle(
    stream: &PyStream,
    py: Python<'_>,
    interval_nanos: u64,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let interval = positive_duration_nanos(interval_nanos, "interval_nanos")?;
    Ok(PyStream::wrap(
        stream.stream.throttle(interval),
        stream.owner.clone_ref(py),
    ))
}

pub(crate) fn debounce(stream: &PyStream, py: Python<'_>, quiet_nanos: u64) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let quiet = positive_duration_nanos(quiet_nanos, "quiet_nanos")?;
    Ok(PyStream::wrap(
        stream.stream.debounce(quiet),
        stream.owner.clone_ref(py),
    ))
}

pub(crate) fn buffer(stream: &PyStream, py: Python<'_>, capacity: usize) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    if capacity == 0 {
        return Err(PyValueError::new_err("capacity must be greater than 0"));
    }
    let buffered = stream
        .stream
        .buffer(capacity)
        .try_map(|values| Python::attach(|py| py_list_value(py, values).map_err(callback_error)));
    Ok(PyStream::wrap(buffered, stream.owner.clone_ref(py)))
}

pub(crate) fn window_tumbling(
    stream: &PyStream,
    py: Python<'_>,
    size_nanos: u64,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let size = positive_duration_nanos(size_nanos, "size_nanos")?;
    let windowed = stream
        .stream
        .window_tumbling(size)
        .try_map(|values| Python::attach(|py| py_list_value(py, values).map_err(callback_error)));
    Ok(PyStream::wrap(windowed, stream.owner.clone_ref(py)))
}

pub(crate) fn window_sliding(
    stream: &PyStream,
    py: Python<'_>,
    size_nanos: u64,
    slide_nanos: u64,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let size = positive_duration_nanos(size_nanos, "size_nanos")?;
    let slide = positive_duration_nanos(slide_nanos, "slide_nanos")?;
    let windowed = stream
        .stream
        .window_sliding(size, slide)
        .try_map(|values| Python::attach(|py| py_list_value(py, values).map_err(callback_error)));
    Ok(PyStream::wrap(windowed, stream.owner.clone_ref(py)))
}

pub(crate) fn collapse(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let collapsed = stream.stream.wire(|w| PyCollapse {
        input: w.on(&stream.stream),
        out: w.output(),
    });
    Ok(PyStream::wrap(collapsed, stream.owner.clone_ref(py)))
}

pub(crate) fn map_batch(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let mapped = stream.stream.wire(|w| PyMapBatch {
        input: w.on(&stream.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(mapped, stream.owner.clone_ref(py)))
}

fn sequence_last(
    py: Python<'_>,
    value: &PyValue,
) -> Result<Option<PyValue>, Box<dyn Error + Send + Sync>> {
    let sequence = value
        .bind(py)
        .cast::<PySequence>()
        .map_err(|err| callback_error(err.into()))?;
    let len = sequence.len().map_err(callback_error)?;
    if len == 0 {
        return Ok(None);
    }
    let item = sequence.get_item(len - 1).map_err(callback_error)?;
    Ok(Some(PyValue::new(item.unbind())))
}

fn call_batch(
    py: Python<'_>,
    func: &Py<PyAny>,
    value: &PyValue,
) -> Result<PyValue, Box<dyn Error + Send + Sync>> {
    let arg = value.clone_py(py);
    let result = func
        .bind(py)
        .call1((arg.bind(py),))
        .map_err(callback_error)?;
    Ok(PyValue::new(result.unbind()))
}
