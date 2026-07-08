use std::error::Error;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBoolMethods, PyIterator};

use crate::channel::PyChildStream;
use crate::error::{callback_error, type_error};
use crate::stream::PyStream;
use crate::value::{py_list_value, PyValue};

struct PyWith {
    left: morel::Input<PyValue>,
    right: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyWith {
    fn step(&mut self, cx: &mut morel::Ctx) {
        if let (Some(left), Some(right)) = (self.left.peek(), self.right.peek()) {
            match Python::attach(|py| call_two(py, &self.func, &left, &right)) {
                Ok(value) => self.out.set(value),
                Err(err) => cx.fail(err),
            }
        }
    }
}

struct PyWithLatest {
    input: morel::Input<PyValue>,
    other: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyWithLatest {
    fn step(&mut self, cx: &mut morel::Ctx) {
        if let Some(other) = self.other.peek() {
            let input = self.input.get();
            match Python::attach(|py| call_two(py, &self.func, &input, &other)) {
                Ok(value) => self.out.set(value),
                Err(err) => cx.fail(err),
            }
        }
    }
}

struct PyGate {
    input: morel::Input<PyValue>,
    open: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
}

impl morel::Operator for PyGate {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let Some(open) = self.open.peek() else {
            return;
        };

        match Python::attach(|py| strict_gate_bool(py, &open)) {
            Ok(true) => self.out.set(self.input.get()),
            Ok(false) => {}
            Err(err) => cx.fail(err),
        }
    }
}

struct PyUnzip {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    index: usize,
}

impl morel::Operator for PyUnzip {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| sequence_item(py, &value, self.index)) {
            Ok(value) => self.out.set(value),
            Err(err) => cx.fail(err),
        }
    }
}

pub(crate) fn with_(
    stream: &PyStream,
    py: Python<'_>,
    other: &PyStream,
    func: Py<PyAny>,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    stream.ensure_same_owner(other)?;
    let combined = stream.stream.wire(|w| PyWith {
        left: w.on(&stream.stream),
        right: w.on(&other.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(combined, stream.owner.clone_ref(py)))
}

pub(crate) fn with_latest(
    stream: &PyStream,
    py: Python<'_>,
    other: &PyStream,
    func: Py<PyAny>,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    stream.ensure_same_owner(other)?;
    let combined = stream.stream.wire(|w| PyWithLatest {
        input: w.on(&stream.stream),
        other: w.watch(&other.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(combined, stream.owner.clone_ref(py)))
}

pub(crate) fn gate(stream: &PyStream, py: Python<'_>, open: &PyStream) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    stream.ensure_same_owner(open)?;
    let gated = stream.stream.wire(|w| PyGate {
        input: w.on(&stream.stream),
        open: w.watch(&open.stream),
        out: w.output(),
    });
    Ok(PyStream::wrap(gated, stream.owner.clone_ref(py)))
}

pub(crate) fn sample(stream: &PyStream, py: Python<'_>, trigger: &PyStream) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    stream.ensure_same_owner(trigger)?;
    Ok(PyStream::wrap(
        stream.stream.sample(&trigger.stream),
        stream.owner.clone_ref(py),
    ))
}

pub(crate) fn unzip(stream: &PyStream, py: Python<'_>) -> PyResult<(PyStream, PyStream)> {
    stream.ensure_can_add_nodes(py)?;
    let left = stream.stream.wire(|w| PyUnzip {
        input: w.on(&stream.stream),
        out: w.output(),
        index: 0,
    });
    let right = stream.stream.wire(|w| PyUnzip {
        input: w.on(&stream.stream),
        out: w.output(),
        index: 1,
    });
    Ok((
        PyStream::wrap(left, stream.owner.clone_ref(py)),
        PyStream::wrap(right, stream.owner.clone_ref(py)),
    ))
}

#[pyfunction]
pub(crate) fn merge(py: Python<'_>, streams: &Bound<'_, PyAny>) -> PyResult<PyStream> {
    let streams = extract_streams(py, streams)?;
    if streams.is_empty() {
        return Err(PyValueError::new_err("merge requires at least one stream"));
    }

    let owner = streams[0].owner.clone_ref(py);
    streams[0].ensure_can_add_nodes(py)?;
    ensure_same_owners(&streams)?;

    let source_refs = streams
        .iter()
        .map(|stream| &stream.stream)
        .collect::<Vec<_>>();
    let merged = morel::merge(&source_refs);
    Ok(PyStream::wrap(merged, owner))
}

#[pyfunction]
pub(crate) fn gather(py: Python<'_>, streams: &Bound<'_, PyAny>) -> PyResult<PyStream> {
    let streams = extract_streams(py, streams)?;
    if streams.is_empty() {
        return Err(PyValueError::new_err("gather requires at least one stream"));
    }

    let owner = streams[0].owner.clone_ref(py);
    streams[0].ensure_can_add_nodes(py)?;
    ensure_same_owners(&streams)?;

    let source_refs = streams
        .iter()
        .map(|stream| &stream.stream)
        .collect::<Vec<_>>();
    let gathered = morel::gather(&source_refs)
        .try_map(|values| Python::attach(|py| py_list_value(py, values).map_err(callback_error)));
    Ok(PyStream::wrap(gathered, owner))
}

fn extract_streams(py: Python<'_>, streams: &Bound<'_, PyAny>) -> PyResult<Vec<PyStream>> {
    let mut extracted = Vec::new();
    for stream in PyIterator::from_object(streams)? {
        let stream = stream?;
        if let Ok(stream) = stream.extract::<PyRef<'_, PyStream>>() {
            extracted.push(PyStream::wrap(
                stream.stream.clone(),
                stream.owner.clone_ref(py),
            ));
            continue;
        }

        if let Ok(stream) = stream.extract::<PyRef<'_, PyChildStream>>() {
            extracted.push(stream.to_py_stream(py)?);
            continue;
        }

        return Err(pyo3::exceptions::PyTypeError::new_err(
            "expected Stream or ChildStream",
        ));
    }
    Ok(extracted)
}

fn ensure_same_owners(streams: &[PyStream]) -> PyResult<()> {
    let first = &streams[0];
    for stream in streams.iter().skip(1) {
        first.ensure_same_owner(stream)?;
    }
    Ok(())
}

fn call_two(
    py: Python<'_>,
    func: &Py<PyAny>,
    left: &PyValue,
    right: &PyValue,
) -> Result<PyValue, Box<dyn Error + Send + Sync>> {
    let left = left.clone_py(py);
    let right = right.clone_py(py);
    let result = func
        .bind(py)
        .call1((left.bind(py), right.bind(py)))
        .map_err(callback_error)?;
    Ok(PyValue::new(result.unbind()))
}

fn strict_gate_bool(py: Python<'_>, value: &PyValue) -> Result<bool, Box<dyn Error + Send + Sync>> {
    value
        .bind(py)
        .cast_exact::<PyBool>()
        .map(PyBoolMethods::is_true)
        .map_err(|_| {
            Box::new(type_error("gate stream must return bool")) as Box<dyn Error + Send + Sync>
        })
}

fn sequence_item(
    py: Python<'_>,
    value: &PyValue,
    index: usize,
) -> Result<PyValue, Box<dyn Error + Send + Sync>> {
    let item = value.bind(py).get_item(index).map_err(callback_error)?;
    Ok(PyValue::new(item.unbind()))
}
