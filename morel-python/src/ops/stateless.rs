use std::error::Error;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBoolMethods};

use crate::error::{callback_error, type_error};
use crate::stream::PyStream;
use crate::value::{py_none_value, PyValue};

struct PyFilter {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    pred: Py<PyAny>,
}

impl morel::Operator for PyFilter {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| filter_predicate(py, &self.pred, &value)) {
            Ok(true) => self.out.set(value),
            Ok(false) => {}
            Err(err) => cx.fail(err),
        }
    }
}

struct PyFilterMap {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyFilterMap {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| call_filter_map(py, &self.func, &value)) {
            Ok(Some(value)) => self.out.set(value),
            Ok(None) => {}
            Err(err) => cx.fail(err),
        }
    }
}

struct PyDistinct {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    last: Option<PyValue>,
}

impl morel::Operator for PyDistinct {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| is_duplicate(py, self.last.as_ref(), &value)) {
            Ok(true) => {}
            Ok(false) => {
                self.last = Some(value.clone());
                self.out.set(value);
            }
            Err(err) => cx.fail(err),
        }
    }
}

struct PyInspect {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyInspect {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| call_with_value_and_time(py, &self.func, &value, cx.now())) {
            Ok(()) => self.out.set(value),
            Err(err) => cx.fail(err),
        }
    }
}

struct PySink {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PySink {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| call_with_value_and_time(py, &self.func, &value, cx.now())) {
            Ok(()) => self.out.set(Python::attach(py_none_value)),
            Err(err) => cx.fail(err),
        }
    }
}

pub(crate) fn map(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let mapped = stream
        .stream
        .try_map(move |value| Python::attach(|py| call_one(py, &func, &value)));
    Ok(PyStream::wrap(mapped, stream.owner.clone_ref(py)))
}

pub(crate) fn filter(stream: &PyStream, py: Python<'_>, pred: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let filtered = stream.stream.wire(|w| PyFilter {
        input: w.on(&stream.stream),
        out: w.output(),
        pred,
    });
    Ok(PyStream::wrap(filtered, stream.owner.clone_ref(py)))
}

pub(crate) fn filter_map(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let filtered = stream.stream.wire(|w| PyFilterMap {
        input: w.on(&stream.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(filtered, stream.owner.clone_ref(py)))
}

pub(crate) fn distinct(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let distinct = stream.stream.wire(|w| PyDistinct {
        input: w.on(&stream.stream),
        out: w.output(),
        last: None,
    });
    Ok(PyStream::wrap(distinct, stream.owner.clone_ref(py)))
}

pub(crate) fn take(stream: &PyStream, py: Python<'_>, n: u64) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    Ok(PyStream::wrap(
        stream.stream.take(n),
        stream.owner.clone_ref(py),
    ))
}

pub(crate) fn inspect(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let inspected = stream.stream.wire(|w| PyInspect {
        input: w.on(&stream.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(inspected, stream.owner.clone_ref(py)))
}

pub(crate) fn sink(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let sunk = stream.stream.wire(|w| PySink {
        input: w.on(&stream.stream),
        out: w.output(),
        func,
    });
    Ok(PyStream::wrap(sunk, stream.owner.clone_ref(py)))
}

fn call_one(
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

fn filter_predicate(
    py: Python<'_>,
    pred: &Py<PyAny>,
    value: &PyValue,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let arg = value.clone_py(py);
    let result = pred
        .bind(py)
        .call1((arg.bind(py),))
        .map_err(callback_error)?;
    strict_bool(&result).map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)
}

fn call_filter_map(
    py: Python<'_>,
    func: &Py<PyAny>,
    value: &PyValue,
) -> Result<Option<PyValue>, Box<dyn Error + Send + Sync>> {
    let arg = value.clone_py(py);
    let result = func
        .bind(py)
        .call1((arg.bind(py),))
        .map_err(callback_error)?;
    if result.is_none() {
        Ok(None)
    } else {
        Ok(Some(PyValue::new(result.unbind())))
    }
}

fn is_duplicate(
    py: Python<'_>,
    last: Option<&PyValue>,
    value: &PyValue,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match last {
        Some(last) => last.eq_py(py, value).map_err(callback_error),
        None => Ok(false),
    }
}

fn call_with_value_and_time(
    py: Python<'_>,
    func: &Py<PyAny>,
    value: &PyValue,
    time: morel::Time,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let arg = value.clone_py(py);
    func.bind(py)
        .call1((arg.bind(py), time.as_nanos()))
        .map(|_| ())
        .map_err(callback_error)
}

fn strict_bool(value: &Bound<'_, PyAny>) -> Result<bool, crate::error::BindingError> {
    value
        .cast_exact::<PyBool>()
        .map(PyBoolMethods::is_true)
        .map_err(|_| type_error("filter predicate must return bool"))
}
