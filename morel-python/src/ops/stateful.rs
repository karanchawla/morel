use std::error::Error;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::error::callback_error;
use crate::stream::PyStream;
use crate::value::{py_f64_value, py_list_value, py_time_pair_value, py_u64_value, PyValue};

struct PyScan {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    acc: PyValue,
    func: Py<PyAny>,
}

impl morel::Operator for PyScan {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| call_two(py, &self.func, &self.acc, &value)) {
            Ok(acc) => {
                self.acc = acc.clone();
                self.out.set(acc);
            }
            Err(err) => cx.fail(err),
        }
    }
}

struct PyReduce {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    acc: Option<PyValue>,
    func: Py<PyAny>,
}

impl morel::Operator for PyReduce {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        let acc = match self.acc.as_ref() {
            Some(acc) => match Python::attach(|py| call_two(py, &self.func, acc, &value)) {
                Ok(acc) => acc,
                Err(err) => {
                    cx.fail(err);
                    return;
                }
            },
            None => value,
        };
        self.acc = Some(acc.clone());
        self.out.set(acc);
    }
}

struct PySum {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    acc: Option<PyValue>,
}

impl morel::Operator for PySum {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        let acc = match self.acc.as_ref() {
            Some(acc) => {
                match Python::attach(|py| acc.add_py(py, &value).map_err(callback_error)) {
                    Ok(acc) => acc,
                    Err(err) => {
                        cx.fail(err);
                        return;
                    }
                }
            }
            None => value,
        };
        self.acc = Some(acc.clone());
        self.out.set(acc);
    }
}

struct PyDelta {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    prev: Option<PyValue>,
}

impl morel::Operator for PyDelta {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        if let Some(prev) = self.prev.as_ref() {
            match Python::attach(|py| value.sub_py(py, prev).map_err(callback_error)) {
                Ok(delta) => self.out.set(delta),
                Err(err) => {
                    cx.fail(err);
                    return;
                }
            }
        }
        self.prev = Some(value);
    }
}

struct PyMean {
    input: morel::Input<PyValue>,
    out: morel::Output<PyValue>,
    count: u64,
    mean: f64,
}

impl morel::Operator for PyMean {
    fn step(&mut self, cx: &mut morel::Ctx) {
        let value = self.input.get();
        match Python::attach(|py| value.float_py(py).map_err(callback_error)) {
            Ok(value) => {
                self.count += 1;
                self.mean += (value - self.mean) / self.count as f64;
                self.out
                    .set(Python::attach(|py| py_f64_value(py, self.mean)));
            }
            Err(err) => cx.fail(err),
        }
    }
}

pub(crate) fn count(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let counted = stream
        .stream
        .count()
        .try_map(|count| Python::attach(|py| py_u64_value(py, count).map_err(callback_error)));
    Ok(PyStream::wrap(counted, stream.owner.clone_ref(py)))
}

pub(crate) fn accumulate(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let accumulated = stream
        .stream
        .accumulate()
        .try_map(|values| Python::attach(|py| py_list_value(py, values).map_err(callback_error)));
    Ok(PyStream::wrap(accumulated, stream.owner.clone_ref(py)))
}

pub(crate) fn timestamp(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let stamped = stream.stream.timestamp().try_map(|(time, value)| {
        Python::attach(|py| py_time_pair_value(py, time, value).map_err(callback_error))
    });
    Ok(PyStream::wrap(stamped, stream.owner.clone_ref(py)))
}

pub(crate) fn scan(
    stream: &PyStream,
    py: Python<'_>,
    init: Py<PyAny>,
    func: Py<PyAny>,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let scanned = stream.stream.wire(|w| PyScan {
        input: w.on(&stream.stream),
        out: w.output(),
        acc: PyValue::new(init),
        func,
    });
    Ok(PyStream::wrap(scanned, stream.owner.clone_ref(py)))
}

pub(crate) fn reduce(stream: &PyStream, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let reduced = stream.stream.wire(|w| PyReduce {
        input: w.on(&stream.stream),
        out: w.output(),
        acc: None,
        func,
    });
    Ok(PyStream::wrap(reduced, stream.owner.clone_ref(py)))
}

pub(crate) fn sum(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let summed = stream.stream.wire(|w| PySum {
        input: w.on(&stream.stream),
        out: w.output(),
        acc: None,
    });
    Ok(PyStream::wrap(summed, stream.owner.clone_ref(py)))
}

pub(crate) fn delta(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let delta = stream.stream.wire(|w| PyDelta {
        input: w.on(&stream.stream),
        out: w.output(),
        prev: None,
    });
    Ok(PyStream::wrap(delta, stream.owner.clone_ref(py)))
}

pub(crate) fn mean(stream: &PyStream, py: Python<'_>) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let mean = stream.stream.wire(|w| PyMean {
        input: w.on(&stream.stream),
        out: w.output(),
        count: 0,
        mean: 0.0,
    });
    Ok(PyStream::wrap(mean, stream.owner.clone_ref(py)))
}

fn call_two(
    py: Python<'_>,
    func: &Py<PyAny>,
    acc: &PyValue,
    value: &PyValue,
) -> Result<PyValue, Box<dyn Error + Send + Sync>> {
    let acc = acc.clone_py(py);
    let value = value.clone_py(py);
    let result = func
        .bind(py)
        .call1((acc.bind(py), value.bind(py)))
        .map_err(callback_error)?;
    Ok(PyValue::new(result.unbind()))
}
