use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::time::Duration;

use pyo3::exceptions::{
    PyAttributeError, PyBaseException, PyRuntimeError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::channel::PyChildStream;
use crate::error::{callback_error, runtime_error};
use crate::graph::PyGraph;
use crate::stream::{PyStream, PyStreamOwner};
use crate::value::{positive_duration_nanos, PyValue};

#[pyclass(unsendable, name = "Input")]
pub(crate) struct PyInput {
    input: morel::Input<PyValue>,
}

#[pymethods]
impl PyInput {
    fn fired(&self) -> bool {
        self.input.fired()
    }

    fn has_value(&self) -> bool {
        self.input.has_value()
    }

    fn peek(&self, py: Python<'_>) -> Py<PyAny> {
        match self.input.peek() {
            Some(value) => value.bind(py).clone().unbind(),
            None => py.None(),
        }
    }

    fn get(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.input
            .peek()
            .map(|value| value.bind(py).clone().unbind())
            .ok_or_else(|| PyRuntimeError::new_err("input has no value"))
    }
}

#[pyclass(unsendable, name = "Output")]
pub(crate) struct PyOutput {
    output: morel::Output<PyValue>,
}

#[pymethods]
impl PyOutput {
    fn set(&self, value: Py<PyAny>) {
        self.output.set(PyValue::new(value));
    }
}

#[pyclass(unsendable, name = "Wire")]
pub(crate) struct PyWire {
    ptr: NonNull<()>,
    active: Rc<Cell<bool>>,
    owner: PyStreamOwner,
    output_created: Rc<Cell<bool>>,
}

#[pymethods]
impl PyWire {
    fn on(&self, py: Python<'_>, stream: &Bound<'_, PyAny>) -> PyResult<Py<PyInput>> {
        self.ensure_active()?;
        let stream = self.extract_stream(py, stream)?;
        let input = self.with_wire(|wire| wire.on(&stream.stream))?;
        Py::new(py, PyInput { input })
    }

    fn watch(&self, py: Python<'_>, stream: &Bound<'_, PyAny>) -> PyResult<Py<PyInput>> {
        self.ensure_active()?;
        let stream = self.extract_stream(py, stream)?;
        let input = self.with_wire(|wire| wire.watch(&stream.stream))?;
        Py::new(py, PyInput { input })
    }

    fn output(&self, py: Python<'_>) -> PyResult<Py<PyOutput>> {
        self.ensure_active()?;
        if self.output_created.replace(true) {
            return Err(PyRuntimeError::new_err(
                "Wire.output() may only be called once",
            ));
        }
        let output = self.with_wire(|wire| wire.output())?;
        Py::new(py, PyOutput { output })
    }

    fn finalize(&self) -> PyResult<()> {
        self.ensure_active()?;
        self.with_wire(|wire| wire.finalize())
    }
}

impl PyWire {
    fn new(
        wire: &mut morel::Wire<'_>,
        owner: PyStreamOwner,
        output_created: Rc<Cell<bool>>,
    ) -> Self {
        Self {
            ptr: NonNull::from(wire).cast(),
            active: Rc::new(Cell::new(true)),
            owner,
            output_created,
        }
    }

    fn active(&self) -> Rc<Cell<bool>> {
        self.active.clone()
    }

    fn with_wire<T>(&self, f: impl FnOnce(&mut morel::Wire<'_>) -> T) -> PyResult<T> {
        self.ensure_active()?;

        // SAFETY: PyWire is constructed only inside the synchronous core
        // Graph::add/Stream::wire callback. The raw pointer targets that
        // callback's `&mut morel::Wire` and `active` is cleared before the
        // callback returns, so Python can never legally dereference it after
        // the borrowed Wire is gone. PyO3 unsendable classes keep this on the
        // owning Python thread.
        let wire = unsafe { &mut *(self.ptr.as_ptr() as *mut morel::Wire<'_>) };
        Ok(f(wire))
    }

    fn ensure_active(&self) -> PyResult<()> {
        if self.active.get() {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("Wire is no longer active"))
        }
    }

    fn ensure_same_graph(&self, stream: &PyStream) -> PyResult<()> {
        if self.owner.same_graph(&stream.owner) {
            Ok(())
        } else {
            Err(PyValueError::new_err(
                "streams must belong to the same graph",
            ))
        }
    }

    fn ensure_same_child_graph(&self, py: Python<'_>, stream: &PyChildStream) -> PyResult<()> {
        match &self.owner {
            PyStreamOwner::Child(graph) if graph.borrow(py).graph_id() == stream.graph_id() => {
                Ok(())
            }
            _ => Err(PyValueError::new_err(
                "streams must belong to the same graph",
            )),
        }
    }

    fn extract_stream(&self, py: Python<'_>, stream: &Bound<'_, PyAny>) -> PyResult<PyStream> {
        if let Ok(stream) = stream.extract::<PyRef<'_, PyStream>>() {
            self.ensure_same_graph(&stream)?;
            return Ok(PyStream::wrap(
                stream.stream.clone(),
                stream.owner.clone_ref(py),
            ));
        }

        if let Ok(stream) = stream.extract::<PyRef<'_, PyChildStream>>() {
            self.ensure_same_child_graph(py, &stream)?;
            return stream.to_py_stream(py);
        }

        Err(PyTypeError::new_err("expected Stream or ChildStream"))
    }
}

#[pyclass(unsendable, name = "Ctx")]
pub(crate) struct PyCtx {
    ptr: NonNull<()>,
    active: Rc<Cell<bool>>,
}

#[pymethods]
impl PyCtx {
    fn now(&self) -> PyResult<u64> {
        self.with_ctx(|cx| cx.now().as_nanos())
    }

    fn started_at(&self) -> PyResult<u64> {
        self.with_ctx(|cx| cx.started_at().as_nanos())
    }

    fn elapsed_nanos(&self) -> PyResult<u64> {
        self.with_ctx(|cx| cx.elapsed().as_nanos() as u64)
    }

    fn is_final(&self) -> PyResult<bool> {
        self.with_ctx(|cx| cx.is_final())
    }

    fn is_live(&self) -> PyResult<bool> {
        self.with_ctx(|cx| cx.is_live())
    }

    fn at_nanos(&self, nanos: u64) -> PyResult<()> {
        self.with_ctx(|cx| cx.at(morel::Time::from_nanos(nanos)))
    }

    fn after_nanos(&self, nanos: u64) -> PyResult<()> {
        self.with_ctx(|cx| cx.after(Duration::from_nanos(nanos)))
    }

    fn every_nanos(&self, nanos: u64) -> PyResult<()> {
        let period = positive_duration_nanos(nanos, "period_nanos")?;
        self.with_ctx(|cx| cx.every(period))
    }

    fn stop(&self) -> PyResult<()> {
        self.with_ctx(|cx| cx.stop())
    }

    fn fail(&self, error: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(message) = error.extract::<String>() {
            return self.with_ctx(|cx| cx.fail(runtime_error(message)));
        }

        if error.is_instance_of::<PyBaseException>() {
            let py_err = PyErr::from_value(error.clone());
            return self.with_ctx(|cx| cx.fail(callback_error(py_err)));
        }

        let py_err = PyErr::from_value(error.clone());
        self.with_ctx(|cx| cx.fail(callback_error(py_err)))
    }
}

impl PyCtx {
    fn new(cx: &mut morel::Ctx<'_>) -> Self {
        Self {
            ptr: NonNull::from(cx).cast(),
            active: Rc::new(Cell::new(true)),
        }
    }

    fn active(&self) -> Rc<Cell<bool>> {
        self.active.clone()
    }

    fn with_ctx<T>(&self, f: impl FnOnce(&mut morel::Ctx<'_>) -> T) -> PyResult<T> {
        if !self.active.get() {
            return Err(PyRuntimeError::new_err("Ctx is no longer active"));
        }

        // SAFETY: PyCtx is created immediately before invoking one Python
        // lifecycle hook with the core `&mut morel::Ctx`. The active guard is
        // cleared as soon as that hook returns, preventing later Python use of
        // the raw pointer after the core context has been dropped.
        let cx = unsafe { &mut *(self.ptr.as_ptr() as *mut morel::Ctx<'_>) };
        Ok(f(cx))
    }
}

pub(crate) struct PyCustomOperator {
    operator: Py<PyAny>,
}

impl morel::Operator for PyCustomOperator {
    fn on_start(&mut self, cx: &mut morel::Ctx) {
        self.call_optional_hook(cx, "on_start");
    }

    fn step(&mut self, cx: &mut morel::Ctx) {
        self.call_optional_hook(cx, "step");
    }

    fn on_stop(&mut self, cx: &mut morel::Ctx) {
        self.call_optional_hook(cx, "on_stop");
    }
}

impl PyCustomOperator {
    fn call_optional_hook(&self, cx: &mut morel::Ctx, name: &str) {
        let result = Python::attach(|py| {
            let operator = self.operator.bind(py);
            let hook = match operator.getattr(name) {
                Ok(hook) => hook,
                Err(err) if err.is_instance_of::<PyAttributeError>(py) => return Ok(()),
                Err(err) => return Err(err),
            };
            let py_ctx = Py::new(py, PyCtx::new(cx))?;
            let active = py_ctx.borrow(py).active();
            let result = hook.call1((py_ctx.bind(py),)).map(|_| ());
            active.set(false);
            result
        });

        if let Err(err) = result {
            cx.fail(callback_error(err));
        }
    }
}

pub(crate) fn graph_add(
    slf: PyRef<'_, PyGraph>,
    py: Python<'_>,
    operator: Py<PyAny>,
) -> PyResult<PyStream> {
    slf.ensure_can_add_nodes()?;
    let owner = Py::from(slf);
    let stream_owner = PyStreamOwner::from(owner.clone_ref(py));
    let stream = owner.borrow(py).graph().try_add(|wire| {
        build_custom_operator(py, wire, stream_owner.clone_ref(py), operator.clone_ref(py))
    })?;
    Ok(PyStream::wrap(stream, owner))
}

pub(crate) fn stream_wire(
    stream: &PyStream,
    py: Python<'_>,
    operator: Py<PyAny>,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    let owner = stream.owner.clone_ref(py);
    let wired = stream.stream.try_wire(|wire| {
        build_custom_operator(py, wire, owner.clone_ref(py), operator.clone_ref(py))
    })?;
    Ok(PyStream::wrap(wired, owner))
}

pub(crate) fn build_custom_operator(
    py: Python<'_>,
    wire: &mut morel::Wire<'_>,
    owner: PyStreamOwner,
    operator: Py<PyAny>,
) -> PyResult<PyCustomOperator> {
    let output_created = Rc::new(Cell::new(false));
    let py_wire = Py::new(py, PyWire::new(wire, owner, output_created.clone()))?;
    let active = py_wire.borrow(py).active();
    let result = operator
        .bind(py)
        .call_method1("wire", (py_wire.bind(py),))
        .map(|_| ());
    active.set(false);

    let err = match result {
        Ok(()) if output_created.get() => None,
        Ok(()) => Some(PyRuntimeError::new_err(
            "operator.wire() must call Wire.output()",
        )),
        Err(err) => Some(err),
    };

    match err {
        Some(err) => Err(err),
        None => Ok(PyCustomOperator { operator }),
    }
}
