#![allow(dead_code)]

use std::error::Error;
use std::fmt;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::GraphError;

#[derive(Debug)]
pub(crate) enum BindingError {
    Type(String),
    Value(String),
    Runtime(String),
}

impl BindingError {
    pub(crate) fn type_error(message: impl Into<String>) -> Self {
        Self::Type(message.into())
    }

    pub(crate) fn value_error(message: impl Into<String>) -> Self {
        Self::Value(message.into())
    }

    pub(crate) fn runtime_error(message: impl Into<String>) -> Self {
        Self::Runtime(message.into())
    }
}

impl fmt::Display for BindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindingError::Type(message)
            | BindingError::Value(message)
            | BindingError::Runtime(message) => f.write_str(message),
        }
    }
}

impl Error for BindingError {}

pub(crate) fn type_error(message: impl Into<String>) -> BindingError {
    BindingError::type_error(message)
}

pub(crate) fn value_error(message: impl Into<String>) -> BindingError {
    BindingError::value_error(message)
}

pub(crate) fn runtime_error(message: impl Into<String>) -> BindingError {
    BindingError::runtime_error(message)
}

#[derive(Debug)]
pub(crate) struct PyCallbackError {
    err: PyErr,
}

impl PyCallbackError {
    pub(crate) fn new(err: PyErr) -> Self {
        Self { err }
    }

    pub(crate) fn clone_py(&self, py: Python<'_>) -> PyErr {
        self.err.clone_ref(py)
    }
}

impl Clone for PyCallbackError {
    fn clone(&self) -> Self {
        Python::attach(|py| Self::new(self.clone_py(py)))
    }
}

impl fmt::Display for PyCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl Error for PyCallbackError {}

pub(crate) fn callback_error(err: PyErr) -> Box<dyn Error + Send + Sync> {
    Box::new(PyCallbackError::new(err))
}

pub(crate) trait ToPyResult<T> {
    fn to_py_result(self) -> PyResult<T>;
}

impl<T, E> ToPyResult<T> for Result<T, E>
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    fn to_py_result(self) -> PyResult<T> {
        self.map_err(|err| error_to_pyerr(err.into()))
    }
}

pub(crate) fn error_to_pyerr(err: Box<dyn Error + Send + Sync>) -> PyErr {
    if err.is::<PyCallbackError>() {
        let err = err
            .downcast::<PyCallbackError>()
            .expect("PyCallbackError type checked before downcast");
        return Python::attach(|py| err.clone_py(py));
    }

    if err.is::<BindingError>() {
        let err = err
            .downcast::<BindingError>()
            .expect("BindingError type checked before downcast");
        return match *err {
            BindingError::Type(message) => PyTypeError::new_err(message),
            BindingError::Value(message) => PyValueError::new_err(message),
            BindingError::Runtime(message) => PyRuntimeError::new_err(message),
        };
    }

    GraphError::new_err(err.to_string())
}

pub(crate) fn morel_error_to_pyerr(err: morel::Error) -> PyErr {
    match err {
        morel::Error::Node { source, .. } => error_to_pyerr(source),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};

    use super::*;

    fn with_python<T>(f: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>) -> PyResult<T> {
        Python::initialize();
        Python::attach(f)
    }

    #[test]
    fn callback_error_roundtrips_original_python_error() -> PyResult<()> {
        with_python(|py| {
            let err = PyValueError::new_err("callback failed");
            let converted = error_to_pyerr(callback_error(err));

            assert!(converted.is_instance_of::<PyValueError>(py));
            assert!(converted.to_string().contains("callback failed"));

            Ok(())
        })
    }

    #[test]
    fn binding_errors_map_to_specific_python_exception_types() -> PyResult<()> {
        with_python(|py| {
            let type_err = error_to_pyerr(Box::new(type_error("bad type")));
            let value_err = error_to_pyerr(Box::new(value_error("bad value")));
            let runtime_err = error_to_pyerr(Box::new(runtime_error("bad runtime")));

            assert!(type_err.is_instance_of::<PyTypeError>(py));
            assert!(type_err.to_string().contains("bad type"));
            assert!(value_err.is_instance_of::<PyValueError>(py));
            assert!(value_err.to_string().contains("bad value"));
            assert!(runtime_err.is_instance_of::<PyRuntimeError>(py));
            assert!(runtime_err.to_string().contains("bad runtime"));

            Ok(())
        })
    }

    #[test]
    fn generic_rust_errors_map_to_graph_error() -> PyResult<()> {
        with_python(|py| {
            let err = error_to_pyerr(Box::new(io::Error::other("plain rust failure")));

            assert!(err.is_instance_of::<GraphError>(py));
            assert!(err.to_string().contains("plain rust failure"));

            Ok(())
        })
    }

    #[test]
    fn morel_node_errors_preserve_callback_python_error() -> PyResult<()> {
        with_python(|py| {
            let graph = morel::Graph::new();
            let mut err = Some(callback_error(PyValueError::new_err(
                "node callback failed",
            )));
            let _stream =
                graph
                    .just(())
                    .try_map(move |_| -> Result<(), Box<dyn Error + Send + Sync>> {
                        Err(err.take().expect("operator should run once"))
                    });

            let morel_err = graph
                .run(morel::Replay::from(morel::Time::EPOCH))
                .expect_err("operator should fail");
            let converted = morel_error_to_pyerr(morel_err);

            assert!(converted.is_instance_of::<PyValueError>(py));
            assert!(converted.to_string().contains("node callback failed"));

            Ok(())
        })
    }
}
