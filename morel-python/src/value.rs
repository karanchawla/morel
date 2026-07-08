#![allow(dead_code)]

use std::fmt;
use std::time::Duration;

use morel::Time;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyList, PyTuple};

pub(crate) struct PyValue {
    object: Py<PyAny>,
}

impl PyValue {
    pub(crate) fn new(object: Py<PyAny>) -> Self {
        Self { object }
    }

    pub(crate) fn clone_py(&self, py: Python<'_>) -> Self {
        Self::new(self.object.clone_ref(py))
    }

    pub(crate) fn bind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.object.bind(py)
    }

    pub(crate) fn eq_py(&self, py: Python<'_>, other: &Self) -> PyResult<bool> {
        self.bind(py).eq(other.bind(py))
    }

    pub(crate) fn add_py(&self, py: Python<'_>, other: &Self) -> PyResult<Self> {
        Ok(Self::new(self.bind(py).add(other.bind(py))?.unbind()))
    }

    pub(crate) fn sub_py(&self, py: Python<'_>, other: &Self) -> PyResult<Self> {
        Ok(Self::new(self.bind(py).sub(other.bind(py))?.unbind()))
    }

    pub(crate) fn float_py(&self, py: Python<'_>) -> PyResult<f64> {
        self.bind(py).extract::<f64>()
    }

    pub(crate) fn extract_i64(&self, py: Python<'_>) -> PyResult<i64> {
        self.bind(py).extract::<i64>()
    }

    pub(crate) fn extract_len(&self, py: Python<'_>) -> PyResult<usize> {
        self.bind(py).len()
    }
}

impl Clone for PyValue {
    fn clone(&self) -> Self {
        Python::attach(|py| self.clone_py(py))
    }
}

impl fmt::Debug for PyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Python::attach(|py| match self.bind(py).repr() {
            Ok(repr) => write!(f, "PyValue({})", repr.to_string_lossy()),
            Err(_) => write!(f, "PyValue(<repr failed>)"),
        })
    }
}

impl PartialEq for PyValue {
    /// Diagnostic equality for Rust APIs that require `PartialEq`.
    ///
    /// Python equality can fail, but `PartialEq` has no error channel. Graph
    /// behavior that must preserve Python exceptions should call `eq_py`
    /// directly and propagate its `PyErr` instead of relying on this impl.
    fn eq(&self, other: &Self) -> bool {
        Python::attach(|py| self.eq_py(py, other).unwrap_or(false))
    }
}

impl fmt::Display for PyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Python::attach(|py| match self.bind(py).str() {
            Ok(value) => write!(f, "{}", value.to_string_lossy()),
            Err(_) => write!(f, "<str failed>"),
        })
    }
}

pub(crate) fn py_none_value(py: Python<'_>) -> PyValue {
    PyValue::new(py.None())
}

pub(crate) fn py_bool_value(py: Python<'_>, value: bool) -> PyValue {
    PyValue::new(PyBool::new(py, value).to_owned().into_any().unbind())
}

pub(crate) fn py_u64_value(py: Python<'_>, value: u64) -> PyResult<PyValue> {
    Ok(PyValue::new(value.into_pyobject(py)?.unbind().into_any()))
}

pub(crate) fn py_f64_value(py: Python<'_>, value: f64) -> PyValue {
    PyValue::new(PyFloat::new(py, value).into_any().unbind())
}

pub(crate) fn py_time_value(py: Python<'_>, time: Time) -> PyResult<PyValue> {
    py_u64_value(py, time.as_nanos())
}

pub(crate) fn py_pair_value(py: Python<'_>, left: PyValue, right: PyValue) -> PyResult<PyValue> {
    let tuple = PyTuple::new(py, [left.object, right.object])?;
    Ok(PyValue::new(tuple.into_any().unbind()))
}

pub(crate) fn py_time_pair_value(py: Python<'_>, time: Time, value: PyValue) -> PyResult<PyValue> {
    py_pair_value(py, py_time_value(py, time)?, value)
}

pub(crate) fn py_list_value(py: Python<'_>, values: Vec<PyValue>) -> PyResult<PyValue> {
    let list = PyList::new(py, values.into_iter().map(|value| value.object))?;
    Ok(PyValue::new(list.into_any().unbind()))
}

pub(crate) fn py_history_value(py: Python<'_>, history: Vec<(Time, PyValue)>) -> PyResult<PyValue> {
    let values = history
        .into_iter()
        .map(|(time, value)| py_time_pair_value(py, time, value))
        .collect::<PyResult<Vec<_>>>()?;
    py_list_value(py, values)
}

pub(crate) fn expect_bool(value: &PyValue) -> PyResult<bool> {
    Python::attach(|py| value.bind(py).extract::<bool>())
}

pub(crate) fn expect_time_nanos(value: &PyValue) -> PyResult<u64> {
    Python::attach(|py| value.bind(py).extract::<u64>())
}

pub(crate) fn positive_duration_nanos(nanos: u64, name: &str) -> PyResult<Duration> {
    if nanos == 0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be greater than 0"
        )));
    }
    Ok(Duration::from_nanos(nanos))
}

#[cfg(test)]
pub(crate) mod tests {
    use pyo3::exceptions::PyValueError;
    use pyo3::types::PyDict;

    use super::*;

    fn with_python<T>(f: impl for<'py> FnOnce(Python<'py>) -> PyResult<T>) -> PyResult<T> {
        Python::initialize();
        Python::attach(f)
    }

    #[test]
    fn clone_preserves_python_equality() -> PyResult<()> {
        with_python(|py| {
            let value = PyValue::new(vec![1, 2, 3].into_pyobject(py)?.unbind().into_any());
            let cloned = value.clone();

            assert!(value.eq_py(py, &cloned)?);
            assert_eq!(value, cloned);

            Ok(())
        })
    }

    #[test]
    fn arithmetic_helpers_use_python_protocols() -> PyResult<()> {
        with_python(|py| {
            let globals = PyDict::new(py);
            py.run(
                c"
class Addable:
    def __init__(self, value):
        self.value = value
    def __add__(self, other):
        return Addable(self.value + other.value + 10)
    def __sub__(self, other):
        return Addable(self.value - other.value - 5)
",
                Some(&globals),
                None,
            )?;
            let cls = globals.get_item("Addable")?.expect("class exists");
            let left = PyValue::new(cls.call1((7,))?.unbind());
            let right = PyValue::new(cls.call1((2,))?.unbind());

            let added = left.add_py(py, &right)?;
            let subbed = left.sub_py(py, &right)?;

            assert_eq!(added.bind(py).getattr("value")?.extract::<i64>()?, 19);
            assert_eq!(subbed.bind(py).getattr("value")?.extract::<i64>()?, 0);

            Ok(())
        })
    }

    #[test]
    fn list_and_history_helpers_roundtrip_to_python() -> PyResult<()> {
        with_python(|py| {
            let list = py_list_value(
                py,
                vec![
                    py_u64_value(py, 1)?,
                    py_bool_value(py, true),
                    py_f64_value(py, 2.5),
                ],
            )?;
            assert_eq!(list.extract_len(py)?, 3);
            assert_eq!(list.bind(py).get_item(0)?.extract::<u64>()?, 1);
            assert!(list.bind(py).get_item(1)?.extract::<bool>()?);
            assert_eq!(list.bind(py).get_item(2)?.extract::<f64>()?, 2.5);

            let history = py_history_value(
                py,
                vec![
                    (Time::from_nanos(100), py_bool_value(py, false)),
                    (Time::from_nanos(250), py_u64_value(py, 9)?),
                ],
            )?;
            let extracted = history.bind(py);
            assert_eq!(extracted.len()?, 2);
            assert_eq!(
                extracted.get_item(0)?.extract::<(u64, bool)>()?,
                (100, false)
            );
            assert_eq!(extracted.get_item(1)?.extract::<(u64, u64)>()?, (250, 9));

            Ok(())
        })
    }

    #[test]
    fn zero_positive_duration_returns_python_value_error() {
        Python::initialize();
        Python::attach(|py| {
            let err = positive_duration_nanos(0, "interval").expect_err("zero should fail");
            assert!(err.is_instance_of::<PyValueError>(py));
            assert!(err.to_string().contains("interval must be greater than 0"));
        });
    }

    #[test]
    fn partial_eq_treats_python_equality_errors_as_not_equal() -> PyResult<()> {
        with_python(|py| {
            let globals = PyDict::new(py);
            py.run(
                c"
class ExplodingEq:
    def __eq__(self, other):
        raise RuntimeError('eq failed')
",
                Some(&globals),
                None,
            )?;
            let cls = globals.get_item("ExplodingEq")?.expect("class exists");
            let left = PyValue::new(cls.call0()?.unbind());
            let right = PyValue::new(cls.call0()?.unbind());

            assert!(left.eq_py(py, &right).is_err());
            assert_ne!(left, right);

            Ok(())
        })
    }
}
