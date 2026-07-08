#[cfg(feature = "serde")]
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList, PyTuple};
#[cfg(feature = "serde")]
use pyo3::types::{PyBool, PyBoolMethods, PyDict, PyFloat, PyFloatMethods, PyInt, PyString};

#[cfg(feature = "serde")]
use crate::error::morel_error_to_pyerr;
use crate::value::PyValue;

#[pyclass(name = "Recording")]
pub(crate) struct PyRecording {
    pub(crate) recording: morel::Recording<PyValue>,
}

#[pymethods]
impl PyRecording {
    #[new]
    fn new() -> Self {
        Self {
            recording: morel::Recording::new(),
        }
    }

    fn take(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for (time, value) in self.recording.take() {
            let nanos = time.as_nanos().into_pyobject(py)?.unbind().into_any();
            let value = value.bind(py).clone().unbind();
            let tuple = PyTuple::new(py, [nanos, value])?;
            list.append(tuple)?;
        }
        Ok(list.into_any().unbind())
    }

    #[cfg(feature = "serde")]
    fn save_json(&self, py: Python<'_>, path: std::path::PathBuf) -> PyResult<()> {
        let entries = self.recording.take();
        let result = write_json_entries(py, &entries, path);
        let restore_result = replace_recording_entries(&self.recording, entries);
        result.and(restore_result)
    }

    #[cfg(feature = "serde")]
    #[staticmethod]
    fn load_json(py: Python<'_>, path: std::path::PathBuf) -> PyResult<Self> {
        use std::io::BufRead;

        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let recording = morel::Recording::new();
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let (nanos, value): (u64, serde_json::Value) = serde_json::from_str(&line)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
            entries.push((morel::Time::from_nanos(nanos), json_to_py_value(py, value)?));
        }
        replace_recording_entries(&recording, entries)?;
        Ok(Self { recording })
    }
}

#[cfg(feature = "serde")]
fn write_json_entries(
    py: Python<'_>,
    entries: &[(morel::Time, PyValue)],
    path: std::path::PathBuf,
) -> PyResult<()> {
    use std::io::Write;

    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);
    for (time, value) in entries {
        let value = py_any_to_json(value.bind(py))?;
        serde_json::to_writer(&mut writer, &(time.as_nanos(), value))
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        writeln!(&mut writer)?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(feature = "serde")]
fn replace_recording_entries(
    recording: &morel::Recording<PyValue>,
    entries: Vec<(morel::Time, PyValue)>,
) -> PyResult<()> {
    recording.take();
    if entries.is_empty() {
        return Ok(());
    }

    let graph = morel::Graph::new();
    let stream = graph.replay_from_log(entries);
    stream.record(recording);
    graph
        .run(morel::Replay::from(morel::Time::EPOCH))
        .map(|_| ())
        .map_err(morel_error_to_pyerr)
}

#[cfg(feature = "serde")]
fn py_any_to_json(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(value) = value.cast_exact::<PyBool>() {
        return Ok(serde_json::Value::Bool(value.is_true()));
    }

    if let Ok(value) = value.cast_exact::<PyInt>() {
        if let Ok(value) = value.extract::<i64>() {
            return Ok(serde_json::Value::Number(value.into()));
        }
        if let Ok(value) = value.extract::<u64>() {
            return Ok(serde_json::Value::Number(value.into()));
        }
        return Err(PyValueError::new_err("integer is out of JSON number range"));
    }

    if let Ok(value) = value.cast_exact::<PyFloat>() {
        let value = value.value();
        let number = serde_json::Number::from_f64(value)
            .ok_or_else(|| PyValueError::new_err("float must be finite"))?;
        return Ok(serde_json::Value::Number(number));
    }

    if let Ok(value) = value.cast_exact::<PyString>() {
        return Ok(serde_json::Value::String(value.to_str()?.to_owned()));
    }

    if let Ok(value) = value.cast_exact::<PyList>() {
        let values = value
            .iter()
            .map(|item| py_any_to_json(&item))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(serde_json::Value::Array(values));
    }

    if let Ok(value) = value.cast_exact::<PyTuple>() {
        let values = value
            .iter()
            .map(|item| py_any_to_json(&item))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(serde_json::Value::Array(values));
    }

    if let Ok(value) = value.cast_exact::<PyDict>() {
        let mut values = serde_json::Map::new();
        for (key, item) in value {
            let key = key
                .cast_exact::<PyString>()
                .map_err(|_| PyTypeError::new_err("JSON object keys must be strings"))?
                .to_str()?
                .to_owned();
            values.insert(key, py_any_to_json(&item)?);
        }
        return Ok(serde_json::Value::Object(values));
    }

    Err(PyTypeError::new_err(format!(
        "object of type {} is not JSON serializable",
        value.get_type().name()?
    )))
}

#[cfg(feature = "serde")]
fn json_to_py_value(py: Python<'_>, value: serde_json::Value) -> PyResult<PyValue> {
    let object = match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(value) => PyBool::new(py, value).to_owned().into_any().unbind(),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.into_pyobject(py)?.unbind().into_any()
            } else if let Some(value) = value.as_u64() {
                value.into_pyobject(py)?.unbind().into_any()
            } else {
                value
                    .as_f64()
                    .expect("serde_json numbers are i64, u64, or finite f64")
                    .into_pyobject(py)?
                    .unbind()
                    .into_any()
            }
        }
        serde_json::Value::String(value) => value.into_pyobject(py)?.unbind().into_any(),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                let value = json_to_py_value(py, value)?;
                list.append(value.bind(py))?;
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                let value = json_to_py_value(py, value)?;
                dict.set_item(key, value.bind(py))?;
            }
            dict.into_any().unbind()
        }
    };
    Ok(PyValue::new(object))
}
