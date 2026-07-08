use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::error::callback_error;
use crate::stream::PyStream;
use crate::value::py_none_value;

pub(crate) fn consume_async(
    stream: &PyStream,
    py: Python<'_>,
    callback: Py<PyAny>,
) -> PyResult<PyStream> {
    stream.ensure_can_add_nodes(py)?;
    stream.owner.mark_requires_detached_run(py);
    let owner = stream.owner.clone_ref(py);
    let consumed = stream
        .stream
        .consume_async_boxed(move |_params, mut input| async move {
            while let Some((time, value)) = input.next().await {
                Python::attach(|py| {
                    let arg = value.clone_py(py);
                    callback
                        .bind(py)
                        .call1((time.as_nanos(), arg.bind(py)))
                        .map(|_| ())
                })
                .map_err(callback_error)?;
            }
            Ok(())
        })
        .map(|()| Python::attach(py_none_value));
    Ok(PyStream::wrap(consumed, owner))
}
