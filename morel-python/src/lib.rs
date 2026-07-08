use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(morel._morel, GraphError, PyException);

#[cfg(feature = "async-io")]
mod async_io;
mod channel;
mod csv;
mod custom;
mod error;
mod graph;
mod ops;
mod recording;
mod stream;
mod value;

#[pymodule]
fn _morel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("GraphError", module.py().get_type::<GraphError>())?;
    module.add_class::<custom::PyInput>()?;
    module.add_class::<custom::PyOutput>()?;
    module.add_class::<custom::PyWire>()?;
    module.add_class::<custom::PyCtx>()?;
    module.add_class::<channel::PyCapacity>()?;
    module.add_class::<channel::PyOnClose>()?;
    module.add_class::<channel::PyChannelTx>()?;
    module.add_class::<channel::PyChannelRx>()?;
    module.add_class::<channel::PyProducer>()?;
    module.add_class::<channel::PyChildGraph>()?;
    module.add_class::<channel::PyChildStream>()?;
    module.add_class::<graph::PyGraph>()?;
    module.add_class::<stream::PyStream>()?;
    module.add_class::<graph::PyReplay>()?;
    module.add_class::<graph::PyLive>()?;
    module.add_class::<graph::PyStop>()?;
    module.add_class::<graph::PySummary>()?;
    module.add_class::<recording::PyRecording>()?;
    module.add_function(wrap_pyfunction!(ops::combine::merge, module)?)?;
    module.add_function(wrap_pyfunction!(ops::combine::gather, module)?)?;
    module.add_function(wrap_pyfunction!(channel::channel, module)?)?;
    module.add_function(wrap_pyfunction!(channel::producer, module)?)?;
    module.add_function(wrap_pyfunction!(channel::worker, module)?)?;
    module.add_function(wrap_pyfunction!(channel::source_worker, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
