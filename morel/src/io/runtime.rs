use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Builder, Handle, Runtime};

use crate::channel::Capacity;
use crate::core::graph::{Ctx, GraphCore};
use crate::core::Time;

use super::error::AsyncIoError;

/// Runtime and buffering settings used by async IO operators.
#[derive(Clone, Debug)]
pub struct AsyncIoConfig {
    pub capacity: Capacity,
    pub runtime: AsyncIoRuntime,
    pub shutdown_timeout: Duration,
}

impl Default for AsyncIoConfig {
    fn default() -> Self {
        Self {
            capacity: Capacity::Bounded(1024),
            runtime: AsyncIoRuntime::GraphLocal,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

impl AsyncIoConfig {
    pub fn with_capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_runtime(mut self, runtime: AsyncIoRuntime) -> Self {
        self.runtime = runtime;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

/// Where async IO tasks are spawned.
#[derive(Clone, Debug)]
pub enum AsyncIoRuntime {
    /// Lazily build a small runtime owned by the graph, dropped with it.
    GraphLocal,
    /// Spawn onto a caller-owned runtime; the caller keeps it alive.
    Handle(Handle),
    /// Spawn onto a shared runtime this node co-owns.
    Runtime(Arc<Runtime>),
}

impl AsyncIoRuntime {
    pub(crate) fn resolve(
        &self,
        core: &Rc<GraphCore>,
        op: &'static str,
    ) -> Result<Handle, AsyncIoError> {
        match self {
            AsyncIoRuntime::Handle(handle) => Ok(handle.clone()),
            AsyncIoRuntime::Runtime(runtime) => Ok(runtime.handle().clone()),
            AsyncIoRuntime::GraphLocal => {
                if let Some(runtime) = core.async_runtime.get() {
                    return Ok(runtime.handle().clone());
                }
                if Handle::try_current().is_ok() {
                    return Err(AsyncIoError::NestedRuntime { op });
                }
                let runtime = Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name("morel-async-io")
                    .build()
                    .map_err(|e| AsyncIoError::task(op, e))?;
                let handle = runtime.handle().clone();
                let _ = core.async_runtime.set(Arc::new(runtime));
                Ok(handle)
            }
        }
    }
}

/// Run context passed to async producer and consumer closures.
#[derive(Clone, Copy, Debug)]
pub struct AsyncRunParams {
    pub is_live: bool,
    pub started_at: Time,
    pub end_at: Option<Time>,
}

pub(crate) fn run_params(cx: &Ctx) -> AsyncRunParams {
    let end_at = cx.core.end_at.get();
    AsyncRunParams {
        is_live: cx.is_live(),
        started_at: cx.started_at(),
        end_at: (end_at != Time::MAX).then_some(end_at),
    }
}

pub(crate) fn make_channel<T>(capacity: Capacity) -> (kanal::Sender<T>, kanal::Receiver<T>) {
    match capacity {
        Capacity::Unbounded => kanal::unbounded(),
        Capacity::Bounded(n) => {
            assert!(n > 0, "bounded async IO capacity must be greater than zero");
            kanal::bounded(n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Graph;

    #[test]
    fn graph_local_runtime_is_lazy_and_cached() {
        let g = Graph::new();
        assert!(
            g.core.async_runtime.get().is_none(),
            "no runtime until used"
        );

        let first = AsyncIoRuntime::GraphLocal.resolve(&g.core, "test").unwrap();
        assert!(g.core.async_runtime.get().is_some());

        let second = AsyncIoRuntime::GraphLocal.resolve(&g.core, "test").unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let _task = second.spawn(async move { tx.send(1).unwrap() });
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        drop(first);
    }

    #[test]
    fn injected_runtimes_bypass_the_graph_cell() {
        let rt = Runtime::new().unwrap();
        let g = Graph::new();

        AsyncIoRuntime::Handle(rt.handle().clone())
            .resolve(&g.core, "test")
            .unwrap();
        AsyncIoRuntime::Runtime(Arc::new(Runtime::new().unwrap()))
            .resolve(&g.core, "test")
            .unwrap();

        assert!(g.core.async_runtime.get().is_none());
    }

    #[test]
    fn graph_local_inside_async_context_is_rejected() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let g = Graph::new();
            let err = AsyncIoRuntime::GraphLocal
                .resolve(&g.core, "test")
                .unwrap_err();
            assert!(matches!(err, AsyncIoError::NestedRuntime { .. }));
            assert!(g.core.async_runtime.get().is_none());
        });
    }
}
