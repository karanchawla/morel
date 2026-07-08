//! Tokio-backed async IO edge nodes.
//!
//! Use [`produce_async`] / [`produce_async_stream`] to bring live async input
//! into a graph, and [`Stream::consume_async`](crate::Stream::consume_async)
//! to feed graph output to an async sink. Producers are live-only; consumers
//! run in live and replay mode and receive `(Time, value)` pairs, so replay
//! exports preserve virtual time. Record a live source with
//! [`Recording`](crate::Recording) and replay it with
//! [`Graph::replay_from_log`](crate::Graph::replay_from_log) for deterministic
//! reruns.
//!
//! The engine stays synchronous. Tasks are spawned onto a lazily created
//! graph-local runtime by default ([`AsyncIoRuntime::GraphLocal`], two worker
//! threads, dropped with the graph) or onto an injected
//! [`AsyncIoRuntime::Handle`]/[`AsyncIoRuntime::Runtime`]. Bridge channels
//! are bounded ([`AsyncIoConfig::capacity`]) and apply real backpressure in
//! both directions. Shutdown is cooperative: tasks are cancelled, given
//! [`AsyncIoConfig::shutdown_timeout`] to finish, and aborted only after
//! that. Expected IO failures are structured [`AsyncIoError`] graph errors,
//! never panics.

mod consumer;
mod error;
mod producer;
mod runtime;

pub use consumer::AsyncInput;
pub use error::AsyncIoError;
pub use producer::{
    produce_async, produce_async_stream, produce_async_stream_with, produce_async_with,
    AsyncProducer,
};
pub use runtime::{AsyncIoConfig, AsyncIoRuntime, AsyncRunParams};
