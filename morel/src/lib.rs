//! Morel is a small DAG-based stream processor with deterministic replay.
//!
//! Building an operator registers its node and edges immediately, returning a
//! typed [`Stream`] handle. Values live in framework-owned slots rather than
//! message queues, and replay advances a virtual clock so the same inputs
//! produce the same run.

pub mod adapters;
pub mod channel;
pub mod core;
#[cfg(feature = "async-io")]
pub mod io;
pub mod ops;

pub use crate::adapters::Recording;
pub use crate::channel::{
    channel, producer, source_worker, worker, Capacity, ChannelError, ChannelRx, ChannelTx,
    OnClose, Producer, ProducerClosed,
};
pub use crate::core::{
    init_clock, Ctx, Error, Graph, Input, Live, NodeId, Operator, Output, Replay, RunSpec, Stop,
    Stream, Summary, Time, WakeError, Waker, Wire,
};
#[cfg(feature = "async-io")]
pub use crate::io::{
    produce_async, produce_async_stream, produce_async_stream_with, produce_async_with, AsyncInput,
    AsyncIoConfig, AsyncIoError, AsyncIoRuntime, AsyncProducer, AsyncRunParams,
};
pub use crate::ops::stateful::ToF64;
pub use crate::ops::{gather, merge};
