//! Cross-graph channels and worker graph helpers.
//!
//! Channels move values between graph runs. In live mode they wake the receiver
//! graph when a value arrives; in replay mode they carry virtual timestamps so
//! the receiver can preserve deterministic time.

mod error;
mod ops;
mod producer;
mod wire;
mod worker;

pub use error::{ChannelError, ProducerClosed};
pub use producer::{producer, Producer};
pub use wire::{channel, Capacity, ChannelRx, ChannelTx, OnClose};
pub use worker::{source_worker, worker};
