use std::sync::atomic::Ordering;
use std::thread;

use crate::channel::{channel, Capacity, OnClose};
use crate::core::{Ctx, Graph, Stream};

use super::error::{ChannelError, ProducerClosed};
use super::wire::{ChannelTx, Packet};

/// Handle passed to an external live producer.
pub struct Producer<T: Send + 'static> {
    tx: ChannelTx<T>,
}

impl<T: Send + 'static> Producer<T> {
    /// Send one live value to the graph.
    ///
    /// Returns [`ProducerClosed`] after the graph has stopped.
    pub fn send(&self, value: T) -> Result<(), ProducerClosed> {
        self.tx
            .send_packet(Packet::Live(value))
            .map_err(|_| ProducerClosed)
    }

    /// True once the receiving graph has stopped or the channel was closed.
    /// Lets polling feeds exit without having to send.
    pub fn is_closed(&self) -> bool {
        self.tx.closed.load(Ordering::Acquire)
    }
}

/// Start a thread that can push live values into `g`.
///
/// Producers are live-only. Running a graph with this operator in replay mode
/// fails before the producer closure is spawned.
pub fn producer<T, F>(g: &Graph, produce: F) -> Stream<Vec<T>>
where
    T: Send + Clone + 'static,
    F: FnOnce(Producer<T>) + Send + 'static,
{
    let (tx, rx) = channel::<T>(Capacity::Unbounded);
    let cancel_tx = tx.clone_for_cancel();
    super::ops::receiver_stream_with_source_start_and_cancel(
        g,
        rx,
        OnClose::Continue,
        move |cx: &mut Ctx| {
            if !cx.is_live() {
                return Err(ChannelError::ProducerReplay {
                    channel: tx.name.to_string(),
                });
            }
            Ok(thread::spawn(move || {
                produce(Producer { tx });
                Ok(())
            }))
        },
        move || cancel_tx.send_close_best_effort(),
    )
}
