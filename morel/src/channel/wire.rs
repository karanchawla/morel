use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::core::{Graph, Stream, Time, Waker};

use super::error::ChannelError;

static NEXT_CHANNEL_ID: AtomicU64 = AtomicU64::new(1);

/// Buffering policy for a channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capacity {
    /// Grow as needed.
    Unbounded,
    /// Hold at most this many packets.
    ///
    /// The capacity must be greater than zero.
    Bounded(usize),
}

/// Receiver behavior when the sending side closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnClose {
    /// Stop the receiver graph once all buffered values have been emitted.
    Stop,
    /// Finish the receiver stream without stopping the graph.
    Continue,
}

#[derive(Debug)]
pub(crate) enum Packet<T> {
    Live(T),
    At(Time, T),
    Watermark(Time),
    Close,
}

/// Sending side of a cross-graph channel.
///
/// Attach a sender to a stream with [`ChannelTx::attach`] or
/// [`ChannelTx::attach_with_heartbeat`]. Sends may block when the channel is
/// bounded and full.
pub struct ChannelTx<T: Send + 'static> {
    pub(crate) tx: kanal::Sender<Packet<T>>,
    pub(crate) wake: Arc<OnceLock<Waker>>,
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) name: Arc<str>,
}

/// Receiving side of a cross-graph channel.
///
/// A receiver can only be materialized into one graph.
pub struct ChannelRx<T: Send + 'static> {
    pub(crate) rx: Option<kanal::Receiver<Packet<T>>>,
    pub(crate) wake: Arc<OnceLock<Waker>>,
    pub(crate) name: Arc<str>,
}

/// Create a channel for values sent between graphs.
///
/// In live mode, each delivered packet wakes the receiving graph. In replay
/// mode, packets carry virtual times and the receiver enforces monotonic time.
pub fn channel<T: Send + 'static>(capacity: Capacity) -> (ChannelTx<T>, ChannelRx<T>) {
    let (tx, rx) = match capacity {
        Capacity::Unbounded => kanal::unbounded(),
        Capacity::Bounded(n) => {
            assert!(n > 0, "bounded channel capacity must be greater than zero");
            kanal::bounded(n)
        }
    };
    let id = NEXT_CHANNEL_ID.fetch_add(1, Ordering::Relaxed);
    let name: Arc<str> = Arc::from(format!("channel#{id}"));
    let wake = Arc::new(OnceLock::new());
    let closed = Arc::new(AtomicBool::new(false));
    (
        ChannelTx {
            tx,
            wake: wake.clone(),
            closed: closed.clone(),
            name: name.clone(),
        },
        ChannelRx {
            rx: Some(rx),
            wake,
            name,
        },
    )
}

impl<T: Send + 'static> ChannelTx<T> {
    pub(crate) fn clone_for_cancel(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            wake: self.wake.clone(),
            closed: self.closed.clone(),
            name: self.name.clone(),
        }
    }

    pub(crate) fn send_packet(&self, packet: Packet<T>) -> Result<(), ChannelError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ChannelError::Closed {
                channel: self.name.to_string(),
                operation: "send",
            });
        }
        self.tx.send(packet).map_err(|_| ChannelError::Closed {
            channel: self.name.to_string(),
            operation: "send",
        })?;
        self.ring();
        Ok(())
    }

    pub(crate) fn send_close_best_effort(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.tx.try_send(Packet::Close);
        self.ring();
    }

    pub(crate) fn ring(&self) {
        if let Some(waker) = self.wake.get() {
            let _ = waker.wake();
        }
    }
}

impl<T: Send + 'static> Drop for ChannelTx<T> {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.tx.try_send(Packet::Close);
            self.ring();
        }
    }
}

impl<T: Send + 'static> ChannelRx<T> {
    pub(crate) fn take_rx(&mut self) -> kanal::Receiver<Packet<T>> {
        self.rx
            .take()
            .expect("channel receiver already materialized")
    }
}

impl<T: Clone + Send + 'static> ChannelRx<T> {
    /// Materialize an unpaced receiver stream in `g`.
    ///
    /// Unpaced replay receivers block until the sending side advances time, so
    /// they are intended for cross-thread channels. A same-graph unpaced replay
    /// channel can deadlock; use [`ChannelRx::into_stream_paced`] there.
    pub fn into_stream(self, g: &Graph, on_close: OnClose) -> Stream<Vec<T>> {
        super::ops::receiver_stream_on_core(&g.core, self, None::<&Stream<()>>, on_close)
    }

    /// Materialize a receiver stream that advances at `pace` instants.
    ///
    /// For same-graph replay channels, construct the sender node before this
    /// receiver so ascending node order lets the sender publish before the
    /// receiver checks the boundary.
    pub fn into_stream_paced<P: Clone + 'static>(
        self,
        pace: &Stream<P>,
        on_close: OnClose,
    ) -> Stream<Vec<T>> {
        let core = pace.core();
        super::ops::receiver_stream_on_core(&core, self, Some(pace), on_close)
    }
}
