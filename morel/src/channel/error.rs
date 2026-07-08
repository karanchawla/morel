use std::fmt;

/// Error reported by channel operators.
#[derive(Debug)]
pub enum ChannelError {
    /// A sender or receiver observed packets that violate the channel protocol.
    Protocol {
        channel: String,
        message: &'static str,
    },
    /// The peer closed while this side was sending or closing.
    Closed {
        channel: String,
        operation: &'static str,
    },
    /// A child graph returned an error.
    ChildRun { channel: String, message: String },
    /// A child thread panicked.
    ChildPanic { channel: String, payload: String },
    /// External producers can only run in live mode.
    ProducerReplay { channel: String },
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelError::Protocol { channel, message } => {
                write!(f, "{channel}: channel protocol error: {message}")
            }
            ChannelError::Closed { channel, operation } => {
                write!(f, "{channel}: channel closed during {operation}")
            }
            ChannelError::ChildRun { channel, message } => {
                write!(f, "{channel}: child graph failed: {message}")
            }
            ChannelError::ChildPanic { channel, payload } => {
                write!(f, "{channel}: child thread panicked: {payload}")
            }
            ChannelError::ProducerReplay { channel } => {
                write!(f, "{channel}: external producer is live-only")
            }
        }
    }
}

impl std::error::Error for ChannelError {}

/// Error returned by [`Producer::send`](crate::channel::Producer::send) after shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerClosed;

impl fmt::Display for ProducerClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("producer channel is closed")
    }
}

impl std::error::Error for ProducerClosed {}

pub(crate) fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
