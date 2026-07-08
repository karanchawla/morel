use std::any::Any;
use std::fmt;

/// Error reported by async IO operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncIoError {
    /// This operator is only valid during live runs.
    LiveOnly { op: &'static str },
    /// The bridge channel closed (shutdown, or the counterpart went away).
    Closed { op: &'static str },
    /// The async task returned an expected error.
    Task { op: &'static str, message: String },
    /// The async task ended without reporting a result.
    TaskDropped { op: &'static str },
    /// The async task panicked.
    ///
    /// This variant is observable only when the final binary is built with
    /// unwinding panics; `panic = "abort"` terminates the process before
    /// Morel can classify the spawned-task panic.
    TaskPanic { op: &'static str, message: String },
    /// The async task did not stop before the shutdown timeout.
    ShutdownTimeout { op: &'static str },
    /// A graph-local runtime cannot be created inside an async context.
    NestedRuntime { op: &'static str },
}

impl AsyncIoError {
    pub fn task(op: &'static str, error: impl fmt::Display) -> Self {
        Self::Task {
            op,
            message: error.to_string(),
        }
    }
}

impl fmt::Display for AsyncIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsyncIoError::LiveOnly { op } => write!(f, "{op} can only run in live mode"),
            AsyncIoError::Closed { op } => write!(f, "{op} channel is closed"),
            AsyncIoError::Task { op, message } => write!(f, "{op} failed: {message}"),
            AsyncIoError::TaskDropped { op } => {
                write!(f, "{op} task ended without reporting completion")
            }
            AsyncIoError::TaskPanic { op, message } => {
                write!(f, "{op} task panicked: {message}")
            }
            AsyncIoError::ShutdownTimeout { op } => {
                write!(f, "{op} task did not stop before shutdown timeout")
            }
            AsyncIoError::NestedRuntime { op } => write!(
                f,
                "{op} cannot create a graph-local runtime inside an async \
                 context; inject AsyncIoRuntime::Handle or AsyncIoRuntime::Runtime"
            ),
        }
    }
}

impl std::error::Error for AsyncIoError {}

/// How a spawned bridge task ended, reported over the completion channel.
pub(crate) enum TaskEnd {
    Finished,
    Failed(String),
    Panicked(String),
}

/// The exact Display of the Closed sentinel, for the clean-shutdown match.
pub(crate) fn closed_message(op: &'static str) -> String {
    AsyncIoError::Closed { op }.to_string()
}

pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
