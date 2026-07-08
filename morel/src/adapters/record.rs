use std::sync::{Arc, Mutex};

use crate::core::graph::{Ctx, Graph, Operator, Stream};
use crate::core::port::{Input, Output};
use crate::core::time::Time;

/// Shared log of `(time, value)` pairs captured by [`Stream::record`].
pub struct Recording<T>(Arc<Mutex<Vec<(Time, T)>>>);

impl<T> Recording<T> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    /// Drain the recording; call after the run.
    pub fn take(&self) -> Vec<(Time, T)> {
        std::mem::take(
            &mut self
                .0
                .lock()
                .expect("recording mutex poisoned while draining"),
        )
    }
}

impl<T> Clone for Recording<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Default for Recording<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct Record<T> {
    input: Input<T>,
    log: Recording<T>,
    out: Output<()>,
}

impl<T: Clone + 'static> Operator for Record<T> {
    fn step(&mut self, cx: &mut Ctx) {
        self.log
            .0
            .lock()
            .expect("recording mutex poisoned while recording")
            .push((cx.now(), self.input.get()));
        self.out.set(());
    }
}

impl<T: Clone + 'static> Stream<T> {
    /// Append every fire of this stream to `log` as `(time, value)`.
    ///
    /// Works in both modes. Recording a live run's external boundaries and
    /// replaying them with [`Graph::replay_from_log`] through the same
    /// topology reproduces the downstream output bit-identically.
    pub fn record(&self, log: &Recording<T>) -> Stream<()> {
        self.wire(|w| Record {
            input: w.on(self),
            log: Recording::clone(log),
            out: w.output(),
        })
    }
}

impl Graph {
    /// Replay a recording taken from a previous run.
    pub fn replay_from_log<T: Clone + 'static>(&self, log: Vec<(Time, T)>) -> Stream<T> {
        self.replay_from_iter(log)
    }
}

#[cfg(feature = "serde")]
impl<T: serde::Serialize> Recording<T> {
    /// Write the recording as JSON-lines: one `[nanos, value]` pair per line.
    pub fn save_json(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        use std::io::Write;

        let mut w = std::io::BufWriter::new(std::fs::File::create(path)?);
        let entries = self
            .0
            .lock()
            .expect("recording mutex poisoned while saving");
        for entry in entries.iter() {
            serde_json::to_writer(&mut w, entry).map_err(std::io::Error::from)?;
            writeln!(w)?;
        }
        w.flush()
    }
}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> Recording<T> {
    /// Load a recording written by [`Recording::save_json`].
    pub fn load_json(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        use std::io::BufRead;

        let mut entries = Vec::new();
        for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
            let entry = serde_json::from_str(&line?).map_err(std::io::Error::from)?;
            entries.push(entry);
        }
        Ok(Self(Arc::new(Mutex::new(entries))))
    }
}
