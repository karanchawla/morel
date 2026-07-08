use std::error::Error;
use std::path::Path;

use crate::adapters::replay::{replay_from_fallible, FallibleItems};
use crate::core::graph::{Graph, Stream};
use crate::core::time::Time;

impl Graph {
    /// Replay rows from a CSV file. `parse` maps a record to `(time, value)`;
    /// rows must be in non-decreasing time order. The first row is treated as
    /// a header and skipped.
    ///
    /// The file is opened eagerly but read incrementally, so large backtests
    /// stream. Open, read, and parse errors fail the run with the file and
    /// line position in the message. Replay-only, like every replay source.
    pub fn replay_from_csv<T, F>(&self, path: impl AsRef<Path>, mut parse: F) -> Stream<T>
    where
        T: Clone + 'static,
        // CSV parse failures use the same run-failure channel as operators,
        // so they must be sendable for worker child graphs.
        F: FnMut(&csv::StringRecord) -> Result<(Time, T), Box<dyn Error + Send + Sync>> + 'static,
    {
        let shown = path.as_ref().display().to_string();
        let items: FallibleItems<T> = match csv::Reader::from_path(path.as_ref()) {
            Ok(reader) => Box::new(reader.into_records().map(move |record| match record {
                Ok(record) => {
                    let line = record.position().map_or(0, |p| p.line());
                    parse(&record).map_err(|e| format!("{shown}: line {line}: {e}"))
                }
                Err(e) => Err(format!("{shown}: {e}")),
            })),
            Err(e) => Box::new(std::iter::once(Err(format!("{shown}: {e}")))),
        };
        replay_from_fallible(self, items)
    }
}
