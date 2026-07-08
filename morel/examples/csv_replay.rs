//! Demonstrates replaying timestamped CSV rows into a deterministic stream graph.

use morel::{Graph, Replay, Time};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CSV_DATA: &str = "\
time_nanos,device,value
0,pump,10
10000000,pump,15
20000000,valve,7
30000000,pump,12
";

#[derive(Clone, Debug, PartialEq)]
struct Reading {
    device: String,
    value: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CsvReplayOutput {
    pub pump_values: Vec<(u64, i64)>,
    pub pump_sum: Option<i64>,
}

pub fn run() -> Result<CsvReplayOutput, Box<dyn std::error::Error>> {
    let path = write_temp_csv()?;
    let output = run_graph(&path);
    let cleanup = std::fs::remove_file(&path);

    match (output, cleanup) {
        (Ok(output), Ok(())) => Ok(output),
        (Ok(_), Err(cleanup_err)) => Err(format!(
            "failed to remove temporary CSV {}: {cleanup_err}",
            path.display()
        )
        .into()),
        (Err(graph_err), Ok(())) => Err(Box::new(graph_err)),
        // If both operations fail, report both so the graph failure is not
        // hidden and the leaked temporary file is still visible to callers.
        (Err(graph_err), Err(cleanup_err)) => Err(format!(
            "graph run failed: {graph_err}; failed to remove temporary CSV {}: {cleanup_err}",
            path.display()
        )
        .into()),
    }
}

fn run_graph(path: &Path) -> Result<CsvReplayOutput, morel::Error> {
    let graph = Graph::new();
    let readings = graph.replay_from_csv(path, parse_row);
    let pump_readings = readings.filter(|reading| reading.device == "pump");
    let pump_values = pump_readings
        .map(|reading| reading.value)
        .timestamp()
        .history();
    let pump_sum = pump_readings.map(|reading| reading.value).sum();

    graph.run(Replay::from(Time::EPOCH))?;

    Ok(CsvReplayOutput {
        pump_values: pump_values_history(
            pump_values
                .peek()
                .expect("pump reading history should emit during replay"),
        ),
        pump_sum: pump_sum.peek(),
    })
}

fn parse_row(
    row: &csv::StringRecord,
) -> Result<(Time, Reading), Box<dyn std::error::Error + Send + Sync>> {
    let time = Time::from_nanos(row.get(0).ok_or("missing time_nanos")?.parse::<u64>()?);
    let device = row.get(1).ok_or("missing device")?.to_string();
    let value = row.get(2).ok_or("missing value")?.parse::<i64>()?;

    Ok((time, Reading { device, value }))
}

fn write_temp_csv() -> std::io::Result<PathBuf> {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    path.push(format!(
        "morel-csv-replay-{}-{nanos}.csv",
        std::process::id()
    ));
    std::fs::write(&path, CSV_DATA)?;
    Ok(path)
}

fn pump_values_history(values: Vec<(Time, (Time, i64))>) -> Vec<(u64, i64)> {
    values
        .into_iter()
        .map(|(_history_time, (event_time, value))| (event_time.as_nanos(), value))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("pump_values={:?}", output.pump_values);
    println!("pump_sum={:?}", output.pump_sum);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_replay_filters_and_sums_pump_values() {
        let output = run().unwrap();

        assert_eq!(
            output.pump_values,
            vec![(0, 10), (10_000_000, 15), (30_000_000, 12)]
        );
        assert_eq!(output.pump_sum, Some(37));
    }

    #[test]
    fn temp_csv_writer_reports_io_result() -> Result<(), Box<dyn std::error::Error>> {
        let path = write_temp_csv()?;

        assert!(path.exists());
        std::fs::remove_file(path)?;

        Ok(())
    }
}
