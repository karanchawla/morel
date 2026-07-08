//! Shows how to persist a recording as JSON-lines, load it, and replay it.

use morel::{Graph, Recording, Replay, Time};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq)]
pub struct RecordingJsonOutput {
    pub file_first_line: String,
    pub loaded_log: Vec<(u64, i64)>,
    pub replayed_log: Vec<(u64, i64)>,
}

pub fn run() -> Result<RecordingJsonOutput, Box<dyn std::error::Error>> {
    let path = temp_jsonl_path()?;
    let output = run_with_path(&path);
    let cleanup = match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && output.is_err() => Ok(()),
        Err(err) => Err(err),
    };

    match (output, cleanup) {
        (Ok(output), Ok(())) => Ok(output),
        (Ok(_), Err(err)) => Err(Box::new(err)),
        (Err(err), Ok(())) | (Err(err), Err(_)) => Err(err),
    }
}

fn run_with_path(path: &Path) -> Result<RecordingJsonOutput, Box<dyn std::error::Error>> {
    let graph = Graph::new();
    let input = graph.replay_from_iter([(ms(10), 1i64), (ms(10), 2), (ms(40), 3)]);
    let recording = Recording::new();
    let _record_input = input.record(&recording);

    graph.run(Replay::from(Time::EPOCH))?;
    recording.save_json(path)?;

    let file_first_line = first_line(path)?;
    let loaded: Recording<i64> = Recording::load_json(path)?;
    let loaded_log = loaded.take();

    let replay_graph = Graph::new();
    let replayed_input = replay_graph.replay_from_log(loaded_log.clone());
    let replayed_recording = Recording::new();
    let _record_replayed = replayed_input.record(&replayed_recording);

    replay_graph.run(Replay::from(Time::EPOCH))?;
    let replayed_log = replayed_recording.take();

    Ok(RecordingJsonOutput {
        file_first_line,
        loaded_log: time_history(loaded_log),
        replayed_log: time_history(replayed_log),
    })
}

fn first_line(path: &Path) -> Result<String, std::io::Error> {
    std::io::BufReader::new(std::fs::File::open(path)?)
        .lines()
        .next()
        .transpose()?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "empty recording"))
}

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn temp_jsonl_path() -> Result<PathBuf, std::time::SystemTimeError> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "morel-recording-json-{}-{nanos}.jsonl",
        std::process::id()
    )))
}

fn time_history<T>(values: Vec<(Time, T)>) -> Vec<(u64, T)> {
    values
        .into_iter()
        .map(|(time, value)| (time.as_nanos(), value))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = run()?;

    println!("file_first_line={}", output.file_first_line);
    println!("loaded_log={:?}", output.loaded_log);
    println!("replayed_log={:?}", output.replayed_log);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_json_round_trips_through_file_and_replay() {
        let output = run().unwrap();

        assert_eq!(output.file_first_line, "[10000000,1]");
        assert_eq!(
            output.loaded_log,
            vec![(10_000_000, 1), (10_000_000, 2), (40_000_000, 3)]
        );
        assert_eq!(output.replayed_log, output.loaded_log);
    }
}
