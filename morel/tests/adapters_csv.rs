use morel::{Graph, Recording, Replay, Time};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn write_csv(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("morel-csv-{}-{name}.csv", std::process::id()));
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path
}

fn parse_row(
    record: &csv::StringRecord,
) -> Result<(Time, i64), Box<dyn std::error::Error + Send + Sync>> {
    let nanos: u64 = record[0].parse()?;
    let value: i64 = record[1].parse()?;
    Ok((Time::from_nanos(nanos), value))
}

#[test]
fn csv_rows_replay_at_their_times() {
    let path = write_csv("happy", "time,value\n10000000,1\n20000000,2\n20000000,3\n");
    let g = Graph::new();
    let src = g.replay_from_csv(&path, parse_row);
    let log = Recording::new();
    src.record(&log);

    g.run(Replay::from(Time::EPOCH)).unwrap();
    std::fs::remove_file(&path).unwrap();

    assert_eq!(log.take(), vec![(ms(10), 1), (ms(20), 2), (ms(20), 3)]);
}

#[test]
fn parse_error_names_the_line() {
    let path = write_csv("bad", "time,value\n10000000,1\nnot-a-number,2\n");
    let g = Graph::new();
    let src = g.replay_from_csv(&path, parse_row);
    let log = Recording::new();
    src.record(&log);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();
    std::fs::remove_file(&path).unwrap();

    assert_eq!(log.take(), vec![(ms(10), 1)], "valid prefix still emits");
    assert!(err.to_string().contains("line 3"), "{err}");
}

#[test]
fn open_failure_fails_the_run() {
    let g = Graph::new();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let missing_dir =
        std::env::temp_dir().join(format!("morel-csv-missing-{}-{unique}", std::process::id()));
    let missing = missing_dir.join("input.csv");
    let _src = g.replay_from_csv(&missing, parse_row);

    let err = g.run(Replay::from(Time::EPOCH)).unwrap_err();

    assert!(err.to_string().contains("input.csv"), "{err}");
}

#[test]
fn large_files_stream_incrementally() {
    let mut contents = String::from("time,value\n");
    for i in 0..50_000u64 {
        contents.push_str(&format!("{},{}\n", i * 1_000, i));
    }
    let path = write_csv("large", &contents);
    let g = Graph::new();
    let sum = g.replay_from_csv(&path, parse_row).sum();

    g.run(Replay::from(Time::EPOCH)).unwrap();
    std::fs::remove_file(&path).unwrap();

    assert_eq!(sum.peek(), Some((0..50_000i64).sum::<i64>()));
}
