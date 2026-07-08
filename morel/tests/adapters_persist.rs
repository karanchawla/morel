use morel::{Graph, Recording, Replay, Time};
use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn ms(n: u64) -> Time {
    Time::EPOCH + Duration::from_millis(n)
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "morel-persist-{label}-{}-{nanos}.jsonl",
        std::process::id()
    ))
}

#[test]
fn save_then_load_round_trips_through_a_file() {
    let path = temp_path("roundtrip");

    let g = Graph::new();
    let src = g.replay_from_iter(vec![(ms(10), 1i64), (ms(10), 2), (ms(40), 3)]);
    let log = Recording::new();
    src.record(&log);
    g.run(Replay::from(Time::EPOCH)).unwrap();
    log.save_json(&path).unwrap();

    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        text.lines().next().unwrap(),
        "[10000000,1]",
        "wire format is one [nanos, value] pair per line"
    );

    let loaded: Recording<i64> = Recording::load_json(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(loaded.take(), vec![(ms(10), 1), (ms(10), 2), (ms(40), 3)]);
}

#[test]
fn load_json_missing_file_is_an_io_error() {
    let missing = temp_path("missing");
    let err = match Recording::<i64>::load_json(&missing) {
        Ok(_) => panic!("missing file unexpectedly loaded"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn load_json_malformed_line_is_invalid_data() {
    let path = temp_path("malformed");
    std::fs::write(&path, "not-json\n").unwrap();

    let err = match Recording::<i64>::load_json(&path) {
        Ok(_) => panic!("malformed file unexpectedly loaded"),
        Err(err) => err,
    };
    std::fs::remove_file(&path).unwrap();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}
