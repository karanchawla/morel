# Morel Rust Examples

These examples are executable tutorials: each one is a small Rust program you can run, read, and test while learning Morel's stream APIs.

Run an example:

```sh
cargo run -p morel --example quick_start
```

Run all Rust examples:

```sh
cargo test -p morel --examples
```

Run feature-gated examples:

```sh
cargo run -p morel --features serde --example recording_json
cargo run -p morel --features async-io --example async_consumer
```

## Suggested Order

| Example | Description |
| --- | --- |
| `quick_start` | Replay prices, filter signals, and inspect history. |
| `operator_gallery` | Tour stateless and stateful stream operators. |
| `stateful_word_count` | Maintain word counts with `scan`. |
| `timed_sensor_windows` | Use timed windows over sensor readings. |
| `fan_in_control` | Combine data streams with control signals. |
| `custom_operator` | Build a custom graph operator. |
| `recording_replay` | Record stream output and replay it. |
| `csv_replay` | Replay events from CSV data. |
| `channels_workers` | Connect channels and worker graphs. |
| `recording_json` | Persist recordings as JSON with `serde`. |
| `async_consumer` | Consume streams from async code. |
