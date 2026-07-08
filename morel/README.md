# Morel

Morel is a deterministic, wicked-fast stream processing framework built for systems where latency and replayability matter.

A lot of critical software is secretly a stream processor: trading systems, robot fleet analytics, simulation harnesses, monitoring pipelines, and event-driven services all have the same skeleton: ordered inputs, stateful transforms, timers, fanout, joins, and outputs that need to be reproduced later.

Morel makes that shape explicit. This crate provides the Rust core: a typed DAG runtime with live execution, deterministic historical replay, cross-graph channels, and worker graphs.

## Features

### Live and replay modes

Build the graph once. Run it against wall time with `Live`, or against virtual time with `Replay`. Replay advances through recorded or scheduled event times instead of sleeping, so historical runs are fast, deterministic, and use the same topology as live execution.

### Typed DAG runtime

Morel graphs are typed DAGs. Operators register their nodes and edges when the graph is built, before execution starts.

The scheduler runs the graph by propagating fired values through framework-owned slots, avoiding a queue per edge and keeping the hot path small.

### Stateful operators

Morel includes the core stream operators needed for real pipelines:

- `map`, `filter`, `filter_map`, `inspect`, `sink`
- `scan`, `reduce`, `count`, `sum`, `mean`, `delta`, `take`, `distinct`, `history`
- `merge`, `gather`, `sample`, `with`, `with_latest`, `gate`, `unzip`
- `delay`, `throttle`, `debounce`, `timestamp`
- batching and time windows

Timed operators use engine time, so they work naturally in both live and replay modes.

### Cross-graph channels and workers

Graphs can talk to other graphs. Live channels move values across graph boundaries. Replay channels preserve timestamps, so multi-graph systems can still be tested and reproduced deterministically.

Worker graphs build on those channels. Morel allows you to move slow, blocking, or isolated work into child graphs.

## Installation

Add the Rust crate from crates.io:

```toml
[dependencies]
morel = "0.1"
```

Enable optional features when you need them:

```toml
[dependencies]
morel = { version = "0.1", features = ["serde", "async-io"] }
```

Available features:

- `serde` — JSON-lines recording persistence
- `async-io` — async producers and consumers
- `net` — network-oriented async feature bundle

## Quick Start

```rust
use morel::{Graph, Replay, Time};

fn main() -> Result<(), morel::Error> {
    let graph = Graph::new();
    let events = graph.replay_from_iter([
        (Time::from_nanos(0), 10),
        (Time::from_nanos(5), 12),
        (Time::from_nanos(10), 15),
    ]);

    let changes = events.delta().history();
    graph.run(Replay::from(Time::EPOCH))?;

    assert_eq!(
        changes.peek(),
        Some(vec![(Time::from_nanos(5), 2), (Time::from_nanos(10), 3)])
    );

    Ok(())
}
```

## Rust Examples

Executable Rust tutorials live in [`examples/`](examples). Start with:

```bash
cargo run -p morel --example quick_start
cargo run -p morel --example operator_gallery
cargo run -p morel --example recording_replay
cargo run -p morel --features serde --example recording_json
cargo run -p morel --features async-io --example async_consumer
```

The examples cover replay, stateful transforms, time windows, fan-in, custom operators, recording/replay, CSV input, channels, workers, and optional async/serde APIs.

## Benchmarks

Criterion benchmarks live in [`benchmarks/`](benchmarks):

```bash
cargo bench -p morel
```

The benchmark report focuses on graph overhead and generic streaming workloads so changes to scheduler internals can be evaluated with repeatable numbers.

## Related Packages

- Repository README: [`../README.md`](../README.md)
- Python package: [`morelpy`](../morel-python)

## License

Morel is dual-licensed under MIT or Apache-2.0, at your option. See [`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE).
