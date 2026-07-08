# Morel

Morel is a deterministic, wicked-fast stream processing framework built for systems where latency and replayability matter.

A lot of critical software is secretly a stream processor: trading systems, robot fleet analytics, simulation harnesses, monitoring pipelines, and event-driven services all have the same skeleton: ordered inputs, stateful transforms, timers, fanout, joins, and outputs that need to be reproduced later.

Morel makes that shape explicit. It gives you a typed DAG runtime with live execution, deterministic historical replay, cross-graph channels, and worker graphs in a small Rust core you can read.

## Features

### Live and replay modes

Build the graph once. Run it against wall time with `Live`, or against virtual time with `Replay`. Replay advances through recorded or scheduled event times instead of sleeping, so historical runs are fast, deterministic, and use the same topology as live execution.

### Typed DAG runtime

Morel graphs are typed DAGs. Operators register their nodes and edges when the graph is built, before execution starts.

The scheduler runs the graph by propagating fired values through framework-owned slots, avoiding a queue per edge and keeping the hot path small.

### Stateful operators

Morel includes the core stream operators needed for real pipelines:

- `map`, `filter`, `scan`, `count`, `take`, `distinct`
- `merge`, `sample`, `with`
- `delay`, `throttle`, `debounce`
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
Enable optional persistence with feature flags:

```toml
[dependencies]
morel = { version = "0.1", features = ["serde"] }
```
Available features:

- `serde` — JSON-lines recording persistence
- `async-io` — async producers and consumers
- `net` — network-oriented async feature bundle

For Python:

```bash
python -m pip install morelpy
```

The Python distribution is named `morelpy`; the import stays `import morel`.

## Rust Examples

Executable Rust tutorials live in [`morel/examples`](morel/examples). Start with
`quick_start.rs`, then move through stateful transforms, time windows, fan-in,
custom operators, recording/replay, CSV input, channels, workers, and optional
async/serde examples.

## Packages

- Rust crate: [`morel`](morel)
- Python package: [`morelpy`](morel-python)

## License

Morel is dual-licensed under MIT or Apache-2.0, at your option. See
[`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE).
