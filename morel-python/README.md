# Morel

Morel is a deterministic, wicked-fast stream processing framework built for systems where latency and replayability matter. It allows you to build a graph, attach sources and operators, then run it in deterministic replay mode or live mode.

This package is a wrapper around the Rust engine, useful for situations where Python ergonomics matter.

## Install

```bash
python -m pip install morelpy
```

The PyPI package is named `morelpy`; the import name is `morel`:

```python
import morel
```

## Quick Start

```python
import morel

graph = morel.Graph()
events = graph.replay_from_iter([
    (0, {"symbol": "MOREL", "price": 100.0}),
    (10, {"symbol": "MOREL", "price": 101.5}),
    (20, {"symbol": "MOREL", "price": 100.8}),
])

prices = events.map(lambda row: row["price"])
signals = prices.delta().map(lambda change: "up" if change > 0 else "down")
history = signals.history()

graph.run(morel.Replay.from_nanos(0))

assert history.peek() == [(10, "up"), (20, "down")]
```

Replay time is integer nanoseconds. A replay run advances through event and timer times without sleeping, so the same graph and input produce the same result every run.

## From Source

Use this path when developing the bindings locally:

```bash
cd morel-python
python -m pip install maturin
python -m maturin develop
```

Build optional bindings with Rust features:

```bash
python -m maturin develop --features serde,async-io
```

- `serde` enables JSON-lines recording persistence.
- `async-io` enables async consumer helpers.
- CSV replay is always available.

## Core Concepts

`Graph` owns the runtime. You create sources and streams from a graph, then call `graph.run(...)` with either `Replay` or `Live`.

`Stream` is a handle to values flowing through the graph. Streams start empty. After a run, `stream.peek()` returns the most recent value or `None`.

Operators return new streams. Common operators include:

- transforms: `map`, `filter`, `scan`, `map_batch`
- state: `count`, `sum`, `mean`, `delta`, `distinct`, `take`
- timing: `delay`, `throttle`, `debounce`, `timestamp`
- grouping: `buffer`, `window_tumbling`, `window_sliding`, `collapse`
- composition: `sample`, `with_latest`, `gate`, `merge`, `gather`
- observation: `history`, `sink`, `record`

## Sources

Morel Python includes deterministic replay sources for in-memory data and CSV:

```python
graph = morel.Graph()
numbers = graph.replay_from_iter([(0, 1), (1_000, 2), (2_000, 3)])

rows = graph.replay_from_csv(
    "events.csv",
    lambda row: (int(row[0]), row[1]),
)
```

For live input, use `morel.producer(...)` when Python code should push values into a graph during a live run.

## Recording

`Stream.record()` captures `(time, value)` pairs during a run. The resulting `Recording` can be replayed directly:

```python
recording = numbers.map(lambda n: n * n).record()
graph.run(morel.Replay.from_nanos(0))

again = morel.Graph()
replayed = again.replay_from_log(recording).history()
again.run(morel.Replay.from_nanos(0))
```

With the `serde` feature, recordings can be saved to and loaded from JSON-lines files.

## Channels And Workers

Channels connect graphs. They are useful when one graph should feed another, or when a section of work belongs in a child graph.

Workers are a convenience layer for child graph patterns. Use them when you want to isolate a blocking or stateful transform from the parent graph while keeping the graph-level dataflow explicit.

## Custom Operators

Use `Graph.add(...)` and `Stream.wire(...)` when the built-in operators are not enough. A custom operator receives a context object, reads `Input` handles, and writes to `Output` handles. This is the escape hatch for domain-specific state machines while still running inside Morel's scheduler.

## Examples

The `examples/` directory contains runnable examples to get you started:

```bash
python examples/run_all.py
```

## Links

- Rust crate and source: <https://github.com/karanchawla/morel>
- Python package name: `morelpy`
- Python import name: `morel`

## License

Morel is dual-licensed under MIT or Apache-2.0, at your option.
