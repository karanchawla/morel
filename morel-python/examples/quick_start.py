#!/usr/bin/env python3
"""Build and run a minimal deterministic replay pipeline.

This example creates a graph, replays timestamped price events, maps each price
into a small record, filters for prices above 100, and reads the resulting
history after the replay finishes. It is the shortest end-to-end path through
Morel's Python API: source, stateless operators, sink-like history collection,
and `Graph.run`.
"""

import morel


def main():
    graph = morel.Graph()
    prices = graph.replay_from_iter([(0, 99.5), (10, 101.25), (20, 98.75), (30, 104.0)])

    signals = (
        prices.map(lambda price: {"price": price, "above_100": price > 100.0})
        .filter(lambda row: row["above_100"])
        .history()
    )

    summary = graph.run(morel.Replay.from_nanos(0))

    print("summary:", summary.steps, summary.started_at, summary.ended_at)
    print("signals:", signals.peek())


if __name__ == "__main__":
    main()
