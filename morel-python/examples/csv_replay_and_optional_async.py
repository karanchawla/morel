#!/usr/bin/env python3
"""Replay CSV input and demonstrate optional async consumer support.

This example creates a temporary CSV file, replays it in a parent graph, and
also replays it inside a source worker graph. The second half checks whether
the package was built with the optional `async-io` feature; when enabled, it
shows `consume_async`, and when disabled it prints a clear fallback message.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import morel


def csv_demo():
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "events.csv"
        path.write_text("time,value\n0,alpha\n10,beta\n", encoding="utf-8")

        graph = morel.Graph()
        csv_events = graph.replay_from_csv(path, lambda row: (int(row[0]), row[1].upper()))
        parent = csv_events.history()

        def build(child):
            return child.replay_from_csv(path, lambda row: (int(row[0]), f"child-{row[1]}"))

        child_events = morel.source_worker(graph, build).collapse().history()
        graph.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))

        print("csv parent:", parent.peek())
        print("csv child:", child_events.peek())


def async_demo():
    if not hasattr(morel.Stream, "consume_async"):
        print("async consumer: rebuild with --features async-io to enable")
        return

    seen = []
    graph = morel.Graph()
    src = graph.replay_from_iter([(0, "a"), (10, "b")])
    ticks = src.consume_async(lambda time_nanos, value: seen.append((time_nanos, value))).history()

    graph.run(morel.Replay.from_nanos(0))

    print("async seen:", seen)
    print("async ticks:", ticks.peek())


def main():
    csv_demo()
    async_demo()


if __name__ == "__main__":
    main()
