#!/usr/bin/env python3
"""Record a replay stream and feed the recording back into another graph.

This example records timestamped device-state events, inspects the in-memory
recording, then replays that log through a fresh graph. If the optional serde
feature is available, it also demonstrates saving and loading the recording as
JSON lines; otherwise it prints the feature-gated fallback message.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import morel


def main():
    graph = morel.Graph()
    recording = morel.Recording()
    events = graph.replay_from_iter(
        [
            (0, {"device": "pump", "state": "on"}),
            (10, {"device": "pump", "state": "steady"}),
            (20, {"device": "pump", "state": "off"}),
        ]
    )

    ticks = events.record(recording).history()
    graph.run(morel.Replay.from_nanos(0))

    log = recording.take()
    replay = morel.Graph()
    replayed = replay.replay_from_log(log).history()
    replay.run(morel.Replay.from_nanos(0))

    print("record ticks:", ticks.peek())
    print("recording:", log)
    print("replayed:", replayed.peek())

    if hasattr(morel.Recording, "load_json") and hasattr(recording, "save_json"):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "recording.jsonl"
            second = morel.Recording()
            graph2 = morel.Graph()
            graph2.replay_from_iter(log).record(second)
            graph2.run(morel.Replay.from_nanos(0))
            second.save_json(path)
            print("json round trip:", morel.Recording.load_json(path).take())
    else:
        print("json round trip: rebuild with --features serde to enable")


if __name__ == "__main__":
    main()
