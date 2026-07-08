#!/usr/bin/env python3
"""Summarize sensor readings with batch, window, and timed operators.

This example replays timestamped sensor readings and demonstrates Morel's
time-aware operators. It computes fixed-size buffered averages, tumbling window
averages, sliding-window latest values, delayed timestamps, throttled readings,
and debounced readings from the same source stream.
"""

import statistics

import morel


def average_batch(values):
    return round(sum(values) / len(values), 2) if values else None


def main():
    graph = morel.Graph()
    readings = graph.replay_from_iter(
        [
            (0, 21.5),
            (10, 21.7),
            (20, 22.4),
            (35, 25.1),
            (45, 24.9),
            (80, 22.0),
            (90, 21.8),
        ]
    )

    buffered = readings.buffer(3).map_batch(average_batch).history()
    tumbling = (
        readings.window_tumbling(size_nanos=30)
        .map_batch(lambda values: round(statistics.mean(values), 2) if values else None)
        .history()
    )
    sliding_latest = readings.window_sliding(size_nanos=40, slide_nanos=20).collapse().history()
    delayed = readings.delay(delay_nanos=5).timestamp().history()
    throttled = readings.throttle(interval_nanos=25).history()
    debounced = readings.debounce(quiet_nanos=20).history()

    graph.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(120)))

    print("buffered averages:", buffered.peek())
    print("tumbling averages:", tumbling.peek())
    print("sliding latest:", sliding_latest.peek())
    print("delayed timestamps:", delayed.peek()[:3])
    print("throttled:", throttled.peek())
    print("debounced:", debounced.peek())


if __name__ == "__main__":
    main()
