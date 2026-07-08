#!/usr/bin/env python3
"""Count words over a replayed text stream.

This example shows how ordinary Python functions fit inside Morel operators.
Each replayed line updates a dictionary with `scan`, then `map` turns the
running counts into the current top three words. Because the input is replayed
at fixed virtual times, the printed timeline is deterministic.
"""

import re

import morel


WORD_RE = re.compile(r"[a-zA-Z]+")


def update_counts(counts, line):
    next_counts = dict(counts)
    for word in WORD_RE.findall(line.lower()):
        next_counts[word] = next_counts.get(word, 0) + 1
    return next_counts


def top_three(counts):
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]


def main():
    graph = morel.Graph()
    lines = graph.replay_from_iter(
        [
            (0, "morel streams replay cleanly"),
            (10, "streams compose with python functions"),
            (20, "python examples should stay deterministic"),
            (30, "morel replay makes tests boring in a good way"),
        ]
    )

    counts = lines.scan({}, update_counts)
    top_words = counts.map(top_three).history()

    graph.run(morel.Replay.from_nanos(0))

    print("top words after each line:")
    for time_nanos, top in top_words.peek():
        print(f"{time_nanos:>2} ns:", top)


if __name__ == "__main__":
    main()
