#!/usr/bin/env python3
"""Run every tutorial example in a stable order.

This helper imports each example module from the local `examples` directory and
calls its `main` function. It is useful as a quick smoke test for the tutorial
suite because each example prints a compact, human-readable result.
"""

import importlib.util
from pathlib import Path


EXAMPLES = [
    "quick_start.py",
    "replay_word_count.py",
    "market_signal_pipeline.py",
    "windowed_sensor_monitor.py",
    "custom_operator_spread.py",
    "channels_and_workers.py",
    "recording_replay.py",
    "csv_replay_and_optional_async.py",
]


def load_module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    root = Path(__file__).resolve().parent
    for name in EXAMPLES:
        print(f"\n=== {name} ===")
        load_module(root / name).main()


if __name__ == "__main__":
    main()
