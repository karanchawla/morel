#!/usr/bin/env python3
"""Use channels, worker graphs, source workers, and live producers.

This example groups the cross-graph APIs in one place. It sends replay values
through a channel, runs a child worker graph that consumes parent input, runs a
source worker that creates its own child inputs, and starts a live producer
thread that sends a small burst into a graph.
"""

import morel


def replay_channel_round_trip():
    graph = morel.Graph()
    source = graph.replay_from_iter([(0, "a"), (10, "b"), (20, "c")])
    tx, rx = morel.channel(morel.Capacity.unbounded())

    tx.attach(source)
    out = rx.into_stream_paced(source, morel.OnClose.continue_()).collapse().history()

    graph.run(morel.Replay.from_nanos(0))
    return out.peek()


def worker_pipeline():
    graph = morel.Graph()
    raw_batches = graph.replay_from_iter([(0, 1), (10, 2), (20, 3), (30, 4)])

    def build(child, child_input):
        weights = child.replay_from_iter([(0, 10), (20, 100)])
        return (
            child_input.collapse()
            .with_latest(weights, lambda value, weight: value * weight)
            .scan(0, lambda total, value: total + value)
        )

    out = morel.worker(raw_batches, build).collapse().history()
    graph.run(morel.Replay.from_nanos(0))
    return out.peek()


def source_worker_pipeline():
    graph = morel.Graph()

    def build(child):
        left = child.replay_from_iter([(0, "left-0"), (10, "left-10")])
        right = child.just("right")
        return morel.merge([left, right])

    out = morel.source_worker(graph, build).collapse().history()
    graph.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))
    return out.peek()


def live_producer_demo():
    graph = morel.Graph()

    def produce(producer):
        producer.send("live-a")
        producer.send("live-b")

    out = morel.producer(graph, produce).history()
    graph.run(morel.Live().stop(morel.Stop.after_seconds(0.05)))
    return out.peek()


def main():
    print("channel:", replay_channel_round_trip())
    print("worker:", worker_pipeline())
    print("source worker:", source_worker_pipeline())
    print("producer:", live_producer_demo())


if __name__ == "__main__":
    main()
