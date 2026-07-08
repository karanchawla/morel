#!/usr/bin/env python3
"""Define custom Python operators and add them to a graph.

This example implements two custom operators with `wire` and `step`: `Spread`
watches bid/ask streams and emits the spread, while `TimerSource` schedules its
own ticks with the run context. It also demonstrates `Graph.add` for source-like
operators and `Stream.wire` for attaching a custom transform to an existing
stream.
"""

import morel


class Spread:
    def __init__(self, bids, asks):
        self.bids = bids
        self.asks = asks

    def wire(self, wire):
        self.bid = wire.on(self.bids)
        self.ask = wire.on(self.asks)
        self.out = wire.output()

    def step(self, _ctx):
        if self.bid.has_value() and self.ask.has_value():
            self.out.set(round(self.ask.get() - self.bid.get(), 4))


class TimerSource:
    def wire(self, wire):
        self.out = wire.output()

    def on_start(self, ctx):
        self.next_value = 1
        ctx.every_nanos(10)

    def step(self, ctx):
        self.out.set(self.next_value)
        self.next_value += 1
        if self.next_value > 3:
            ctx.stop()


def main():
    graph = morel.Graph()
    bids = graph.replay_from_iter([(0, 100.0), (10, 100.5), (20, 100.25)])
    asks = graph.replay_from_iter([(0, 100.3), (10, 101.0), (20, 100.4)])

    spread_history = graph.add(Spread(bids, asks)).history()
    doubled_timer = graph.add(TimerSource()).map(lambda value: value * 2).history()
    watched = bids.wire(Spread(bids, asks)).take(2).history()

    graph.run(morel.Replay.from_nanos(0))

    print("spread:", spread_history.peek())
    print("timer source:", doubled_timer.peek())
    print("stream.wire spread first two:", watched.peek())


if __name__ == "__main__":
    main()
