#!/usr/bin/env python3
"""Combine price streams into simple derived signals.

This example uses generic stream-processing operators on a price feed:
`delta` computes price moves, `mean` tracks running state, `with_latest`
combines streams, `gate` controls when signals pass, `sample` reads the latest
price on heartbeat events, and `merge`/`gather` demonstrate fan-in patterns.
"""

import morel


def main():
    graph = morel.Graph()
    prices = graph.replay_from_iter(
        [
            (0, 100.0),
            (10, 101.5),
            (20, 99.5),
            (30, 103.0),
            (40, 106.0),
            (50, 104.5),
        ]
    )
    risk_open = graph.replay_from_iter([(0, False), (20, True), (50, False)])
    heartbeat = graph.replay_from_iter([(15, "heartbeat"), (35, "heartbeat"), (55, "heartbeat")])

    move = prices.delta()
    mean_price = prices.mean()
    score = move.with_latest(mean_price, lambda change, avg: round(change / avg, 4))
    gated_score = score.gate(risk_open).history()
    sampled_price = prices.sample(heartbeat).history()
    merged_events = morel.merge(
        [
            prices.map(lambda price: ("price", price)),
            move.map(lambda change: ("change", change)),
        ]
    ).history()
    latest_snapshot = morel.gather([prices, mean_price]).history()

    graph.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(55)))

    print("risk-gated score:", gated_score.peek())
    print("heartbeat sampled price:", sampled_price.peek())
    print("merged event tail:", merged_events.peek()[-4:])
    print("latest [price, mean]:", latest_snapshot.peek()[-1])


if __name__ == "__main__":
    main()
