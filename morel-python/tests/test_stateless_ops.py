import pytest
import morel


def test_map_filter_filter_map_take_distinct():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 2), (30, 3), (40, 4)])
    out = (
        src.map(lambda x: x * 10)
        .filter(lambda x: x >= 20)
        .distinct()
        .filter_map(lambda x: None if x == 30 else f"v{x}")
        .take(2)
        .history()
    )
    g.run(morel.Replay.from_nanos(0))
    assert out.peek() == [(10, "v20"), (40, "v40")]


def test_inspect_sink_and_try_sink_callbacks():
    g = morel.Graph()
    seen = []
    sunk = []
    try_sunk = []
    src = g.replay_from_iter([(0, "a"), (10, "b")])
    src.inspect(lambda value, time: seen.append((time, value))).sink(
        lambda value, time: sunk.append((time, value))
    ).try_sink(lambda value, time: try_sunk.append((time, value)))
    g.run(morel.Replay.from_nanos(0))
    assert seen == [(0, "a"), (10, "b")]
    assert sunk == [(0, "a"), (10, "b")]
    assert try_sunk == [(0, None), (10, None)]


def test_callback_exception_preserves_type():
    class Boom(Exception):
        pass

    g = morel.Graph()
    src = g.just(1).map(lambda _: (_ for _ in ()).throw(Boom("map failed")))
    with pytest.raises(Boom, match="map failed"):
        g.run(morel.Replay.from_nanos(0))


def test_filter_requires_bool_result():
    g = morel.Graph()
    src = g.just(1).filter(lambda _: "not bool")
    with pytest.raises(TypeError, match="filter predicate must return bool"):
        g.run(morel.Replay.from_nanos(0))


def test_distinct_propagates_python_equality_error():
    class ExplodingEq:
        def __eq__(self, other):
            raise RuntimeError("eq failed")

    g = morel.Graph()
    out = g.replay_from_iter([(0, ExplodingEq()), (10, ExplodingEq())]).distinct()
    with pytest.raises(RuntimeError, match="eq failed"):
        g.run(morel.Replay.from_nanos(0))


def test_stateless_operators_while_owner_graph_is_running_return_runtime_error():
    g = morel.Graph()
    src = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    methods = [
        lambda: src.map(lambda value: value),
        lambda: src.try_map(lambda value: value),
        lambda: src.filter(lambda value: True),
        lambda: src.filter_map(lambda value: value),
        lambda: src.distinct(),
        lambda: src.take(1),
        lambda: src.inspect(lambda value, time: None),
        lambda: src.sink(lambda value, time: None),
        lambda: src.try_sink(lambda value, time: None),
    ]
    for method in methods:
        with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
            method()

    assert g.step() is True
    assert g.end().steps == 1
