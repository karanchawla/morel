import os

import pytest

import morel


HAS_ASYNC_IO = hasattr(morel.Stream, "consume_async")
EXPECT_ASYNC_IO = os.environ.get("MOREL_PYTHON_EXPECT_ASYNC_IO") == "1"


def require_async_io():
    if not HAS_ASYNC_IO:
        if EXPECT_ASYNC_IO:
            pytest.fail("expected morel-python to expose async-io bindings")
        pytest.skip("morel-python was not built with the async-io feature")


def test_async_io_api_surface_is_feature_gated():
    if EXPECT_ASYNC_IO:
        assert hasattr(morel.Stream, "consume_async")
    elif HAS_ASYNC_IO:
        assert hasattr(morel.Stream, "consume_async")
    else:
        assert not hasattr(morel.Stream, "consume_async")


def test_generic_async_python_producers_are_not_advertised():
    assert not hasattr(morel, "produce_async")
    assert not hasattr(morel, "produce_async_stream")


def test_consume_async_consumes_replay_values_with_time():
    require_async_io()

    seen = []
    g = morel.Graph()
    src = g.replay_from_iter([(0, "a"), (10, "b"), (20, "c")])
    ticks = src.consume_async(lambda time_nanos, value: seen.append((time_nanos, value))).history()

    g.run(morel.Replay.from_nanos(0))

    assert seen == [(0, "a"), (10, "b"), (20, "c")]
    assert ticks.peek() == [(0, None), (10, None), (20, None)]


def test_source_worker_child_stream_consume_async():
    require_async_io()

    seen = []
    g = morel.Graph()

    def build(child):
        src = child.replay_from_iter([(0, "a"), (10, "b")])
        assert hasattr(morel.ChildStream, "consume_async")
        return src.consume_async(
            lambda time_nanos, value: seen.append((time_nanos, value))
        )

    out = morel.source_worker(g, build).collapse().history()

    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))

    assert seen == [(0, "a"), (10, "b")]
    assert out.peek() == [(0, None), (10, None)]


def test_consume_async_while_owner_graph_is_running_returns_runtime_error():
    require_async_io()

    g = morel.Graph()
    src = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        src.consume_async(lambda _time_nanos, _value: None)

    assert g.step() is True
    assert g.end().steps == 1


def test_consume_async_callback_exception_preserves_original_type():
    require_async_io()

    g = morel.Graph()
    src = g.just("x")

    def consume(_time_nanos, _value):
        raise ValueError("async consumer failed")

    src.consume_async(consume)

    with pytest.raises(ValueError, match="async consumer failed"):
        g.run(morel.Replay.from_nanos(0))
