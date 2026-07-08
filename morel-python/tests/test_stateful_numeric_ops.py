import pytest
import morel


def test_count_accumulate_history_timestamp():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 10), (10, 20), (20, 30)])
    counted = src.count().history()
    accumulated = src.accumulate()
    stamped = src.timestamp().history()
    hist = src.history()
    g.run(morel.Replay.from_nanos(0))
    assert counted.peek() == [(0, 1), (10, 2), (20, 3)]
    assert accumulated.peek() == [10, 20, 30]
    assert stamped.peek() == [(0, (0, 10)), (10, (10, 20)), (20, (20, 30))]
    assert hist.peek() == [(0, 10), (10, 20), (20, 30)]


def test_scan_reduce_sum_delta_mean():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3), (30, 4)])
    scanned = src.scan(0, lambda acc, value: acc + value).history()
    reduced = src.reduce(lambda acc, value: acc + value).history()
    summed = src.sum().history()
    delta = src.delta().history()
    mean = src.mean().history()
    g.run(morel.Replay.from_nanos(0))
    assert scanned.peek() == [(0, 1), (10, 3), (20, 6), (30, 10)]
    assert reduced.peek() == [(0, 1), (10, 3), (20, 6), (30, 10)]
    assert summed.peek() == [(0, 1), (10, 3), (20, 6), (30, 10)]
    assert delta.peek() == [(10, 1), (20, 1), (30, 1)]
    assert mean.peek() == [(0, 1.0), (10, 1.5), (20, 2.0), (30, 2.5)]


def test_numeric_errors_fail_run_cleanly():
    g = morel.Graph()
    g.replay_from_iter([(0, object())]).mean()
    with pytest.raises(TypeError):
        g.run(morel.Replay.from_nanos(0))


def test_scan_callback_exception_preserves_type():
    class Boom(Exception):
        pass

    def fail(acc, value):
        raise Boom(f"scan failed at {value}")

    g = morel.Graph()
    g.just(1).scan(0, fail)
    with pytest.raises(Boom, match="scan failed at 1"):
        g.run(morel.Replay.from_nanos(0))


def test_reduce_callback_exception_preserves_type():
    class Boom(Exception):
        pass

    def fail(acc, value):
        raise Boom(f"reduce failed at {value}")

    g = morel.Graph()
    g.replay_from_iter([(0, 1), (10, 2)]).reduce(fail)
    with pytest.raises(Boom, match="reduce failed at 2"):
        g.run(morel.Replay.from_nanos(0))


def test_sum_protocol_exception_preserves_type():
    class BadAdd:
        def __add__(self, other):
            raise ValueError("add failed")

    g = morel.Graph()
    g.replay_from_iter([(0, BadAdd()), (10, BadAdd())]).sum()
    with pytest.raises(ValueError, match="add failed"):
        g.run(morel.Replay.from_nanos(0))


def test_delta_protocol_exception_preserves_type():
    class BadSub:
        def __sub__(self, other):
            raise ValueError("sub failed")

    g = morel.Graph()
    g.replay_from_iter([(0, BadSub()), (10, BadSub())]).delta()
    with pytest.raises(ValueError, match="sub failed"):
        g.run(morel.Replay.from_nanos(0))


def test_stateful_operators_while_owner_graph_is_running_return_runtime_error():
    g = morel.Graph()
    src = g.just(1)
    g.begin(morel.Replay.from_nanos(0))

    methods = [
        lambda: src.count(),
        lambda: src.accumulate(),
        lambda: src.timestamp(),
        lambda: src.scan(0, lambda acc, value: acc + value),
        lambda: src.reduce(lambda acc, value: acc + value),
        lambda: src.sum(),
        lambda: src.delta(),
        lambda: src.mean(),
        lambda: src.history(),
    ]
    for method in methods:
        with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
            method()

    assert g.step() is True
    assert g.end().steps == 1
