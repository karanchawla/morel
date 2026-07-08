import morel
import pytest


def test_just_peek_is_empty_until_run():
    g = morel.Graph()
    s = g.just(42)
    assert s.peek() is None
    summary = g.run(morel.Replay.from_nanos(0))
    assert summary.steps == 1
    assert s.peek() == 42


def test_replay_from_iter_and_history_use_integer_nanos():
    g = morel.Graph()
    src = g.replay_from_iter([(10, "a"), (20, "b")])
    hist = src.history()
    summary = g.run(morel.Replay.from_nanos(0))
    assert summary.started_at == 0
    assert summary.ended_at == 20
    assert hist.peek() == [(10, "a"), (20, "b")]


def test_begin_step_end_manual_replay():
    g = morel.Graph()
    src = g.replay_from_iter([(10, 1), (20, 2)])
    g.begin(morel.Replay.from_nanos(0))
    assert g.step() is True
    assert src.peek() == 1
    assert g.step() is True
    assert src.peek() == 2
    assert g.step() is False
    summary = g.end()
    assert summary.steps == 2


def test_stream_keeps_temporary_graph_alive():
    s = morel.Graph().just("kept alive")
    # Do not implement map yet for Task 3; use the source stream directly.
    summary = s.run(morel.Replay.from_nanos(0))
    assert summary.steps == 1
    assert s.peek() == "kept alive"


def test_lifecycle_misuse_returns_python_runtime_errors():
    g = morel.Graph()
    g.just("x")

    with pytest.raises(RuntimeError, match="graph is not running"):
        g.end()

    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="graph is already running"):
        g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="graph is already running"):
        g.run(morel.Replay.from_nanos(0))

    assert g.step() is True
    assert g.end().steps == 1


def test_stream_run_while_owner_graph_is_running_returns_runtime_error():
    g = morel.Graph()
    s = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="graph is already running"):
        s.run(morel.Replay.from_nanos(0))

    assert g.step() is True
    assert g.end().steps == 1


def test_adding_sources_while_running_returns_runtime_error():
    g = morel.Graph()
    g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.just("late")

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.ticker(period_nanos=1)

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.replay_from_iter([(0, "late")])

    assert g.step() is True
    assert g.end().steps == 1


def test_history_while_owner_graph_is_running_returns_runtime_error():
    g = morel.Graph()
    s = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        s.history()

    assert g.step() is True
    assert g.end().steps == 1
