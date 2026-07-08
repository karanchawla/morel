import subprocess
import sys

import morel
import pytest


def test_same_graph_paced_channel_roundtrip():
    g = morel.Graph()
    src = g.replay_from_iter([(0, "a"), (10, "b")])
    tx, rx = morel.channel(morel.Capacity.unbounded())

    tx.attach(src)
    out = rx.into_stream_paced(src, morel.OnClose.continue_()).collapse().history()

    g.run(morel.Replay.from_nanos(0))

    assert out.peek() == [(0, "a"), (10, "b")]


def test_live_producer_sends_values():
    g = morel.Graph()

    def produce(p):
        p.send("a")
        p.send("b")

    out = morel.producer(g, produce).collapse().history()

    g.run(morel.Live().stop(morel.Stop.after_seconds(0.05)))

    values = [value for _, value in out.peek()]
    assert values in (["b"], ["a", "b"])


def test_worker_builds_child_graph_from_input_stream():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3)])

    def build(child, child_input):
        assert isinstance(child, morel.ChildGraph)
        return child_input.collapse().map(lambda value: value * 10)

    out = morel.worker(src, build).collapse().history()

    g.run(morel.Replay.from_nanos(0))

    assert out.peek() == [(0, 10), (10, 20), (20, 30)]


def test_worker_child_stream_exposes_stateless_stateful_and_batch_ops():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3), (30, 5)])

    def build(_child, child_input):
        return (
            child_input.collapse()
            .filter(lambda value: value % 2 == 1)
            .scan(0, lambda acc, value: acc + value)
            .map(lambda value: value * 10)
        )

    out = morel.worker(src, build).collapse().history()

    g.run(morel.Replay.from_nanos(0))

    assert out.peek() == [(0, 10), (20, 40), (30, 90)]


def test_worker_child_stream_exposes_combine_ops():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3)])

    def build(child, child_input):
        values = child_input.collapse()
        weights = child.replay_from_iter([(0, 10), (20, 100)])
        open_stream = child.replay_from_iter([(0, False), (10, True), (20, True)])
        return values.with_latest(weights, lambda value, weight: value * weight).gate(open_stream)

    out = morel.worker(src, build).collapse().history()

    g.run(morel.Replay.from_nanos(0))

    assert out.peek() == [(10, 20), (20, 300)]


def test_worker_child_graph_add_and_module_combine_helpers():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2)])

    class EmptySource:
        def wire(self, w):
            w.output()

    def build(child, child_input):
        assert child_input.peek() is None
        before = child.len()
        custom = child.add(EmptySource())
        assert child.len() > before
        assert custom.peek() is None
        values = child_input.collapse()
        constant = child.just(10)
        return morel.gather([values, constant]).map(lambda values: sum(values))

    out = morel.worker(src, build).collapse().history()

    g.run(morel.Replay.from_nanos(0))

    assert out.peek() == [(0, 11), (10, 12)]


def test_source_worker_child_stream_exposes_history_windows_wire_and_record():
    g = morel.Graph()
    recording = morel.Recording()

    class ChecksWire:
        def __init__(self, source):
            self.source = source

        def wire(self, w):
            w.on(self.source)
            w.output()

    def build(child):
        source = child.replay_from_iter([(0, 1), (10, 2), (20, 3)])
        source.wire(ChecksWire(source))
        source.record(recording)
        return (
            source.buffer(2)
            .map_batch(lambda batch: tuple(batch))
            .window_tumbling(size_nanos=15)
            .history()
        )

    out = morel.source_worker(g, build).collapse().history()

    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(30)))

    assert recording.take() == [(0, 1), (10, 2), (20, 3)]
    assert out.peek()[0] == (15, [(15, [(1, 2)])])


def test_source_worker_builds_child_source_stream_with_finite_replay_stop():
    g = morel.Graph()

    def build(child):
        assert isinstance(child, morel.ChildGraph)
        return child.replay_from_iter([(0, "a"), (10, "b")])

    out = morel.source_worker(g, build).collapse().history()

    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))

    assert out.peek() == [(0, "a"), (10, "b")]


def test_source_worker_module_merge_accepts_child_streams():
    g = morel.Graph()

    def build(child):
        left = child.replay_from_iter([(0, "a")])
        right = child.replay_from_iter([(10, "b")])
        return morel.merge([left, right])

    out = morel.source_worker(g, build).collapse().history()

    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))

    assert out.peek() == [(0, "a"), (10, "b")]


def test_channel_tx_and_rx_are_one_shot():
    g = morel.Graph()
    src = g.just("x")
    tx, rx = morel.channel(morel.Capacity.unbounded())

    tx.attach(src)
    with pytest.raises(RuntimeError, match="channel transmitter already attached"):
        tx.attach(src)

    rx.into_stream_paced(src, morel.OnClose.continue_())
    with pytest.raises(RuntimeError, match="channel receiver already materialized"):
        rx.into_stream_paced(src, morel.OnClose.continue_())


def test_channel_capacity_bounded_zero_raises_value_error():
    with pytest.raises(ValueError, match="capacity must be greater than 0"):
        morel.Capacity.bounded(0)


def test_channel_same_graph_guards():
    g1 = morel.Graph()
    g2 = morel.Graph()
    source = g1.just("x")
    heartbeat = g2.just(None)
    tx, _rx = morel.channel(morel.Capacity.unbounded())

    with pytest.raises(ValueError, match="streams must belong to the same graph"):
        tx.attach_with_heartbeat(source, heartbeat)


def test_channel_and_worker_additions_while_running_raise_runtime_error():
    g = morel.Graph()
    src = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    tx, rx = morel.channel(morel.Capacity.unbounded())
    methods = [
        lambda: tx.attach(src),
        lambda: tx.attach_with_heartbeat(src, src),
        lambda: rx.into_stream(g, morel.OnClose.continue_()),
        lambda: rx.into_stream_paced(src, morel.OnClose.continue_()),
        lambda: morel.producer(g, lambda p: p.send("late")),
        lambda: morel.worker(src, lambda child, child_input: child_input.collapse()),
        lambda: morel.source_worker(g, lambda child: child.just("late")),
    ]

    for method in methods:
        with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
            method()

    assert g.step() is True
    assert g.end().steps == 1


def test_producer_callback_exception_surfaces_as_graph_error():
    g = morel.Graph()

    def produce(_p):
        raise ValueError("producer failed")

    morel.producer(g, produce)

    with pytest.raises(morel.GraphError, match="producer failed"):
        g.run(morel.Live().stop(morel.Stop.after_seconds(0.05)))


def test_worker_build_callback_exception_preserves_original_type():
    class BuildBoom(Exception):
        pass

    g = morel.Graph()
    src = g.just("x")

    def build(_child, _child_input):
        raise BuildBoom("worker build failed")

    morel.worker(src, build)

    with pytest.raises(BuildBoom, match="worker build failed"):
        g.run(morel.Replay.from_nanos(0))


def test_source_worker_build_callback_exception_preserves_original_type():
    class BuildBoom(Exception):
        pass

    g = morel.Graph()

    def build(_child):
        raise BuildBoom("source worker build failed")

    morel.source_worker(g, build)

    with pytest.raises(BuildBoom, match="source worker build failed"):
        g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(0)))


def test_worker_child_runtime_callback_exception_preserves_original_type():
    class ChildBoom(Exception):
        pass

    g = morel.Graph()
    src = g.just("x")

    def build(_child, child_input):
        return child_input.collapse().map(
            lambda value: (_ for _ in ()).throw(ChildBoom(f"child failed for {value}"))
        )

    morel.worker(src, build).collapse().history()

    with pytest.raises(ChildBoom, match="child failed for x"):
        g.run(morel.Replay.from_nanos(0))


def test_escaped_worker_child_handles_fail_cleanly_after_run():
    escaped = {}
    g = morel.Graph()
    src = g.just("x")

    def build(child, child_input):
        escaped["child"] = child
        escaped["input"] = child_input
        escaped["mapped"] = child_input.collapse().map(lambda value: value)
        return escaped["mapped"]

    morel.worker(src, build).collapse().history()
    g.run(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["child"].len()
    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["input"].collapse()
    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["mapped"].filter(lambda value: True)
    with pytest.raises(RuntimeError, match="child streams cannot run their graph directly"):
        escaped["mapped"].run(morel.Replay.from_nanos(0))

    escaped.clear()


def test_escaped_source_worker_child_handles_fail_cleanly_after_run():
    escaped = {}
    g = morel.Graph()

    def build(child):
        escaped["child"] = child
        escaped["source"] = child.just("x")
        escaped["mapped"] = escaped["source"].map(lambda value: value)
        return escaped["mapped"]

    morel.source_worker(g, build).collapse().history()
    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(0)))

    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["child"].len()
    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["source"].collapse()
    with pytest.raises(RuntimeError, match="ChildGraph is no longer active"):
        escaped["mapped"].history()
    with pytest.raises(RuntimeError, match="child streams cannot run their graph directly"):
        escaped["mapped"].run(morel.Replay.from_nanos(0))

    escaped.clear()


def test_detached_run_preserves_normal_python_callback_exception_with_producer():
    class Boom(Exception):
        pass

    def fail(value):
        raise Boom(f"normal callback failed for {value}")

    g = morel.Graph()
    morel.producer(g, lambda p: None)
    g.just("x").map(fail)

    with pytest.raises(Boom, match="normal callback failed for x"):
        g.run(morel.Live().stop(morel.Stop.after_seconds(0.05)))


def test_detached_manual_end_preserves_normal_python_callback_exception_with_producer():
    class Boom(Exception):
        pass

    def fail(value):
        raise Boom(f"manual callback failed for {value}")

    g = morel.Graph()
    morel.producer(g, lambda p: None)
    g.just("x").map(fail)

    g.begin(morel.Live().stop(morel.Stop.after_seconds(0.05)))
    g.step()
    with pytest.raises(Boom, match="manual callback failed for x"):
        g.end()


def test_detached_run_preserves_normal_python_callback_exception_with_worker():
    class Boom(Exception):
        pass

    def fail(value):
        raise Boom(f"worker mixed callback failed for {value}")

    g = morel.Graph()
    src = g.just("x")
    morel.worker(src, lambda _child, child_input: child_input.collapse())
    src.map(fail)

    with pytest.raises(Boom, match="worker mixed callback failed for x"):
        g.run(morel.Replay.from_nanos(0))


def test_manual_replay_worker_step_does_not_deadlock():
    code = r"""
import morel

g = morel.Graph()
src = g.replay_from_iter([(0, 1)])
out = morel.worker(src, lambda _child, child_input: child_input.collapse()).collapse().history()
g.begin(morel.Replay.from_nanos(0))
assert g.step() is True
assert g.end().steps >= 1
assert out.peek() == [(0, 1)]
"""

    subprocess.run([sys.executable, "-c", code], check=True, timeout=5)


def test_manual_replay_source_worker_step_does_not_deadlock():
    code = r"""
import morel

g = morel.Graph()
out = morel.source_worker(
    g,
    lambda child: child.replay_from_iter([(0, "x")]),
).collapse().history()
g.begin(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(0)))
assert g.step() is True
assert g.end().steps >= 1
assert out.peek() == [(0, "x")]
"""

    subprocess.run([sys.executable, "-c", code], check=True, timeout=5)
