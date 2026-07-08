import pytest

import morel


class EchoOperator:
    def __init__(self, source):
        self.source = source
        self.output = None

    def wire(self, w):
        self.input = w.on(self.source)
        self.output = w.output()

    def step(self, cx):
        self.output.set((cx.now(), cx.started_at(), cx.elapsed_nanos(), self.input.get()))


def test_graph_add_custom_operator_records_context_and_history():
    g = morel.Graph()
    src = g.replay_from_iter([(10, "a"), (20, "b")])

    hist = g.add(EchoOperator(src)).history()

    summary = g.run(morel.Replay.from_nanos(0))

    assert summary.started_at == 0
    assert summary.ended_at == 20
    assert hist.peek() == [
        (10, (10, 0, 10, "a")),
        (20, (20, 0, 20, "b")),
    ]


def test_stream_wire_custom_operator_method_variant():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 2), (5, 3)])

    class Multiply:
        def wire(self, w):
            self.input = w.on(src)
            self.output = w.output()

        def step(self, cx):
            self.output.set(self.input.get() * 10)

    hist = src.wire(Multiply()).history()

    g.run(morel.Replay.from_nanos(0))

    assert hist.peek() == [(0, 20), (5, 30)]


def test_step_exception_preserves_original_python_type():
    class Boom(Exception):
        pass

    class Explodes:
        def wire(self, w):
            self.output = w.output()

        def on_start(self, cx):
            cx.at_nanos(cx.now())

        def step(self, cx):
            raise Boom("step failed")

    g = morel.Graph()
    g.add(Explodes())

    with pytest.raises(Boom, match="step failed"):
        g.run(morel.Replay.from_nanos(0))


def test_wire_exception_returns_original_python_type_immediately():
    class WireBoom(Exception):
        pass

    hooks = []

    class ExplodesInWire:
        def wire(self, w):
            raise WireBoom("wire failed")

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    g = morel.Graph()
    before = g.len()

    with pytest.raises(WireBoom, match="wire failed"):
        g.add(ExplodesInWire())

    assert g.len() == before
    g.run(morel.Replay.from_nanos(0))
    assert hooks == []


def test_stream_wire_exception_returns_original_python_type_immediately():
    class WireBoom(Exception):
        pass

    hooks = []

    class ExplodesInWire:
        def wire(self, w):
            raise WireBoom("stream wire failed")

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    g = morel.Graph()
    src = g.just("x")
    before = g.len()

    with pytest.raises(WireBoom, match="stream wire failed"):
        src.wire(ExplodesInWire())

    assert g.len() == before
    g.run(morel.Replay.from_nanos(0))
    assert hooks == []


def test_failed_wire_missing_output_leaves_graph_unchanged_and_does_not_run_hooks():
    hooks = []

    class MissingOutput:
        def wire(self, w):
            pass

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    g = morel.Graph()
    before = g.len()

    with pytest.raises(RuntimeError, match="operator.wire\\(\\) must call Wire.output\\(\\)"):
        g.add(MissingOutput())

    assert g.len() == before
    g.run(morel.Replay.from_nanos(0))
    assert hooks == []


def test_failed_wire_double_output_leaves_graph_unchanged_and_does_not_run_hooks():
    hooks = []

    class DoubleOutput:
        def wire(self, w):
            w.output()
            w.output()

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    g = morel.Graph()
    before = g.len()

    with pytest.raises(RuntimeError, match="Wire.output\\(\\) may only be called once"):
        g.add(DoubleOutput())

    assert g.len() == before
    g.run(morel.Replay.from_nanos(0))
    assert hooks == []

def test_adding_custom_operator_while_graph_running_raises_runtime_error():
    g = morel.Graph()
    src = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.add(EchoOperator(src))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        src.wire(EchoOperator(src))

    assert g.step() is True
    assert g.end().steps == 1


def test_wire_and_ctx_wrappers_are_stale_after_callbacks_return():
    captured = {}

    class Captures:
        def wire(self, w):
            captured["wire"] = w
            self.output = w.output()

        def on_start(self, cx):
            captured["start_cx"] = cx
            cx.at_nanos(cx.now())

        def step(self, cx):
            captured["step_cx"] = cx
            self.output.set("ran")

    g = morel.Graph()
    out = g.add(Captures())

    with pytest.raises(RuntimeError, match="Wire is no longer active"):
        captured["wire"].finalize()

    with pytest.raises(RuntimeError, match="Wire is no longer active"):
        captured["wire"].output()

    g.run(morel.Replay.from_nanos(0))
    assert out.peek() == "ran"

    with pytest.raises(RuntimeError, match="Ctx is no longer active"):
        captured["start_cx"].now()

    with pytest.raises(RuntimeError, match="Ctx is no longer active"):
        captured["step_cx"].now()


def test_input_peek_has_value_fired_and_get_behaviour():
    g = morel.Graph()
    src = g.replay_from_iter([(0, "a"), (10, "b")])
    watched = g.replay_from_iter([(10, "late")])

    class Probe:
        def wire(self, w):
            self.input = w.on(src)
            self.watched = w.watch(watched)
            self.output = w.output()

        def step(self, cx):
            try:
                watched_get = self.watched.get()
            except RuntimeError as exc:
                watched_get = type(exc).__name__

            self.output.set(
                {
                    "now": cx.now(),
                    "input_fired": self.input.fired(),
                    "input_has_value": self.input.has_value(),
                    "input_peek": self.input.peek(),
                    "input_get": self.input.get(),
                    "watched_fired": self.watched.fired(),
                    "watched_has_value": self.watched.has_value(),
                    "watched_peek": self.watched.peek(),
                    "watched_get": watched_get,
                }
            )

    hist = g.add(Probe()).history()

    g.run(morel.Replay.from_nanos(0))

    assert hist.peek() == [
        (
            0,
            {
                "now": 0,
                "input_fired": True,
                "input_has_value": True,
                "input_peek": "a",
                "input_get": "a",
                "watched_fired": False,
                "watched_has_value": False,
                "watched_peek": None,
                "watched_get": "RuntimeError",
            },
        ),
        (
            10,
            {
                "now": 10,
                "input_fired": True,
                "input_has_value": True,
                "input_peek": "b",
                "input_get": "b",
                "watched_fired": True,
                "watched_has_value": True,
                "watched_peek": "late",
                "watched_get": "late",
            },
        ),
    ]


def test_wire_rejects_streams_from_another_graph_without_panicking():
    g1 = morel.Graph()
    foreign = morel.Graph().just("foreign")
    hooks = []

    class CrossGraph:
        def wire(self, w):
            w.on(foreign)
            self.output = w.output()

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    before = g1.len()

    with pytest.raises(ValueError, match="streams must belong to the same graph"):
        g1.add(CrossGraph())

    assert g1.len() == before
    g1.run(morel.Replay.from_nanos(0))
    assert hooks == []


def test_cross_graph_watch_failure_leaves_graph_unchanged_and_does_not_run_hooks():
    g1 = morel.Graph()
    foreign = morel.Graph().just("foreign")
    hooks = []

    class CrossGraphWatch:
        def wire(self, w):
            w.watch(foreign)
            self.output = w.output()

        def on_start(self, cx):
            hooks.append("start")

        def step(self, cx):
            hooks.append("step")

        def on_stop(self, cx):
            hooks.append("stop")

    before = g1.len()

    with pytest.raises(ValueError, match="streams must belong to the same graph"):
        g1.add(CrossGraphWatch())

    assert g1.len() == before
    g1.run(morel.Replay.from_nanos(0))
    assert hooks == []


def test_ctx_timer_finalize_stop_and_fail_helpers():
    g = morel.Graph()
    lifecycle = []

    class Timed:
        def wire(self, w):
            w.finalize()
            self.output = w.output()

        def on_start(self, cx):
            lifecycle.append(("start", cx.now(), cx.started_at(), cx.elapsed_nanos(), cx.is_live()))
            cx.at_nanos(5)
            cx.after_nanos(10)
            cx.every_nanos(20)

        def step(self, cx):
            if cx.is_final():
                self.output.set(("final", cx.now()))
                return
            self.output.set(("step", cx.now()))
            if cx.now() >= 25:
                cx.stop()

        def on_stop(self, cx):
            lifecycle.append(("stop", cx.now(), cx.is_final()))

    hist = g.add(Timed()).history()

    summary = g.run(morel.Replay.from_nanos(0).stop(morel.Stop.never()))

    assert summary.ended_at == 40
    assert lifecycle == [("start", 0, 0, 0, False), ("stop", 40, True)]
    assert hist.peek() == [
        (5, ("step", 5)),
        (10, ("step", 10)),
        (20, ("step", 20)),
        (40, ("step", 40)),
        (40, ("final", 40)),
    ]

    class Fails:
        def wire(self, w):
            self.output = w.output()

        def on_start(self, cx):
            cx.fail("custom failure")

    failing = morel.Graph()
    failing.add(Fails())
    with pytest.raises(RuntimeError, match="custom failure"):
        failing.run(morel.Replay.from_nanos(0))


def test_ctx_fail_with_python_exception_preserves_original_type():
    g = morel.Graph()

    class Fails:
        def wire(self, w):
            self.output = w.output()

        def on_start(self, cx):
            cx.fail(ValueError("typed failure"))

    g.add(Fails())

    with pytest.raises(ValueError, match="typed failure"):
        g.run(morel.Replay.from_nanos(0))


def test_ctx_fail_with_string_preserves_runtime_error():
    g = morel.Graph()

    class Fails:
        def wire(self, w):
            self.output = w.output()

        def on_start(self, cx):
            cx.fail("string failure")

    g.add(Fails())

    with pytest.raises(RuntimeError, match="string failure"):
        g.run(morel.Replay.from_nanos(0))
