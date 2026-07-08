import pytest
import morel


def test_timed_delay_throttle_debounce():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3), (100, 4)])
    delayed = src.delay(delay_nanos=5).history()
    throttled = src.throttle(interval_nanos=50).history()
    debounced = src.debounce(quiet_nanos=30).history()
    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(140)))
    assert delayed.peek() == [(5, 1), (15, 2), (25, 3), (105, 4)]
    assert throttled.peek() == [(0, 1), (100, 4)]
    assert debounced.peek() == [(50, 3), (130, 4)]


def test_buffer_windows_collapse_and_map_batch():
    g = morel.Graph()
    src = g.replay_from_iter([(0, 1), (10, 2), (20, 3), (30, 4)])
    buffered = src.buffer(2).history()
    collapsed = src.buffer(2).collapse().history()
    mapped = src.buffer(2).map_batch(lambda batch: sum(batch)).history()
    tumbling = src.window_tumbling(size_nanos=25).history()
    sliding = src.window_sliding(size_nanos=25, slide_nanos=10).history()
    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(60)))
    assert buffered.peek() == [(10, [1, 2]), (30, [3, 4])]
    assert collapsed.peek() == [(10, 2), (30, 4)]
    assert mapped.peek() == [(10, 3), (30, 7)]
    assert tumbling.peek()[0] == (25, [1, 2, 3])
    assert sliding.peek() == [
        (10, [1, 2]),
        (20, [1, 2, 3]),
        (30, [2, 3, 4]),
        (40, [3, 4]),
        (50, [4]),
    ]


def test_collapse_uses_last_item_of_non_empty_python_sequences():
    g = morel.Graph()
    collapsed = g.replay_from_iter(
        [
            (0, [1, 2]),
            (10, (3, 4)),
            (20, []),
            (30, "abc"),
        ]
    ).collapse().history()
    g.run(morel.Replay.from_nanos(0))
    assert collapsed.peek() == [(0, 2), (10, 4), (30, "c")]


def test_map_batch_callback_exception_preserves_type():
    class Boom(Exception):
        pass

    def fail(batch):
        raise Boom(f"map_batch failed for {batch}")

    g = morel.Graph()
    g.replay_from_iter([(0, 1), (10, 2)]).buffer(2).map_batch(fail)
    with pytest.raises(Boom, match=r"map_batch failed for \[1, 2\]"):
        g.run(morel.Replay.from_nanos(0))


def test_batch_operator_invalid_zero_values_raise_value_error():
    g = morel.Graph()
    src = g.just(1)

    with pytest.raises(ValueError, match="interval_nanos must be greater than 0"):
        src.throttle(interval_nanos=0)
    with pytest.raises(ValueError, match="quiet_nanos must be greater than 0"):
        src.debounce(quiet_nanos=0)
    with pytest.raises(ValueError, match="capacity must be greater than 0"):
        src.buffer(0)
    with pytest.raises(ValueError, match="size_nanos must be greater than 0"):
        src.window_tumbling(size_nanos=0)
    with pytest.raises(ValueError, match="size_nanos must be greater than 0"):
        src.window_sliding(size_nanos=0, slide_nanos=1)
    with pytest.raises(ValueError, match="slide_nanos must be greater than 0"):
        src.window_sliding(size_nanos=1, slide_nanos=0)


def test_delay_zero_passes_through_same_step():
    g = morel.Graph()
    out = g.replay_from_iter([(0, "a"), (10, "b")]).delay(delay_nanos=0).history()
    g.run(morel.Replay.from_nanos(0))
    assert out.peek() == [(0, "a"), (10, "b")]


def test_task6_operators_while_owner_graph_is_running_return_runtime_error():
    g = morel.Graph()
    src = g.just(1)
    g.begin(morel.Replay.from_nanos(0))

    methods = [
        lambda: src.delay(delay_nanos=1),
        lambda: src.throttle(interval_nanos=1),
        lambda: src.debounce(quiet_nanos=1),
        lambda: src.buffer(1),
        lambda: src.window_tumbling(size_nanos=1),
        lambda: src.window_sliding(size_nanos=1, slide_nanos=1),
        lambda: src.collapse(),
        lambda: src.map_batch(lambda batch: batch),
    ]
    for method in methods:
        with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
            method()

    assert g.step() is True
    assert g.end().steps == 1


def test_with_with_latest_gate_sample_merge_gather_unzip():
    g = morel.Graph()
    a = g.replay_from_iter([(0, 1), (10, 2), (20, 3)])
    b = g.replay_from_iter([(0, 10), (20, 20)])
    gate = g.replay_from_iter([(0, False), (10, True), (20, False)])
    trigger = g.replay_from_iter([(5, "tick"), (15, "tick")])

    with_out = a.with_(b, lambda x, y: x + y).history()
    with_latest_out = a.with_latest(b, lambda x, y: (x, y)).history()
    gated = a.gate(gate).history()
    sampled = a.sample(trigger).history()
    merged = morel.merge([a.map(lambda x: f"a{x}"), b.map(lambda x: f"b{x}")]).history()
    gathered = morel.gather([a, b]).history()
    left, right = a.with_(b, lambda x, y: (x, y)).unzip()
    left_h = left.history()
    right_h = right.history()

    g.run(morel.Replay.from_nanos(0))

    assert with_out.peek() == [(0, 11), (10, 12), (20, 23)]
    assert with_latest_out.peek() == [(0, (1, 10)), (10, (2, 10)), (20, (3, 20))]
    assert gated.peek() == [(10, 2)]
    assert sampled.peek() == [(5, 1), (15, 2)]
    assert merged.peek()[0] == (0, "a1")
    assert gathered.peek() == [(0, [1, 10]), (10, [2, 10]), (20, [3, 20])]
    assert left_h.peek() == [(0, 1), (10, 2), (20, 3)]
    assert right_h.peek() == [(0, 10), (10, 10), (20, 20)]


def test_with_callback_exception_preserves_type():
    class Boom(Exception):
        pass

    def fail(x, y):
        raise Boom(f"with failed for {x}, {y}")

    g = morel.Graph()
    a = g.replay_from_iter([(0, 1)])
    b = g.replay_from_iter([(0, 10)])
    a.with_(b, fail)

    with pytest.raises(Boom, match="with failed for 1, 10"):
        g.run(morel.Replay.from_nanos(0))


def test_gate_rejects_non_bool_latest_open_value():
    g = morel.Graph()
    source = g.replay_from_iter([(10, 1)])
    open_stream = g.replay_from_iter([(0, 1)])
    source.gate(open_stream)

    with pytest.raises(TypeError, match="gate stream must return bool"):
        g.run(morel.Replay.from_nanos(0))


def test_empty_merge_and_gather_raise_value_error():
    with pytest.raises(ValueError, match="merge requires at least one stream"):
        morel.merge([])
    with pytest.raises(ValueError, match="gather requires at least one stream"):
        morel.gather([])


def test_task7_combine_operators_while_owner_graph_is_running_return_runtime_error():
    g = morel.Graph()
    a = g.just(1)
    b = g.just(2)
    trigger = g.just("tick")
    pairs = a.with_(b, lambda x, y: (x, y))
    g.begin(morel.Replay.from_nanos(0))

    methods = [
        lambda: a.with_(b, lambda x, y: x + y),
        lambda: a.sample(trigger),
        lambda: pairs.unzip(),
        lambda: morel.merge([a, b]),
        lambda: morel.gather([a, b]),
    ]
    for method in methods:
        with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
            method()

    assert g.step() is True
    assert g.end().steps == 1
