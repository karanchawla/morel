import morel
import pytest


HAS_RECORDING_JSON = hasattr(morel.Recording(), "save_json") and hasattr(
    morel.Recording, "load_json"
)

requires_recording_json = pytest.mark.skipif(
    not HAS_RECORDING_JSON, reason="morel serde feature is not enabled"
)


def test_recording_take_and_replay_from_log_round_trip():
    g = morel.Graph()
    rec = morel.Recording()
    src = g.replay_from_iter(
        [
            (0, {"kind": "start", "values": [1, True, None]}),
            (10, {"kind": "stop", "values": [2, False, "done"]}),
        ]
    )
    ticks = src.record(rec).history()

    g.run(morel.Replay.from_nanos(0))

    log = rec.take()
    assert ticks.peek() == [(0, None), (10, None)]
    assert log == [
        (0, {"kind": "start", "values": [1, True, None]}),
        (10, {"kind": "stop", "values": [2, False, "done"]}),
    ]
    assert rec.take() == []

    replay = morel.Graph()
    out = replay.replay_from_log(log).history()
    replay.run(morel.Replay.from_nanos(0))
    assert out.peek() == log


@requires_recording_json
def test_recording_json_lines_round_trip(tmp_path):
    path = tmp_path / "recording.jsonl"
    g = morel.Graph()
    rec = morel.Recording()
    g.replay_from_iter(
        [
            (5, {"n": 1, "items": ["a", None]}),
            (15, [True, False, 3.25, "x"]),
        ]
    ).record(rec)
    g.run(morel.Replay.from_nanos(0))

    expected = [
        (5, {"n": 1, "items": ["a", None]}),
        (15, [True, False, 3.25, "x"]),
    ]
    rec.save_json(path)
    loaded = morel.Recording.load_json(path)

    assert rec.take() == expected
    assert loaded.take() == expected


def test_replay_from_csv_uses_parse_callback_and_skips_header(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text("time,value,label\n0,1,a\n10,2,b\n")
    rows = []

    def parse(row):
        rows.append(row)
        return (int(row[0]), {"value": int(row[1]), "label": row[2]})

    g = morel.Graph()
    out = g.replay_from_csv(path, parse).history()
    g.run(morel.Replay.from_nanos(0))

    assert rows == [["0", "1", "a"], ["10", "2", "b"]]
    assert out.peek() == [
        (0, {"value": 1, "label": "a"}),
        (10, {"value": 2, "label": "b"}),
    ]


def test_source_worker_child_graph_replay_from_csv(tmp_path):
    path = tmp_path / "child-events.csv"
    path.write_text("time,value\n0,a\n10,b\n")

    def parse(row):
        return (int(row[0]), row[1].upper())

    g = morel.Graph()

    def build(child):
        assert hasattr(morel.ChildGraph, "replay_from_csv")
        return child.replay_from_csv(path, parse)

    out = morel.source_worker(g, build).collapse().history()

    g.run(morel.Replay.from_nanos(0).stop(morel.Stop.at_nanos(10)))

    assert out.peek() == [(0, "A"), (10, "B")]


def test_record_and_replay_from_log_while_running_return_runtime_error():
    g = morel.Graph()
    src = g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        src.record(morel.Recording())

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.replay_from_log([(0, "late")])

    assert g.step() is True
    assert g.end().steps == 1


def test_replay_from_csv_while_running_returns_runtime_error(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text("time,value\n0,a\n")
    g = morel.Graph()
    g.just("x")
    g.begin(morel.Replay.from_nanos(0))

    with pytest.raises(RuntimeError, match="cannot add nodes while graph is running"):
        g.replay_from_csv(path, lambda row: (int(row[0]), row[1]))

    assert g.step() is True
    assert g.end().steps == 1


def test_replay_from_csv_preserves_parse_callback_exception(tmp_path):
    class CsvBoom(Exception):
        pass

    path = tmp_path / "events.csv"
    path.write_text("time,value\n0,a\n")
    g = morel.Graph()
    g.replay_from_csv(
        path, lambda row: (_ for _ in ()).throw(CsvBoom("parse failed"))
    )

    with pytest.raises(CsvBoom, match="parse failed"):
        g.run(morel.Replay.from_nanos(0))


def test_replay_from_csv_rejects_malformed_parse_result(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text("time,value\n0,a\n")
    g = morel.Graph()
    g.replay_from_csv(path, lambda row: "not a pair")

    with pytest.raises(TypeError, match="csv parse callback must return"):
        g.run(morel.Replay.from_nanos(0))
