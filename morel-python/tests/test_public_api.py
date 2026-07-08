import morel


def test_morel_public_api_exports_intended_names():
    expected = {
        "Capacity",
        "ChannelRx",
        "ChannelTx",
        "ChildGraph",
        "ChildStream",
        "Ctx",
        "Graph",
        "GraphError",
        "Input",
        "Live",
        "OnClose",
        "Output",
        "Producer",
        "Recording",
        "Replay",
        "Stop",
        "Stream",
        "Summary",
        "Wire",
        "channel",
        "gather",
        "merge",
        "producer",
        "source_worker",
        "worker",
    }

    assert set(morel.__all__) == expected | {"__version__"}
    assert len(morel.__all__) == len(set(morel.__all__))
    for name in expected:
        assert hasattr(morel, name)


def test_wingfoil_aliases_are_not_exported():
    wingfoil_aliases = {
        "average",
        "collect",
        "constant",
        "difference",
        "limit",
        "with_time",
    }

    assert wingfoil_aliases.isdisjoint(morel.__all__)
    for name in wingfoil_aliases:
        assert not hasattr(morel, name)


def test_module_level_async_producers_are_not_exported():
    async_producers = {"produce_async", "produce_async_stream"}

    assert async_producers.isdisjoint(morel.__all__)
    for name in async_producers:
        assert not hasattr(morel, name)
