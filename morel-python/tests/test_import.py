import morel


def test_import_exposes_version():
    assert isinstance(morel.__version__, str)
    assert morel.__version__


def test_import_exposes_initial_classes():
    for name in ["Graph", "Stream", "Replay", "Live", "Stop", "GraphError"]:
        assert hasattr(morel, name)


def test_graph_error_is_instantiable_exception_with_message():
    err = morel.GraphError("x")

    assert isinstance(err, Exception)
    assert str(err) == "x"
