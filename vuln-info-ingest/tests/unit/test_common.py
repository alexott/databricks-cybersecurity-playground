import pytest

from common import params


class FakeWidgets:
    def __init__(self, values):
        self.values = values
        self.created = []

    def text(self, name, default):
        self.created.append(name)

    def get(self, name):
        return self.values.get(name, "")


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


def test_get_params_converts_types():
    dbu = FakeDbutils({"catalog": "c", "schema": "s", "volume": "v", "threads": " 8 ", "ratio": "0.9"})
    p = params.get_params(dbu, {**params.COMMON_PARAMS, "threads": int, "ratio": float})
    assert p == {"catalog": "c", "schema": "s", "volume": "v", "threads": 8, "ratio": 0.9}
    assert dbu.widgets.created == ["catalog", "schema", "volume", "threads", "ratio"]


def test_get_params_missing_names_parameter():
    dbu = FakeDbutils({"catalog": "c", "schema": "  "})
    with pytest.raises(ValueError, match="schema"):
        params.get_params(dbu, params.COMMON_PARAMS)


def test_get_params_bad_number_names_parameter():
    dbu = FakeDbutils({"threads": "many"})
    with pytest.raises(ValueError, match="threads"):
        params.get_params(dbu, {"threads": int})


def test_table_name_quotes_identifiers():
    p = {"catalog": "cyber-sec", "schema": "vu`ln", "volume": "ref"}
    assert params.table_name(p, "osv_raw") == "`cyber-sec`.`vu``ln`.`osv_raw`"
    assert params.volume_name(p) == "`cyber-sec`.`vu``ln`.`ref`"


def test_volume_path():
    p = {"catalog": "c", "schema": "s", "volume": "reference"}
    assert params.volume_path(p, "osv_checkpoints") == "/Volumes/c/s/reference/osv_checkpoints"
