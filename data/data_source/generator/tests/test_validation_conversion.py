"""Compare cached staging conversion with the independent scalar conversion rules."""

from dataclasses import replace
from unittest import mock

import pytest

from data.data_source.generator import validation
from data.data_source.generator.formats import Field, Source, TYPES, load_sources


class StageStub:
    """Keep tiny staged rows in memory without writing Parquet files."""

    def __init__(self):
        """Provide the partition count, source cache, and captured output rows."""
        self.partitions = 3
        self._source_plans = {}
        self.rows = []

    def add(self, name, row, schema):
        """Retain the produced row and schema for an exact comparison."""
        self.rows.append((name, row, schema))


def _representative(source):
    """Build values that exercise every source conversion category."""
    row = {}
    for field in source.fields:
        datatype = TYPES.get(field.type, field.type)
        row[field.name] = ("2020-01-01" if datatype == "DATE" else
                           "2020-01-01 08:00:00" if datatype == "DATETIME" else
                           "1.23" if ",2)" in datatype else
                           "1" if datatype.startswith(("NUM(", "SNUM(")) or datatype == "BOOLEAN" else "A")
    return row


def _scalar_stage(stage, source, row, day, key):
    """Use the XML scalar helpers as an uncached conversion reference."""
    result = validation._typed(source, row)
    result.update(_flag=row.get("_flag", "I"), _order=row.get("_order", 0), _day=day,
                  _partition=str(key) if source.name == "DailyMarket" else str(int(key) % stage.partitions))
    stage.add(source.name, result, validation._source_schema(source))


def _capture(function, stage, source, row, key=5):
    """Capture ordered value types and schemas, or the exact exception and message."""
    try:
        function(stage, source, row, "2020-01-01", key)
        name, record, schema = stage.rows[-1]
        return "passed", name, tuple((key, type(value), repr(value)) for key, value in record.items()), tuple(schema.items())
    except Exception as error:
        return "failed", type(error), str(error)


def test_all_sources_and_malformed_values_match():
    """Preserve conversions and failure order across source fields and Trade versions."""
    sources = list(load_sources().values())
    sources.append(load_sources()["Trade"].daily_version)
    sources.append(Source("Fractional", "fractional.txt", "|", (
        Field("integer", "NUM(9)"), Field("money", "SNUM(9,2)"),
        Field("fractional_text", "NUM(9,3)"), Field("plain_text", "CHAR(8)"))))
    changes = (None, "", "bad", "NaN", "Infinity", "1.001", "-0.00", "+1", " 1 ", "1e2", 1, True, 1.5)
    for source in sources:
        base = _representative(source)
        variants = [base, {}, {**base, "_flag": "U", "_order": 9, "ignored": "value"}]
        variants.extend({**base, field.name: value} for field in source.fields for value in changes)
        stage = StageStub()
        for row in variants:
            assert _capture(validation._stage_source, stage, source, row) == _capture(_scalar_stage, StageStub(), source, row)
        assert len(stage._source_plans) == 1
        assert stage._source_plans[id(source)][2] == validation._source_schema(source)


@pytest.mark.parametrize("row", [
    {"first": "bad", "second": "NaN"}, {"first": "1", "second": "1.001"},
    {"first": "", "second": None, "text": ""}, {"first": True, "second": "1e2"},
])
def test_partial_rows_and_first_error_order(row):
    """Keep absent text distinct from empty text and fail in source field order."""
    source = Source("Partial", "partial.txt", "|", (
        Field("first", "NUM(9)"), Field("second", "SNUM(9,2)"), Field("text", "CHAR(8)")))
    assert _capture(validation._stage_source, StageStub(), source, row) == _capture(_scalar_stage, StageStub(), source, row)


def test_warm_plan_does_not_resolve_types_or_schema():
    """Avoid repeated metadata work after the source plan exists."""
    source = load_sources()["Trade"]
    stage = StageStub()
    row = _representative(source)
    validation._stage_source(stage, source, row, "initial", 5)
    with mock.patch.object(validation, "_field_type", side_effect=AssertionError("Repeated type resolution")), \
            mock.patch.object(validation, "_source_schema", side_effect=AssertionError("Repeated schema creation")):
        validation._stage_source(stage, source, row, "initial", 5)
    assert stage.rows[0] == stage.rows[1]
    assert stage.rows[0][2] is stage.rows[1][2]


def test_partition_keys_and_metadata_match():
    """Keep default metadata and key conversion, including invalid keys."""
    for source in (load_sources()["Trade"], load_sources()["DailyMarket"]):
        for key in ("2019-01-01", "5", -2, 1.5, None):
            row = {**_representative(source), "_flag": None, "_order": None}
            assert _capture(validation._stage_source, StageStub(), source, row, key) == _capture(_scalar_stage, StageStub(), source, row, key)


def test_source_identity_does_not_hash_all_fields():
    """Retain equal but distinct source definitions without hashing their fields."""
    first = load_sources()["Trade"]
    second = replace(first)
    stage = StageStub()
    with mock.patch.object(Source, "__hash__", side_effect=AssertionError("Source hashing is forbidden")), \
            mock.patch.object(validation, "_compile_plan", wraps=validation._compile_plan) as compile_plan:
        for source in (first, first, second, second):
            validation._stage_source(stage, source, _representative(source), "initial", 5)
        assert compile_plan.call_count == 2
    assert stage._source_plans[id(first)][0] is first
    assert stage._source_plans[id(second)][0] is second


def test_trade_fallback_and_source_versions_remain_distinct():
    """Keep the channel schema fallback without adding it to old-version rows."""
    source = load_sources()["Trade"]
    stage = StageStub()
    for version in (source, source.daily_version, source):
        validation._stage_source(stage, version, _representative(version), "initial", 5)
    assert "T_CHANNEL" not in stage.rows[0][1]
    assert "T_CHANNEL" in stage.rows[1][1]
    assert "T_CHANNEL" not in stage.rows[2][1]
    assert all(schema["T_CHANNEL"] == validation.pl.String for _, _, schema in stage.rows)
    assert stage.rows[0][2] == stage.rows[1][2]
    assert len(stage._source_plans) == 2


def test_source_cache_lifetime_matches_stage(tmp_path):
    """Keep plans local to one stage and leave scalar XML conversion unchanged."""
    first = validation._Stage(tmp_path / "first", 2)
    second = validation._Stage(tmp_path / "second", 2)
    source = load_sources()["Trade"]
    validation._stage_source(first, source, _representative(source), "initial", 5)
    assert len(first._source_plans) == 1
    assert second._source_plans == {}
    assert first.rows == 1
    assert first.schemas["Trade"] == validation._source_schema(source)
    assert validation._typed(source, {})["T_ID"] is None
