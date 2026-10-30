"""Regression tests for schema-variant selection.

The bug this file exists for: DP1 split an evolved file into a historical half
and a delta half, unioned them, and forgot to filter either half by field count.
Every line was therefore emitted twice, once under each schema.

It did not look like a bug. Where the business key covers every column,
deduplication collapsed the twin and the row count came out right, which is why
the doubling survived in the first place. Where the key covers only some
columns, the mis-parsed twin survives with different values in the columns the
two shapes disagree about, and the count comes out quietly high.

These tests are about the arithmetic of the split, not about Spark. The rule is
that the two variants are disjoint and their union is exactly the input, and
that is checkable without a cluster.
"""

import pytest

from dp1 import schemas
from dp1.variants import column_list, delimiter_pattern, split_counts, validate_split


def _split(field_counts, historical_width, delta_width):
    """Thin wrapper so each test reads as one call."""
    return split_counts(field_counts, historical_width, delta_width)


class TestVariantsAreDisjoint:
    """The two branches must not both claim the same line."""

    @pytest.mark.parametrize("schema", schemas.DELTA_TABLES, ids=lambda s: s.name)
    def test_historical_width_and_delta_width_differ(self, schema):
        """If the widths matched, the split would be meaningless."""
        assert schema.column_count(1) != schema.column_count(2)

    @pytest.mark.parametrize("schema", schemas.DELTA_TABLES, ids=lambda s: s.name)
    def test_every_input_line_lands_in_exactly_one_branch(self, schema):
        """The union is the input, not twice the input.

        This is the assertion the original code failed. A line has one field
        count, so it satisfies exactly one of the two width tests.
        """
        historical_width = schema.column_count(1)
        delta_width = schema.column_count(2)

        counts = _split([historical_width] * 100, historical_width, delta_width)
        assert counts["historical"] == 100, "a Batch1 file must not double"
        assert counts["delta"] == 0
        assert counts["unmatched"] == 0

        counts = _split([delta_width] * 100, historical_width, delta_width)
        assert counts["delta"] == 100, "a Batch2 file must not double"
        assert counts["historical"] == 0
        assert counts["unmatched"] == 0

    @pytest.mark.parametrize("schema", schemas.DELTA_TABLES, ids=lambda s: s.name)
    def test_a_file_that_mixes_both_shapes_keeps_every_line_once(self, schema):
        """A writer that changed shape mid-file is the case the split exists for."""
        historical_width = schema.column_count(1)
        delta_width = schema.column_count(2)
        mixed = [historical_width] * 30 + [delta_width] * 70

        counts = _split(mixed, historical_width, delta_width)
        assert counts["historical"] == 30
        assert counts["delta"] == 70
        assert counts["unmatched"] == 0
        accounted = counts["historical"] + counts["delta"] + counts["unmatched"]
        assert accounted == len(mixed), "a line was emitted twice or lost"

    def test_a_row_matching_neither_shape_is_counted_not_guessed(self):
        """A schema change nobody declared must show up, not be padded.

        Without this, a 17-field row in a 16-field file would be truncated to 16
        and read as a valid row with the last column missing.
        """
        counts = _split([17, 14, 16], 14, 16)
        assert counts["unmatched"] == 1
        assert counts["historical"] == 1
        assert counts["delta"] == 1


class TestSplitIsValidated:
    """A split that loses a line must raise, not return a smaller count."""

    def test_validate_split_passes_on_a_clean_file(self):
        counts = validate_split(schemas.TRADE, [14] * 50 + [16] * 50)
        assert counts["total"] == 100
        assert counts["historical"] == 50
        assert counts["delta"] == 50

    def test_validate_split_reports_unmatched_rather_than_raising(self):
        """An undeclared shape is a warning about the data, not a code error."""
        counts = validate_split(schemas.TRADE, [14, 16, 15])
        assert counts["unmatched"] == 1
        assert counts["total"] == 3

    def test_the_helper_is_importable_without_pyspark(self):
        """This module must stay free of Spark so its tests need no cluster."""
        import dp1.variants

        assert not hasattr(dp1.variants, "SparkSession")


class TestPositionalMapping:
    """The payload must mean the same thing in both shapes.

    Every delta batch puts identifying columns in front of the payload. Most add
    two: an operation code and a surrogate row id. Trade adds three, because it
    also gains the history id that gives a trade its business identity. That
    asymmetry is why Trade is the one table whose business key differs between
    batches.
    """

    IDENTIFIERS = {"operation", "row_id", "business_id"}

    def test_the_payload_is_the_same_suffix_in_both_shapes(self):
        """This is what makes a union by name correct and by position wrong.

        The tail of the two shapes has to be the same columns in the same order,
        or the delta values land two places to the left of where they belong.
        """
        for schema in schemas.DELTA_TABLES:
            historical = column_list(schema, 1)
            delta = column_list(schema, 2)

            # The payload starts at the first column that is not an identifier.
            cut = next(
                i for i, name in enumerate(historical) if name not in self.IDENTIFIERS
            )
            payload = historical[cut:]
            assert delta[-len(payload):] == payload, schema.name

    def test_nothing_but_identifiers_precedes_the_payload(self):
        for schema in schemas.DELTA_TABLES:
            delta = column_list(schema, 2)
            historical = column_list(schema, 1)

            cut = next(
                i for i, name in enumerate(historical) if name not in self.IDENTIFIERS
            )
            inserted = len(delta) - len(historical[cut:])
            assert set(delta[:inserted]) <= self.IDENTIFIERS, (
                f"{schema.name}: {delta[:inserted]} sits ahead of the payload "
                "and is not a set of identifiers"
            )

    def test_trade_carries_one_more_identifier_than_the_others(self):
        """Trade is the special case, and the special case is the point.

        A trade's identity is its history id, which does not exist in the
        historical load. The delta batch inserts operation, row id and history id
        where the historical batch has only a row id, so the business key has to
        follow.
        """
        assert column_list(schemas.TRADE, 2)[:3] == [
            "operation",
            "row_id",
            "business_id",
        ]
        assert column_list(schemas.TRADE, 1)[:1] == ["row_id"]
        assert "business_id" not in column_list(schemas.TRADE, 1)

        for schema in schemas.DELTA_TABLES:
            if schema.name == "Trade":
                continue
            assert column_list(schema, 2)[:2] == ["operation", "row_id"], schema.name

    def test_a_pipe_delimiter_is_escaped_for_split(self):
        """`split` takes a regular expression, so a bare pipe is an alternation."""
        assert delimiter_pattern("|") == "\\|"
        assert delimiter_pattern(",") == ","


class TestDuplicateRateSanity:
    """The bronze row count must match the file, not double it.

    The generator injects a 2 percent offline duplicate rate by default, so a
    file of N lines must produce between N and 1.1N bronze rows. The doubling
    bug put it at 2N, which is outside any sane duplicate rate and was the only
    reason it was caught.
    """

    @pytest.mark.parametrize(
        "schema,lines",
        [
            (schemas.TRADE, 7980),
            (schemas.CASH_TRANSACTION, 7390),
            (schemas.WATCH_HISTORY, 18367),
        ],
    )
    def test_bronze_count_stays_near_the_file_line_count(self, schema, lines):
        historical_width = schema.column_count(1)
        delta_width = schema.column_count(2)

        counts = validate_split(schema, [historical_width] * lines)
        produced = counts["historical"] + counts["delta"] + counts["unmatched"]

        assert produced == lines, "the split changed the number of rows"
        assert produced <= lines * 1.1, "more rows than the file has lines"
