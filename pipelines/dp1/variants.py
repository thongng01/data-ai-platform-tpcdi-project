"""Schema-variant selection, with no Spark in it.

The decision about which column list a row has, and the arithmetic of splitting
a file by field count, are pure. They do not need a cluster, a DataFrame or a
Spark session, so they live here rather than inside
`landing_to_bronze.py`.

That split is not tidiness for its own sake. The bug this module was written to
make impossible lives in exactly this logic: an evolved file is split into a
historical half and a delta half and the halves are unioned, and an unfiltered
union emits every line twice, once under each schema. It survived review because
deduplication hid the doubling for the tables whose business key covers every
column, so the bug was visible only as a row count that was too high somewhere
else.

Keeping the arithmetic importable means a test can assert the union equals its
input without a Spark session, which takes a second instead of four minutes.
"""

from __future__ import annotations

from .schemas import TableSchema


def delimiter_pattern(delimiter: str) -> str:
    """Return a regular expression that matches the delimiter literally.

    Spark's ``split`` takes a regular expression. A pipe-delimited file must
    have its pipe escaped, or the expression reads as alternation and splits on
    nothing. PySpark also resolves the overloaded JVM method differently for a
    string pattern than for a Column one, and a plain string is the form that is
    unambiguous.

    Args:
        delimiter: The literal delimiter.

    Returns:
        A regular expression matching the delimiter literally.
    """
    return "\\" + delimiter if delimiter == "|" else delimiter


def column_list(schema: TableSchema, batch_seq: int) -> list[str]:
    """Return the column names this batch declares for a table.

    Args:
        schema: The table's schema contract.
        batch_seq: The batch number.

    Returns:
        Column names in file order.
    """
    return list(schema.columns_for(batch_seq))


def split_counts(field_counts, historical_width: int, delta_width: int) -> dict:
    """Count how a set of rows divides between the two schema variants.

    Each row contributes the number of delimited fields in its line. The
    historical branch takes rows matching the historical width, the delta branch
    takes rows matching the delta width, and the rest are counted separately
    rather than being padded or truncated into one of the two.

    Args:
        field_counts: An iterable of field counts, one per input line.
        historical_width: Field count of the historical shape.
        delta_width: Field count of the delta shape.

    Returns:
        A dict with ``historical``, ``delta``, ``unmatched`` and ``total``. The
        first three sum to ``total``, so a caller can assert the split neither
        duplicated nor dropped a line.
    """
    historical = 0
    delta = 0
    unmatched = 0
    total = 0

    for count in field_counts:
        total += 1
        if count == historical_width:
            historical += 1
        elif count == delta_width:
            delta += 1
        else:
            unmatched += 1

    return {
        "historical": historical,
        "delta": delta,
        "unmatched": unmatched,
        "total": total,
    }


def validate_split(schema: TableSchema, field_counts) -> dict:
    """Split a file's rows by schema variant and refuse a lossy split.

    This is the guard that turns the doubling bug into an exception rather than
    a row count nobody checks. It raises when the two branches do not between
    them account for every line, because that means either the split is
    unfiltered or the file has a shape nobody declared.

    Args:
        schema: The table's schema contract.
        field_counts: An iterable of field counts, one per input line.

    Returns:
        The split counts, with ``total`` included.

    Raises:
        ValueError: When the two variants do not account for every line.
    """
    counts = split_counts(field_counts, schema.column_count(1), schema.column_count(2))
    accounted = counts["historical"] + counts["delta"] + counts["unmatched"]
    if accounted != counts["total"]:
        raise ValueError(
            f"{schema.name}: the split accounted for {accounted} of "
            f"{counts['total']} lines"
        )
    return counts


__all__ = [
    "column_list",
    "delimiter_pattern",
    "split_counts",
    "validate_split",
]