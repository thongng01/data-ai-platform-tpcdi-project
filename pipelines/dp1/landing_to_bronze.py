"""Landing to bronze: read the source files exactly as they arrived.

Bronze is a faithful copy with provenance attached. Nothing is cleaned, nothing
is deduplicated, nothing is typed. A value that looks wrong stays exactly as
wrong as it was, because bronze is the record of what the source system
actually sent, and a pipeline that fixes data on the way in has no way to show
what it fixed.

Two problems are handled here and nowhere else.

Schema evolution. Batch1 `Trade.txt` has 14 pipe-delimited fields and Batch2
has 16, because an operation code and a business history id appear at the
front. A reader that reads Batch1, keeps the column list, and applies it to
Batch2 reads every row shifted by two columns and produces plausible-looking
numbers. So the column list is chosen per row from the row's own field count,
and the counts are reported.

Whitespace. Text fields contain spaces, on purpose. The parser splits on the
delimiter, never on whitespace, and no value is ever re-tokenised.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import ALL_TABLES, HISTORICAL_ONLY, SCHEMAS_BY_NAME, TableSchema
from .variants import column_list as _column_list
from .variants import delimiter_pattern as _pattern

LOG = logging.getLogger("dp1.bronze")


def _positional_columns(names: list[str], parts, field_count) -> list:
    """Build one column expression per name, reading positions out of ``parts``.

    A row whose field count does not match gets nulls for every column, rather
    than being silently padded or truncated. Truncation is the dangerous case:
    it produces a row that looks valid and carries the wrong values.

    Args:
        names: Column names in file order.
        parts: Column expression holding the split fields as an array.
        field_count: Column expression holding the row's field count.

    Returns:
        A list of column expressions.
    """
    expected = len(names)
    array = F.col(parts)
    return [
        F.when(F.col("_field_count") == expected, array[index]).otherwise(F.lit(None).cast("string"))
        .alias(name)
        for index, name in enumerate(names)
    ]


def _read_one(session: SparkSession, schema: TableSchema, config) -> DataFrame | None:
    """Read one source file into a bronze DataFrame.

    Args:
        session: The SparkSession.
        schema: The table's schema contract.
        config: The job configuration.

    Returns:
        The DataFrame, or None when the batch does not carry this file.
    """
    path = config.landing_path(f"{schema.name}.txt")

    if not _path_exists(session, path):
        LOG.info("%s: not in %s, skipping", schema.name, config.batch_id)
        return None

    raw = session.read.text(path)
    # The generator writes CRLF. Strip the CR or the last field of every row
    # carries a trailing carriage return that no type cast will remove.
    frame = raw.select(
        F.regexp_replace(F.col("value"), r"\r$", "").alias("_line")
    )
    frame = frame.withColumn(
        "_field_count", F.size(F.split(F.col("_line"), _pattern(schema.delimiter)))
    )
    frame = frame.withColumn("parts", F.split(F.col("_line"), _pattern(schema.delimiter)))

    historical_names = _column_list(schema, 1)
    delta_names = _column_list(schema, 2)

    if len(historical_names) == len(delta_names):
        # No evolution for this file: one column list regardless of batch.
        frame = frame.select(*_positional_columns(delta_names, "parts", "_field_count"))
        branch_counts = {"single": frame.count()}
    else:
        # Evolved file. Two column lists of different lengths cannot come out of
        # one select, so the file is split and the two halves unioned.
        #
        # Each half MUST be filtered by field count. Without the filter every
        # line is emitted twice, once under each interpretation, and the union
        # doubles the table. It does not always look wrong: where the business
        # key covers every column, deduplication collapses the pair and hides
        # it. Where the key covers only some columns, the mis-parsed twin
        # survives with different values in the columns the two shapes disagree
        # about, and the count comes out quietly high.
        historical = (
            frame.where(F.col("_field_count") == len(historical_names))
            .select(*_positional_columns(historical_names, "parts", "_field_count"))
        )
        delta = (
            frame.where(F.col("_field_count") == len(delta_names))
            .select(*_positional_columns(delta_names, "parts", "_field_count"))
        )
        # byName, not by position. A positional union aligns the two shapes
        # index by index, so the delta operation code lands in the first
        # historical column and every value after it shifts one place left.
        frame = historical.unionByName(delta, allowMissingColumns=True)

        # Rows whose field count matches neither shape. A schema change nobody
        # declared shows up here rather than as a silently short row.
        unmatched = frame.count() - historical.count() - delta.count()
        if unmatched:
            LOG.warning(
                "%s: %d rows matched neither the %d-field nor the %d-field shape",
                schema.name,
                unmatched,
                len(historical_names),
                len(delta_names),
            )
        branch_counts = {
            "historical": historical.count(),
            "delta": delta.count(),
            "unmatched": unmatched,
        }

    LOG.info(
        "%s: %d rows, branches %s",
        schema.name,
        frame.count(),
        branch_counts,
    )

    # Empty string means NULL in every TPC-DI file. Converting here rather than
    # at cast time keeps one rule for every column.
    for column in frame.columns:
        frame = frame.withColumn(
            column, F.when(F.col(column) == "", F.lit(None)).otherwise(F.col(column))
        )

    frame = frame.withColumns(
        {
            "ingest_batch_id": F.lit(config.batch_id),
            "ingest_batch_seq": F.lit(config.batch_seq),
            "ingest_ts": F.current_timestamp(),
            "ingest_source_file": F.lit(f"{schema.name}.txt"),
        }
    )
    return frame


def _path_exists(session: SparkSession, path: str) -> bool:
    """Check whether a path exists, without leaking the FileSystem instance.

    S3A caches directory listings, so a FileSystem that has just deleted
    something will still report it. The instance is closed on the way out so the
    next caller gets a fresh cache.

    Args:
        session: The SparkSession.
        path: An s3a:// path.

    Returns:
        True when the path exists.
    """
    jvm = session.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(session.sparkContext._jsc.hadoopConfiguration())
    try:
        return bool(fs.exists(hadoop_path))
    finally:
        fs.close()


def create_external_table(
    session: SparkSession, target: str, frame: DataFrame, location: str, partition_by=()
) -> None:
    """Create an external Hive table over a Parquet path.

    Every table in the lakehouse is external, and that is not a style choice.
    Spark refuses to create a *managed* table whose location already exists.
    SeaweedFS's S3 gateway reports an empty prefix as an existing directory, so
    a managed CREATE TABLE fails with LOCATION_ALREADY_EXISTS on a path that
    lists as empty. Declaring the location skips the check. dbt's
    `location_root` produces exactly this shape.

    Args:
        session: The SparkSession.
        target: Fully qualified table name.
        frame: A DataFrame whose schema defines the columns.
        location: The s3a:// path the table owns.
        partition_by: Names of partition columns, which must come last.
    """
    partition_columns = list(partition_by)
    # A Hive partition column is declared once, in the PARTITIONED BY clause, and
    # is not repeated in the column list. The frame still carries it, because
    # the writer needs its value to build the dt= directories; only the DDL
    # drops it.
    data_columns = [
        field
        for field in frame.schema.fields
        if field.name not in partition_columns
    ]

    columns = ", ".join(
        f"{field.name} {field.dataType.simpleString()}" for field in data_columns
    )
    partition_clause = ""
    if partition_columns:
        declared = ", ".join(f"{name} STRING" for name in partition_columns)
        partition_clause = f"PARTITIONED BY ({declared})\n"

    session.sql(f"DROP TABLE IF EXISTS {target}")
    session.sql(
        f"CREATE EXTERNAL TABLE {target} ({columns})\n"
        f"{partition_clause}"
        f"STORED AS PARQUET\n"
        f"LOCATION '{location}'"
    )


def landing_to_bronze(session: SparkSession, config) -> dict:
    """Read every source file in the batch into a bronze table.

    Args:
        session: The SparkSession.
        config: The job configuration.

    Returns:
        A mapping of bronze table name to row count written.
    """
    session.sql(f"CREATE DATABASE IF NOT EXISTS {config.bronze_db}")
    written: dict[str, int] = {}

    for schema in ALL_TABLES:
        if schema.name in HISTORICAL_ONLY and config.is_incremental:
            LOG.info("%s is historical-only, absent from %s", schema.name, config.batch_id)
            continue

        frame = _read_one(session, schema, config)
        if frame is None:
            continue

        target = f"{config.bronze_db}.{schema.bronze_table}"
        location = config.bronze_path(schema.bronze_table)

        create_external_table(session, target, frame, location)
        frame.write.mode(config.write_mode).save(location)

        count = frame.count()
        written[schema.bronze_table] = count
        LOG.info("bronze %s: %d rows", schema.bronze_table, count)

    LOG.info("bronze tables written: %s", sorted(written))
    return written


__all__ = [
    "SCHEMAS_BY_NAME",
    "create_external_table",
    "landing_to_bronze",
]