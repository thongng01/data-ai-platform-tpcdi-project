"""Bronze to silver: type the data, remove duplicates, partition by event time.

Silver is one row per real-world thing, typed, with no duplicates and no
arrival-order columns. Three things change from bronze and nothing else does.

Types. Every column becomes its declared type. A value that will not cast does
not become null silently; it lands in a quarantine column so the row survives
and the problem is visible.

Duplicates. The generator injects them, and a whole row repeated twice
overcounts every sum built on it. Deduplication keys on the *business key*, not
the row id. For a trade that is the history id plus status: in a delta batch the
same trade arrives twice, once submitted and once completed, and those are two
different states of one trade, not two trades.

Partitioning. Silver partitions on the event timestamp's date, never on the
batch id or the arrival date. A trade from 2016 delivered in a 2017 batch belongs
to 2016, because every query about it is about when it happened.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .landing_to_bronze import create_external_table
from .schemas import ALL_TABLES, COLUMN_TYPES, DELTA_TABLES, HISTORICAL_ONLY, SCHEMAS_BY_NAME

LOG = logging.getLogger("dp1.silver")

# Columns that describe arrival, not the event. They stay in bronze as the
# audit trail and do not survive into silver.
ARRIVAL_COLUMNS = ("ingest_ts", "ingest_source_file")


def _cast_columns(frame: DataFrame) -> DataFrame:
    """Apply the declared types, sending uncastable values to a side column.

    Spark's ``cast`` returns null for a value it cannot parse, and a null that
    looks like a legitimately missing value is the one failure mode a pipeline
    should never have. So each cast is checked: a row whose source value was
    non-empty and whose cast value is null has failed, and its original text is
    kept in ``_rejected_columns``.

    Args:
        frame: The bronze DataFrame.

    Returns:
        The typed DataFrame, with ``_rejected_columns`` when anything failed.
    """
    rejected = []
    for column in frame.columns:
        declared = COLUMN_TYPES.get(column)
        if declared is None:
            continue
        source = F.col(column)
        cast = source.cast(declared)
        rejected.append(F.when(source.isNotNull() & cast.isNull(), source))
        # Plain column name. Spark treats a backtick as part of the name in
        # withColumn, so withColumn("`amount`", ...) adds a second column called
        # "`amount`" and leaves the original string column in place.
        frame = frame.withColumn(column, cast)

    if rejected:
        # Every value is cast to string first. concat_ws needs one type across
        # all its arguments, and a boolean column among string columns makes it
        # a type error rather than a coercion.
        combined = F.concat_ws(
            ",", *[F.coalesce(value.cast("string"), F.lit("")) for value in rejected]
        )
        frame = frame.withColumn("_rejected_columns", combined)

    return frame


def _add_partition(frame: DataFrame, schema, partition_column: str) -> DataFrame:
    """Derive the partition column from the event timestamp.

    Args:
        frame: The typed DataFrame.
        schema: The table's schema contract.
        partition_column: The name of the column to add.

    Returns:
        The frame with a date-string partition column appended.
    """
    if not schema.event_time or schema.event_time not in frame.columns:
        # Reference tables and holding history have no event time. They are
        # small and are re-read whole, so they get no partition column.
        return frame
    return frame.withColumn(
        partition_column,
        F.to_date(F.col(schema.event_time)).cast("string"),
    )


def _deduplicate(frame: DataFrame, schema, strategy: str, config_batch_seq: int) -> DataFrame:
    """Remove duplicate deliveries of the same business record.

    Args:
        frame: The typed DataFrame.
        schema: The table's schema contract.
        strategy: ``latest`` or ``all``.
        config_batch_seq: The batch number, which picks the business key.

    Returns:
        The deduplicated frame.
    """
    if strategy == "all":
        LOG.warning("%s: dedup disabled, duplicates kept on purpose", schema.name)
        return frame

    key = [column for column in schema.key_for(config_batch_seq) if column in frame.columns]
    if not key:
        LOG.info("%s: no business key columns present, nothing to deduplicate", schema.name)
        return frame

    before = frame.count()
    # Window rather than dropDuplicates: dropDuplicates keeps an arbitrary row
    # among equals, and "arbitrary" is not reproducible across runs. Ordering by
    # ingest_batch_seq and then the event time makes the choice deterministic.
    order = [F.col("ingest_batch_seq").desc()]
    if schema.event_time and schema.event_time in frame.columns:
        order.insert(0, F.col(schema.event_time).desc())

    ranked = frame.withColumn("_rank", F.row_number().over(Window.partitionBy(*key).orderBy(*order)))
    kept = ranked.where(F.col("_rank") == 1).drop("_rank")
    after = kept.count()

    if before != after:
        LOG.info("%s: dropped %d duplicate rows", schema.name, before - after)
    return kept


def _drop_arrival_columns(frame: DataFrame) -> DataFrame:
    """Remove the columns that describe arrival rather than the event."""
    present = [column for column in ARRIVAL_COLUMNS if column in frame.columns]
    if not present:
        return frame
    return frame.drop(*present)


def _clear_location(session: SparkSession, location: str) -> bool:
    """Delete everything under a silver table's location.

    `overwrite` with the dynamic partition overwrite mode replaces only the
    partitions that receive data. Anything else under the location survives: a
    file written by an earlier run that was partitioned differently, a leftover
    `.spark-staging-*` tree from a killed job, a stray file at the table root.
    Those then get counted by a reader that lists the directory, so a re-run is
    not idempotent and the job's own row count disagrees with the query engine's.

    A batch job owns its silver location, so clearing it first is the cheap way
    to make a re-run mean the same thing twice. `append` is the mode that
    deliberately keeps what is there.

    Args:
        session: The SparkSession.
        location: The s3a:// path the table owns.

    Returns:
        True when something was deleted.
    """
    jvm = session.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(session.sparkContext._jsc.hadoopConfiguration())
    try:
        if not fs.exists(path):
            return False
        LOG.info("clearing %s before write", location)
        fs.delete(path, True)
        return True
    finally:
        # Closing drops the S3A listing cache, so the write that follows sees an
        # empty prefix rather than the one this call just filled.
        fs.close()


def bronze_to_silver(session: SparkSession, config) -> dict:
    """Turn every bronze table from this batch into a silver table.

    Only the current batch is read, not all of bronze. Silver is written with
    ``overwrite`` for an incremental batch, so a re-run of Batch3 replaces
    Batch3's contribution and leaves Batch1 and Batch2 alone. That is the one
    thing ``overwrite`` on a partitioned table gets right here: the partition
    overwrite mode is dynamic, so only partitions that receive data are
    replaced.

    Args:
        session: The SparkSession.
        config: The job configuration.

    Returns:
        A mapping of silver table name to a dict of row counts before and after
        deduplication.
    """
    session.sql(f"CREATE DATABASE IF NOT EXISTS {config.silver_db}")
    written: dict[str, dict] = {}

    for schema in ALL_TABLES:
        bronze_table = schema.bronze_table
        source_name = f"{config.bronze_db}.{bronze_table}"
        if not _table_exists(session, source_name):
            LOG.info("%s: not in bronze, skipping", bronze_table)
            continue
        source = session.table(source_name)
        if not _has_rows(source):
            LOG.info("%s: no bronze rows, skipping", bronze_table)
            continue

        # Only this batch's rows travel forward. A bronze table accumulates every
        # batch run so far, and re-reading all of it on every run is quadratic.
        scoped = source.where(F.col("ingest_batch_id") == config.batch_id)

        typed = _cast_columns(scoped)
        partitioned = _add_partition(typed, schema, config.partition_column)
        # partitioned, not typed. Deduplicating the unpartitioned frame and then
        # writing it drops the dt column on the floor, so no dt= directory is
        # created and the table is written unpartitioned.
        deduplicated = _deduplicate(
            partitioned, schema, config.dedup_strategy, config.batch_seq
        )
        final = _drop_arrival_columns(deduplicated)

        table_name = schema.table
        target = f"{config.silver_db}.{table_name}"
        location = config.silver_path(table_name)

        partition_columns = (
            [config.partition_column] if config.partition_column in final.columns else []
        )
        create_external_table(session, target, final, location, partition_columns)

        if config.write_mode == "overwrite":
            _clear_location(session, location)

        if partition_columns:
            # Varargs, not a list. partitionBy(["dt"]) creates one column whose
            # name is "['dt']", so no dt= directory appears and the repair that
            # follows registers nothing.
            final.write.mode(config.write_mode).partitionBy(*partition_columns).save(location)
            # A path write is not a table write. The dt= directories exist on the
            # object store and the metastore has never heard of them, so the
            # table reads as empty until the partitions are registered.
            session.sql(f"MSCK REPAIR TABLE {target}")
        else:
            final.write.mode(config.write_mode).save(location)

        written[table_name] = {
            "bronze_rows": source.count(),
            "batch_rows": scoped.count(),
            "silver_rows": final.count(),
        }
        LOG.info("silver %s: %s", table_name, written[table_name])

    LOG.info("silver tables written: %s", sorted(written))
    return written


def _table_exists(session: SparkSession, name: str) -> bool:
    """Return whether a table is registered in the session's catalog.

    A table that was never written is not an error. A delta batch does not carry
    the reference tables at all, and a batch that failed mid-run leaves the
    later bronze tables absent.

    Args:
        session: The SparkSession.
        name: A fully qualified table name.

    Returns:
        True when the table exists.
    """
    try:
        return session.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - a missing database is the same answer
        return False


def _has_rows(frame: DataFrame) -> bool:
    """Return whether a DataFrame has at least one row, cheaply.

    Args:
        frame: The DataFrame to test.

    Returns:
        True when at least one row exists.
    """
    return frame.take(1) != []


__all__ = [
    "DELTA_TABLES",
    "HISTORICAL_ONLY",
    "SCHEMAS_BY_NAME",
    "bronze_to_silver",
]