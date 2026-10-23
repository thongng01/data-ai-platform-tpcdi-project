"""Prove the Spark cluster can write to the lakehouse and register the table.

Run against a live cluster:

    docker compose --profile compute run --rm spark-jobs \
        /opt/pipelines/_wiring_check.py

It does the whole round trip a pipeline depends on, in four steps:

  1. create a Hive database and an external Parquet table in the metastore
  2. write rows into two dt= partitions
  3. read them back with Spark SQL
  4. leave the catalog row where Trino can find it

Step 4 is checked separately, with tools/sql_file.py, because Trino has to be
pointed at the same metastore.

The table is external, not managed, and that is not a style choice. Spark
refuses to create a *managed* table whose location already exists. SeaweedFS's
S3 gateway returns a directory entry for an empty prefix, so a location with no
objects in it still reads as existing and the CREATE fails with
LOCATION_ALREADY_EXISTS on a prefix that lists as empty. An external table
declares its location and skips the check. Every lakehouse table in this project
is external for the same reason, which is also what dbt-spark's `location_root`
produces.
"""

import sys

from pyspark.sql import SparkSession

DB = "silver"
TABLE = "wiring_check"
LOCATION = f"s3a://lakehouse/warehouse/{DB}.db/{TABLE}"


def clear_location(spark, location):
    """Delete every object under a prefix, if there are any.

    Best effort. Nothing in the pipeline depends on this succeeding, and the
    empty-prefix case it cannot clean up is exactly the case the external-table
    workaround exists to avoid.

    Args:
        spark: The active SparkSession.
        location: The s3a:// prefix to clear.
    """
    config = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(config)
    try:
        if fs.exists(path):
            fs.delete(path, True)
            print("=== removed stale objects under", location)
    except Exception as exc:  # noqa: BLE001 - cleanup must not fail the check
        print("=== could not clear", location, "because", exc)
    finally:
        fs.close()


def main() -> int:
    """Run the round trip and return a process exit code."""
    spark = (
        SparkSession.builder.appName("edai-wiring-check")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== spark version:", spark.version)

    clear_location(spark, LOCATION)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"DROP TABLE IF EXISTS {DB}.{TABLE}")
    # Hive DDL, not the DataSource V2 `USING parquet` syntax. Two reasons:
    # V2 requires partition columns to be declared in the column list, which is
    # the opposite of the Hive convention; and the Hive form writes the SerDe
    # and file-format metadata Trino's Hive connector expects, where the V2 form
    # writes a table Trino can find but describes differently.
    spark.sql(
        f"""
        CREATE EXTERNAL TABLE {DB}.{TABLE} (
            id BIGINT,
            ticker STRING,
            amount DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
        LOCATION '{LOCATION}'
        """
    )

    rows = [
        (i, f"SYM{i % 5:03d}", float(i) * 1.5, "2026-01-01" if i < 50 else "2026-01-02")
        for i in range(100)
    ]
    frame = spark.createDataFrame(rows, "id BIGINT, ticker STRING, amount DOUBLE, dt STRING")
    frame.write.mode("overwrite").partitionBy("dt").save(LOCATION)

    # The write produced dt= directories on the object store and nothing else.
    # Spark's catalog has never heard of them, so the table reads as empty until
    # the partitions are registered. This is the single most important
    # operational fact about writing to a Hive-catalogued lakehouse: a path
    # write is not a table write.
    spark.sql(f"MSCK REPAIR TABLE {DB}.{TABLE}")

    # Spark's own catalog still points at the table; a plain read proves the
    # bytes landed, and reading the path directly proves it without the catalog.
    total, days = spark.sql(
        f"SELECT count(*), count(DISTINCT dt) FROM {DB}.{TABLE}"
    ).collect()[0]
    print(f"=== spark read back through the catalog: {total} rows, {days} partitions")
    assert total == 100, f"expected 100 rows, got {total}"
    assert days == 2, f"expected 2 partitions, got {days}"

    direct = spark.read.parquet(LOCATION)
    assert direct.count() == 100, "reading the path directly gave a different count"

    spark.sql(f"SELECT dt, count(*) FROM {DB}.{TABLE} GROUP BY dt ORDER BY dt").show(10)

    # The metastore row is what Trino follows to the same bytes. If Spark and
    # Trino disagree about the location, this is where it shows.
    for row in spark.sql(f"DESCRIBE FORMATTED {DB}.{TABLE}").collect():
        if str(row["col_name"] or "").startswith("Location"):
            print("=== catalog location:", row["data_type"])

    spark.stop()
    print("=== OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())