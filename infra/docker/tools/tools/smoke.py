"""Smoke tests for the platform that is already running.

Every check is independent and reports its own verdict, because a failure in
one says nothing about the others. The script exits non-zero if any check
fails, so Airflow can run it as a pipeline gate.

Run it with:

    docker compose --profile tools run --rm tools

The checks, in order:

  1. s3_write            an object can be written to SeaweedFS and read back
  2. s3_multipart        a multi-part upload completes (large enough to
                         exceed the 8 MB multipart threshold)
  3. s3_delete            a delete hides the object from a listing
  4. trino_info          the Trino coordinator answers and lists its catalogs
  5. hive_metastore      the hive catalog loads, which proves Trino reached the
                         metastore over Thrift
  6. trino_read_parquet  Trino reads Parquet that something other than Trino
                         wrote, from SeaweedFS, through the metastore
  7. partition_register  a new dt= directory is registered and becomes visible
  8. postgres_query      the warehouse database answers

Check 6 is the one the storage design depends on. Trino documents AWS S3 and
MinIO as the S3-compatible stores it tests against; SeaweedFS is neither.
"""

import datetime
import io
import json
import logging
import os
import sys
import uuid

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import trino
from botocore.config import Config

LOG = logging.getLogger("smoke")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "smoke")

PGHOST = os.environ.get("PGHOST", "postgres")
PGUSER = os.environ.get("PGUSER", "edai")
PGPASSWORD = os.environ["PGPASSWORD"]
PGDATABASE = os.environ.get("DB_WAREHOUSE", "warehouse")

SPIKE_SCHEMA = "smoke"
SPIKE_TABLE = "spike_parquet"

_results = []


def s3_client():
    """Return an S3 client pointed at SeaweedFS with path-style addressing."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def connection(catalog="hive", schema=SPIKE_SCHEMA):
    """Open a Trino connection against one catalog and schema."""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
        schema=schema,
        http_scheme="http",
    )


def check(name):
    """Decorate a function so its result is recorded as one named check.

    Args:
        name: The check name reported in the summary.
    """

    def decorate(func):
        def wrapper():
            LOG.info("--- %s", name)
            try:
                detail = func()
            except Exception as exc:  # noqa: BLE001 - a smoke test reports, it does not raise
                LOG.error("FAIL %s: %s", name, exc)
                _results.append((name, False, f"{type(exc).__name__}: {exc}"))
                return
            LOG.info("PASS %s: %s", name, detail)
            _results.append((name, True, detail))

        wrapper.__name__ = func.__name__
        return wrapper

    return decorate


def _sample_frame(rows=1000):
    """Build a small Parquet table with a mix of column types.

    The column types matter: Trino reads Parquet by its own type mapping, and a
    table of only strings would not exercise the numeric or timestamp paths.

    Args:
        rows: How many rows to build.

    Returns:
        A pyarrow Table.
    """
    base = datetime.datetime(2026, 1, 1)
    return pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "account_id": pa.array([i % 97 for i in range(rows)], type=pa.int64()),
            "ticker": pa.array([f"SYM{i % 13:03d}" for i in range(rows)], type=pa.string()),
            "shares": pa.array([float(i % 71) for i in range(rows)], type=pa.float64()),
            "trade_ts": pa.array(
                [base + datetime.timedelta(seconds=i) for i in range(rows)],
                type=pa.timestamp("us"),
            ),
        }
    )


def sql(conn, statement):
    """Run one statement and return all its rows as a list of tuples.

    A cursor is used rather than `Connection.execute` because the cursor API is
    the stable half of the DB-API surface.

    Args:
        conn: An open Trino connection.
        statement: SQL to run.

    Returns:
        A list of row tuples.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        return cursor.fetchall()
    finally:
        cursor.close()


def _parquet_bytes(frame):
    """Serialise a pyarrow table to compressed Parquet bytes.

    Args:
        frame: The pyarrow table.

    Returns:
        The Parquet payload.
    """
    buffer = io.BytesIO()
    pq.write_table(frame, buffer, compression="zstd")
    return buffer.getvalue()


@check("s3_write")
def check_s3_write():
    """Write a Parquet object and read it back byte for byte."""
    client = s3_client()
    key = f"smoke/plain-{uuid.uuid4().hex}.parquet"
    payload = _parquet_bytes(_sample_frame(10))

    client.put_object(Bucket=BUCKET, Key=key, Body=payload)
    fetched = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert len(fetched) == len(payload), "object came back a different size"
    assert pq.read_table(io.BytesIO(fetched)).num_rows == 10, "row count changed"
    client.delete_object(Bucket=BUCKET, Key=key)
    return f"{len(payload)} bytes round-tripped through {BUCKET}/{key}"


@check("s3_multipart")
def check_s3_multipart():
    """Upload an object larger than the 8 MB multipart threshold.

    The AWS SDK switches to multipart above a threshold and signs each part
    separately. That is a different code path from a single PutObject, and it is
    where an S3-compatible store most often breaks.
    """
    client = s3_client()
    key = f"smoke/multipart-{uuid.uuid4().hex}.bin"
    size = 24 * 1024 * 1024
    body = b"\0" * size

    client.put_object(Bucket=BUCKET, Key=key, Body=body)
    head = client.head_object(Bucket=BUCKET, Key=key)
    assert head["ContentLength"] == size, f"expected {size}, got {head['ContentLength']}"
    client.delete_object(Bucket=BUCKET, Key=key)
    return f"{size // (1024 * 1024)} MB multipart upload succeeded"


@check("s3_delete")
def check_s3_delete():
    """Confirm a deleted object disappears from a listing.

    SeaweedFS does not implement S3 object versioning, so a delete does not
    leave a version or a delete marker behind. The landing zone therefore
    relies on listing, not on version ids, when it needs to know what is
    present.
    """
    client = s3_client()
    key = f"smoke/delete-{uuid.uuid4().hex}.txt"
    client.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    def listed():
        return [o["Key"] for o in client.list_objects_v2(Bucket=BUCKET, Prefix=key).get("Contents", [])]

    assert key in listed(), "object not listed after put"
    client.delete_object(Bucket=BUCKET, Key=key)
    assert key not in listed(), "object still listed after delete"
    return "deleted object no longer appears in list_objects_v2"


@check("trino_info")
def check_trino_info():
    """Confirm the coordinator is up and lists its catalogs."""
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    rows = sql(conn, "SHOW CATALOGS")
    names = sorted(row[0] for row in rows)
    assert "hive" in names, f"hive catalog missing, saw {names}"
    assert "postgresql" in names, f"postgresql catalog missing, saw {names}"
    return f"catalogs: {', '.join(names)}"


@check("hive_metastore")
def check_hive_metastore():
    """Confirm Trino can reach the Hive Metastore.

    `SHOW SCHEMAS` on the hive catalog is a metastore round trip. If the
    metastore were down, this is where it shows.
    """
    conn = connection(catalog="hive", schema="information_schema")
    rows = sql(conn, "SHOW SCHEMAS FROM hive")
    return f"{len(rows)} schemas visible: {', '.join(sorted(r[0] for r in rows)[:6])}"


@check("trino_read_parquet")
def check_trino_read_parquet():
    """Write Parquet with pyarrow, register it, and read it back through Trino.

    This is the check the whole storage design rests on. Trino documents AWS S3
    and MinIO as its tested S3-compatible endpoints. SeaweedFS is neither, so
    this proves the combination rather than assuming it.
    """
    client = s3_client()
    prefix = f"smoke/{SPIKE_SCHEMA}/{SPIKE_TABLE}"
    frame = _sample_frame(1000)

    buffer = io.BytesIO()
    pq.write_table(frame, buffer, compression="zstd")
    client.put_object(Bucket=BUCKET, Key=f"{prefix}/part-00000.parquet", Body=buffer.getvalue())

    conn = connection()
    payload = _parquet_bytes(frame)
    client.put_object(Bucket=BUCKET, Key=f"{prefix}/part-00000.parquet", Body=payload)

    sql(conn, f'DROP TABLE IF EXISTS hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')
    sql(
        conn,
        f'CREATE SCHEMA IF NOT EXISTS hive."{SPIKE_SCHEMA}" '
        f"WITH (location = 's3://{BUCKET}/smoke/')",
    )
    sql(
        conn,
        f"""
        CREATE TABLE hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}" (
            id BIGINT,
            account_id BIGINT,
            ticker VARCHAR,
            shares DOUBLE,
            trade_ts TIMESTAMP
        )
        WITH (format = 'PARQUET', external_location = 's3://{BUCKET}/{prefix}/')
        """,
    )

    total, distinct_tickers = sql(
        conn,
        f'SELECT count(*), count(DISTINCT ticker) FROM hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"',
    )[0]
    assert total == 1000, f"expected 1000 rows, got {total}"
    assert distinct_tickers == 13, f"expected 13 tickers, got {distinct_tickers}"

    sql(conn, f'DROP TABLE hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')
    return "1000 rows written by pyarrow, read by Trino from SeaweedFS"


@check("partition_register")
def check_partition_register():
    """Add a new dt= directory and register it, then confirm Trino sees it.

    A new file inside a registered partition is visible with no action. A new
    partition directory is not. This is the recurring step every batch pipeline
    needs, so it is tested rather than assumed.
    """
    client = s3_client()
    prefix = f"smoke/{SPIKE_SCHEMA}/{SPIKE_TABLE}"
    frame = _sample_frame(250)

    for partition in ("dt=2026-01-01", "dt=2026-01-02"):
        buffer = io.BytesIO()
        pq.write_table(frame, buffer, compression="zstd")
        client.put_object(
            Bucket=BUCKET,
            Key=f"{prefix}/{partition}/part-00000.parquet",
            Body=buffer.getvalue(),
        )

    conn = connection()
    sql(conn, f'DROP TABLE IF EXISTS hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')
    sql(
        conn,
        f"""
        CREATE TABLE hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}" (
            id BIGINT, account_id BIGINT, ticker VARCHAR, shares DOUBLE,
            trade_ts TIMESTAMP, dt VARCHAR
        )
        WITH (
            format = 'PARQUET',
            partitioned_by = ARRAY['dt'],
            external_location = 's3://{BUCKET}/{prefix}/'
        )
        """,
    )

    # Before registration the table is empty: the directories exist, the
    # metastore has never heard of them.
    before = sql(conn, f'SELECT count(*) FROM hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')[0][0]

    for partition in ("dt=2026-01-01", "dt=2026-01-02"):
        sql(
            conn,
            "CALL hive.system.register_partition("
            f"schema_name => '{SPIKE_SCHEMA}', "
            f"table_name => '{SPIKE_TABLE}', "
            f"partition_columns => ARRAY['dt'], "
            f"partition_values => ARRAY['{partition[3:]}'], "
            f"location => 's3://{BUCKET}/{prefix}/{partition}/')",
        )

    after, distinct_partitions = sql(
        conn,
        f'SELECT count(*), count(DISTINCT dt) FROM hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"',
    )[0]
    assert after == 500, f"expected 500 rows after registration, got {after}"
    assert distinct_partitions == 2, f"expected 2 partitions, got {distinct_partitions}"

    sql(conn, f'DROP TABLE hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')
    return f"{before} rows before registration, {after} rows and {distinct_partitions} partitions after"


@check("postgres_query")
def check_postgres_query():
    """Confirm the warehouse database answers and the metadata tables exist."""
    import psycopg2

    conn = psycopg2.connect(
        host=PGHOST, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0].split(",")[0]
            cur.execute("SELECT count(*) FROM pg_database WHERE datname = %s", (PGDATABASE,))
            assert cur.fetchone()[0] == 1, "warehouse database is missing"
    finally:
        conn.close()
    return version


CHECKS = [
    check_s3_write,
    check_s3_multipart,
    check_s3_delete,
    check_trino_info,
    check_hive_metastore,
    check_trino_read_parquet,
    check_partition_register,
    check_postgres_query,
]


def main():
    """Run every check and print a JSON summary. Exit 1 if any check failed."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    for check_fn in CHECKS:
        check_fn()

    passed = sum(1 for _, ok, _ in _results if ok)
    print(json.dumps({"checks": _results}, indent=2))
    print(f"\n{passed}/{len(_results)} checks passed")
    return 0 if passed == len(_results) else 1


if __name__ == "__main__":
    sys.exit(main())
