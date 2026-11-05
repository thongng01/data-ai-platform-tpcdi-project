"""Check S3 storage, Trino's Hive catalog, and PostgreSQL on a running stack.

Each check reports success or failure, and the runner continues after a failed
check. The script exits nonzero if any check fails. Some checks share a schema
and table name, so run only one copy at a time. These checks write and delete
test data; they do not start services.

Run it with:

    docker compose --profile tools run --rm tools

The checks, in order:

  1. s3_write            an object can be written to SeaweedFS and read back
  2. s3_multipart        a multipart upload completes with an 8 MiB threshold
  3. s3_delete            a delete hides the object from a listing
  4. trino_info          the Trino coordinator answers and lists its catalogs
  5. hive_metastore      the Hive catalog returns schema metadata
  6. trino_read_parquet  Trino reads Parquet that something other than Trino
                         wrote, from SeaweedFS, through the metastore
  7. partition_register  a new dt= directory is registered and becomes visible
  8. postgres_query      the warehouse database answers

The Parquet and partition checks test Hive catalog access to files on SeaweedFS.
They do not test Delta Lake tables or the Spark and Flink writers.
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
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

LOG = logging.getLogger("smoke")

# Required credentials are read without defaults, so missing deployment
# settings fail before any check can connect under a fallback identity.
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
        # Keep the bucket in the path instead of requiring bucket-specific DNS.
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
        """Wrap a check so a raised error does not stop the remaining checks."""

        def wrapper():
            """Run one check and append its outcome to the process-wide results."""
            LOG.info("--- %s", name)
            # Catch each check's failure so later checks can still report.
            try:
                detail = func()
            except Exception as exc:  # noqa: BLE001
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
    # Use timestamps without a time zone to match the Trino TIMESTAMP columns.
    # Arrow's "us" unit stores microseconds, not milliseconds or seconds.
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
    """Run one Trino statement, fetch all result rows, and close its cursor.

    DB-API exposes statement execution and result retrieval on the cursor.
    Closing it in `finally` also releases it when execution or retrieval fails.

    Args:
        conn: An open Trino connection.
        statement: SQL to run.

    Returns:
        The result rows returned by the Trino cursor.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        return cursor.fetchall()
    finally:
        cursor.close()


def _parquet_bytes(frame):
    """Serialize a PyArrow table to Zstandard-compressed Parquet bytes.

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
    """Write Parquet, check its retrieved byte length and row count, then delete it."""
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
    """Upload 24 MiB in multipart mode and verify the stored object size.

    The managed upload API switches to multipart above the explicit 8 MiB
    threshold. A direct `put_object` call never makes that switch, regardless
    of object size. Multipart requests test a separate S3 API sequence.
    """
    client = s3_client()
    key = f"smoke/multipart-{uuid.uuid4().hex}.bin"
    size = 24 * 1024 * 1024
    body = b"\0" * size

    # MiB is 1024 * 1024 bytes. Three 8 MiB parts exceed S3's 5 MiB minimum
    # for nonfinal parts and force the multipart path for this 24 MiB payload.
    client.upload_fileobj(
        io.BytesIO(body),
        BUCKET,
        key,
        Config=TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
        ),
    )
    head = client.head_object(Bucket=BUCKET, Key=key)
    assert head["ContentLength"] == size, f"expected {size}, got {head['ContentLength']}"
    client.delete_object(Bucket=BUCKET, Key=key)
    return f"{size // (1024 * 1024)} MiB multipart upload succeeded"


@check("s3_delete")
def check_s3_delete():
    """Confirm a deleted object disappears from a listing.

    The check lists with the unique key as its prefix, then tests exact-key
    membership before and after deletion. It does not test version history
    or retention.
    """
    client = s3_client()
    key = f"smoke/delete-{uuid.uuid4().hex}.txt"
    client.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    def listed():
        """Return visible keys that start with this check's unique object key."""
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
    """Check that Trino can return schema metadata from the Hive catalog.

    `SHOW SCHEMAS` asks the Hive connector for schema metadata. A successful
    result confirms metadata access, but cached metadata can hide an outage.
    """
    conn = connection(catalog="hive", schema="information_schema")
    rows = sql(conn, "SHOW SCHEMAS FROM hive")
    return f"{len(rows)} schemas visible: {', '.join(sorted(r[0] for r in rows)[:6])}"


@check("trino_read_parquet")
def check_trino_read_parquet():
    """Write Parquet with PyArrow and read it through a Hive external table.

    Row and distinct-ticker counts check that Trino can read the stored files
    using metadata in Hive Metastore. This does not compare every field value.
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

    # The external table points to files written outside Trino. Dropping its
    # catalog entry leaves those files available for the replacement table.
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
    """Write two date-partition prefixes, register them, and check their rows.

    This Hive table discovers partitions through Hive Metastore, not through
    an S3 directory scan. Register each partition so Trino knows its location.
    Delta Lake tables use their transaction log instead of this procedure.
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

    # The new Hive table has no registered partitions even though S3 holds
    # partition files. Record the count to show the effect of registration.
    before = sql(conn, f'SELECT count(*) FROM hive."{SPIKE_SCHEMA}"."{SPIKE_TABLE}"')[0][0]

    for partition in ("dt=2026-01-01", "dt=2026-01-02"):
        sql(
            conn,
            "CALL hive.system.register_partition("
            f"schema_name => '{SPIKE_SCHEMA}', "
            f"table_name => '{SPIKE_TABLE}', "
            f"partition_columns => ARRAY['dt'], "
            # Strip "dt=" from the key prefix to supply only the date value.
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
    """Read the PostgreSQL version and confirm the warehouse's catalog entry."""
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
    """Run checks and print accumulated results; return 1 if any result failed.

    Results stay in the module-level list. The command runs this function once;
    another call in the same process also includes results from earlier calls.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    for check_fn in CHECKS:
        check_fn()

    passed = sum(1 for _, ok, _ in _results if ok)
    print(json.dumps({"checks": _results}, indent=2))
    print(f"\n{passed}/{len(_results)} checks passed")
    return 0 if passed == len(_results) else 1


if __name__ == "__main__":
    sys.exit(main())
