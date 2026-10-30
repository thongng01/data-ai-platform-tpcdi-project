"""Create the buckets and databases the platform needs, then exit.

Run once after `docker compose up -d`, via `make bootstrap`. Safe to re-run: a
bucket that already exists is skipped and a database that already exists is
skipped.

Two facts drive the layout this creates.

The landing bucket is separate from the lakehouse bucket because it stands in
for another team's storage. The generator writes there, ingest reads from there,
and nothing in the platform is allowed to write into it. Keeping it in a
different bucket makes that a storage-level rule rather than a convention.

The warehouse, metastore, Airflow and DataHub databases are separate databases
in one Postgres instance. Separate databases, not separate containers: a
metastore table and a gold table can then never collide on a name, which is
the actual reason the lab's single-database shortcut is unsafe.
"""

import logging
import os
import sys
import time

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

LOG = logging.getLogger("bootstrap")

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "landing")

PGHOST = os.environ.get("PGHOST", "postgres")
PGUSER = os.environ.get("PGUSER", "edai")
PGPASSWORD = os.environ.get("PGPASSWORD", "edai_local_dev")

PG_DATABASES = [
    os.environ.get("DB_WAREHOUSE", "warehouse"),
    os.environ.get("DB_METASTORE", "metastore"),
    os.environ.get("DB_AIRFLOW", "airflow"),
    os.environ.get("DB_DATAHUB", "datahub"),
]


def _s3_client():
    """Build an S3 client that talks to SeaweedFS rather than to AWS."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        # SeaweedFS serves buckets at the host root, not at a subdomain.
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def create_buckets(client, buckets):
    """Create each bucket if it is missing.

    Args:
        client: A boto3 S3 client.
        buckets: Iterable of bucket names.
    """
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for name in buckets:
        if name in existing:
            LOG.info("bucket %s already exists", name)
            continue
        try:
            client.create_bucket(Bucket=name)
            LOG.info("created bucket %s", name)
        except ClientError as exc:  # pragma: no cover - race with a second run
            if exc.response["Error"]["Code"] in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                LOG.info("bucket %s appeared concurrently", name)
            else:
                raise


def seed_prefixes(client, bucket, prefixes):
    """Drop a marker object under each prefix so the prefix exists.

    S3 has no directories. Spark asks the filesystem whether
    `s3a://lakehouse/spark-logs` is a directory before it writes an event log,
    gets FileNotFound, and refuses to start. A zero-byte object makes the
    prefix listable.

    Args:
        client: A boto3 S3 client.
        bucket: The bucket the prefixes live in.
        prefixes: Iterable of prefix strings, without a leading or trailing slash.
    """
    for prefix in prefixes:
        key = f"{prefix.strip('/')}/_keep"
        try:
            client.put_object(Bucket=bucket, Key=key, Body=b"")
            LOG.info("seeded prefix s3://%s/%s", bucket, prefix)
        except ClientError as exc:  # pragma: no cover - network edge case
            LOG.warning("could not seed %s: %s", prefix, exc)


def create_databases(names, host=PGHOST, user=PGUSER, password=PGPASSWORD):
    """Create each database if it is missing.

    Postgres has no `CREATE DATABASE IF NOT EXISTS`, so the check and the
    create are separate. `pg_database` is the system catalog that lists them.
    A duplicate_database error is swallowed because a second bootstrap run
    racing this one is not a failure.

    Args:
        names: Iterable of database names.
        host: Postgres host.
        user: Superuser role name.
        password: Password for that role.
    """
    conn = psycopg2.connect(host=host, user=user, password=password, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database")
            existing = {row[0] for row in cur.fetchall()}
            for name in names:
                if name in existing:
                    LOG.info("database %s already exists", name)
                    continue
                try:
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                    LOG.info("created database %s", name)
                except psycopg2.errors.DuplicateDatabase:
                    LOG.info("database %s appeared concurrently", name)
    finally:
        conn.close()


def wait_for_postgres(timeout=120):
    """Block until Postgres accepts a connection on the default database.

    Compose already gates `postgres` on a healthcheck, but the healthcheck
    only proves the server is up. This proves the `postgres` maintenance
    database is queryable, which is what CREATE DATABASE attaches to.
    """
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            psycopg2.connect(
                host=PGHOST, user=PGUSER, password=PGPASSWORD, dbname="postgres"
            ).close()
            return
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(2)
    raise TimeoutError(f"postgres not ready after {timeout}s: {last_error}")


def main():
    """Run every bootstrap step and exit non-zero on the first failure."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    wait_for_postgres()
    LOG.info("postgres is reachable at %s", PGHOST)

    create_databases(PG_DATABASES)
    client = _s3_client()
    create_buckets(client, [LAKEHOUSE_BUCKET, LANDING_BUCKET])
    seed_prefixes(
        client,
        LAKEHOUSE_BUCKET,
        # Spark writes its event log here and checks the prefix exists first.
        ["spark-logs", "warehouse", "tmp", "bronze", "silver", "gold"],
    )

    LOG.info("bootstrap complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
