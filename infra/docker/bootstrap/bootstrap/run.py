"""Create shared buckets, prefix markers, and PostgreSQL databases.

Existing buckets and databases stay in place on repeated runs. The landing
bucket holds source files separately from lakehouse data. This script creates
buckets, but it does not set access policies.

The warehouse, Hive Metastore, Airflow, and DataHub use separate databases on
one PostgreSQL server. Their tables cannot conflict because each database has
its own catalog. Prefix markers help Spark find paths before its first write;
a failed marker write produces a warning and does not stop bootstrap.
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

# Required settings fail at import if they are missing. Do not use fallback
# credentials: a missing deployment setting must not select another identity.
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "landing")

PGHOST = os.environ.get("PGHOST", "postgres")
PGUSER = os.environ.get("PGUSER", "edai")
PGPASSWORD = os.environ["PGPASSWORD"]

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
        # Put the bucket in the URL path. Container DNS resolves the service
        # name, not a separate hostname for each bucket.
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
        except ClientError as exc:
            # Another bootstrap run can create a bucket after our listing.
            # Accept only the two already-exists responses; raise other errors.
            if exc.response["Error"]["Code"] in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                LOG.info("bucket %s appeared concurrently", name)
            else:
                raise


def seed_prefixes(client, bucket, prefixes):
    """Write an empty marker at each prefix and warn if a write fails.

    S3 stores object keys, not directories. A key such as `spark-logs/_keep`
    lets Hadoop's S3A filesystem find a child when it checks the event-log
    directory. Without a child, Spark can reject the path before writing.

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
        except ClientError as exc:
            # Marker failure does not stop database and bucket setup. A later
            # Spark start can still fail if its required prefix is missing.
            LOG.warning("could not seed %s: %s", prefix, exc)


def create_databases(names, host=PGHOST, user=PGUSER, password=PGPASSWORD):
    """Create each database if it is missing.

    PostgreSQL has no `CREATE DATABASE IF NOT EXISTS`, so the check and the
    create are separate. The `pg_database` catalog lists existing databases.
    A duplicate-database error is accepted because another bootstrap run can
    create a database between the check and the create.

    Args:
        names: Iterable of database names.
        host: PostgreSQL host.
        user: Role with permission to create databases.
        password: Password for that role.
    """
    conn = psycopg2.connect(host=host, user=user, password=password, dbname="postgres")
    # CREATE DATABASE cannot run inside a transaction. Autocommit sends each
    # statement on its own instead of opening an implicit transaction.
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
                    # Database names are SQL identifiers, not value parameters.
                    # Identifier quotes names safely, including embedded quotes.
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                    LOG.info("created database %s", name)
                except psycopg2.errors.DuplicateDatabase:
                    LOG.info("database %s appeared concurrently", name)
    finally:
        conn.close()


def wait_for_postgres(timeout=120):
    """Retry connections to the maintenance database until timeout seconds.

    This uses the bootstrap role and the `postgres` database, as database
    creation does. A successful connection confirms authentication and database
    access, not permission to create databases. Retries pause for two seconds;
    a connection attempt can take longer than the timeout because it has no
    separate connection timeout.
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
    """Create shared state, stopping on errors except prefix-marker warnings."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    wait_for_postgres()
    LOG.info("postgres is reachable at %s", PGHOST)

    create_databases(PG_DATABASES)
    client = _s3_client()
    create_buckets(client, [LAKEHOUSE_BUCKET, LANDING_BUCKET])
    seed_prefixes(
        client,
        LAKEHOUSE_BUCKET,
        # The event-log path must be visible before Spark starts. The other
        # markers also make empty working and table prefixes listable.
        ["spark-logs", "warehouse", "tmp", "bronze", "silver", "gold"],
    )

    LOG.info("bootstrap complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
