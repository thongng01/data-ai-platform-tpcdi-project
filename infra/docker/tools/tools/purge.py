"""Delete everything under one or more object-store prefixes.

Used when a Spark job died between creating a table and writing its first
partition, leaving a directory that makes the next `CREATE TABLE` fail with
LOCATION_ALREADY_EXISTS. Spark's `DROP TABLE` does not help in that case: it
drops the catalog row, the directory stays, and Spark refuses to reuse the name.

    docker compose --profile tools run --rm tools python -m tools.purge \
        lakehouse/warehouse/silver.db/wiring_check

Refuses to delete a whole bucket. Only prefixes below a bucket are accepted, and
the prefix must be non-empty.
"""

import logging
import os
import sys

from botocore.exceptions import ClientError

from .smoke import s3_client

LOG = logging.getLogger("purge")


def purge(client, bucket, prefix, batch_size=1000):
    """Delete every object under one prefix.

    S3 listing is paginated and deletes are limited to 1000 keys per request, so
    this loops until a pass finds nothing left.

    Args:
        client: A boto3 S3 client.
        bucket: The bucket name.
        prefix: The key prefix, without a leading slash.
        batch_size: Keys per delete request.

    Returns:
        The number of objects deleted.
    """
    deleted = 0
    while True:
        keys = [
            {"Key": obj["Key"]}
            for obj in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        ]
        if not keys:
            return deleted
        for start in range(0, len(keys), batch_size):
            chunk = keys[start : start + batch_size]
            client.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
            deleted += len(chunk)
            LOG.info("deleted %d objects under s3://%s/%s", len(chunk), bucket, prefix)


def main(argv):
    """Purge each prefix given on the command line."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = s3_client()

    if not argv:
        print("usage: python -m tools.purge <bucket/prefix> [<bucket/prefix> ...]")
        return 2

    total = 0
    for target in argv:
        if "/" not in target:
            print(f"refusing to purge a whole bucket: {target}", file=sys.stderr)
            return 2
        bucket, _, prefix = target.partition("/")
        if not prefix:
            print(f"refusing to purge a whole bucket: {target}", file=sys.stderr)
            return 2
        try:
            total += purge(client, bucket, prefix)
        except ClientError as exc:
            print(f"failed on {target}: {exc}", file=sys.stderr)
            return 1

    print(f"deleted {total} objects")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))