"""Delete everything under one or more object-store prefixes.

A failed Spark job can leave objects at a table location. A later table
creation can then fail with LOCATION_ALREADY_EXISTS. Dropping an external
table removes its catalog entry but can leave its files. Use this command
only when the stored objects are no longer needed.

    docker compose --profile tools run --rm tools python -m tools.purge \
        lakehouse/warehouse/silver.db/wiring_check

The command-line entry point rejects a target with no prefix. It matches raw
S3 key prefixes, not directory boundaries: `table` also matches `table_backup`.
Use a trailing slash when the target must be limited to one directory prefix.
"""

import logging
import os
import sys

from botocore.exceptions import ClientError

from .smoke import s3_client

LOG = logging.getLogger("purge")


def purge(client, bucket, prefix, batch_size=1000):
    """Delete every object under one prefix.

    Each listing returns one page. After deleting it, list the same prefix
    again until no objects remain. No continuation token is needed because
    the objects from earlier pages have been removed.

    The caller must provide a nonempty prefix and a batch size from 1 to 1000,
    which is S3's delete-request limit. This helper does not validate them.
    Per-key errors in a successful delete response are not inspected, so the
    count reports requested deletions rather than confirmed deletions.

    Args:
        client: A boto3 S3 client.
        bucket: The bucket name.
        prefix: The key prefix, without a leading slash.
        batch_size: Maximum keys per delete request, from 1 to 1000.

    Returns:
        The number of keys submitted for deletion.
    """
    deleted = 0
    while True:
        keys = [
            {"Key": obj["Key"]}
            for obj in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        ]
        if not keys:
            return deleted
        # S3 accepts at most 1000 keys per delete request. Split a listing
        # page into chunks so callers can choose a smaller request size.
        for start in range(0, len(keys), batch_size):
            chunk = keys[start : start + batch_size]
            client.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
            deleted += len(chunk)
            LOG.info("deleted %d objects under s3://%s/%s", len(chunk), bucket, prefix)


def main(argv):
    """Delete each requested prefix and stop on invalid targets or S3 errors.

    Earlier targets can already be deleted when a later target fails. Targets
    with an empty prefix are rejected before that target reaches the helper.
    """
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
