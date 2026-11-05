"""Print up to 200 objects from the first listing page for each S3 prefix.

An S3 listing shows stored files, not catalog entries. Compare this output
with a Trino query when a table is missing or returns no rows. Either the
files or the catalog entry can be missing.

    docker compose --profile tools run --rm tools python -m tools.ls \
        lakehouse warehouse/
"""

import sys

from .smoke import s3_client


def main(argv):
    """Print a bounded listing for each complete bucket and prefix pair.

    An unpaired final argument is ignored. A listing uses only the first S3
    response page and prints at most 200 objects from that page.
    """
    if not argv:
        print("usage: python -m tools.ls <bucket> <prefix> [<bucket> <prefix> ...]")
        return 2

    client = s3_client()
    # Consume two arguments per target. A final argument without a partner
    # does not form a target and is not included in this list.
    pairs = [(argv[i], argv[i + 1]) for i in range(0, len(argv) - 1, 2)]

    for bucket, prefix in pairs:
        print(f"--- s3://{bucket}/{prefix}")
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        if not contents:
            print("    (no objects)")
        for obj in contents[:200]:
            print(f"    {obj['Key']}  {obj['Size']} bytes")
        # IsTruncated describes S3 pagination, not the 200-object print limit.
        # This warning does not report objects omitted only by that print limit.
        if response.get("IsTruncated"):
            print("    ... truncated, more pages remain")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
