"""List the objects under one object-store prefix.

A small diagnostic, because "the table exists but I cannot see it" has two
very different causes: objects present but no catalog row, or a catalog row
with no objects. Only one of them is a bug in the pipeline.

    docker compose --profile tools run --rm tools python -m tools.ls \
        lakehouse warehouse/
"""

import sys

from .smoke import s3_client


def main(argv):
    """List every object under each prefix given on the command line."""
    if not argv:
        print("usage: python -m tools.ls <bucket> <prefix> [<bucket> <prefix> ...]")
        return 2

    client = s3_client()
    pairs = [(argv[i], argv[i + 1]) for i in range(0, len(argv) - 1, 2)]

    for bucket, prefix in pairs:
        print(f"--- s3://{bucket}/{prefix}")
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        if not contents:
            print("    (no objects)")
        for obj in contents[:200]:
            print(f"    {obj['Key']}  {obj['Size']} bytes")
        if response.get("IsTruncated"):
            print("    ... truncated, more pages remain")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))