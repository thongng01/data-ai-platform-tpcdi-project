"""Check that both lakehouse buckets accept a write from the host.

A one-line probe, because "the landing bucket is empty" has three possible
causes and this separates them: the bucket does not exist, the credentials are
wrong, or the S3 gateway is failing on the put itself.

    ../.venv/bin/python misc/tools/s3_probe.py
"""

import os
import sys

import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:8333")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "edaiaccess")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "edaisecret123")
BUCKETS = ("lakehouse", "landing")


def main() -> int:
    """List the buckets, then try one small write to each."""
    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )

    names = sorted(b["Name"] for b in client.list_buckets().get("Buckets", []))
    print("buckets:", names)

    failures = 0
    for bucket in BUCKETS:
        key = "probe/smoke.txt"
        try:
            client.put_object(Bucket=bucket, Key=key, Body=b"hello")
            listed = [
                o["Key"]
                for o in client.list_objects_v2(Bucket=bucket, Prefix=key).get("Contents", [])
            ]
            print(f"{bucket}: put ok, listed {listed}")
            client.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:  # noqa: BLE001 - a probe reports, it does not raise
            failures += 1
            print(f"{bucket}: FAILED {type(exc).__name__}: {exc}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())