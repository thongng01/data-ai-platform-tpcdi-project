"""Copy a generator run into the landing bucket.

The rubric requires the generated data to be stored somewhere the ingest can
read it from, standing in for another team's storage. That is what the landing
bucket is: `datagen` writes local files, this module puts them in object
storage, and DP1 reads them from there. Nothing downstream reads the generator's
local disk, so the platform behaves the same whether the generator ran on this
machine or somewhere else.

The landing bucket is separate from the lakehouse bucket on purpose. The rule
"nothing writes into landing except the generator" then holds at the storage
layer rather than by convention.

boto3 rather than Hadoop's FileSystem. Hadoop's copy-from-local path
relativises the source against the destination's parent and fails outright when
one is a local path and the other an s3a URI, and driving it from a Spark
driver means the copy waits on an executor for no reason. A plain S3 client
writes bytes and returns.
"""

from __future__ import annotations

import logging
import os

LOG = logging.getLogger("dp1.landing")

# The generator's manifest describes the run; it is not data. It stays local so
# the run is reproducible from its seed, and DP1 does not read it.
MANIFEST_NAME = "manifest.json"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "edaiaccess")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "edaisecret123")


def s3_client():
    """Build an S3 client for SeaweedFS.

    Path-style addressing is required: SeaweedFS serves every bucket at the
    endpoint root, not at a per-bucket subdomain.

    Returns:
        A boto3 S3 client.
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def split_uri(uri: str) -> tuple[str, str]:
    """Split an s3a:// URI into a bucket and a key prefix.

    Args:
        uri: A URI such as ``s3a://landing/tpcdi/Batch1``.

    Returns:
        A (bucket, prefix) pair.
    """
    without_scheme = uri.split("://", 1)[-1]
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix.strip("/")


def upload_batch(local_root: str, batch_id: str, config) -> dict:
    """Copy one batch directory into the landing bucket.

    Args:
        local_root: Directory holding Batch1, Batch2, ... subdirectories.
        batch_id: The batch to copy.
        config: A JobConfig.

    Returns:
        A dict with the destination prefix and the number of files copied.

    Raises:
        FileNotFoundError: When the local batch directory does not exist.
    """
    source_dir = os.path.join(local_root, batch_id)
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"no such batch directory: {source_dir}")

    destination = config.landing_path()
    bucket, prefix = split_uri(destination)
    client = s3_client()

    existing = {
        obj["Key"]
        for obj in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    }

    copied = 0
    total_bytes = 0
    for name in sorted(os.listdir(source_dir)):
        local = os.path.join(source_dir, name)
        if not os.path.isfile(local):
            continue
        key = f"{prefix}/{name}"
        if key in existing:
            LOG.info("skipped %s, already in landing", name)
            continue
        with open(local, "rb") as handle:
            client.put_object(Bucket=bucket, Key=key, Body=handle.read())
        size = os.path.getsize(local)
        copied += 1
        total_bytes += size
        LOG.info("uploaded %s (%d bytes)", name, size)

    LOG.info("uploaded %d files, %d bytes to %s", copied, total_bytes, destination)
    return {"destination": destination, "files": copied, "bytes": total_bytes}


__all__ = ["MANIFEST_NAME", "S3_ENDPOINT", "s3_client", "split_uri", "upload_batch"]
