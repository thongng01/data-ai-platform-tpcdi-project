# Use SeaweedFS as the lakehouse object store

The lakehouse bronze, silver and gold zones live in SeaweedFS over its
S3-compatible API. Trino queries them through a Hive Metastore. We chose
SeaweedFS over MinIO, which is what course lab l8 uses.

Status: **in force and verified.** The verification is in
[`../../.scratch/coursework/research/R13-trino-seaweedfs-spike.md`](../../.scratch/coursework/research/R13-trino-seaweedfs-spike.md).

## Context

Earlier notes named SeaweedFS. Lab l8, the course storage lab, uses MinIO with a
Hive Metastore and Trino, and no SeaweedFS exists anywhere in the labs. MinIO
would have meant reusing the lab compose wiring with no integration risk.

That risk was the reason to spike before building anything. Trino's own
documentation says it tests its S3 filesystem against AWS S3 and MinIO, and
SeaweedFS is neither.

## Decision

Use SeaweedFS. The rubric's worked example, row R8, names MinIO and Postgres as
where generated data should sit before bronze ingest. We follow the rubric
example rather than a lab default, and keep the storage layer swappable behind
the S3 endpoint so lab MinIO stays a drop-in fallback.

## Verification

Ticket 13 spiked it before any pipeline depended on it. Trino 476 reads Parquet
out of SeaweedFS through the S3 API with a Hive Metastore in front. The check
writes Parquet with pyarrow rather than with Trino, registers it in the
metastore, and reads it back through Trino, so it measures the combination
instead of assuming it.

Eight smoke tests cover the platform and all pass. They are permanent
infrastructure now, in `infra/docker/tools/tools/smoke.py`, so a regression
fails `make verify`.

The fallback to lab MinIO was never needed.

## Consequences

**Two SeaweedFS behaviours cost real time**, and neither is in Trino's
documentation. Both are recorded in
[`../../docs/IaC.md`](../../docs/IaC.md).

An empty prefix reads as an existing directory. Spark refuses to create a
managed table whose location exists, so every managed `CREATE TABLE` fails with
`LOCATION_ALREADY_EXISTS` on a path that lists as empty. Every lakehouse table
is external. dbt's `location_root` produces the same shape, so this does not
constrain dbt.

Each S3 bucket is a collection, and a collection needs a volume. Once the volume
server reaches its configured maximum, every write to the newest bucket fails
with an `InternalError` that reads like a gateway fault. `-volume.max=0` lets
the count follow free disk space.

Trino also needs `s3.exclusive-create=false`, which is not in its documentation.
It sends `If-None-Match: *` on writes by default and SeaweedFS answers 501.

**The earlier cost estimate was wrong.** We originally planned three containers,
one each for the master, the volume server and the filer, on the assumption
that `weed compose` runs them separately. `weed server` runs all four roles,
including the S3 gateway, in one process, so the platform runs a single
SeaweedFS container and saves roughly a gigabyte of RAM. That matters on a 15
GB machine where every container competes for the same memory.

**The fallback path was never built.** MinIO would still slot in behind the same
S3 endpoint, but nothing depends on it and nothing tests it. If Trino or
SparkSQL ever regresses against SeaweedFS, that wiring is then unproven and
will need its own spike.