"""Bootstrap package: prepares shared state before the platform starts.

Run once via `make bootstrap`. Creates the S3 buckets the lakehouse zones live
in and the Postgres databases the platform uses. Every step is idempotent.
"""