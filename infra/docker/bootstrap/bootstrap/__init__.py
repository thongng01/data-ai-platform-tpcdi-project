"""Bootstrap package: prepares shared state before the platform starts.

Run via `docker compose --project-directory infra --profile init run --rm bootstrap`. Creates the S3 buckets the lakehouse zones live
in and the Postgres databases the platform uses. Every step is idempotent.
"""
