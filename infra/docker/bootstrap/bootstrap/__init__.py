"""Create shared S3 buckets and PostgreSQL databases for the platform.

Existing buckets and databases stay in place on repeated runs. Prefix markers
use fixed keys, so repeated writes replace the same empty objects.
"""
