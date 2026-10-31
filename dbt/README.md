# dbt transformations

This directory is reserved for dbt models, profiles, macros, and data tests. The [project design](../docs/Project-Design.md#21-accepted-choices-proposals--deferred-work) assigns bronze-to-silver-and-gold processing to DP2 and offline feature computation to DP3. The division of work among Spark, dbt, PostgreSQL, and Trino remains open. [Status](../docs/Status.md#what-exists) records implementation progress.

The compute configuration mounts this directory at `/opt/dbt`. Its `dbt` profile provides a Spark Thrift endpoint on port 10001. A dbt connection must select a supported adapter and method; the endpoint alone does not define a dbt project. [IaC section 8.2](../docs/IaC.md#82-a-hiveserver2-takes-the-whole-worker) explains resource contention with the Thrift application.

Incremental update support depends on the table format and adapter. SeaweedFS stores objects; it does not define SQL `MERGE` support. The earlier implementation uses plain Parquet with Hive Metastore. Choosing append, partition replacement, or a transactional table format requires explicit rules for history, corrections, and reruns.
