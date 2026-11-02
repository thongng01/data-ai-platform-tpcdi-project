# dbt transformations

This directory is reserved for dbt models, profiles, macros, and data tests. The [Layer 1 design](../../docs/Data-Platform.md#3-batch-processing-and-storage-optimization) assigns bronze-to-silver-and-gold processing to DP2 and offline feature computation to DP3.

The compute configuration mounts this directory at `/opt/dbt`. Its `dbt` profile provides a Spark Thrift endpoint on port 10001. A dbt connection must select a supported adapter and method; the endpoint alone does not define a dbt project. [IaC section 8.2](../../docs/IaC.md#82-a-hiveserver2-takes-the-whole-worker) explains resource contention with the Thrift application.

Incremental update support depends on the table format and adapter. SeaweedFS stores objects; it does not define SQL `MERGE` support on its own. Supporting transactional `MERGE` in the lakehouse requires Delta Lake or Iceberg, whereas mutable warehouse updates can also run in PostgreSQL.
