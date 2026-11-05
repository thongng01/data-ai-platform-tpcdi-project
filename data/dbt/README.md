# dbt Transformations

This directory holds the target dbt-postgres project for Gold warehouse models, macros, and tests. In [DP2](../../docs/Data-Platform.md#3-batch-processing-and-storage-optimization), Spark converts Bronze to Silver in Delta Lake and copies the rows Gold needs into a PostgreSQL load schema. dbt builds Gold from those local rows; it does not move data between engines.

Gold uses PostgreSQL keys, indexes, and transactional writes. Type 2 slowly changing dimension (SCD2) models build versions from source effective dates rather than dbt snapshots, because the initial load already contains years of changes. A corrected date rebuilds the affected history and later dates in order. The daily cash transaction and balance facts use the same dbt logic for initial-load file data and streamed cash transactions retained in Silver.

The target uses no Spark Thrift server or dbt-spark adapter. The current Compose mount at `/opt/dbt` remains until the infrastructure update.
