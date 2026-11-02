# Layer 1: Data Platform

This directory holds the Layer 1 data generation, batch processing, warehouse modeling, stream processing, orchestration, quality validation, and metadata governance components described in [Data Platform](../docs/Data-Platform.md).

| Subdirectory | Purpose |
| --- | --- |
| [`generator/`](generator/README.md) | Polars TPC-DI source generator, declarative format definitions, and validation suite. |
| [`spark/`](spark/README.md) | PySpark batch pipelines (`DP1` Staging to Bronze, `DP2` Bronze to Silver, `DP3` Gold to Offline Features) and optimization benchmarks. |
| [`dbt/`](dbt/README.md) | dbt project for Gold warehouse models, SCD2 dimensions, and schema tests. |
| `flink/` | PyFlink streaming jobs for continuous `CashTransaction` processing, deduplication, and window aggregation. |
| [`airflow/`](airflow/dags/README.md) | Airflow DAGs (`DP1-DP3`, feature materialization, and RAG ingestion), connections, and variables. |
| `soda/` | Soda data-quality check definitions per Medallion zone. |
| [`datahub/`](datahub/recipes/README.md) | DataHub ingestion recipes and validation assertion publishers. |
