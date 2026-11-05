# Layer 1: Data Platform

This directory holds the Layer 1 data generation, batch processing, warehouse modeling, stream processing, orchestration, quality validation, and metadata governance components described in [Data Platform](../docs/Data-Platform.md).

| Subdirectory | Purpose |
| --- | --- |
| `data_source/` | Target source department: a generator for business behavior and source recording, and a publisher for file delivery and the cash transaction stream. The existing [generator proof of concept (POC)](generator/README.md) remains in `generator/` until the code migration. |
| [`spark/`](spark/README.md) | PySpark batch pipelines (`DP1` upstream to staging to Bronze, `DP2` Bronze to Silver and the PostgreSQL load schema, `DP3` Gold to batch features) and optimization benchmarks. |
| [`dbt/`](dbt/README.md) | dbt project for Gold warehouse models, type 2 slowly changing dimensions (SCD2), and schema tests. |
| `flink/` | PyFlink streaming jobs for cash transaction ingestion into Bronze and Silver, followed by live tables, windows, cash-flow features, and rescoring requests. |
| [`airflow/`](airflow/dags/README.md) | Airflow DAGs (directed acyclic graphs) for `DP1-DP3`, feature materialization, and retrieval-augmented generation (RAG) ingestion, plus connections and variables. |
| `gx/` | Target location for GX Core checks at each pipeline validate stage; results go to DataHub. |
| [`datahub/`](datahub/recipes/README.md) | DataHub ingestion recipes and validation assertion publishers. |
