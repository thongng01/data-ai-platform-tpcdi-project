# Airflow DAGs

This directory is reserved for pipeline DAGs. The accepted pipeline boundaries are:

| Pipeline | Responsibility |
|---|---|
| DP1 | Pull source file versions from upstream into staging, parse them into Bronze, and validate. Bronze preserves delivered text, duplicates, and provenance. Flink Job 1 ingests the cash transaction stream separately. |
| DP2 | Clean and integrate Bronze into Silver, copy the needed rows into PostgreSQL, and run dbt-postgres to build Gold, with validation. |
| DP3 | Compute offline features and validate them. |

These boundaries come from the [Data Platform design](../../../docs/Data-Platform.md#5-orchestration-quality-and-governance).

The Airflow configuration mounts this directory at `/opt/airflow/dags` and mounts `data/airflow/connections.yaml` and `data/airflow/variables.yaml` for initialization. DAGs read service connections and settings through Airflow rather than embedding deployment values in task code. [IaC](../../../docs/IaC.md#44-orchestration) describes the orchestration service configuration.
