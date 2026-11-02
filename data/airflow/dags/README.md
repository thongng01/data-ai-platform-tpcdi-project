# Airflow DAGs

This directory is reserved for pipeline DAGs. The accepted pipeline boundaries are:

| Pipeline | Responsibility |
|---|---|
| DP1 | Ingest source data into bronze and validate it. Bronze preserves logical source records or received events, including duplicates and provenance. |
| DP2 | Clean and integrate bronze records into silver and build gold outputs, with validation. |
| DP3 | Compute offline features and validate them. |

These boundaries come from the [Data Platform design](../../../docs/Data-Platform.md#5-orchestration-quality-and-governance).

The Airflow configuration mounts this directory at `/opt/airflow/dags` and mounts `data/airflow/connections.yaml` and `data/airflow/variables.yaml` for initialization. DAGs read service connections and settings through Airflow rather than embedding deployment values in task code. [IaC](../../../docs/IaC.md#44-orchestration) describes the orchestration service configuration.
