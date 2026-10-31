# Airflow DAGs

This directory is reserved for pipeline DAGs. The accepted pipeline boundaries are:

| Pipeline | Responsibility |
|---|---|
| DP1 | Ingest source data into bronze and validate it. Bronze preserves logical source records or received events, including duplicates and provenance. |
| DP2 | Clean and integrate bronze records into silver and build gold outputs, with validation. |
| DP3 | Compute offline features and validate them. |

These boundaries come from the [project design](../../docs/Project-Design.md#21-accepted-choices-proposals--deferred-work). The existing batch code still continues from bronze to silver within DP1. [Status](../../docs/Status.md#what-exists) records implementation progress.

The Airflow configuration mounts this directory at `/opt/airflow/dags` and mounts `orchestration/connections.yaml` and `orchestration/variables.yaml` for initialization. DAGs should read service connections and settings through Airflow rather than embed deployment values in task code. Initialization currently ignores import failures, so those values need verification before task execution. [IaC](../../docs/IaC.md#44-orchestration) describes the executor and configuration limits.
