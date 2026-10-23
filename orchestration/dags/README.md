# Airflow DAGs

Nothing here yet. The DAG files are the next ticket after DP2.

## What will land here

| DAG | Pipeline | Stages |
|---|---|---|
| `dp1_ingest.py` | landing to bronze to silver | `publish`, `ingest`, `validate` |
| `dp2_model.py` | silver to gold | `dbt run`, `dbt test`, `validate` |
| `dp3_features.py` | offline feature table | `build`, `validate` |
| `publish_lineage.py` | governance | emits a DataHub `DataProcessInstance` per DAG run |

Each DAG reads its connections from `connections.yaml` and its settings from
`variables.yaml`, both seeded by `airflow-init`. No DAG hardcodes a hostname, a
bucket or a password.

## Why the directories have to exist now

`compose.orchestration.yaml` bind-mounts `dags/`, `connections.yaml` and
`variables.yaml`. Docker creates a missing bind source as an empty root-owned
directory rather than failing, so without this folder the Airflow stack starts
with an empty DAG folder and no connections, and nothing says so. That is the
same trap recorded in `docs/IaC.md` finding 8.5.