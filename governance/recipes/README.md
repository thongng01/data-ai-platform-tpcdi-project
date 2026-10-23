# DataHub ingestion recipes

Nothing here yet. Governance is the next block after DP2 and the Flink job.

## What will land here

| File | Source | What it publishes |
|---|---|---|
| `dp1_lineage.yml` | Hive Metastore | `raw_` and `stg_` datasets, plus a `DataProcessInstance` naming DP1 as the producer |
| `dp2_lineage.yml` | Postgres | `dim_` and `fact_` datasets, plus a `DataProcessInstance` naming DP2 |
| `dp3_lineage.yml` | Postgres | `feat_` datasets, plus a `DataProcessInstance` naming DP3 |
| `validation_assertions.py` | Soda scan results | `Assertion` entities, so validation results appear on the table pages |

The rubric asks for three things on DataHub: the tables, which Airflow pipeline
produced each one, and the validation results for each one. The third needs the
`DataProcessInstance` entity, because that is what ties a table to the run that
made it. A dataset ingested on its own has no producer.

## Why the directories have to exist now

`compose.governance.yaml` bind-mounts `../governance`. Docker creates a missing
bind source as an empty root-owned directory rather than failing, so the
`datahub-ingest` container would start with nothing to ingest and no error.

Run a recipe with:

```bash
make up-governance
docker compose --profile governance run --rm datahub-ingest \
    /opt/governance/recipes/dp1_lineage.yml
```