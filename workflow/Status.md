# Status

Updated 2026-11-02. Earlier session logs and cleanup history are in `local/archive/status-log-2026-10.md`; Git holds earlier versions of every document.

## Current Position
- **Repository and documentation system (Step 1 complete)**:
  - **`docs/`** holds only the five public system documents ([Data Specification](../docs/Data-Specification.md), [Data Platform](../docs/Data-Platform.md), [ML Platform](../docs/ML-Platform.md), [Agent Platform](../docs/Agent-Platform.md), and [Infrastructure](../docs/IaC.md)), mapped 1-to-1 to the sections of [README](../README.md) with zero `(open)` or `Status.md` workflow clutter.
  - **`workflow/`** holds our reusable engineering workflow, authoring guides ([Workflow Overview](README.md), [Documentation Guide](Documentation-Guide.md), [Design Method](Design-Method.md), [Diagram Guide](Diagram-Guide.md), [Diagram Examples](Diagram-Examples.md)), working state (`Status.md`), POC records ([Polars POC](records/Polars-POC.md)), and verification and diagram tools (`workflow/tools/`).
  - **`local/`** holds all gitignored scratch files, archives, coursework rubric and proposal files (`local/course/`), reference checkouts (`local/tpcdi/`, `local/generator-feasibility/`), and sample runs (`local/sample_run/`), replacing `misc/`.
- **Repository code layout (Variant A2)**: Organized into the four platform pillars (`data/`, `ml/`, `agents/`, `infra/`), with direct CLI commands replacing the earlier `Makefile` wrapper and unit tests co-located inside each component. The Polars generator lives in [`data/generator/`](../data/generator/README.md) (50 tests, 1.01 GB validated output at 125.16 MiB peak RSS; see [POC record](records/Polars-POC.md)). The superseded `datagen`, `dp1`, and `Makefile` files are archived in `local/archive/prototype-v0/`.
- **Delivery targets**: Final deadline about two months after 2026-10-26. First-week target: Layer 1 nearly complete, plus a working Layer 2 flow or a substantial foundation.

---

## Accepted Decisions, Proposals, & Deferred Scope

| Subject | State | Choice & Rationale |
| --- | --- | --- |
| **Business domain & sources** | Decided | Use the TPC-DI retail brokerage domain and all 18 business and reference source families. Exclude benchmark-only audit metadata (`<name>_audit.csv`, `Audit` table) and `BatchDate.txt`. |
| **Historical range input** | Decided | Operator enters inclusive UTC `start_date` and `end_date` run parameters when triggering the historical load. `end_date` dates the historical `Prospect.csv` snapshot, and the first daily batch is the next day ([Data Platform](../docs/Data-Platform.md#61-source-data-and-delivery)). |
| **Load cycle extension** | Decided | Keep the historical load (Batch 1) followed by daily batches (Batch 2+) extended up to one year, with one batch per UTC calendar date (including weekends and holidays). |
| **Quarterly FINWIRE** | Decided | Release each financial reporting quarter on the last calendar day of the following month in UTC, inside that day's `incremental/YYYYMMDD/` folder, named by the posting quarter of `PTS` while keeping the reporting period in each `FIN` row. |
| **Static HR and reference data** | Decided | Load `HR.csv` and all six reference files during the historical load and keep them static for the run. Ongoing HR and reference updates are deferred. |
| **Source naming & completeness** | Decided | Keep standard names under `historical/`; use `incremental/YYYYMMDD/<Source>_YYYYMMDD.<ext>` for daily files. Completeness uses a predefined expected-file list per delivery; a zero-record file means no activity. |
| **Medallion zones & buckets** | Decided | Three SeaweedFS buckets: `staging` (raw batch files), `lakehouse` (`bronze/` 1:1 records with provenance and `silver/` cleaned records), and `platform` (logs, checkpoints, artifacts). One active regenerable dataset at a time ([Data Platform](../docs/Data-Platform.md#62-object-storage-layout-and-dataset-scope)). |
| **Data versioning baseline** | Decided | Raw file retention in `staging` for replay recovery, provenance and parse failures in Bronze, and SCD2 valid-time history in Gold. Corrected deliveries, table snapshots, and dead-letter topics wait for a requirement ([Data Platform](../docs/Data-Platform.md#63-data-versioning-baseline)). |
| **Airflow pipelines** | Decided | `DP1` ingests source files to Bronze; `DP2` transforms Bronze to Silver and Gold; `DP3` computes offline feature tables (`feat_*`). Each pipeline runs an ingest stage and a Soda validate stage and publishes lineage and assertions to DataHub. |
| **Cash streaming & ML use case** | Decided | Stream post-Batch-1 `CashTransaction` events through Kafka and Flink; forecast future net account cash movement and flag `current_cash + forecast_net_movement < configured_minimum[customer_tier]` ([ML Platform](../docs/ML-Platform.md#1-prediction-task-and-minimum-cash-policy)). |
| **Workspace, docs, & 4-pillar repo** | Decided | Separate `docs/` (5 public system docs), `workflow/` (status, guides, `check_docs.py`, `diagrams/`), and `local/` (gitignored scratch and archives); organize code into `data/`, `ml/`, `agents/`, and `infra/` without a root `Makefile` or root `tests/` ([Workflow Overview](README.md#5-design-decisions-and-rationale-for-this-system)). |
| **Schema & table naming convention** | Decided | Enforce `snake_case` with rubric prefixes (`bronze.raw_*`, `silver.stg_*`, `gold.dim_*`, `gold.fact_*`, `gold.feat_*`) and `_snake_case` metadata columns across the system ([Workflow Overview](README.md#3-unified-naming-conventions)). Apply to `Data-Specification.md`, ERDs, and `README.md` in Step 2. |
| **Gold storage & SQL modeling** | Proposed | Store Gold warehouse, streaming, and ML tables in PostgreSQL and `lakehouse` tables, with Spark and dbt dividing batch transformations. |
| **Online serving flow** | Proposed | Flink updates cash state and publishes streaming features; a separate scoring worker calls KServe, applies the tier policy, and writes forecast and alert records for the Web API. |
| **Catch-up, reruns, & obligations** | Deferred | Catch-up loads, reruns, `BatchID` assignment, and explicit account obligation schedules are deferred. |

---

## Open Decisions (Step 2 Backlog)

| Area | Open Decision | What Depends on It |
| --- | --- | --- |
| **1. Calendar & Delivery** | Historical load length (e.g., 2 years); delivery pacing (simulated clock for daily files and Kafka cash events); late-file handling; whether to simplify `FINWIRE` availability to release `data_date` without custom `Financial` columns. | Generator date bounds, delivery runner, and Gold `Financial` schema. |
| **2. Bronze, Silver, & Gold Schemas** | Lakehouse table format (Delta Lake vs. Iceberg vs. Parquet); Spark vs. dbt vs. PostgreSQL split; applying the decided `raw_`, `stg_`, `dim_`, `fact_`, `feat_` `snake_case` naming across `Data-Specification.md` and `README.md`; exact `bronze.raw_*` provenance columns, `silver.stg_*` schemas, and `gold.feat_*` schemas (`event_timestamp` and `created`); Bronze and Silver ERDs (`rubric.xlsx` rows `34-38`). | `DP1`, `DP2`, and `DP3` implementations and zone ERDs. |
| **3. Generator & Faults** | Production generator engine (Polars vs. DuckDB vs. Spark), 100 GB scale settings, fault parameters (skew, schema evolution, ~2% offline duplicates, late arrivals, ~1.5% streaming duplicates), and generator summary schema. | Generator rebuild and Spark and Flink baseline-vs.-treated experiments. |
| **4. Cash Stream (Flink)** | Deterministic Event ID format; window sizes; watermark and lateness limits; late-balance correction rules; account key resolution when a cash event precedes its daily batch. | Flink job implementation, Gold cash sinks, and Flink UI evidence. |
| **5. ML Forecast & RAG** | Forecast horizon, scoring cadence, feature list, synthetic account behavior rules, model family, evaluation metrics, Tiers 1, 2, and 3 minimum amounts, `Forecast and alert` table schema, and brokerage RAG document sources. | Generator behavior model, `DP3`, Feast views, KFP pipeline, KServe, and RAG DAG. |
| **6. Layer 3 & Novel Ideas** | Specific brokerage agent scenarios and prompts, and selection of the 2 Layer 1 novel ideas (`20 pts`) and 2 Layer 3 novel ideas (`4 pts`). | Layer 1 and Layer 3 novel-idea implementations. |

### Local References for Later Phases
- `local/tpcdi/NOTES.md`: Index of the AlaaAlmutawa TPC-DI staging and warehouse SQL and its defects (for `DP2` SQL).
- `local/archive/Card-Fraud-Platform.md`: Reference patterns for Spark skew and deduplication benchmarks, the Flink window checker, and the scoring service.
- `local/archive/diagram-references/`: Seven external platform diagrams for layout comparison.

---

## Next Step
1. Start **Step 2**: Lock down the concrete choices in Areas 1 through 5 above (beginning with the Calendar, Delivery, and Bronze, Silver, and Gold schema design), then build the generator and Layer 1 pipelines.

---

## What Exists

Runtime measurements and test counts are those of the earlier build, except the Polars POC row.

| Area | Location | State |
|---|---|---|
| Source generator | `data/generator/` | Polars implementation covering all 18 source families plus `BatchDate.txt` (an earlier convention) with simplified connected histories, YAML configuration, bounded calculations, ordered exports, and independent validation. Passed 50 tests; produced 1.01 GB at 125.16 MiB peak RSS ([POC record](records/Polars-POC.md)). Fault injection, predictive behavior, and `incremental/YYYYMMDD/` naming remain to be added. |
| Archived v0 prototype | `local/archive/prototype-v0/` | Earlier `datagen` and `dp1` prototype code, removed from the tracked working tree. |
| Core platform & Spark | `infra/`, `data/spark/` | SeaweedFS, Hive Metastore, Trino, PostgreSQL, and Spark (`edai/spark:3.5.3`). The `tools` smoke check passed 8 tests in the earlier build. |
| Layer 1 pipelines (`DP1-DP3`, Flink, dbt, Airflow, Soda, DataHub) | `data/` (`spark/`, `dbt/`, `flink/`, `airflow/`, `soda/`, `datahub/`) | Directory structure and Compose mounts in place; jobs, models, DAGs, checks, and recipes are not built yet. |
| Layer 2 ML & Layer 3 Agents | `ml/`, `agents/` | Directory structure in place (`feature_repo/`, `training/`, `kfp/`, `serving/`, `rag/`; `mcp/`, `coordinator/`, `gateway/`, `prompts/`, `registry/`). Not built yet. |

`infra/.env` is committed on purpose with local development defaults so Docker Compose can parse the files on a fresh clone.

## Known Defects in Earlier Prototype Code

These defects were found by inspecting the earlier prototype code. They are recorded so they are not repeated during the rebuild.

| Defect | Where | Cause and Effect |
|---|---|---|
| Silver load deletes earlier batches | `local/archive/prototype-v0/pipelines/dp1/bronze_to_silver.py` | Clears the whole table location before an overwrite, which is the default mode. Loading Batch 2 deletes Batch 1 rows. |
| Unmatched rows are never reported | `local/archive/prototype-v0/pipelines/dp1/landing_to_bronze.py` | Counts unmatched rows after replacing the frame with the union of matched shapes, so unexpected field counts disappear without a warning. |
| Regenerated files are not uploaded | `local/archive/prototype-v0/pipelines/dp1/publish_landing.py` | Skips uploading a file whose name already exists in the bucket. |
| Lateness settings & tax-rate loop in v0 `datagen` | `local/archive/prototype-v0/generator/` | Lateness rates were unwired, and tax-rate generation requested 320 codes from a space of 90. |
| Cast failures in v0 `dp1` | `local/archive/prototype-v0/pipelines/dp1/bronze_to_silver.py` | Failed casts stayed in silver as NULL, and `_rejected_columns` did not name the failed column. |
| Missing build contexts | `infra/` | Compose files reference `docker/airflow`, `docker/flink`, and `docker/datahub-ingest`, which do not exist yet. |
| Airflow executor and worker conflict | `infra/compose.orchestration.yaml` | `LocalExecutor` runs tasks in the scheduler, but a separate service runs a worker command, and imports use `|| true`. |
| Small files & slow Spark shutdown | `local/archive/prototype-v0/pipelines/dp1/` | Wrote ~1 Parquet file per date partition (1,800 ~5 KB files at scale 0.02) and blocked while closing the event log against SeaweedFS. |
