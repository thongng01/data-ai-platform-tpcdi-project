# Status

Updated 2026-11-01. The session log, notes up to 2026-10-30, and the 2026-10-31 to 2026-11-01 cleanup history are in `misc/archive/status-log-2026-10.md`; Git holds earlier versions of every document.

## Current Position
- **Data specification**: Owner review continues in [Data Specification](Data-Specification.md). Section 2 defines the processing cycle: the historical load with `start_date` and `end_date` run parameters, daily batches on UTC calendar dates, FINWIRE in the daily batch of its quarterly release date, and the cash stream after Batch 1. Source naming, delivery completeness, buckets, dataset scope, and the data versioning baseline are decided. Open items are in [Section 11](Data-Specification.md#11-open-decisions); financial availability fields and Gold ERD refinements belong to the Gold schema work.
- **Design**: [Project Design](Project-Design.md#2-decisions--rationale) records each decision with its rationale: source and delivery (Section 2.4), object storage (Section 2.5), and data versioning (Section 2.6). Catch-up, reruns, corrections, `BatchID` assignment, and HR and reference evolution are deferred. ML evaluation uses held-out time periods only.
- **Design method**: [Design Method](Design-Method.md) describes how decisions are framed, explored, and chosen. The [documentation guide](Documentation-Guide.md#decision-records) defines decision records, and `AGENTS.md` directs agents to both.
- **README**: Data section follows the specification. The remaining content awaits owner review.
- **Work order**: Refine and define the data specification, then design the overall project and its layers, then build the generator.
- **Existing code**: Not a design constraint. The generator, pipelines, tests, infrastructure, and Makefile will be rebuilt. The Polars POC supersedes `datagen` as the starting point for the generator rebuild. The What Exists and Known Defects tables below describe the earlier build.
- **Generator**: Polars POC complete: 50 tests, 1.01 GB validated output, 125 MiB peak RSS. See the [POC record](records/Polars-POC.md). Production engine: open.
- **Delivery targets**: Final deadline about two months after 2026-10-26. First-week target: Layer 1 nearly complete, plus a working Layer 2 flow or a substantial foundation.
- **Paseo instruction**: The 2026-10-30 Paseo instruction change applies to new sessions after the owner restarts the daemon.
- **Diagrams**: Diagram sizes are `small`, `medium`, and `large`, replacing `standard` and `master`. ERDs are Mermaid `.mmd` files that the shared tool renders with ELK and the project theme; each table carries a kind tag (`entity`, `event`, `state`, `reference`) that sets its header color. Documents embed PNG for every size, because the Paseo mobile app does not show SVG images. Tables, fields, and data relationships appear only in ERDs; the draw.io tool no longer creates tables or relationship connectors. The gold layer ERD is embedded in both the data specification and the README.

## Open Decisions
- **Data**: See [Open Decisions](Data-Specification.md#11-open-decisions) in the data specification.
- **Design**: See [Open Questions](Project-Design.md#41-open-questions) in Project Design.

## Next Step
1. The source delivery and staging contract is complete enough to build on. Choose the next data specification topic from the [open decisions](Data-Specification.md#11-open-decisions). Do not reopen deferred HR and reference evolution, catch-up, or reruns.

## What Exists

Runtime measurements and test counts are those of the earlier build, except the Polars POC row.

| Area | Location | State |
|---|---|---|
| Generator | `generator/` | Writes selected TPC-DI-shaped files and configurable problems. The earlier record lists 31 tests. Missing: `HR.csv`, `Prospect.csv`, the `Customer.txt` and `Account.txt` deltas, a price walk, and the lateness wiring. Reference-based redesign is selected ([Decisions & Rationale](Project-Design.md#2-decisions--rationale)). |
| Polars generator POC | `generator/poc_polars/` | Covers the 18 source families plus `BatchDate.txt`, an earlier convention, with simplified connected histories, YAML configuration and source definitions, bounded calculations, ordered exports, and independent validation. Passed 50 tests. The largest validated pilot produced 1.01 GB at 125.16 MiB peak RSS. It does not implement the complete reference scheduler, fault controls, predictive behavior, or downstream integration. |
| Characteristics inspector | `generator/src/datagen/inspect.py` | Built and tested. It reads whole files and assumes CRLF, so it does not scale to 100 GB unchanged. |
| Core platform | `infra/` | SeaweedFS, Hive Metastore, Trino, and PostgreSQL. `make verify` passed 8 smoke tests in the earlier build. |
| Spark cluster | `infra/` | Image `edai/spark:3.5.3`, which has Python 3.8.10. |
| DP1 code | `pipelines/dp1/` | Landing to bronze to silver, run in the earlier build. The revised DP1 ends at bronze. The code is not reorganized. |
| Pipeline contract tests | `tests/` | The earlier record lists 55 schema, deduplication, and variant tests, with 86 tests in the combined suite. They do not establish that the Spark jobs are correct. |
| DP2, dbt | `dbt/` | Compose profile `dbt` exists. No dbt project. |
| DP3, features | none | Not started. |
| Airflow | `orchestration/` | Compose files and connection and variable seed files exist. Initialization ignores import failures, so successful import is unverified. No DAGs. |
| Kafka and Flink | `infra/` | Compose profile `stream` exists. No job. |
| Soda validation | none | Not started. |
| DataHub | `governance/` | Compose profile `governance` exists. No ingestion recipes. |
| ML platform | none | Not started. |

`infra/.env` is committed on purpose. It holds local development defaults and no production credential. Docker Compose reads it when it parses the files, so a fresh clone needs it.

## Known Defects

Source inspection found these defects. They are not a fix list. The requirements and the reference design decide which code survives.

| Defect | Where | Cause and effect |
|---|---|---|
| A silver load deletes earlier batches | `pipelines/dp1/bronze_to_silver.py` | The job clears the whole table location before an overwrite, and overwrite is the default mode. Loading Batch2 therefore deletes the Batch1 rows. |
| Unmatched rows are never reported | `pipelines/dp1/landing_to_bronze.py` | The job counts unmatched rows after it replaces the frame with the union of the two matched shapes. The count is always zero, so rows with an unexpected field count disappear without a warning. |
| Regenerated files are not uploaded | `pipelines/dp1/publish_landing.py` | The upload skips a file whose name already exists in the landing bucket. After regeneration, the bucket keeps the old file. |
| Lateness settings do nothing | `generator/` | The lateness and out-of-order rates are not wired into the Trade writer, so a configured rate produces no late rows. |
| Tax rate generation can hang | `generator/src/datagen/specs_reference.py` | Codes come from 10 countries and the digits 1 to 9, so only 90 codes exist. The loop runs until it has the requested count, which is 320 at scale factor 1.0, so it cannot finish above 90. |
| Cast failures are hard to use | `pipelines/dp1/bronze_to_silver.py` | Failed casts stay in silver as NULL. `_rejected_columns` joins one value per typed column with commas, so every row has a value, even with no failure, and the value does not name the failed column. |
| Missing build contexts | `infra/` | Compose files reference `docker/airflow`, `docker/flink`, and `docker/datahub-ingest`, which do not exist. Those images cannot build. |
| Airflow executor and worker conflict | `infra/compose.orchestration.yaml` | LocalExecutor runs tasks under the scheduler, but a separate service uses a worker command. The service roles do not match the selected executor. |
| Airflow import failures do not fail initialization | `infra/compose.orchestration.yaml` | Connection and variable imports use `|| true`. A successful initialization exit does not establish that those settings exist. |
| Flink submit mounts the wrong pipeline directory | `infra/compose.streaming.yaml` | `../../pipelines` resolves outside coursework when the base Compose directory is `infra/`. The container can receive an unintended empty directory. |

Two problems from the earlier build remain unsolved:

- **Small files.** DP1 writes about one Parquet file per date partition. At scale 0.02, `stg_trade_history` had 1,800 files of about 5 KB, and the write took minutes.
- **Slow Spark shutdown.** The earlier run blocked while closing the event log against SeaweedFS. The code waits up to 90 seconds, then writes its report. `stopped_cleanly` records shutdown separately from the processing result ([shutdown and reports](IaC.md#811-shutdown-deadlines-and-run-reports)).
