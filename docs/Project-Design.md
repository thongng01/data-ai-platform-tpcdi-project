# Project Design

This document records the coursework requirements, the project's design decisions and rationale, cross-cutting engineering constraints, and open questions. The [data specification](Data-Specification.md) defines the data model and schemas; [Status](Status.md) tracks implementation progress.

**Decided** means accepted by the owner. **Proposed** means shortlisted for review. **Open** means not yet chosen. **Deferred** means future work outside the current scope.

---

## 1. Coursework Requirements

### 1.1 Data Generation & Problem Injection

The course rubric requires at least 100 GB of offline data (more if 100 GB is too small to show optimization effects), measured high-cardinality columns (`distinct / total`), and controlled offline and streaming data problems with unoptimized baseline vs. treated runs.

| Problem | Definition | What Fails Without Handling | Required Evidence |
| --- | --- | --- | --- |
| **Volume** | At least 100 GB of saved offline source data in staging. | Fixed overheads dominate small runs, hiding optimization effects. | Measured bytes, row counts, and high-cardinality ratios (`distinct / total`) of the saved files. |
| **Skew** | A few values of a key hold a large share of rows on a key that Spark partitions, joins, or groups by. | One task receives most rows of a shuffle, stalls the stage, and spills to disk or runs out of memory. | Key distribution; Spark UI task durations and shuffle sizes (max vs. median); runtime before and after. |
| **Schema evolution** | A source schema changes between files or batches (added, renamed, removed, or retyped column). | Fixed-schema readers fail, or they drop or null the changed column without an error. | Schema before and after; which files carry each schema; read result after handling. |
| **Offline duplicates** | The same record appears more than once in the saved source data (for example, ~2%). | Counts and sums inflate; duplicate changes create extra SCD2 versions. | Duplicate definition, injected rate, and row/metric counts before and after deduplication. |
| **Late arrivals** | Streaming events arrive behind Flink's watermark after their window has closed or emitted. | Late events are dropped or require window/balance revision. | Delayed event manifest, delay distribution, and Flink window/balance results with and without handling. |
| **Streaming duplicates** | The same event is delivered more than once over Kafka (for example, ~1.5%). | Windows and running balances count the event twice. | Event ID contract, injected duplicate rate, and window/balance results before and after deduplication. |
| **Window processing** | Flink groups events into event-time windows. | No windowed streaming features or aggregates exist for downstream use. | Flink window code, explanation, and Flink UI screenshots. |

**Measurement and Evidence Rules**:
- **Measure produced data**: Report volume, skew, cardinality, duplicate rates, schema variants, and delivery delays as measured from the generated files and stream, not merely from configured parameters.
- **Separate fault injection from business history**: Keep native TPC-DI irregularities, synthetic ML account behavior, and injected rubric faults separately configurable and measurable.
- **Baseline vs. treated**: Every Spark and Flink problem-handling claim requires a baseline run and a treated run on the same input data, paired with Spark UI or Flink UI screenshots and an explanation of the symptom, methods tried, and measured gain.

### 1.2 Platform & Operations Requirements

| Area | Required Capabilities & Deliverables |
| --- | --- |
| **Layer 1: Lakehouse & Warehouse** | Medallion zones (`Staging`, `Bronze`, `Silver`, `Gold`) on object storage (SeaweedFS) with Hive Metastore, Trino SQL access, and Gold warehouse tables. SCD2 history dimensions, timestamped feature tables (`event_timestamp` and availability timestamp), ERDs per zone, and storage/query optimization comparisons. |
| **Layer 1: Orchestration & Governance** | Three Airflow pipelines (`DP1` source to bronze, `DP2` bronze to silver/gold, `DP3` offline features), each with ingest and validate stages (Soda) configured via Airflow connections and variables. DataHub catalog recording table lineage and validation results. |
| **Layer 2: Machine Learning** | Feast feature store (point-in-time historical retrieval and online materialization), Kubeflow Pipelines training workflow, MLflow experiment and model tracking, KServe model serving, and a document chunking and embedding retrieval (RAG) pipeline with lineage and validation. |
| **Layer 3: Agents** | LLM serving endpoint, MCP data/ML/retrieval tools, gateway routing and access policies, agent registry and coordination, and versioned prompt management. |
| **Infrastructure & Operations** | Docker Compose profiles (reviewed for image size and dependencies), Terraform and Ansible IaC, CI/CD pipelines, gateway/TLS/rate-limiting/secrets, observability (metrics, logs, traces, model and agent telemetry), and data/API/load testing. |
| **Novel Ideas** | Two demonstrated novel ideas beyond the baseline coursework requirements. |

---

## 2. Decisions & Rationale

### 2.1 Accepted Choices, Proposals, & Deferred Work

| Subject | State | Choice & Rationale |
| --- | --- | --- |
| **Business domain & sources** | Decided | Use the TPC-DI retail brokerage domain and all 18 business and reference source files plus `BatchDate.txt`. Exclude benchmark-only audit metadata (`<name>_audit.csv`, `Audit` table). Preserves a complete, connected multi-format source model. |
| **Load cycle extension** | Decided | Keep TPC-DI's historical load (Batch 1) followed by daily batches (Batch 2+), extended up to one year of daily batches to support rolling ML features and streaming comparisons. Each daily batch keeps its own `BatchID` and `BatchDate.txt`. Historical length and longer-run FINWIRE/HR/reference refresh rules remain open. |
| **Medallion zones** | Decided | **Staging** holds immutable raw batch files. **Bronze** preserves source records and received Kafka events 1:1 with provenance. **Silver** holds cleaned, typed, deduplicated, and conformed records. **Gold** holds the 19-table warehouse, cash stream outputs, and ML tables ([Data Specification](Data-Specification.md#4-data-zones-medallion-architecture)). |
| **Airflow pipelines** | Decided | **DP1** ingests source files to Bronze; **DP2** transforms Bronze to Silver and Gold; **DP3** computes offline feature tables. Each pipeline runs an ingest stage and a Soda validation stage. |
| **Object store & SQL catalog** | Decided | Store Staging, Bronze, and Silver on SeaweedFS via its S3 API, register schemas in Hive Metastore, and query with Trino ([IaC](IaC.md)). |
| **Gold storage & SQL modeling** | Proposed | Store Gold warehouse, streaming, and ML tables in PostgreSQL, with Spark and dbt dividing batch transformations. Exact physical placement and engine boundaries depend on the final Silver and Gold schemas. |
| **Generator approach** | Decided | Build a configurable generator from the TPC-DI specification and the Databricks reference generator, with YAML/JSON configuration, coherent entity histories, synthetic ML behavior, and independent fault injectors. |
| **Generator engine** | Open | The autonomous Polars POC (`generator/poc_polars/`) passed 50 tests and generated 1.01 GB at 125.16 MiB peak RSS ([Polars POC record](records/Polars-POC.md)). Production engine selection (Polars vs. DuckDB vs. Spark) remains open. |
| **Cash streaming** | Decided | Stream `CashTransaction` records after Batch 1 through Kafka and Flink into Bronze deliveries, Silver unique events, and Gold daily balances (`FactCashBalances`), running balances, and window aggregates. Assign a stable Event ID before publication. |
| **ML prediction task** | Decided | Forecast future net account cash movement over a chosen horizon and add it to current cash (`projected_cash = current_cash + forecast_net_movement`). |
| **Minimum-cash policy** | Decided | Flag accounts where `projected_cash < configured_minimum[customer_tier]`, using `DimCustomer.Tier` (1, 2, 3) linked through `DimAccount`. Decouples the regression forecast from policy thresholds. |
| **Predictive behavior** | Decided | Design synthetic account activity profiles in the generator alongside the cash forecast task so features carry intentional signal while trades, cash, and holdings remain consistent. Behavior model details remain open. |
| **Online serving flow** | Proposed | Flink updates cash state and publishes streaming features; a separate scoring worker calls the KServe model endpoint, applies the tier policy, and writes retry-safe forecast/alert records for an API. Keeps model latency off the streaming ingestion path. |
| **Obligation schedule** | Deferred | Explicit payment schedules (`projected_cash = current_cash + forecast_other_net_movement - known_due_payments`) and intraday margin/liquidity coverage are deferred to future work. |
| **Existing code** | Decided | The existing code, Compose files, and Makefile are not constraints on the design. The generator, pipelines, tests, and infrastructure will be rebuilt; reuse a part only where it fits the new design. Makefile targets are convenience commands, added at the end. |
| **Document structure** | Decided | `Data-Specification.md` holds the cross-layer data contract; `Project-Design.md` holds requirements, decisions, constraints, and open questions; `README.md` summarizes the whole system; dedicated layer docs (`Data-Platform.md`, `ML-Platform.md`, `Agent-Platform.md`) join `IaC.md` as each layer is built. |

### 2.2 Selected Streaming & ML Use Case Rationale

`CashTransaction` and account cash-flow forecasting were selected together because they connect streaming, warehouse state, and ML without inventing a separate domain:
- **Action semantics**: Every incremental `CashTransaction` row is an insert-only cash movement (`CT_CA_ID`, `CT_DTS`, signed `CT_AMT`, `CT_NAME`), whereas `Trade` rows require reconstructing multi-step status lifecycles (`SBMT` -> `PNDG` -> `CMPT`/`CNCL`), `HoldingHistory` lacks timestamps, and `DailyMarket` has only daily resolution.
- **Defined baseline calculation**: TPC-DI already defines how daily cash movements roll into `FactCashBalances` (prior balance plus the day's net movement), giving a clear batch reference check for Flink's streaming state.
- **Policy separation**: Forecasting signed net cash movement and comparing `current_cash + forecast_net_movement` against a tier-based minimum (`DimCustomer.Tier`) lets policy thresholds change without retraining the model.

<details>
<summary>Alternative Streaming & ML Candidates Considered</summary>

| Area | Candidate | Why Not Selected as Primary |
| --- | --- | --- |
| **Streaming** | Trade activity (`Trade.txt`) | Requires joining historical status rows and separating trade identity from change identity; non-completed rows must mask execution amounts. |
| **Streaming** | Watch activity (`WatchHistory.txt`) | Tracks `ACTV`/`CNCL` intervals per customer/security, but has no monetary measure or link to account cash balances. |
| **Streaming** | Holding changes (`HoldingHistory.txt`) | Has no native timestamp or account/security columns without joining `DimTrade`; `HH_AFTER_QTY` is a post-trade balance, not an additive delta. |
| **Streaming** | Daily market (`DailyMarket.txt`) | One row per security per day; replaying daily closes through Kafka does not create intraday market ticks. |
| **ML** | Direct low-cash classifier | Bakes a fixed threshold into training labels; changing the tier minimum requires relabeling and retraining. |
| **ML** | Trading-activity or limit-order completion | Depends on order-book dynamics that TPC-DI does not model. |
| **ML** | Market-volume forecast or prospect conversion | Does not use the real-time account cash stream. |

</details>

### 2.3 Proposed Evaluation & Online Serving Flow

**Evaluation Comparison**:
1. **Baseline**: Simple heuristic forecast from historical cash movement.
2. **Batch ML model**: Trained on historical batch features only.
3. **Batch + Streaming ML model**: Trained with recent Flink window features added, evaluated on the same prediction cutoffs, horizons, held-out time periods, and generator seeds. Measure regression error alongside alert recall, false alerts, and lead time.

**Proposed Online Serving Steps**:

| Step | Component | Responsibility |
| --- | --- | --- |
| 1 | **Flink** | Update cash state, write Silver/Gold cash tables, and publish windowed feature values with timestamps. |
| 2 | **Scoring worker** | Read eligible online features on an update trigger or schedule and request a prediction. |
| 3 | **KServe model service** | Return `forecast_net_movement` from the offline-trained model. |
| 4 | **Policy & alert service** | Resolve the account owner's current `DimCustomer.Tier`, apply `configured_minimum[tier]`, and write the forecast and alert record. |
| 5 | **Alert API** | Expose latest forecasts, shortfalls, and model/policy versions to applications and Layer 3 agents. |

---

## 3. Engineering Constraints

### 3.1 Storage & Lakehouse Zones

| Concern | Constraint & Rule |
| --- | --- |
| **Bucket isolation** | Keep raw staging files and lakehouse table directories separated so ingestion jobs cannot overwrite raw source extracts. |
| **Event vs. arrival dates** | Keep business event timestamps (`CT_DTS`, `T_DTS`), batch dates (`BatchDate`), and system arrival dates in separate columns. Partitioning by arrival date alone misses late-arriving historical events in event-date queries. |
| **External tables on SeaweedFS** | Declare lakehouse tables with explicit `LOCATION` clauses. SeaweedFS reports empty prefixes as existing directories, which causes Spark managed-table creation to fail ([IaC Section 7.2](IaC.md#72-every-lakehouse-table-is-external)). |
| **Partition registration** | Register new Hive-backed Parquet partitions through a catalog write or partition repair; writing files to a new `dt=` path alone does not make them visible to Trino ([IaC Section 8.1](IaC.md#81-a-path-write-is-not-a-table-write)). |
| **Table updates (`MERGE`)** | Plain Parquet supports append and partition overwrite, not transactional row-level `MERGE`. Selecting `dbt-spark` incremental `merge` or in-place SCD2/trade updates on the lakehouse requires Delta Lake, Iceberg, or Hudi, or performing mutable Gold updates in PostgreSQL. |

### 3.2 History, SCD2, & Batch-to-Stream Consistency

- **XML omitted vs. empty fields**: In `CustomerMgmt.xml`, an omitted attribute/element keeps the previous version's value, whereas an explicitly empty element (`<C_M_NAME/>`) clears the value to `NULL`. Silver must preserve this distinction before Gold SCD2 carry-forward runs.
- **Carry-forward window support**: Spark supports `last(col, ignorenulls=True)` over ordered windows; PostgreSQL 16 lacks `IGNORE NULLS` on window functions and requires grouping or subquery techniques if SCD2 versions are built in SQL.
- **Daily collapse and propagation**: Collapse multiple same-day changes to one entity into a single daily SCD2 version, and propagate customer version changes to `DimAccount` and company version changes to `DimSecurity` ([Data Specification Section 8](Data-Specification.md#8-core-integration-rules)).
- **Batch-to-stream cash handoff**:
  1. Initialize Flink account balances from the completed Batch 1 Gold `FactCashBalances` state (keyed by natural `AccountID`, since `SK_AccountID` changes across SCD2 versions).
  2. Assign a deterministic Event ID to each post-Batch-1 cash movement before Kafka publication so Bronze records every delivery and Silver/Flink deduplicate repeated deliveries.
  3. Give each Gold cash table a single writer during normal operation so batch validation replays do not double-count live balances.

**Stream verification**: Check the Flink results against an expected result that is calculated outside Flink.
- **Event identity**: For a file replay, derive `event_id` from provenance: file name, batch, line number, and file hash. A generated event keeps one ID from export through publication. A Kafka offset identifies a delivery position, not the business event.
- **Two clocks**: Keep `event_time` from `CT_DTS` and `published_at` from the delivery. Source `DATETIME` values have no time zone, so record the interpretation instead of assuming UTC.
- **Baseline first**: Run one replay without faults. Then run a second replay with a fixed seed and a saved fault manifest. Kafka keeps order within a partition, so the producer must create the delays and reordering.
- **Ordering**: Partition by account to keep per-account order. Do not claim a global order across partitions.
- **Expected result**: Compute deduplicated cash sums and daily balances independently, then compare each run for missing events, duplicate effects, late events, and time to final agreement. A cash total can decrease, so the largest emitted value does not identify the final result.
- **Out-of-order updates**: An older update must not overwrite newer state because it arrived later.

### 3.3 Generator

- **Bounded memory**: Generate records in bounded chunks while preserving cross-batch entity state (active customers, accounts, brokers, securities, and open trades/holdings). Streaming file sinks alone do not bound memory if intermediate joins or sorts materialize full history in RAM.
- **Seed stability**: Deterministic IDs and random draws must depend on entity keys and named seeds, not chunk size or worker partition count.
- **Run manifest**: Save the resolved configuration, reference commit, runtime versions, dataset schemas, row and byte counts, and file hashes for every run.

**Reference generator corrections**: The [Databricks TPC-DI generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen) at commit `2ac319db` is the reference. Its pinned local checkout is `misc/generator-feasibility/reference`. Code reused from it needs these corrections:

| Problem in the Reference | Effect | Correction |
| --- | --- | --- |
| Non-completed trades keep execution amounts. | `T_TRADE_PRICE`, `T_CHRG`, `T_COMM`, and `T_TAX` have values on non-`CMPT` rows, which the specification forbids. | Set these fields to `NULL` unless the status is `CMPT`. |
| A sell picks an arbitrary smaller trade ID as its holding origin. | A smaller ID does not mean the same account and security or an earlier completed buy, so holdings are not continuous. | Derive holding changes from ordered trades per account and security. Check origin, time, and before and after quantities. |
| FINWIRE records go to separate files per record type, in arbitrary order. | The output does not match the quarterly `FINWIREyyyyQq` files ordered by `PTS`. | Write quarterly files ordered by `PTS`, with fixed field widths. |
| Native errors are partial. | No invalid dates of birth or tiers, only valid credit ratings, and no invalid fees or commissions, so `DImessages` alert paths are not exercised. | Define native irregularities separately from rubric faults and measure them. |
| Audit code can copy saved counts. | Audit totals can describe an earlier run instead of the produced data. | Compute observations from each produced run. |
| The generic writer does not sort, and some seeds and IDs depend on defaults, driver time zone, or `monotonically_increasing_id`. | File order and repeat runs are not guaranteed. | Sort exports explicitly, and route every seed and ID through the configuration. |

**Fault configuration**: Keep generation settings, source faults, and delivery faults in separate configuration sections, so each effect is measurable.
- **Fault manifest**: For each injected fault, record the original record, the fault type, and the changed payload or delivery.
- **Duplicate rate**: Define the denominator. With `r` extra copies per original record, the duplicate share of emitted rows is `r / (1 + r)`, not `r`.
- **Skew**: Create skew by choosing valid account and security identities, then derive trades, cash, and holdings from those choices. Rewriting foreign keys after the dependent facts exist breaks relationships.
- **Schema evolution**: Record which batch introduces each schema version. Parsers must tell an absent field from a present `NULL` where the difference matters.
- **Delivery faults**: Delayed and duplicated events keep their business event time and stable event ID.

---

## 4. Open Questions & References

### 4.1 Open Questions

| Area | Open Question | What Depends on It |
| --- | --- | --- |
| **Data Model** | What are the exact Silver table schemas, and which Gold refinements (such as adding a surrogate key or direct link to `Prospect`) do we adopt? | DP2 transformations, ERDs for Silver/Gold, and foreign-key constraints. |
| **Load Cycle** | How many years of history does Batch 1 cover, and do `FINWIRE`, `HR`, or reference tables refresh during a 1-year daily run? | Generator calendar bounds and DP2 incremental rules. |
| **Storage & Engines** | How do Spark, dbt, Trino, and PostgreSQL divide Silver and Gold storage and transformations, and which lakehouse table format is used? | DP1 to DP3 job code, `MERGE` support, and Compose profiles. |
| **Generator & Faults** | Which production generator engine (Polars vs. DuckDB vs. Spark), scale factor for 100 GB, and exact offline fault parameters (skew key/share, schema evolution change, duplicate rate) do we lock in? | Generator build, characteristics report, and Spark baseline vs. treated experiments. |
| **Cash Stream** | Do cash events enter Kafka via direct generator publishing or timed file replay, and what are the Event ID format, window sizes, watermark/lateness limits, and late-balance correction rules? | Flink job implementation, Gold cash sinks, and Flink UI evidence. |
| **ML Forecast & Policy** | What are the forecast horizon, scoring cadence, feature list, synthetic account behavior rules, model family, evaluation metrics, and tier minimum amounts (Tiers 1, 2, 3)? | Generator behavior model, DP3 feature tables, Feast views, KFP pipeline, and alert policy config. |
| **Text / RAG** | Which brokerage document sources and retrieval task does the text pipeline serve? | Document ingestion, chunking, Feast vector storage, and Layer 3 retrieval tool. |
| **Agents** | What user workflow, MCP tool contracts, model/framework, and gateway policies define Layer 3? | Layer 3 implementation and end-to-end demo. |
| **Pipeline Reruns** | How do batch retries and reruns reset or overwrite state from a known batch boundary? | Airflow DAG idempotency and partition/table replacement logic. |
| **Novel Ideas** | Which two novel ideas do we demonstrate and measure for the coursework rubric? | Project design and evaluation evidence. |

### 4.2 Working Records & Local References

The [Polars POC record](records/Polars-POC.md) holds the generator POC evidence: 50 tests, 1.01 GB of validated output at 125.16 MiB peak RSS.

These local reference notes are for later phases. Git ignores `misc/archive/` and `misc/tpcdi/`, so a fresh clone does not have them; Git history keeps the earlier `docs/research/` copies.

| Path | Use It When |
| --- | --- |
| `misc/tpcdi/NOTES.md` | Writing DP2 SQL: index of the AlaaAlmutawa TPC-DI staging and warehouse SQL, with the defects in those queries. |
| `misc/archive/Card-Fraud-Platform.md` | Building the Spark skew and deduplication benchmarks, the Flink window checker, and the scoring service: patterns from a reference coursework repository. |
| `misc/archive/diagram-references/` | Drawing the architecture and deployment diagrams: seven external platform diagrams for layout comparison. |
