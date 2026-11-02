# Layer 1: Data Platform

The data platform generates TPC-DI brokerage data, ingests raw batch files and streaming cash events into Medallion zones (`Staging`, `Bronze`, `Silver`, `Gold`), computes analytical warehouse tables and offline ML features, schedules pipelines in Airflow with Soda quality gates, and records lineage and validation results in DataHub. The [data specification](Data-Specification.md) defines the source formats, warehouse schemas, and integration rules.

---

## 1. Platform Architecture and Medallion Zones

| # | Zone | Purpose | Grain | Storage |
| --- | --- | --- | --- | --- |
| 1 | **Staging** | Immutable raw batch files as delivered. Post-Batch-1 cash movements enter through Kafka instead. | One file | SeaweedFS `staging` bucket |
| 2 | **Bronze** | Source records and received Kafka events 1:1 as delivered, including duplicates and parse failures, with provenance. | One source record or one delivery | SeaweedFS `lakehouse` bucket, `bronze/` tables |
| 3 | **Silver** | Cleaned, typed, deduplicated, and conformed records ready for relational joins. | One validated business record | SeaweedFS `lakehouse` bucket, `silver/` tables |
| 4 | **Gold** | The 19-table warehouse, streaming cash outputs, and offline feature tables. | Per business entity, event, or window | PostgreSQL and `lakehouse` tables |

Operational files (Spark event logs, Flink checkpoints, and MLflow artifacts) use the separate `platform` bucket. Hive Metastore registers lakehouse table schemas and locations, Spark and dbt execute batch transformations, Flink processes the continuous cash stream, and Trino provides federated SQL access across zones.

### Retention and Versioning Baseline
- **Raw retention**: `staging` keeps every delivered file unchanged for the active dataset, so replaying `staging` rebuilds Bronze, Silver, and Gold.
- **Provenance and rejects**: Bronze records delivery provenance and unparseable payloads; business data alerts are logged in `DImessages`.
- **Business history (valid time)**: Gold SCD2 dimensions (`DimCustomer`, `DimAccount`, `DimBroker`, `DimCompany`, `DimSecurity`) record effective date ranges, and fact tables store the surrogate key valid on the event date.

---

## 2. Data Generation and Problem Injection

The generator produces connected source records across all 18 TPC-DI source families using the [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf), the official SF3 sample dataset, and the [Databricks TPC-DI generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen) as references. Generation writes files locally first; a separate delivery step uploads batch files to `staging` and publishes post-Batch-1 cash events to Kafka.

### Simulated Data Problems and Required Evidence

| Problem | Domain Behavior | What Fails Without Handling | Measured Evidence |
| --- | --- | --- | --- |
| **Volume** | At least 100 GB of saved offline source files in `staging`, plus measured high-cardinality columns (`distinct / total`). | Fixed overheads dominate small runs and hide optimization gains. | Measured bytes, row counts, and high-cardinality ratios from the generated files. |
| **Skew** | A small subset of active accounts and securities holds a large share of rows on keys that Spark partitions, joins, or groups by. | One task receives most rows of a shuffle, stalls the stage, and spills to disk or runs out of memory. | Key distribution; Spark UI task durations and shuffle sizes (max vs. median); runtime before and after. |
| **Schema evolution** | A source schema changes between batches (added, renamed, removed, or retyped column). | Fixed-schema readers fail, or they drop or null the changed column without an error. | Schema before and after; which files carry each schema; read result after handling. |
| **Offline duplicates** | Repeated source records (~2%) appear in saved batch files from upstream extract retries. | Counts and sums inflate; duplicate changes create extra SCD2 versions. | Duplicate definition, injected rate, and row and metric counts before and after deduplication. |
| **Late arrivals** | Streaming cash events arrive behind Flink's watermark after their event-time window has emitted. | Late events are dropped or require window and daily balance revision. | Measured event and delivery times, delay distribution, and Flink results with and without handling. |
| **Streaming duplicates** | At-least-once Kafka delivery sends the same cash event more than once (~1.5%) with the same Event ID. | Windows and running balances count the cash movement twice. | Event ID contract, injected duplicate rate, and window and balance results before and after deduplication. |
| **Window processing** | Flink groups cash events into event-time windows per account. | No windowed streaming features or aggregates exist for downstream scoring. | Flink window code, explanation, and Flink UI screenshots. |

### Generator Engineering Rules
- **Bounded memory**: Generate records in bounded chunks while preserving cross-batch entity state (active customers, accounts, brokers, securities, and open trades and holdings).
- **Seed stability**: Derive deterministic IDs and random draws from entity keys and named seeds rather than chunk size or worker partition count, so identical settings, seeds, and generator version reproduce identical files.
- **Generator summary**: Record generation settings (scale, seeds, date bounds, and fault settings), per-file row and byte counts, and measured data characteristics in one local summary file outside `staging` so ingestion never reads it.
- **Fault separation**: Keep native TPC-DI irregularities, synthetic ML account behavior, offline source faults, and streaming delivery faults in separate configuration sections. With `r` extra copies per original record, the duplicate share of emitted rows is `r / (1 + r)`. Inject skew by choosing valid account and security identities before deriving trades, cash, and holdings so foreign keys stay consistent.

<details>
<summary>Reference Generator Corrections</summary>

Code adapted from the Databricks reference generator (`local/generator-feasibility/reference`) applies these corrections:

| Problem in the Reference | Effect | Correction |
| --- | --- | --- |
| Non-completed trades keep execution amounts. | `T_TRADE_PRICE`, `T_CHRG`, `T_COMM`, and `T_TAX` have values on non-`CMPT` rows, which the specification forbids. | Set these fields to `NULL` unless the status is `CMPT`. |
| A sell picks an arbitrary smaller trade ID as its holding origin. | A smaller ID does not guarantee the same account and security or an earlier completed buy, breaking holding continuity. | Derive holding changes from ordered trades per account and security, checking origin, time, and before and after quantities. |
| FINWIRE records go to separate files per record type, in arbitrary order. | Output does not match quarterly `FINWIREyyyyQq` files ordered by `PTS`. | Write quarterly files ordered by `PTS` with fixed field widths. |
| Native errors are partial. | No invalid dates of birth or tiers, only valid credit ratings, and no invalid fees or commissions, leaving `DImessages` alert paths unexercised. | Define native irregularities separately from rubric faults and measure them. |
| Audit code can copy saved counts. | Audit totals can describe an earlier run instead of the produced data. | Compute observations directly from each produced run. |
| Generic writer does not sort, and some seeds depend on driver time zone or `monotonically_increasing_id`. | File order and repeat runs are not guaranteed. | Sort exports explicitly and route every seed and ID through configuration. |

</details>

---

## 3. Batch Processing and Storage Optimization

Spark and dbt execute the batch pipelines across three stages:
- **Source parsing (`DP1`)**: Reads XML (`CustomerMgmt.xml`), fixed-width (`FINWIREyyyyQq`), pipe-delimited (`.txt`), and comma-delimited (`.csv`) files from `staging` into `bronze/` tables, preserving provenance and capturing unparseable rows.
- **Cleaning and integration (`DP2` Silver)**: Applies types, deduplicates repeated deliveries while preserving legitimate business updates (such as multiple updates to one trade), flattens XML actions while preserving omitted versus explicitly empty (`<C_M_NAME/>`) elements, and separates event timestamps from `data_date` and arrival timestamps.
- **Warehouse modeling (`DP2` Gold)**: Builds the 5 SCD2 dimensions (collapsing multiple same-day changes into one daily version and propagating customer changes to `DimAccount` and company changes to `DimSecurity`), mutable `DimTrade`, 4 fact tables, current-state `Prospect` and `Financial`, 6 reference tables, and `DImessages`.
- **Feature computation (`DP3`)**: Computes historical account feature snapshots (`feat_*` tables) with explicit `event_timestamp` and `created` availability columns for Feast.
- **Storage optimization**: Compares unoptimized baseline layouts against file compaction, partitioning, and sorting on lakehouse tables, and evaluates indexing and query plans (`EXPLAIN ANALYZE`) on warehouse tables.

### Storage and SCD2 Engineering Constraints

| Concern | Constraint & Rule |
| --- | --- |
| **Bucket isolation** | Keep raw delivered files in `staging` and table locations in `lakehouse` so a job that overwrites a table location cannot delete raw source files. |
| **SeaweedFS volumes** | Each bucket is a separate SeaweedFS collection ([IaC Section 8.3](IaC.md#83-the-object-store-runs-out-of-volumes)); configure volume growth count and volume size limits to avoid exhausting volume slots across buckets. |
| **External tables** | Declare lakehouse tables with explicit `LOCATION` clauses because SeaweedFS reports empty prefixes as existing directories ([IaC Section 7.2](IaC.md#72-every-lakehouse-table-is-external)). |
| **Partition registration** | Register new Hive-backed partitions through a catalog write or partition repair so Trino sees newly written partitions ([IaC Section 8.1](IaC.md#81-a-path-write-is-not-a-table-write)). |
| **Transactional updates (`MERGE`)** | In-place `MERGE` on lakehouse tables requires a transactional table format (Delta Lake or Iceberg), whereas plain Parquet supports only append and partition overwrite. |
| **Carry-forward windows** | Spark supports `last(col, ignorenulls=True)` over ordered windows; PostgreSQL 16 lacks `IGNORE NULLS` on window functions and requires grouping or subquery techniques when building SCD2 versions in SQL. |
| **Financial availability** | Populate `Financial.FI_YEAR`, `FI_QTR`, and `FI_QTR_START_DATE` from the `FIN` record rather than the file name, resolve `SK_CompanyID` at `PTS`, and combine quarterly earnings across SCD2 versions by natural company identity without leaking future reports into earlier P/E or feature calculations. |

---

## 4. Stream Processing (Kafka and Flink)

After the historical load (Batch 1), new `CashTransaction` movements are published to Kafka and processed continuously by Flink.

| Cash Event Field | Type | Meaning |
| --- | --- | --- |
| Event ID | Project-defined identifier | Identifies the business cash movement; repeated deliveries keep the same ID. |
| `CT_CA_ID` | `NUM(11)` | Account ID |
| `CT_DTS` | `DATETIME` | Business event timestamp (UTC) |
| `CT_AMT` | `SNUM(10,2)` | Signed cash amount (+ adds cash, - removes cash) |
| `CT_NAME` | `CHAR(100)` | Transaction description |
| Delivery time | Kafka timestamp | System arrival timestamp (UTC); used to measure lateness |

| Stream Output | Zone | Grain | Contents |
| --- | --- | --- | --- |
| Received events | Bronze | One delivery | Every Kafka message as delivered, including duplicates |
| Cash events | Silver | One cash movement | Validated cash movements with duplicate deliveries removed |
| `FactCashBalances` | Gold | One account per day | End-of-day balance: previous balance plus the UTC day's net movement |
| Running balance | Gold | One account | Latest account cash balance after each accepted event |
| Window aggregates | Gold | One account per window | Windowed inflow, outflow, net movement, and transaction count |

### Batch-to-Stream Handoff and Verification Rules
1. **Bootstrap from Batch 1**: Flink initializes account cash state from the completed Batch 1 Gold `FactCashBalances` rows keyed by natural `AccountID` (since `SK_AccountID` changes across SCD2 versions) and never reprocesses historical transactions.
2. **Single writer and idempotency**: Deterministic Event IDs let Bronze record every delivery while Silver and Flink deduplicate repeated deliveries. Each Gold cash table has a single writer during normal operation.
3. **Late-event handling**: An event's business date is the UTC date of `CT_DTS`. A late event updates its day's balance and every subsequent day's balance; out-of-order arrival must never overwrite newer account state with older state.
4. **Independent verification**: Partition Kafka by account to preserve per-account ordering. Run a fault-free baseline replay followed by a seeded fault replay (measuring actual delivery delays and duplicate rates at the producer), and verify Flink outputs against an independent batch calculation of deduplicated sums and daily balances. Because signed cash totals can decrease, final agreement is checked by state equality rather than maximum emitted value.

---

## 5. Orchestration, Quality, and Governance

Airflow schedules three batch pipelines using shared Airflow Connections and Variables. Each pipeline pairs an ingestion or transformation stage with a Soda data-quality validation stage, and publishes dataset lineage and validation assertions to DataHub.

| Pipeline | Scope | Validation & Lineage |
| --- | --- | --- |
| **DP1** | Ingests raw source files from `staging` into `bronze/` tables. | Soda checks file completeness, row counts, and parse-failure rates; DataHub records `staging` -> `bronze` lineage and assertions. |
| **DP2** | Transforms `bronze/` into `silver/` and builds the `gold` warehouse tables. | Soda checks deduplication, primary keys, referential integrity, and SCD2 date ranges; DataHub records `bronze` -> `silver` -> `gold` lineage and assertions. |
| **DP3** | Computes offline feature tables (`feat_*`) from Gold history. | Soda checks feature null rates and `event_timestamp <= created` point-in-time invariants; DataHub records `gold` -> `feat_*` lineage and assertions. |

---

## 6. Layer 1 Design Decisions and Rationale

### 6.1 Source Data and Delivery

The source contract relies only on files that external providers actually send and gives each timestamp a single meaning:
- **Provider realism**: Rely on data files and their paths rather than benchmark-only control files or manifests.
- **One meaning per date**: Separate business date (`data_date`), event timestamp (`CT_DTS`, `T_DTS`), publication timestamp (`PTS`), and arrival timestamp.
- **Stability**: Delayed delivery or retries must not shift the logical batch date to which a record belongs.
- **Simplicity**: Simulate a source behavior only when it affects downstream processing or satisfies a coursework requirement.

| Decision | Selected Choice | Main Reason |
| --- | --- | --- |
| **Batch date source** | No `BatchDate.txt`; encode `data_date` in `incremental/YYYYMMDD/<Source>_YYYYMMDD.<ext>` | `BatchDate.txt` is a benchmark artifact; real daily extracts carry their date in the folder and file name. |
| **Historical file names** | Standard TPC-DI names under `historical/` without a date suffix; pass `start_date` and `end_date` as run parameters | The historical load is an intentional one-time operation, and stating its range avoids guessing from record timestamps. |
| **Time zone and calendar** | UTC midnight-to-midnight, one batch per calendar date (including weekends) | Avoids 23-hour and 25-hour daylight-saving days and handles cash and customer activity that occurs outside exchange hours. |
| **FINWIRE cadence** | Quarterly `FINWIREyyyyQq` delivered in the daily folder of the release date (last calendar day of the month after the quarter), named by the posting quarter of `PTS` | Models periodic earnings releases with delayed availability while keeping the reporting period inside each `FIN` row (clauses 2.2.2.8 and 4.5.14). |
| **HR and reference data** | Loaded once in Batch 1 and kept static | Matches TPC-DI; ongoing reference updates would require effective dates and propagation rules that no downstream feature uses. |
| **Generation vs. delivery** | Generate locally first; deliver to `staging` and Kafka in a separate step | Allows changing delivery pacing and streaming faults without regenerating 100 GB of source files. |
| **Delivery completeness** | Pipeline-side list of expected files per batch; zero-record file means no activity | Needs no provider manifest, and S3 makes each object visible only after its upload completes. |
| **Generator summary** | One local summary file of settings, counts, and measured characteristics | Provides verification evidence without hashing 100 GB during ingestion or polluting `staging`. |

<details>
<summary>Alternatives Considered</summary>

| Decision | Alternative | Why Not Selected |
| --- | --- | --- |
| **Batch date source** | `Batch<N>/` folders with `BatchDate.txt` (TPC-DI default) | Folder name carries only a sequence number and requires an extra file to know the date. |
| **Daily date meaning** | Extraction date or arrival date | Transport delays and retries shift the batch date and require a second rule to find the business day. |
| **Time zone** | `America/New_York` | Daylight-saving transitions create 23-hour and 25-hour days. |
| **FINWIRE cadence** | Historical only (clause 1.3.5) or per-company filing calendars | Historical-only leaves companies and earnings static for a year; per-company calendars add scheduling rules unused by the cash forecast. |
| **FINWIRE placement** | Separate `quarterly/` prefix | Adds a second location to watch and an ordering dependency between two ingestion pipelines. |
| **Daily folder format** | Hive partition syntax (`data_date=2026-04-30/`) | DP1 reads `staging` by explicit path and registers no external catalog table over raw text and XML files. |
| **Delivery completeness** | Provider control file or fixed wait timeout | Depends on metadata a provider may not send, or guesses completion from wall-clock time. |
| **Historical range** | Inferred from max record timestamp | A quiet final day shifts the `Prospect.csv` snapshot date and the first daily batch date. |

</details>

### 6.2 Object Storage Layout and Dataset Scope

Bucket boundaries separate data whose writer, rebuild cost, or lifetime differs; prefixes organize data whose requirements match:
- **Rebuild cost**: Raw provider deliveries cannot be recreated from downstream tables and need the strongest boundary.
- **Writer and lifetime**: Tables that share writers and retention belong in one bucket; operational logs and checkpoints have a separate lifecycle.
- **Boundary cost**: In SeaweedFS, each bucket is a separate collection with its own volumes ([IaC Section 8.3](IaC.md#83-the-object-store-runs-out-of-volumes)).

| Bucket | Contents | Writer | Rebuild After Loss | Reason for Boundary |
| --- | --- | --- | --- | --- |
| `staging` | Delivered raw batch files (`historical/`, `incremental/`) | Delivery step (external providers) | Not possible from downstream zones | Protects raw inputs from table-overwrite jobs; separate writer |
| `lakehouse` | Bronze and Silver tables under `bronze/` and `silver/` | Spark and Flink | Rebuilt by replaying `staging` | Shared writers, owner, and lifecycle; separated by prefix |
| `platform` | Spark event logs, Flink checkpoints, MLflow artifacts | Platform services | Operational state only | Retention and cleanup differ from lakehouse tables |

- **Single active dataset**: The platform holds one generated dataset (one timeline) at a time. Baseline and treated runs compare execution over the same timeline, so storage keys and table names need no dataset prefix. Switching datasets clears all zones and Kafka and Flink state first.

<details>
<summary>Alternatives Considered</summary>

| Alternative | Why Not Selected |
| --- | --- |
| One bucket with prefixes only | A table-overwrite bug on the wrong prefix could delete raw files in `staging`. |
| Separate `bronze` and `silver` buckets | Both zones share writers (Spark and Flink), owner, and lifetime, and each extra bucket consumes a SeaweedFS collection. |
| Combining `lakehouse` and operational logs in one bucket | Operational logs and checkpoints have different cleanup policies from analytical tables. |
| Dataset prefixes across all buckets and tables | Every pipeline, table name, and Kafka topic would carry a dataset identifier for cross-dataset coexistence that the system does not use. |

</details>

### 6.3 Data Versioning Baseline

The versioning design separates **valid time** (when a fact was true in the business, tracked by SCD2) from **transaction time** (when the platform recorded a row, tracked by provenance and table snapshots), starting from mechanisms the architecture already provides:

| Concern | Baseline Mechanism | Revisit Trigger |
| --- | --- | --- |
| **Raw recovery** | `staging` retains every delivered file unchanged; replay rebuilds Bronze, Silver, and Gold. | Provider file corrections would add S3 object versioning on `staging`. |
| **Provenance** | Bronze records the source delivery and timestamp on every row. | None |
| **Business history (valid time)** | Gold SCD2 dimensions and effective-date surrogate key joins on facts. | None |
| **Rejected records** | Bronze retains unparseable rows; business rule alerts go to `DImessages`. | Invalid streaming payloads would add a Flink side-output dead-letter topic. |
| **Table snapshots (transaction time)** | Replay from `staging` plus transactional table metadata (Delta Lake or Iceberg). | None |

### 6.4 Streaming Source Selection

`CashTransaction` was selected as the streaming source after comparing all six dynamic TPC-DI sources against seven streaming criteria:
- **Action semantics**: Every post-Batch-1 `CashTransaction` record is an insert-only cash movement (`CT_CA_ID`, `CT_DTS`, signed `CT_AMT`, `CT_NAME`) that directly changes an account balance.
- **Timestamp resolution**: Each record carries a second-resolution `CT_DTS` event timestamp.
- **Defined baseline**: TPC-DI defines how daily cash movements roll into `FactCashBalances`, providing an exact batch reference to verify Flink's streaming state.
- **Direct ML connection**: Windowed cash inflows, outflows, and running balances feed the Layer 2 account cash-flow forecast and minimum-cash alert policy.

<details>
<summary>Alternative Streaming Sources Considered</summary>

| Candidate | Source | Why Not Selected as Primary Stream |
| --- | --- | --- |
| **Trade activity** | `Trade.txt`, `TradeHistory.txt` | Requires reconstructing multi-step trade lifecycles (`SBMT` -> `PNDG` -> `CMPT`/`CNCL`) and masking execution amounts on non-completed rows. |
| **Watch activity** | `WatchHistory.txt` | Tracks `ACTV` and `CNCL` intervals per customer and security, but carries no monetary measure or link to account cash balances. |
| **Holding changes** | `HoldingHistory.txt` | Lacks a native timestamp, account ID, or security symbol without joining `DimTrade`, and `HH_AFTER_QTY` is a post-trade balance rather than an additive delta. |
| **Daily market** | `DailyMarket.txt` | Has only daily resolution (one row per security per day); streaming daily closes through Kafka does not create intraday market ticks. |
| **Customer and account CDC** | `Customer.txt`, `Account.txt` | Low-frequency profile and status updates better suited to daily batch SCD2 maintenance. |

</details>
