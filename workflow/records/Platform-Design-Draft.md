# Platform Design Draft

This working record holds the Layer 1 data platform design and the Layer 2 problem framing agreed in the design sessions of 2026-11-02 to 2026-11-04, with their reasons and rejected alternatives, so that nothing is lost before the spec pass moves them into `docs/`. The generation model is in the [Data Generation Draft](Data-Generation-Draft.md); its four layers are called "generation layers" here, to keep them apart from the platform's Layers 1 to 3. [Status](../Status.md) lists every decision in short form and holds the open questions. DP1 to DP3 are the three Layer 1 data pipelines (section 4.1). SCD2 is a type 2 slowly changing dimension, which keeps every version of a record.

Each section marks its content as **Decided** (accepted by the owner), **Proposed** (direction agreed, details open), or **Parked** (kept for later).

## 1. Target Workload and Capacity

### 1.1 Workload

**Decided.** The scale factor sets the firm's size; one unit is about 1 GB of offline files over the reference calendar ([Data Generation Draft](Data-Generation-Draft.md#scale-factor)). The factor 100 dataset is generated once: a 5-year historical load of about 29 GB and about 71 GB of offline files for the 7-year incremental period, so the offline files reach rubric row `r5`'s 100 GB minimum. Cash in the incremental period travels through Kafka, an estimated 18 GB more, counted separately. The design handles larger scales by principle (below). Each environment, such as this machine or a later cloud deployment, runs the largest factor it handles well, and each demo or experiment processes only as much as shows its effect, usually the historical load plus some daily batches.

**Proposed (the scaling path is open in Status).** The design target is the mid-size firm at factor 100. Larger firms are projections from the same model:

| Firm size | Scale factor | Accounts at cutover | Trades per trading day | Offline files per year at that size | Use in this project |
|---|---|---|---|---|---|
| Medium (design target) | 100 | About 550,000 | About 77,000 | About 8 GB | Generated in full; processed in parts on this machine |
| Large (5x) | 500 | About 2.75 million | About 385,000 | About 40 GB | Projection only |
| Very large (50x) | 5,000 | About 27.5 million | About 3.9 million | About 400 GB | Projection only. Schwab averaged about 8.5 million trades per day in November 2025. |

| Component | Medium | Large | Very large |
|---|---|---|---|
| SeaweedFS and Delta | One node | One node | Add volume servers |
| Spark | Small daily batches | More cores for the historical load; same jobs | A multi-node cluster; same jobs |
| Kafka and Flink | Normal-day peak about 6 events per second; about 18 on market-shock days, plus bursts after outages | About 30 per second on a normal day | About 300 per second on a normal day; add partitions and parallelism |
| Trino | One node | One node | Add workers |
| PostgreSQL (Gold) | About 330 million rows at cutover; about 1.1 billion with all seven years of the incremental period | About 1.65 billion rows at cutover; partition facts by date | Fact history moves to Delta Gold tables read through Trino, or to a distributed warehouse; dimensions and live tables stay in PostgreSQL |

- **First limit:** PostgreSQL is the first component to reach its limit, because it grows only with a bigger machine.
- **Streaming:** volume never limits the cash stream. The stream exists because cash arrives as events in the incremental period, and because the live view and rescoring need fresh data.
- **Spark:** the medium firm alone fits a single-node engine. Spark is justified by growth, because the same jobs keep working at 5 or 50 times the size.
- **Method:** sizes come from business drivers (accounts, trades per account) times bytes per row measured in the TPC-DI scale factor 3 sample (SF3). A later version should state low, expected, and high values and use throughput measured in our own pipelines.

### 1.2 Development Machine

**Decided.** The platform runs on Ubuntu 24.04 in WSL 2 (the Windows Subsystem for Linux) on Windows 11, with an Intel Core i7-12700H. `C:\Users\XWIND-PC\.wslconfig`, written on 2026-11-02 and active (confirmed on 2026-11-04), caps WSL memory at 24 GiB with 8 GiB of swap. It sets no processor cap, so all 20 threads stay available. There is no NVIDIA GPU.

| Group that runs together | Sum of current Compose memory limits |
|---|---|
| Batch: core services, Spark master, worker, and Thrift, Airflow | About 16.75 GiB |
| Streaming: core services, Kafka, Flink | About 11.5 GiB |
| Governance: core services, Kafka, DataHub with OpenSearch, Airflow | About 14.75 GiB |
| A pipeline run with live lineage: core services, Spark (no Thrift), Airflow, Kafka, DataHub | About 20.75 GiB, or about 18.25 GiB without Trino |
| All long-running Layer 1 services | About 26 GiB, excluding the conflicting Airflow worker |

- Limits are ceilings, not measured use. The planned Compose changes remove Spark Thrift (2 GiB) and the Airflow worker.
- Layer 1 services run in groups, because all of them together exceed the WSL cap.
- With the current Spark defaults (1 GiB executor heap, 2 cores, `spark.memory.fraction 0.5`), each task gets about 180 MiB of execution memory. Large experiment runs need a larger worker, about 10 to 12 cores and 8 to 10 GiB.
- Disk: the peak is about 400 to 500 GB with every copy present (local generated files, `upstream`, `staging`, the raw and clean Kafka topics, Bronze and Silver, Gold, Spark spill). About 831 GB is free on the drive that holds the WSL disk. Deleting the local generated files after they are fed removes one full copy.
- The machine cannot run llm-d's reference setups, which need NVIDIA GPUs or a CPU with AVX-512 or AMX. Hosting for Layers 2 and 3 is open in Status.

## 2. Source Boundary and Storage

**Decided.** The source systems belong to another department, as in rubric row `r8`. The platform pulls batch files from that department's storage and consumes its event stream. The publisher (`data/data_source/publisher/`) plays the department's side.

```text
Source department                         |  Our platform
                                          |
generator -> publisher -> upstream bucket  --DP1 pulls-->    staging -> Bronze -> Silver -> Gold
                     \-> raw cash topic    --Flink reads-->  Job 1 -> clean topic -> Job 2
```

| Bucket | Contents | Written by | Read by |
|---|---|---|---|
| `upstream` | Files as the source systems publish them (`historical/`, `incremental/YYYYMMDD/`) | The publisher only | DP1 |
| `staging` | The platform's immutable copy of every file version it pulls; the path layout is open in Status | DP1 | DP1, replays |
| `lakehouse` | Bronze and Silver Delta tables under `bronze/` and `silver/` | Spark and Flink | Spark, Flink, Trino |
| `platform` | Spark event logs, Flink checkpoints, MLflow artifacts | Platform services | Platform services |

| Channel | Owned by the source side | What the platform does | Enforcement |
|---|---|---|---|
| Batch | `upstream` bucket | DP1's ingest stage pulls new and changed files into `staging`, then parses them into Bronze | SeaweedFS identities: the publisher writes `upstream`; the platform can only read it |
| Stream | `tpcdi.cash_transaction.raw` topic | Flink Job 1 consumes it; the platform owns the clean, features, and rescore topics | A convention until Kafka authentication and topic permissions come with the security baseline, because they change every Kafka client, including DataHub |

The platform holds one active dataset at a time. Switching datasets or publisher profiles clears the buckets, the Kafka topics, and the Flink state.

| Option | Why not selected |
|---|---|
| The publisher writes straight into `staging` | Works, but the department boundary is only implied, and the source side could overwrite the platform's raw copy |
| A simulated trading database that DP1 extracts with change data capture (CDC) through Debezium or Airbyte | The most realistic choice for internal systems, but it replaces TPC-DI's change files and adds large work to the generator and DP1. It stays a Layer 1 novel-idea candidate. |

## 3. Layer 1 Tools

**Decided.**

| Function | Tool | Role |
|---|---|---|
| Object storage | SeaweedFS (S3 API) | The four buckets |
| Table catalog | Hive Metastore | Table names, schemas, and locations for Spark and Trino |
| Lakehouse table format | Delta Lake | Bronze and Silver tables |
| Batch processing | Spark | DP1, Silver, the push into PostgreSQL, the offline data problems |
| Warehouse database | PostgreSQL | The load, `gold`, and `ops` schemas |
| Gold SQL models | dbt-postgres | Gold warehouse tables, built from a PostgreSQL load schema |
| Lakehouse query engine | Trino | SQL over Delta tables; queries that join lake data with PostgreSQL |
| Stream transport | Kafka, with Kafbat UI | Cash topics; topics, messages, and consumer lag in a web UI |
| Stream processing | Flink 2.2, Python DataStream API | Job 1 and Job 2 |
| Orchestration | Airflow 3 with `LocalExecutor` | DP1 to DP3, the three Layer 1 data pipelines, and later pipelines |
| Data quality | GX Core (Great Expectations) | Validate stages; results go to DataHub through DataHub's GX plugin |
| Metadata and lineage | DataHub | Catalog, lineage (Airflow plugin, Spark agent, dbt ingestion, and Flink jobs declared at deploy time), quality results |

- **Work split by type:** Trino serves large analytical reads over the lake. PostgreSQL serves keys, transactions, and fast lookups. Gold exists once, in PostgreSQL.
- **Catalog roles:** DataHub is not a query-time catalog like Unity Catalog. Hive Metastore resolves table names for the engines, each system enforces its own access, and DataHub collects metadata, lineage, and quality results.
- **Versions:** start with Spark 3.5 and Delta 3.3, plus Flink 2.2 with the Delta 4.4 Flink connector. An end-to-end smoke test confirms them: Flink writes a Delta table on SeaweedFS, then Spark and Trino read it. Spark 4 with Delta 4 is the fallback.

| Decision | Choice | Main reason |
|---|---|---|
| Table format | Delta Lake | Exposure to the Databricks and Fabric model; `MERGE`, compaction, and Z-order in Spark; a native Trino connector |
| Gold location | PostgreSQL | Real keys, foreign keys, atomic SCD2 loads, and indexes (rubric rows `r21`, `r37`); Gold SQL in dbt is easy to review |
| Gold build | Spark pushes the Silver rows Gold needs for a batch into a load schema; dbt-postgres builds Gold | dbt runs SQL on one connection and does not move data between systems, so the rows must reach PostgreSQL first |
| SCD2 history | Our own SQL over effective dates, not dbt snapshots | The historical load already holds years of history; snapshots record changes only at each run |
| Orchestration | Airflow 3 | Airflow 2 reached end of life on 2026-04-22, nothing is built yet, and DataHub's Airflow plugin supports Airflow 3 |
| Data quality | GX Core | An established Python framework with a DataHub plugin that sends results as assertions; open-source Soda would need our own code to send results to DataHub |
| Stream engine | Flink 2.2 | The newest version that the Kafka 5.0, JDBC (Java database connectivity) 4.1, and Delta 4.4 connectors all support; Flink writes the Delta tables itself |
| Kafka UI | Kafbat UI | Apache 2.0 license; one small container |

| Alternative | Why not selected |
|---|---|
| Iceberg | More common as an open standard and has a mature Flink sink, but Delta matches the owner's learning goal and covers every requirement |
| Plain Parquet | No row-level updates; SCD2 and deduplication would rewrite whole tables, which caused a v0 bug that deleted earlier batches |
| Gold in Delta with PostgreSQL only for serving | Close to Fabric, but the Gold logic would run on Spark, and indexing and foreign keys would apply only to the serving copy |
| dbt-spark | Needs the Spark Thrift server; dbt is wanted for Gold SQL, not for Spark jobs |
| dbt-trino writing into PostgreSQL | Maintained by Starburst; Trino's `MERGE` into PostgreSQL is not transactional, and its `UPDATE` allows only constant values |
| Spark builds Gold directly | Gold logic in PySpark is harder to review than SQL models |
| pg_duckdb reading Silver in place | Untested with SeaweedFS and our Delta features; changes the image of the shared PostgreSQL instance. Set aside, and a novel-idea candidate. |
| Flink 1.20 with Spark copy jobs for Bronze and Silver | Splits stream landing across two engines; dropped in favor of Flink writing Delta directly |
| Soda Core | Compact YAML checks, but no built-in path to DataHub in the open-source version |
| Redpanda Console | Polished, but under the Business Source License |

## 4. Batch Processing

### 4.1 Pipelines

**Decided.**

| Pipeline | Ingest stage | Validate stage (starting list) |
|---|---|---|
| DP1 | Pull new and changed files from `upstream` into `staging`; parse each into its Bronze table | File completeness, row counts, parse failures |
| DP2 | Bronze to Silver (Delta); push the Silver rows Gold needs into the PostgreSQL load schema; dbt-postgres builds Gold | Duplicates, keys, relationships, SCD2 date ranges, load reconciliation |
| DP3 | Feature tables (`feat_*`) from Gold | Null rates, `event_timestamp <= created` |

The full GX check catalog is open in Status.

### 4.2 Bronze Tables

**Decided.** Bronze has one table per source. Every field is stored as text, exactly as delivered, plus metadata columns (the initial set is proposed in section 4.7). Silver converts types. Bronze can be built now and changed cheaply, because it can always be rebuilt from `staging`, or from the raw topic for stream deliveries. Cash has two Bronze tables: one for the historical file, and one for stream deliveries that also stores Kafka partition, offset, and delivery time.

### 4.3 Dependencies and Late Files

**Decided.** A table builds when its own file and every table it depends on are complete for that day. A late file holds back only the tables that depend on it. Held tables run when the file arrives, so they need no rerun.

| Source | Depends on | Others that depend on it |
|---|---|---|
| `Customer.txt` | Nothing that day | Accounts, watches, cash facts |
| `Account.txt` | Customers | Trades, holdings, cash facts |
| `Trade.txt` | Accounts, securities | Holdings, cash facts, settlement checks |
| `HoldingHistory.txt` | Trades | Nothing |
| `WatchHistory.txt` | Customers, securities | Nothing |
| `DailyMarket.txt` | Securities, financials | Nothing |
| `Prospect.csv` (monthly) | Customers, for matching | Customer marketing fields, which keep last month's values until it arrives |
| FINWIRE (quarterly) | Nothing | Securities, financials, market facts, and through securities also trades, watches, and cash facts |

This gives two chains: customers, accounts, trades, then holdings and cash; and FINWIRE, securities and financials, then daily prices. They meet at trades and watches, which need securities, so a late FINWIRE file on its release day also holds back trades, watches, and cash facts.

| `ops.load_status` column | Meaning |
|---|---|
| `data_date`, `source` | Which file for which day |
| `expected`, `arrived_at`, `etag` | Whether and when it arrived, and which version: the ETag that the object store records on upload |
| `bronze_at`, `silver_at`, `gold_at`, `run_id` | How far it has been processed, and by which run |
| `state` | Waiting, late, loaded, or held, with the reason |

A completeness view lists each date as complete or partial, with the missing sources. Dashboards show the latest complete date by default and label partial dates. A failed freshness check raises a DataHub incident; routing it to the source's owner is part of the proposed ownership model in section 7.

| Alternative | Why not selected |
|---|---|
| Wait for the whole day before loading anything | Correct, but one late file delays every table |
| Load what arrived and rerun later | Users see incomplete numbers in tables that depend on the missing file, and every late file causes a rerun |

### 4.4 Data Errors and Quarantine

**Decided.** The data errors are TPC-DI's own. The handling rules:

- Bronze always keeps the raw value.
- DP2 never drops a record that other records reference.
- DP2 repairs a value only when the correct value is certain. Otherwise, DP2 keeps the record, sets the bad field to NULL or keeps it as delivered (per error, in the table below), and adds quality metadata: a flag, a rule code, or both.
- A record that cannot be used at all goes to a quarantine table with the rule and the reason.
- No step ever invents a value.

| Error | Where | Handling |
|---|---|---|
| Invalid customer tier | Customer | Keep the record; Silver sets the tier to NULL and adds quality metadata. The outreach policy uses a default threshold for an unknown tier (tier thresholds are open). |
| Date of birth out of range | Customer | Keep the record, set the date to NULL, add quality metadata |
| Invalid credit rating | Company | Keep the record, set the rating to NULL, add quality metadata |
| Fee or commission larger than the trade value | Trade | Keep the record and the recorded value, add quality metadata; cash settles at the correct value |
| Row that cannot be parsed, or a wrong field count | Any file | Quarantine the row |
| Stream event that cannot be parsed | Cash stream | Flink Job 1 writes it to a quarantine table |

Quarantine tables live in the Bronze zone and are cataloged in DataHub, and GX measures them. TPC-DI's `DImessages` is replaced by GX results in DataHub, quarantine tables, quality metadata columns, and `ops.load_status`.

### 4.5 Schema Evolution

**Decided.** From 2019-07-01 (the date is a setting), `Trade.txt` rows carry a 15th field, `T_CHANNEL`, added at the end. This is our addition; TPC-DI keeps each file's format fixed across batches.

1. **Contract:** the source definitions record version 1 (14 fields, until 2019-06-30) and version 2 (15 fields, from 2019-07-01).
2. **Read (DP1):** read lines as text, split on `|`, and check the field count against the version expected for the `data_date`. Rows that match neither version go to quarantine.
3. **Bronze:** store each version as delivered. The first version 2 write adds `t_channel` through Delta schema merge, so the table history shows the version where the column appeared, which is the "schema before and after" evidence.
4. **Silver:** map both versions to one schema. NULL in `channel` means "not recorded before the upgrade", not "no channel".
5. **Gold:** `dim_trade` gains `channel`. Adding a nullable column in PostgreSQL does not rewrite the table.
6. **Features:** channel features exist only from 2019-07-01, so the model must handle missing values before that date.

The baseline reads `Trade.txt` with the fixed 14-field schema. Spark's default `PERMISSIVE` mode drops extra fields, so the job succeeds and loses the channel without an error. The evidence is the count of non-null channel values, the Delta schema history, and which files carry each version.

### 4.6 Offline Duplicates

**Decided.** Retried extracts are rare (generation layer 4 in the [Data Generation Draft](Data-Generation-Draft.md#layer-4-delivery)). A low rate still gives a full experiment. The cost of finding duplicates depends on how much data Spark compares, not on how many duplicates exist. The rate decides how wrong the results are without deduplication.

| Data | Duplicate key |
|---|---|
| Cash | `CT_ID` |
| Other daily files | The full row, including `CDC_DSN`, so a legitimate second update of the same trade is kept |
| Historical files | The full row |

### 4.7 Gold Model Changes

**Decided.**

- Gold is the `gold` schema in PostgreSQL. It holds three kinds of tables: the warehouse tables that dbt builds, the live tables that Flink Job 2 writes, and the `feat_*` tables that DP3 builds. The `ops` schema holds `ops.load_status` and `ops.stream_progress`.
- The warehouse model has 19 tables: TPC-DI's 19 minus `DImessages`, plus `fact_cash_transactions`.
- `fact_cash_transactions` has one row per cash movement, keyed by `CT_ID`, with keys to the account, customer, date, and time dimensions, to `dim_trade` for trade settlements, and to `dim_security` for dividends. It is partitioned by date.
- Once a day, Spark pushes the day's Silver cash rows into the load schema, and dbt builds `fact_cash_transactions` and then `fact_cash_balances` from it, with one model for historical and streamed dates.
- `fact_market_history` keeps TPC-DI's price-to-earnings ratio (P/E), 52-week high and low, and dividend yield.

**Proposed (open in Status).** Metadata columns across zones:

| Column | Bronze | Silver | Gold | Meaning |
|---|---|---|---|---|
| `_source_file`, `_record_num` | Yes | Yes | No | Where the record came from |
| `_data_date` | Yes | Yes | Yes | Business date of the delivery |
| `_ingested_at` | Yes | Yes | No | When the platform loaded it |
| `_run_id` | Yes | Yes | Yes | The run that wrote the row |
| `_schema_version` | Yes | Yes | No | Source schema version, such as `trade_v1` |
| CDC flag and sequence number | Yes | Yes | No | Change type and order |
| `_quality_flags` | No | Yes | Yes | Rule codes the row matched, such as `INVALID_TIER` |
| `valid_from_ts`, `valid_to_ts`, `is_current` | No | No | SCD2 dimensions | Version validity (rubric row `r35`) |

Each rule code points to one entry in a rule catalog with its description, owner, severity, and whether it is business or technical.

## 5. Stream Processing

### 5.1 Topics and Messages

**Decided.**

| Topic | Contents | Kept for | Written by | Read by |
|---|---|---|---|---|
| `tpcdi.cash_transaction.raw` | Every delivery, duplicates included | The whole dataset | Publisher | Job 1 |
| `tpcdi.cash_transaction.clean` | Each event once, with `cash_type` and `trade_id` | The whole dataset | Job 1 | Job 2 and later consumers |
| `tpcdi.account_cash.features` | Cash feature rows | 7 days | Job 2 | A small Feast push job |
| `tpcdi.account.rescore` | Requests to score one account now | 7 days | Job 2 | The scoring worker |

- Every topic is keyed by account ID, which keeps each account's events in order, and has 4 partitions.
- Messages are JSON, with a JSON Schema file in the repository as the contract.
- Amounts are decimal text, because JSON numbers are usually read as floating point.
- The Kafka timestamp is the delivery time.

```json
{"ct_id": 81234567, "ct_ca_id": 1001, "ct_dts": "2019-03-05T14:30:00Z", "ct_amt": "-400.00", "ct_name": "Withdrawal", "schema_version": 1}
```

### 5.2 Job 1: Raw Topic to Bronze and Silver

**Decided.** For each event:

1. Append it to the Bronze stream table with its Kafka partition, offset, and delivery time.
2. Check the fields and parse `CT_NAME`. An event that cannot be parsed goes to the quarantine table.
3. If its `CT_ID` was seen for the same account in the last 24 hours, drop it and count it as a duplicate.
4. Otherwise, append it to Silver and to the clean topic. Both writes commit at checkpoints, so a restart writes nothing twice; the clean topic uses Kafka transactions.
5. At each checkpoint, write a progress marker to `ops.stream_progress`: every event up to time T is processed. DP2 waits until the marker passes the end of the day plus 10 minutes before it builds that day's cash facts.

### 5.3 Job 2: Live Tables, Features, and Triggers

**Decided.**

| Part | How it works | Output |
|---|---|---|
| Live balance | Kept per account and updated on every event. On its first start, the job loads each account's closing balance of the historical period from Gold. | Live tables: current balance and today's movements |
| Hourly window | 1-hour event-time windows per account: inflow, outflow, net, and count per cash type | A live hourly table; the window code for rubric row `r19` |
| Daily window | 1-day windows per account. The job keeps the last 30 daily totals and computes 7- and 30-day sums and the days since the last deposit. | One row per account per day on the features topic |
| Rescoring trigger | Each withdrawal is checked against the rule: at least half of the cash before it, and at least USD 1,000. This is a default, to be aligned with the tier thresholds. | A request on the rescore topic |

- Watermark: 30 seconds behind the newest event time seen.
- Allowed lateness: 10 minutes. A late event within it updates its window.
- Events later than that go to a late-events table.
- Upserts to PostgreSQL always write full totals, never increments, so repeated writes after a restart stay correct.

### 5.4 The Cash Path

**Decided.** In the incremental period, cash events arrive only through Kafka; daily batches carry no cash file.

| Stage | Shape | Written by | When |
|---|---|---|---|
| Historical file in `upstream`, then `staging` | TPC-DI pipe format with `CT_ID` as the first field, 2014 to 2018 | Publisher, then DP1 | Once |
| Raw topic | One message per delivery | Publisher | Continuously from 2019-01-01 |
| Bronze | Two tables: historical file rows and stream deliveries | DP1 and Job 1 | Once, then continuously |
| Silver | One cash table for all dates, keyed by `CT_ID`, with `cash_type` and `trade_id` | DP2 for history, before the stream starts; Job 1 afterward | Once, then at each checkpoint |
| Gold warehouse tables | `fact_cash_transactions` and `fact_cash_balances` | dbt in DP2 | Daily, for closed days |
| Gold live tables | Current balance, today's movements, hourly windows | Job 2 | Within about a minute |
| Feast | Cash window features | Feast push job, from the features topic | Continuously |

One day, for example 2019-03-05:

1. During the day, cash events stream in. Live tables update within about a minute, and Silver receives the events at each checkpoint.
2. After 24:00 UTC, the progress marker passes the end of the day plus 10 minutes.
3. At 02:00 on March 6, the daily files for March 5 arrive in `upstream`.
4. DP1 and DP2 run. DP2's cash step waits for the marker, pushes March 5's Silver cash into PostgreSQL, and dbt builds that day's cash facts.
5. Reconciliation checks that settlements match completed trades, that each dividend equals one quarter of the annual dividend per share times the shares held, and that Flink's live end-of-day balance matches the warehouse fact.
6. DP3 and daily scoring run.
7. A March 5 event that arrives after step 4 still reaches Silver. DP2 then reruns cash facts from March 5 forward; the detection rule is open in Status (area 7).

| Consumer | Reads | When |
|---|---|---|
| Business intelligence (BI) and analysts | `fact_cash_balances`, `fact_cash_transactions` | After each daily run |
| Web API and the Layer 3 agent | Live tables | Any time |
| Daily scoring | Feast online store | After DP3 |
| Triggered rescoring | The rescore topic | When a large withdrawal arrives |
| Model training | Feast offline store | When training runs |
| GX reconciliation | Silver cash, trades, holdings, live tables, warehouse facts | After each daily run |

### 5.5 Operations

**Decided.** 30-second checkpoints; parallelism 2 per job, which fills the TaskManager's 4 slots; restarts from the last checkpoint; lineage declared in DataHub when a job is deployed. Monitoring uses the Flink UI, Kafbat UI, and counts of Kafka lag, duplicates dropped, late events, and quarantined events.

**Proposed.** Every service exposes metrics in Prometheus format (section 9).

**Parked.** Maintenance jobs wait until the base system works. Sketch: a separate Airflow pipeline (DAG) compacts the Flink-written Delta tables while Job 1 is paused at a savepoint, because Spark and Flink must not write one Delta table at the same time on SeaweedFS. An Airflow pool lets one Spark job run at a time across all DAGs.

### 5.6 Reasons and Alternatives

**Decided.**

| Decision | Choice | Main reason |
|---|---|---|
| Job layout | Two jobs chained through the clean topic | Each job owns one zone step. A crash in Job 2 does not stop landing, and a replay of the clean topic rebuilds the live tables without touching Bronze and Silver. Later consumers read clean events instead of repeating deduplication. |
| Python API | DataStream | Flink SQL drops events after the watermark and has no supported late-data output |
| Topics for features and rescoring | Flink writes requests to topics | A slow Feast or scoring worker cannot stall the stream |
| 30-day sums | The last 30 daily totals per account | A sliding 30-day window that moves every hour keeps 720 overlapping copies of each event |
| Warehouse cash facts | Built daily by dbt from Silver | One logic for all dates; surrogate keys resolve after the day's batch, even for accounts opened that day; one writer per table; Flink's live balance becomes an independent check |
| Cash identity | `CT_ID` in the file and in the stream | Real systems give every transaction an identifier; one deduplication rule covers both channels |
| Message format | JSON with a schema file | Readable in any tool and needs no extra service; a schema registry with Avro is optional later |

| Alternative | Why not selected |
|---|---|
| One Flink job that writes all zones | Fewer parts, but one failure stops landing, and rebuilding the Gold outputs replays and re-appends Bronze and Silver |
| Delta upserts from Flink | The connector supports them, but Bronze and Silver store events, which never change, and it adds experimental features to our risk |
| Flink writes daily movement, and a PostgreSQL view computes balances | Replaced by the dbt model above, which resolves surrogate keys and keeps one writer per table |

## 6. Reruns, Corrections, and Versioning

### 6.1 Why Processing Runs Again

**Decided.**

| Reason | Example | In scope |
|---|---|---|
| A run fails partway | DP2 crashes while building one day | Yes, the most common reason |
| Upstream data arrives corrected | The provider resends a day's customer file | Yes, about once a month in the realistic publisher profile |
| Upstream data arrives late | A file or cash event arrives after its day ran | Yes |
| Our own logic had a bug | The SCD2 model dated a version wrong | Yes, as a backfill |
| New logic needs history | A new feature for past years | Yes, as a backfill |
| The platform was down | Nothing ran for several days | Yes, as a catch-up |
| Someone edits data by hand | A business user fixes a record in an app | No |

### 6.2 Rules

**Decided.**

- Every write is safe to repeat.
- A rerun covers only the affected `data_date` or date range, then every later processed date in order. Changes flow downstream in two directions: through the zones, and forward in time, because SCD2 versions, fact keys, and balances build on earlier days.
- Gold is not rebuilt in full.

| Write method | How it works | Used for |
|---|---|---|
| Replace everything | Delete the table, then write it again | Reference tables |
| Replace a slice | Delete one day's rows, then write that day again | Silver, Gold facts, feature tables |
| Upsert | Update the row if its key exists, insert it if not | Flink live tables |
| Append only | Add rows and never change them | Bronze deliveries |
| Add a version | Close the current version and add a new one; a correction from day D rebuilds the versions from day D onward | SCD2 dimensions |

| Zone | On a correction from day D |
|---|---|
| `staging` | The corrected file is pulled as a new version; the old version stays |
| Bronze | The corrected delivery is appended next to the old one, identified by file path and the ETag that the object store records on upload |
| Silver | Day D is rebuilt from the newest delivery |
| Gold facts | Day D and every later day are rebuilt |
| Gold SCD2 dimensions | Versions from day D onward are rebuilt in date order |
| Feature tables | The affected days' rows are replaced, with a new `created` time |
| Flink live tables | Not corrected; they show the current state only, and a replay of the clean topic rebuilds them |
| Risk scores | Kept as written (score storage is proposed in section 8) |

Feature tables are inputs, not records. Reproducibility comes from versioning each training run's full dataset together with the model in MLflow (rubric row `r49`). By default, Feast's point-in-time join filters on `event_timestamp` only and returns the newest `created` value. Passing `filter_by_created_timestamp=True` returns only values created by the entity time, and not every offline store supports it.

| Question | Answered by |
|---|---|
| What did the provider send and change? | Every version in `staging` and Bronze |
| What did a table look like before? | Delta table history for Bronze and Silver; for Gold, a rebuild from Silver, or a backup copy (proposed) |
| Which run wrote this row? | `_run_id` (proposed) and the Airflow run log |
| Why was it rerun, and who was told? | A DataHub incident |
| What else did the change affect? | DataHub lineage |

**Proposed (open in Status).**

- `_data_date` and `_run_id` replace TPC-DI's `BatchID`.
- Affected Gold tables are copied to a backup schema before a large backfill.
- DP2 detects very late cash events for closed days.
- Airflow runs one DAG run per `data_date`, keeps dates in order, catches up after downtime, and runs backfills from its UI and CLI.

## 7. Data Quality and Governance

**Decided.** GX Core runs the validate stages, and DataHub shows lineage and quality results (sections 3 and 4).

**Proposed.** Ownership routing:

| | Business data owner | Platform engineer |
|---|---|---|
| Who | One owner per domain: customers and accounts, trading, prospects, market data | Whoever runs the platform |
| Problems | Invalid tiers, dates of birth, fees; a missing daily file from their system | Job failures, parse errors, unexpected schemas, Kafka lag, memory limits |
| Where they look | DataHub (domains, ownership, glossary terms, schema and field descriptions, assertions, incidents, lineage, data contracts), plus BI dashboards and data access | Airflow, Spark, Flink, and Kafbat UIs; Grafana |
| Effect on the pipeline | Usually flag and continue | Often stop before bad data spreads |

- Every GX check is tagged business or technical, and every dataset has a registered owner, so alerts reach the right person.
- Row-level detail, such as the customer IDs behind a failing check, comes from the quality flags and quarantine tables through Trino.
- Open-source DataHub sends notifications through DataHub Actions; owner subscriptions are a DataHub Cloud feature.

## 8. Layer 2 Problem: Account Attrition

### 8.1 Why the Problem Changed

**Decided.** The earlier minimum-cash forecast had no harm to prevent. A brokerage rejects a withdrawal larger than available cash, so the client sells first, and nothing bad happens that an alert could stop. The course rubric asks for an ML problem that uses both offline and streaming features; it does not ask for a streaming model.

| Criterion | Question |
|---|---|
| A real decision | Who acts on the prediction, and what do they do? |
| A cost of being late or wrong | What does the firm lose without it? |
| An observed label | Does the outcome appear in the source data? |
| Batch and stream inputs | Do both kinds of features contribute? |
| Rubric fit | Does it work with Feast, Kubeflow Pipelines, KServe, and the agents? |

| Candidate | Why not selected |
|---|---|
| Minimum-cash alert | No decision and no cost, as above |
| External net flow forecast | A better target for the same weak decision |
| Fraudulent withdrawal | The label would be invented; it fits stream scoring but not our sources |
| Margin-call warning | Covers only margin accounts and needs margin requirements based on holdings value |
| Next-week trading activity | Small business cost |

### 8.2 The Problem

**Decided.**

- **Prediction:** whether an account closes within a horizon.
- **Policy:** the account's broker gets an outreach task when the closure probability reaches the threshold for the customer's tier. Tier 1, the wealthiest, is flagged at a lower probability than tier 3. The policy is separate from the model, so changing a threshold needs no retraining.
- **Label:** account closures, which already appear in `Account.txt` and `dim_account`.
- **Signal:** the leaving period, look-alikes, and risk factors in generation layer 2 ([Data Generation Draft](Data-Generation-Draft.md#layer-2-attrition-signal)).
- **Withdrawals:** never larger than available cash; margin accounts may borrow within their limit.
- **Evaluation:** a simple rule against the model; optionally, the model with and without the cash features, which shows what the cash information is worth.

**The stream's role.** In the incremental period, cash arrives only through Kafka. The stream is therefore the delivery channel for cash data and the producer of the cash features. Daily scoring does not depend on its speed; the live view for the agent and the rescoring trigger do.

| Features | Producer | Path to Feast |
|---|---|---|
| Tenure, tier, trading activity, holdings and their value, watches canceled, recent account updates | DP3 from Gold | `feat_*` tables, materialized to the online store by Airflow (rubric row `r46`) |
| Withdrawals, deposits, and sale settlements over 1, 7, and 30 days; external net flow; days since the last deposit; current cash | Flink Job 2 | The features topic, then a Feast push job into the online and offline stores (rubric row `r47`) |

Each feature has one definition and one producer, which keeps training and serving values from drifting apart.

| Scoring path | Type | When | Scope |
|---|---|---|---|
| Daily run | Batch | Once a day, after DP3 | All open accounts |
| Triggered rescoring | Online, event-driven | When Job 2 sees a large withdrawal | One account, through the rescore topic and the scoring worker |
| Agent request | Online, on demand | When a broker asks the agent about a client | One account; rubric row `r50` requires an inference service that the agent can call |

Every path reads features defined once in Feast.

**Proposed (open in Status).**

- A 30-day horizon. About 0.25% of open accounts close in any 30 days, about 1,375 per month at 550,000 accounts.
- Weekly training snapshots, keeping all positives and about 2% of negatives: about 7 million rows instead of about 275 million.
- The daily run sends features to KServe in batched requests, so every prediction passes through one serving path.
- Every path uses the MLflow model tagged `production` (rubric row `r49` asks for `latest` or `production` tags).
- Keep the latest score per account and the full history only for flagged accounts. Daily scores for every account would add about 210 million rows in 2019, rising to about 330 million in 2025 (factor 100, based on the average number of accounts in each year).

## 9. Cross-Cutting Concerns

**Decided.** The "Map the Problem" table in [Design Method](../Design-Method.md#3-map-the-problem) is the checklist for every design. Each concern is placed in one of three levels:

| Level | Rule | Concerns placed so far |
|---|---|---|
| Must build | The rubric grades it | Secrets in Vault, gateway login and rate limits, HTTPS, observability dashboards, tests, CI/CD, Terraform and Ansible, A/B rollout |
| Build now because it is cheap | Small now, painful to add later | Safe reruns and versioning, marking columns that hold personal data, separate database users per service, parameterized SQL in the API and Model Context Protocol (MCP) tools, data retention rules |
| Optional | A novel idea or spare time | Brokers see only the accounts they manage and analysts see personal data masked (a Layer 1 novel-idea candidate), full audit logs |

**Proposed.** Observability direction: every service exposes metrics in Prometheus format, OpenTelemetry carries traces and logs, and Grafana is the one dashboard over metrics, logs, and traces (rubric rows `r73` to `r75` and `r82` to `r87`). Tool UIs stay for detailed debugging, and DataHub with GX covers data health.
