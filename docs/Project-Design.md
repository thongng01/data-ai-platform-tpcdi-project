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
| **Offline duplicates** | The same record appears more than once in the saved source data (for example, ~2%). | Counts and sums inflate; duplicate changes create extra SCD2 versions. | Duplicate definition, injected rate, and row and metric counts before and after deduplication. |
| **Late arrivals** | Streaming events arrive behind Flink's watermark after their window has closed or emitted. | Late events are dropped or require window and balance revision. | Measured event and delivery times, delay distribution, and Flink results with and without handling. |
| **Streaming duplicates** | The same event is delivered more than once over Kafka (for example, ~1.5%). | Windows and running balances count the event twice. | Event ID contract, injected duplicate rate, and window and balance results before and after deduplication. |
| **Window processing** | Flink groups events into event-time windows. | No windowed streaming features or aggregates exist for downstream use. | Flink window code, explanation, and Flink UI screenshots. |

**Measurement and Evidence Rules**:
- **Measure produced data**: Report volume, skew, cardinality, duplicate rates, schema variants, and delivery delays as measured from the generated files and stream, not merely from configured parameters.
- **Separate fault injection from business history**: Keep native TPC-DI irregularities, synthetic ML account behavior, and injected rubric faults separately configurable and measurable.
- **Baseline vs. treated**: Every Spark and Flink problem-handling claim requires a baseline run and a treated run on the same input data, paired with Spark UI or Flink UI screenshots and an explanation of the symptom, methods tried, and measured gain.

### 1.2 Platform & Operations Requirements

| Area | Required Capabilities & Deliverables |
| --- | --- |
| **Layer 1: Lakehouse & Warehouse** | Medallion zones (`Staging`, `Bronze`, `Silver`, `Gold`) on object storage (SeaweedFS) with Hive Metastore, Trino SQL access, and Gold warehouse tables. SCD2 history dimensions, timestamped feature tables (`event_timestamp` and availability timestamp), ERDs per zone, and storage and query optimization comparisons. |
| **Layer 1: Orchestration & Governance** | Three Airflow pipelines (`DP1` source to bronze, `DP2` bronze to silver/gold, `DP3` offline features), each with ingest and validate stages (Soda) configured via Airflow connections and variables. DataHub catalog recording table lineage and validation results. |
| **Layer 2: Machine Learning** | Feast feature store (point-in-time historical retrieval and online materialization), Kubeflow Pipelines training workflow, MLflow experiment and model tracking, KServe model serving, and a document chunking and embedding retrieval (RAG) pipeline with lineage and validation. |
| **Layer 3: Agents** | LLM serving endpoint, MCP tools for data, ML, and retrieval, gateway routing and access policies, agent registry and coordination, and versioned prompt management. |
| **Infrastructure & Operations** | Docker Compose profiles (reviewed for image size and dependencies), Terraform and Ansible IaC, CI/CD pipelines, gateway, TLS, rate limiting, and secrets, observability (metrics, logs, traces, model and agent telemetry), and data/API/load testing. |
| **Novel Ideas** | Two demonstrated novel ideas beyond the baseline coursework requirements. |

---

## 2. Decisions & Rationale

### 2.1 Accepted Choices, Proposals, & Deferred Work

| Subject | State | Choice & Rationale |
| --- | --- | --- |
| **Business domain & sources** | Decided | Use the TPC-DI retail brokerage domain and all 18 business and reference source families. Exclude benchmark-only audit metadata (`<name>_audit.csv`, `Audit` table). The project does not generate or ingest `BatchDate.txt`; the historical range is a run parameter of the historical load. |
| **Historical range input** | Decided | The operator enters inclusive UTC `start_date` and `end_date` values as run parameters when triggering the historical load. The historical load is an intentional, one-time operation, so its range is stated, not inferred. The end date dates the historical `Prospect.csv` snapshot, and the first daily batch is the next day ([Section 2.4](#24-source-data--delivery-rationale)). |
| **Load cycle extension** | Decided | Keep the historical load (Batch 1) followed by daily batches (Batch 2+), extended up to one year to support rolling ML features and streaming comparisons. Produce one batch per calendar date, including weekends and holidays, so the delivery calendar does not depend on trading activity. Each daily batch has its own `data_date`. Historical length remains open; `BatchID` assignment is deferred with catch-up and reruns. |
| **Quarterly FINWIRE** | Decided | Release each financial reporting quarter on the last calendar day of the following month, in UTC. Name the file by its posting quarter and retain the original reporting period in each `FIN` row. This models a periodic earnings feed, not statutory filing deadlines. Gold copies the report period from the row and resolves the company version at `PTS` ([TPC-DI clauses 2.2.2.8 and 4.5.14](TPC-DI/TPC-DI_v1.1.0.pdf)). All postings in a quarter's file release together on its release date. Release time of day, extra delivery delay, and how often companies and securities change remain open ([Section 2.4](#24-source-data--delivery-rationale)). |
| **Static HR and reference data** | Decided | Load HR and all six reference files during the historical load and keep them static for the run. Preserve the existing `DimBroker` history columns with one initial version per broker; add no reference version fields. |
| **HR and reference evolution** | Deferred | Revisit ongoing HR and reference updates and additional SCD2 history later. Delivery frequencies, source effective dates, reference version keys, and propagation rules are outside the current source contract. |
| **Source naming** | Decided | Keep standard historical names under `historical/`, including `FINWIREyyyyQq`. Use `incremental/YYYYMMDD/<Source>_YYYYMMDD.<extension>` for daily files. This preserves the historical formats and keeps a daily file's date visible when it is copied out of its folder. Folder dates use plain `YYYYMMDD`. A quarterly FINWIRE file arrives in the folder of its release date, such as `incremental/20260430/FINWIRE2026Q2` ([Section 2.4](#24-source-data--delivery-rationale)). |
| **Daily data date** | Decided | `data_date` is the business date represented by a daily extract. The folder and file name date encode it, not the extraction or arrival date, so delayed delivery and retries do not shift the logical batch date. Use UTC, with each day including its starting midnight and excluding the next midnight. Interpret source timestamps without a time zone as UTC. This avoids daylight-saving changes ([Section 2.4](#24-source-data--delivery-rationale)). |
| **Medallion zones** | Decided | **Staging** holds immutable raw batch files. **Bronze** preserves source records and received Kafka events 1:1 with provenance. **Silver** holds cleaned, typed, deduplicated, and conformed records. **Gold** holds the 19-table warehouse, cash stream outputs, and ML tables ([Data Specification](Data-Specification.md#4-data-zones-medallion-architecture)). |
| **Airflow pipelines** | Decided | **DP1** ingests source files to Bronze; **DP2** transforms Bronze to Silver and Gold; **DP3** computes offline feature tables. Each pipeline runs an ingest stage and a Soda validation stage. |
| **Object store & SQL catalog** | Decided | Store Staging, Bronze, and Silver on SeaweedFS via its S3 API, register schemas in Hive Metastore, and query with Trino ([IaC](IaC.md)). |
| **Object storage buckets** | Decided | Use three buckets: `staging` for delivered raw files, `lakehouse` for Bronze and Silver tables under `bronze/` and `silver/`, and `platform` for operational files. Rename `landing` to `staging`. Raw files stand for provider deliveries, which cannot be rebuilt, so they get their own bucket ([Section 2.5](#25-object-storage-layout-rationale)). |
| **Data versioning** | Decided | Baseline: `staging` keeps every delivered file unchanged, so replay rebuilds all tables. Bronze keeps provenance and parse failures, and SCD2 keeps business history. Corrected deliveries, table snapshots, and dead-letter handling wait for a requirement ([Section 2.6](#26-data-versioning-rationale)). |
| **Dataset scope** | Decided | The platform holds one generated dataset at a time: one timeline from the historical range through the daily batches. A dataset is disposable and is regenerated from its settings, seeds, and generator version, so storage keys and table names have no dataset level. Switching datasets clears every zone and the Kafka and Flink state first. Each result records the generator settings it used ([Section 2.5](#25-object-storage-layout-rationale)). |
| **Gold storage & SQL modeling** | Proposed | Store Gold warehouse, streaming, and ML tables in PostgreSQL, with Spark and dbt dividing batch transformations. Exact physical placement and engine boundaries depend on the final Silver and Gold schemas. |
| **Generator approach** | Decided | Build a configurable generator from the TPC-DI specification and the Databricks reference generator, with YAML/JSON configuration, coherent entity histories, synthetic ML behavior, and independent fault injectors. |
| **Generation and delivery** | Decided | Generate source data locally first. Deliver batch files to Staging and cash events to Kafka in a separate step, so delivery timing can change without regenerating the data. Delivery timing remains open. |
| **Delivery completeness** | Decided | Keep a predefined list of expected files for each delivery on the pipeline side. A delivery is complete when every expected file is present; a zero-record file means no activity. Do not depend on a provider control or manifest file, because a provider can send only the data files. Late-file handling remains open ([Section 2.4](#24-source-data--delivery-rationale)). |
| **Catch-up and reruns** | Deferred | Multiple-date loads, out-of-order arrival, repeated and corrected deliveries, and `BatchID` assignment wait for pipeline design. |
| **Generator summary** | Decided | Keep one reference summary of generation settings, output counts, and simulated data characteristics on local disk beside the generated dataset. It is never uploaded, and ingestion does not read it. File hashes, full fault extracts, and separate characteristics reports are not required by default. The file name and exact fields remain open. |
| **Generator engine** | Open | The autonomous Polars POC (`generator/poc_polars/`) passed 50 tests and generated 1.01 GB at 125.16 MiB peak RSS ([Polars POC record](records/Polars-POC.md)). Production engine selection (Polars vs. DuckDB vs. Spark) remains open. |
| **Cash streaming** | Decided | Stream `CashTransaction` records after Batch 1 through Kafka and Flink into Bronze deliveries, Silver unique events, and Gold daily balances (`FactCashBalances`), running balances, and window aggregates. Assign a stable Event ID before publication. |
| **ML evaluation split** | Decided | Evaluate on held-out time periods within one dataset: train on earlier dates and test on later dates. Several generator seeds are not used. |
| **ML prediction task** | Decided | Forecast future net account cash movement over a chosen horizon and add it to current cash (`projected_cash = current_cash + forecast_net_movement`). |
| **Minimum-cash policy** | Decided | Flag accounts where `projected_cash < configured_minimum[customer_tier]`, using `DimCustomer.Tier` (1, 2, 3) linked through `DimAccount`. Decouples the regression forecast from policy thresholds. |
| **Predictive behavior** | Decided | Design synthetic account activity profiles in the generator alongside the cash forecast task so features carry intentional signal while trades, cash, and holdings remain consistent. Behavior model details remain open. |
| **Online serving flow** | Proposed | Flink updates cash state and publishes streaming features; a separate scoring worker calls the KServe model endpoint, applies the tier policy, and writes retry-safe forecast and alert records for an API. Keeps model latency off the streaming ingestion path. |
| **Obligation schedule** | Deferred | Explicit payment schedules (`projected_cash = current_cash + forecast_other_net_movement - known_due_payments`) and intraday margin and liquidity coverage are deferred to future work. |
| **Existing code** | Decided | The existing code, Compose files, and Makefile are not constraints on the design. The generator, pipelines, tests, and infrastructure will be rebuilt; reuse a part only where it fits the new design. Makefile targets are convenience commands, added at the end. |
| **Document structure** | Decided | `Data-Specification.md` holds the cross-layer data contract; `Project-Design.md` holds requirements, decisions, constraints, and open questions; `README.md` summarizes the whole system; dedicated layer docs (`Data-Platform.md`, `ML-Platform.md`, `Agent-Platform.md`) join `IaC.md` as each layer is built. |

### 2.2 Selected Streaming & ML Use Case Rationale

`CashTransaction` and account cash-flow forecasting were selected together because they connect streaming, warehouse state, and ML without inventing a separate domain:
- **Action semantics**: Every `CashTransaction` row after Batch 1 is an insert-only cash movement (`CT_CA_ID`, `CT_DTS`, signed `CT_AMT`, `CT_NAME`), whereas `Trade` rows require reconstructing multi-step status lifecycles (`SBMT` -> `PNDG` -> `CMPT`/`CNCL`), `HoldingHistory` lacks timestamps, and `DailyMarket` has only daily resolution.
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
2. **Batch ML model**: Trained on batch features only.
3. **Batch + Streaming ML model**: Trained with recent Flink window features added, evaluated on the same prediction cutoffs, horizons, and held-out time periods. Measure regression error alongside alert recall, false alerts, and lead time.

**Proposed Online Serving Steps**:

| Step | Component | Responsibility |
| --- | --- | --- |
| 1 | **Flink** | Update cash state, write Silver and Gold cash tables, and publish windowed feature values with timestamps. |
| 2 | **Scoring worker** | Read eligible online features on an update trigger or schedule and request a prediction. |
| 3 | **KServe model service** | Return `forecast_net_movement` from the offline-trained model. |
| 4 | **Policy & alert service** | Resolve the account owner's current `DimCustomer.Tier`, apply `configured_minimum[tier]`, and write the forecast and alert record. |
| 5 | **Alert API** | Expose latest forecasts, shortfalls, and model and policy versions to applications and Layer 3 agents. |

### 2.4 Source Data & Delivery Rationale

The source contract relies only on what real providers send and gives each date one meaning. These criteria decided it:
- **Provider realism**: Rely on data files with their names and dates. A control file, a manifest, or a batch-date file is a benefit that a provider may not supply.
- **One meaning per date**: Keep business date, extraction date, publication date, and arrival date in separate fields.
- **Stability**: A delayed delivery or a retry must not change the batch to which a record belongs.
- **Benchmark fit**: Keep TPC-DI formats and transformations unless a change models real operation better.
- **Simplicity**: Simulate a behavior only when it changes processing or supports a coursework requirement.

| Decision | Choice | Main Reason |
| --- | --- | --- |
| **Batch date source** | No `BatchDate.txt`; the date is in the daily folder and file name | `BatchDate.txt` is a benchmark convenience file. Real extracts carry their date in the name or path. |
| **Historical file names** | Standard TPC-DI names without a date suffix | The historical load is one intentional operation, and FINWIRE names already carry their quarter. |
| **Daily date meaning** | `data_date` is the business date that the extract represents | Late delivery and retries do not shift it. |
| **Time zone and day** | UTC, from midnight up to the next midnight | No 23-hour or 25-hour days; one clock for batches, windows, and forecast cutoffs |
| **Batch calendar** | One batch per calendar date | Cash and customer changes do not follow the trading calendar. |
| **FINWIRE cadence** | Quarterly; each quarter's results release on the last day of the following month | A cadence different from daily OLTP, and delayed availability that creates point-in-time rules |
| **FINWIRE file quarter** | The posting quarter of `PTS`; the reporting period stays in each `FIN` record | TPC-DI creates a new FINWIRE file each quarter with records in `PTS` order (clause 2.2.2.8). |
| **FINWIRE placement** | The daily folder of its release date | A simple provider drops each file with that day's delivery. The release date's batch applies company and security postings before that day's trades and prices. |
| **Daily folder format** | Plain `YYYYMMDD` | Providers name drop folders by date, and no engine reads Staging as a partitioned table. |
| **HR and reference data** | Static for the run | TPC-DI loads them once. Updates would need effective dates and propagation rules that no feature needs yet. |
| **Generation and delivery** | Separate steps | Delivery timing and faults can change without regenerating data. |
| **Delivery completeness** | A pipeline-side list of expected files; a zero-record file means no activity | It needs no provider metadata, and S3 makes each object visible only when complete. |
| **Historical range** | `start_date` and `end_date` run parameters for the historical load | An intentional one-time load needs no provider metadata, and a stated range avoids guessing from record dates. |
| **Generator summary** | One local summary of settings, counts, and measured characteristics | Enough for checks and evidence, without hashing 100 GB or extracting every faulty record |

- **Simplification**: All FINWIRE postings in a quarter release together, so new securities appear at most once a quarter. The release rule simulates an earnings feed, not filing deadlines; earnings announcements can precede formal filings ([SEC](https://www.investor.gov/sites/default/files/readan8k.pdf)).
- **Limit**: The expected-file list cannot detect a file that the provider uploads already truncated. Parsing and validation must catch that case.
- **No range check**: The historical load does not check record dates against the stated range, and it does not detect a truncated historical extract. The rubric does not require either check.
- **Deferred**: Catch-up loads, reruns, corrected deliveries, and `BatchID` assignment return with pipeline design. HR and reference updates return when a feature needs them.

<details>
<summary>Alternatives Considered</summary>

Clauses refer to the [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf).

| Decision | Alternative | Why Not Selected |
| --- | --- | --- |
| **Batch date source** | `Batch<N>/` folders with `BatchDate.txt`, as in TPC-DI | The folder name carries only a sequence number, and the date depends on an extra file (clause 2.2.2.2). |
| **Batch date source** | Date in the folder only, standard file names | A file copied out of its folder loses its date. |
| **Batch date source** | Date in the file name only, one flat folder | Listing one date means scanning every daily file. |
| **Daily date meaning** | Extraction date, the TPC-DI meaning of `BatchDate` | A late extract shifts the date, and a second rule must find the represented day. |
| **Daily date meaning** | Arrival date | Transport delays and retries change it. |
| **Time zone** | `America/New_York` | Daylight-saving changes create 23-hour and 25-hour days. |
| **Batch calendar** | Business days only | Needs a rule that moves weekend activity into a later batch. |
| **FINWIRE cadence** | Historical only, as in TPC-DI (clause 1.3.5) | Company, security, and financial data would stay unchanged for a year. |
| **FINWIRE cadence** | Staggered delays per company | More scheduling rules and checks for little forecast benefit |
| **FINWIRE cadence** | Detailed filing calendars | Detail that the cash forecast does not use |
| **FINWIRE file quarter** | The reporting quarter | A quarter's file would change after the quarter ends, when its results are published. |
| **FINWIRE placement** | A separate `quarterly/` prefix | Shows the cadence, but adds a second location to watch and an ordering dependency between two pipelines |
| **Daily folder format** | `name=value` folders, such as `data_date=2026-04-30/` | Lets Spark, Hive, and Trino read folders as a partition column. AWS recommends it in raw buckets so a catalog can register raw partitions ([AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/defining-bucket-names-data-lakes/naming-structure-data-layers.html)). DP1 reads Staging by date path and registers no table over it. |
| **HR and reference data** | SCD2 history with ongoing updates | Needs delivery frequencies, effective dates, version keys, and propagation rules; deferred |
| **Generation and delivery** | The generator writes directly to S3 and Kafka | A change to delivery timing or faults would require regenerating the data. |
| **Delivery completeness** | Provider control file with file names and record counts | Depends on metadata that a provider may not send |
| **Delivery completeness** | Wait for a fixed time or until files stop changing | A time limit only guesses. A missing file looks the same as a late one without an expected list. |
| **Generator summary** | File hashes, full fault extracts, and a separate characteristics report | Hashing 100 GB costs time, and per-record extracts exceed what the checks need. |
| **Generator summary** | Stored in `staging` | Ingestion could read it. |
| **Historical range** | Provider control file with the range | Depends on metadata that a provider may not send |
| **Historical range** | Inferred from the latest record date | A quiet final day shifts the snapshot date and the first daily batch, and a truncated extract looks complete. |
| **Historical range** | The day before the first daily folder | The first daily delivery must exist before the historical load runs. |
| **Historical range** | A fixed project setting | Must match the generator settings by hand. The one-time load can take its range at the moment it runs. |

</details>

### 2.5 Object Storage Layout Rationale

Buckets separate data whose requirements differ; prefixes organize data whose requirements match. A bucket boundary needs a difference in at least one of these attributes:
- **Writer and owner**: A separate writer can later receive separate credentials.
- **Rebuild cost**: Data that cannot be recreated from an earlier zone needs the strongest protection.
- **Lifetime**: Data can be kept, replaced, or disposable.
- **Audience**: Services read storage paths; people query catalog tables.
- **Boundary cost**: Each bucket adds configuration and credentials. In SeaweedFS, each bucket is a separate collection with its own volumes ([IaC Section 8.3](IaC.md#83-the-object-store-runs-out-of-volumes)).

| Bucket | Contents | Writer | Rebuild After Loss | Reason for the Boundary |
| --- | --- | --- | --- | --- |
| `staging` | Delivered raw files | Delivery step, standing in for external providers | Not possible; providers do not resend history | Only data that cannot be rebuilt; different writer |
| `lakehouse` | Bronze and Silver tables under `bronze/` and `silver/` | Spark and Flink | From Staging and Bronze | Both zones share writers, owner, and lifetime, so prefixes separate them |
| `platform` | Spark event logs; later Flink checkpoints and MLflow artifacts | Platform services | Not needed | Lifetime differs from data |

- **Raw files as provider data**: The delivery step stands in for external providers, and a real provider cannot resend its history on demand. The design therefore treats Staging as data that cannot be rebuilt. Regenerating a dataset is a development tool, not part of the modeled system.
- **Outside object storage**: The generator summary stays on local disk, so ingestion cannot read it. Gold uses PostgreSQL (proposed).
- **One active dataset**: The project compares pipeline runs and deliveries over one timeline, not separate datasets. Baseline and treated runs use the same input, and stream replays reuse one dataset. A dataset is regenerated from its settings, so it needs no storage boundary. Replacing a dataset clears every zone first, because uploading over an existing dataset leaves stale files. Local disk can keep several generated datasets, so a switch costs a delivery and a reload instead of a regeneration.
- **Access**: A bucket enforces access only when each component has its own credentials; the security work adds them. Services use storage credentials. People query tables through Trino, where table-level rules apply.
- **Name**: `staging` matches the zone name and the TPC-DI "Staging Area." Vendors use the term differently; AWS calls processed data its "stage" layer.

<details>
<summary>Platform Guidance & Alternatives Considered</summary>

| Source | Guidance | Reason Given | Application Here |
| --- | --- | --- | --- |
| [AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/defining-bucket-names-data-lakes/naming-structure-data-layers.html) | One bucket per layer; keep raw files in their original format. | Archiving, versioning, access, and encryption can differ by layer. | Applies to raw files. Bronze and Silver share these requirements here. |
| [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture) | One lakehouse per layer, each in its own workspace; Gold can be a warehouse. | Governance at the layer level. | One team owns both table zones. Gold in PostgreSQL matches the warehouse pattern. |
| [Azure Storage team](https://azure.github.io/Storage/docs/analytics/hitchhikers-guide-to-the-datalake/) | Start with one storage account; justify each additional one. | Each account adds management work. | Each bucket here has a stated reason. |
| [Databricks](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/storage) | Separate storage by environment, business unit, or domain; raw files can sit outside the lakehouse; give people no direct storage access. | Outside parties need direct access to landing locations. | `staging` stays outside `lakehouse`; people query through Trino. |

| Fabric | Our System |
| --- | --- |
| OneLake, one per tenant | One SeaweedFS instance |
| Workspace | Bucket |
| Lakehouse `Tables/` and `Files/` | Hive table locations and raw file prefixes |
| Shortcut | External table `LOCATION`; Spark reading `staging` directly |
| OneLake security roles, enforced in every engine | SeaweedFS rights for files and Trino rules for tables, enforced separately |

| Alternative | Why Not Selected |
| --- | --- |
| Two buckets: `staging`, and `lakehouse` with logs | Logs would share cleanup and access with tables, although their lifetime differs. |
| One bucket per zone: `staging`, `bronze`, `silver` | Bronze and Silver share writers, owner, and lifetime. Flink writes both zones, so access rules would not follow zones. Each extra bucket adds a SeaweedFS collection. |
| One bucket with prefixes only | A job that clears the wrong prefix could reach raw files, which cannot be rebuilt. |
| A dataset prefix in `staging` only | Downstream tables, Gold, and Kafka would still mix datasets. |
| Dataset isolation in every system | Every component would carry a dataset name for comparisons that the project does not make. |
| A separate deployment per dataset | Too heavy for one machine with about 900 GB of free disk. |

</details>

### 2.6 Data Versioning Rationale

"Data versioning" covers several separate questions. The baseline answers each one with a mechanism that the design already has, and defers the rest until a requirement needs them. These criteria decided it:
- **Recovery**: Replaying the raw files can rebuild every table.
- **History**: A question about what was true on a past date has an answer.
- **Traceability**: Each row leads back to the delivery that brought it.
- **Simplicity**: A versioning mechanism is added only when a requirement or a failure needs it.

Two clocks separate the kinds of history. **Valid time** is when a fact was true in the business; SCD2 records it. **Transaction time** is when the platform recorded or changed a row; table snapshots record it.

| Kind | Question It Answers | Baseline | State |
| --- | --- | --- | --- |
| **Raw retention** | Can every table be rebuilt? | `staging` keeps every delivered file unchanged; replay rebuilds Bronze, Silver, and Gold. | Decided |
| **Provenance** | Which delivery brought this row? | Bronze preserves provenance; its columns remain open. | Decided |
| **Business history** (valid time) | What was true on a past date? | SCD2 dimensions; facts store the dimension version valid on the event date | Decided |
| **Rejected records** | What could not be processed, and why? | Bronze keeps parse failures; data alerts go to `DImessages`. | Decided |
| **Corrected deliveries** | What did a file contain before a provider corrected it? | None | Deferred with corrections |
| **Table snapshots** (transaction time) | What did a table contain before a load? | Replay from Staging only | Deferred; returns with the table format choice |
| **Dead-letter handling** | Where do records go that fail after Bronze, or in the stream? | None | Deferred |
| **Branch-style versioning** | Can changes be tested on an isolated copy of the lake? | None | Not selected |
| **Model and feature versions** | Which model and features produced a forecast? | MLflow and Feast | Layer 2 design |

- **Revisit triggers**: Corrected deliveries bring object versioning or versioned keys in `staging`; SeaweedFS supports S3 object versioning ([SeaweedFS](https://github.com/seaweedfs/seaweedfs/wiki/S3-Object-Versioning)). Choosing Iceberg or Delta Lake brings table snapshots and time travel with the format ([Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/), [Delta Lake](https://delta.io/learn/getting-started/)). Invalid stream events bring a dead-letter topic fed by a Flink side output ([Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/)).

<details>
<summary>Alternatives Considered</summary>

| Alternative | Why Not Selected Now |
| --- | --- |
| Object versioning on `staging` now | The baseline never overwrites a Staging object, so no second version would form. Versioning matters only with corrected deliveries. |
| Table snapshots and time travel now | The table format is not chosen yet. Replay from Staging covers recovery until then. |
| Branch-style versioning with [lakeFS](https://docs.lakefs.io/) or Nessie | Adds a service and a branch workflow for isolated experiments that one person on one timeline does not need. |
| A dead-letter queue now | No requirement injects invalid events, and Bronze already keeps parse failures. |

</details>

---

## 3. Engineering Constraints

### 3.1 Storage & Lakehouse Zones

| Concern | Constraint & Rule |
| --- | --- |
| **Bucket isolation** | Keep raw delivered files in the `staging` bucket and table locations in `lakehouse`, so a job that clears or overwrites a table location cannot remove raw files ([Section 2.5](#25-object-storage-layout-rationale)). |
| **SeaweedFS volumes per bucket** | Each bucket is a separate collection with its own volumes. Apply the volume settings in [IaC Section 8.3](IaC.md#83-the-object-store-runs-out-of-volumes) when the infrastructure is rebuilt. |
| **Event vs. arrival dates** | Keep business event timestamps (`CT_DTS`, `T_DTS`), `data_date`, and system arrival dates in separate columns. Partitioning by arrival date alone misses late-arriving historical events in event-date queries. |
| **External tables on SeaweedFS** | Declare lakehouse tables with explicit `LOCATION` clauses. SeaweedFS reports empty prefixes as existing directories, which causes Spark managed-table creation to fail ([IaC Section 7.2](IaC.md#72-every-lakehouse-table-is-external)). |
| **Partition registration** | Register new Hive-backed Parquet partitions through a catalog write or partition repair; writing files to a new `dt=` path alone does not make them visible to Trino ([IaC Section 8.1](IaC.md#81-a-path-write-is-not-a-table-write)). |
| **Table updates (`MERGE`)** | Plain Parquet supports append and partition overwrite, not transactional row-level `MERGE`. Selecting `dbt-spark` incremental `merge` or in-place SCD2 and trade updates on the lakehouse requires Delta Lake, Iceberg, or Hudi, or performing mutable Gold updates in PostgreSQL. |

### 3.2 History, SCD2, & Batch-to-Stream Consistency

- **XML omitted vs. empty fields**: In `CustomerMgmt.xml`, an omitted attribute/element keeps the previous version's value, whereas an explicitly empty element (`<C_M_NAME/>`) clears the value to `NULL`. Silver must preserve this distinction before Gold SCD2 carry-forward runs.
- **Carry-forward window support**: Spark supports `last(col, ignorenulls=True)` over ordered windows; PostgreSQL 16 lacks `IGNORE NULLS` on window functions and requires grouping or subquery techniques if SCD2 versions are built in SQL.
- **Daily collapse and propagation**: Collapse multiple same-day changes to one entity into a single daily SCD2 version, and propagate customer version changes to `DimAccount` and company version changes to `DimSecurity` ([Data Specification Section 8](Data-Specification.md#8-core-integration-rules)).
- **Financial reporting vs. publication**: Generate `FIN.Year`, `Quarter`, and `QtrStartDate` from the completed reporting period, and `PTS` and `PostingDate` from publication. The file quarter follows `PTS`. Gold must retain publication and availability metadata so P/E calculations and ML features cannot use future reports. Combine earnings by natural company identity across SCD2 versions. Physical metadata fields and the missing-earnings policy remain open.
- **Historical availability**: Preserve source publication dates separately from actual historical load times. Define the business-clock mapping for replay before using platform availability in historical training; today's ingestion timestamp is not a historical publication timestamp.
- **New source identities**: Publish required company and security identities before normal downstream activity can refer to them. Define how late source deliveries repair earlier dimension joins instead of using whichever version is current on arrival.
- **Batch-to-stream cash handoff**:
  1. Initialize Flink account balances from the completed Batch 1 Gold `FactCashBalances` state (keyed by natural `AccountID`, since `SK_AccountID` changes across SCD2 versions).
  2. Assign a deterministic Event ID to each post-Batch-1 cash movement before Kafka publication so Bronze records every delivery and Silver and Flink deduplicate repeated deliveries.
  3. Give each Gold cash table a single writer during normal operation so batch validation replays do not double-count live balances.

**Stream verification**: Check the Flink results against an expected result that is calculated outside Flink.
- **Event identity**: Each cash movement keeps one Event ID across repeated deliveries. File replay must preserve that identity; its derivation rule remains open. A Kafka offset identifies a delivery position, not the business event.
- **Two clocks**: Keep the event time from `CT_DTS` and the delivery time from Kafka. Interpret source `DATETIME` values as UTC under the project's time convention; they do not carry a time zone in the source text. Record delivery timestamps in UTC as well.
- **Baseline first**: Run one replay without faults. Then run a second replay with a fixed seed and recorded fault settings. Measure actual delivery delays and duplicate rates during replay; generation settings do not establish delivery behavior. Kafka keeps order within a partition, so the producer must create the delays and reordering.
- **Ordering**: Partition by account to keep per-account order. Do not claim a global order across partitions.
- **Expected result**: Compute deduplicated cash sums and daily balances independently, then compare each run for missing events, duplicate effects, late events, and time to final agreement. A cash total can decrease, so the largest emitted value does not identify the final result.
- **Out-of-order updates**: An older update must not overwrite newer state because it arrived later.

### 3.3 Generator

- **Bounded memory**: Generate records in bounded chunks while preserving cross-batch entity state (active customers, accounts, brokers, securities, and open trades and holdings). Streaming file sinks alone do not bound memory if intermediate joins or sorts materialize full history in RAM.
- **Seed stability**: Deterministic IDs and random draws must depend on entity keys and named seeds, not chunk size or worker partition count. Datasets are regenerable, so the same settings, seeds, and generator version must reproduce the same files.
- **Generator summary**: Record generation settings (scale, seeds, date bounds, and simulated problems), per-file row and byte counts, and observed data characteristics in one reference output. Distinguish configured targets from measurements of produced data. Keep the summary outside source data; ingestion must not read it. Exact file name and fields: open.
- **Summary scope**: Report the selected characteristics, such as cardinality, skew, schema changes, and duplicates. Do not require file hashes, full fault extracts, or a separate characteristics report by default. Measure delivery behavior in the separate delivery step, not from generation counters.

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
- **Fault reporting**: Summarize simulated problems and their observed characteristics in the generator summary rather than copying every affected record by default.
- **Duplicate rate**: Define the denominator. With `r` extra copies per original record, the duplicate share of emitted rows is `r / (1 + r)`, not `r`.
- **Skew**: Create skew by choosing valid account and security identities, then derive trades, cash, and holdings from those choices. Rewriting foreign keys after the dependent facts exist breaks relationships.
- **Schema evolution**: Record which batch introduces each schema version. Parsers must tell an absent field from a present `NULL` where the difference matters.
- **Delivery faults**: Delayed and duplicated events keep their business event time and stable event ID.

---

## 4. Open Questions & References

### 4.1 Open Questions

| Area | Open Question | What Depends on It |
| --- | --- | --- |
| **Data Model** | What are the Bronze provenance columns, Silver schemas, financial publication and availability fields, and other Gold refinements? | DP2 transformations, Gold ERD refinements, and temporal foreign-key rules. |
| **Load Cycle** | How many years of history does Batch 1 cover? What are the FINWIRE release time of day, extra delivery delay, frequency of company and security changes, and late-update policy? | Generator calendar bounds, source availability, and DP2 update rules. |
| **Storage & Engines** | How do Spark, dbt, Trino, and PostgreSQL divide Silver and Gold storage and transformations, and which lakehouse table format is used? The proposal describes a lakehouse with Bronze, Silver, and Gold zones plus a separate data warehouse. | DP1 to DP3 job code, `MERGE` support, and Compose profiles. |
| **Source Delivery & Staging** | Resolve the remaining [source contract questions](Data-Specification.md#11-open-decisions): late-file handling, delivery timing (the simulated clock), and account keys for cash events that precede their daily batch. | Delivery step, source discovery, and ingestion behavior. |
| **Generator & Faults** | Which production engine (Polars vs. DuckDB vs. Spark), scale settings for 100 GB, and fault parameters do we choose? What are the summary's file name and fields? | Generator build, generator summary, and Spark baseline vs. treated experiments. |
| **Cash Stream** | How does the separate delivery step publish generated cash events to Kafka? What are the Event ID format, window sizes, watermark and lateness limits, and late-balance correction rules? | Flink job implementation, Gold cash sinks, and Flink UI evidence. |
| **ML Forecast & Policy** | What are the forecast horizon, scoring cadence, feature list, synthetic account behavior rules, model family, evaluation metrics, and tier minimum amounts (Tiers 1, 2, 3)? | Generator behavior model, DP3 feature tables, Feast views, KFP pipeline, and alert policy config. |
| **Text / RAG** | Which brokerage document sources and retrieval task does the text pipeline serve? | Document ingestion, chunking, Feast vector storage, and Layer 3 retrieval tool. |
| **Agents** | What user workflow, MCP tool contracts, model and framework, and gateway policies define Layer 3? | Layer 3 implementation and end-to-end demo. |
| **Pipeline Reruns** | How do catch-up loads, retries, and reruns reset or overwrite state from a known batch boundary? How is `BatchID` assigned? Deferred to pipeline design. | Airflow DAG idempotency and partition and table replacement logic. |
| **Novel Ideas** | Which two novel ideas do we demonstrate and measure for the coursework rubric? | Project design and evaluation evidence. |

### 4.2 Working Records & Local References

The [Polars POC record](records/Polars-POC.md) holds the generator POC evidence: 50 tests, 1.01 GB of validated output at 125.16 MiB peak RSS.

These local reference notes are for later phases. Git ignores `misc/archive/` and `misc/tpcdi/`, so a fresh clone does not have them; Git history keeps the earlier `docs/research/` copies.

| Path | Use It When |
| --- | --- |
| `misc/tpcdi/NOTES.md` | Writing DP2 SQL: index of the AlaaAlmutawa TPC-DI staging and warehouse SQL, with the defects in those queries. |
| `misc/archive/Card-Fraud-Platform.md` | Building the Spark skew and deduplication benchmarks, the Flink window checker, and the scoring service: patterns from a reference coursework repository. |
| `misc/archive/diagram-references/` | Drawing the architecture and deployment diagrams: seven external platform diagrams for layout comparison. |
