# Data and AI Platform for TPC-DI Financial Data

**Contents**

[Overview](#overview) | [Repository Structure](#repository-structure) | [Business Domain and Data](#business-domain-and-data) | [Layer 1: Data Platform](#layer-1-data-platform) | [Layer 2: Machine Learning Platform](#layer-2-machine-learning-platform) | [Layer 3: Agent Platform](#layer-3-agent-platform) | [Infrastructure and Operations](#infrastructure-and-operations) | [Getting Started](#getting-started)

## Overview

This project is a data and AI system for a retail brokerage, based on the TPC-DI domain. It generates related financial records, processes them through batch and streaming pipelines, and uses the resulting data for analytics, machine learning, and agent applications.

- **Layer 1 ([Data Platform](docs/Data-Platform.md))**: Turns raw batch files and streaming cash events into validated lakehouse and warehouse tables, using SeaweedFS, Hive Metastore, Trino, PostgreSQL, Spark, dbt, Kafka, Flink, Airflow, Soda, and DataHub.
- **Layer 2 ([Machine Learning Platform](docs/ML-Platform.md))**: Forecasts future net account cash movement and applies a tier-based minimum-cash policy, using Feast, Kubeflow Pipelines, MLflow, and KServe, plus document chunking and embedding retrieval.
- **Layer 3 ([Agent Platform](docs/Agent-Platform.md))**: Connects language models to project data, retrieved documents, and model predictions through gateways, MCP tools, and agent coordination.

## Repository Structure

```text
data-ai-platform-tpcdi-project/
├── data/             # Layer 1: Data platform (generator, spark, dbt, flink, airflow, soda, datahub)
├── ml/               # Layer 2: ML platform (feature_repo, training, kfp, serving, rag)
├── agents/           # Layer 3: Agent platform (mcp, coordinator, gateway, prompts, registry)
├── infra/            # Infrastructure, containers, cloud IaC, CI/CD, security, and observability
├── docs/             # System documentation: data specification, Layer 1-3 designs, and infrastructure
└── workflow/         # Engineering workflow guides, status, working records, and verification tools
```

Each main section below summarizes one part of the system and links to its detailed document in `docs/`: [Data Specification](docs/Data-Specification.md), [Data Platform](docs/Data-Platform.md), [ML Platform](docs/ML-Platform.md), [Agent Platform](docs/Agent-Platform.md), and [Infrastructure](docs/IaC.md).

## Business Domain and Data

The project uses the TPC-DI retail brokerage as its business domain and raw source data. The [data specification](docs/Data-Specification.md) holds the full detail.

### Business Domains & Origin Systems

| Business Domain | Origin System | Business Context |
| --- | --- | --- |
| **Customers & Accounts** | CMS (history) & OLTP (daily) | Customer profiles, demographics, and brokerage accounts managed by brokers. |
| **Brokers** | Internal HR DB | Employee records. Brokers are specifically employees with job code `314`. |
| **Trading & Holdings** | OLTP Trading DB | Trade lifecycles (submission to completion or cancellation) and resulting security holdings. |
| **Cash** | OLTP Trading DB | Money moving into and out of accounts, such as deposits, withdrawals, and cash from trades. |
| **Market** | OLTP Trading DB | Daily price and volume observations for each security. |
| **Watch Lists** | OLTP Trading DB | Securities that customers watch, added and removed over time. |
| **Prospects** | External Marketing Provider | Potential customers with demographic data (some overlap with existing customers). |
| **Companies & Securities** | Financial Newswire (FINWIRE) | Companies, the securities they issue, and quarterly financial reports. |
| **Reference Data** | Static Reference Files | Calendar, time of day, industries, statuses, tax rates, and trade types. |

### Physical Source Files & Ingestion Rules

The project uses all 18 TPC-DI business and reference source families. It does not generate or ingest `BatchDate.txt`. Metadata used only for formal benchmark audit validation is also excluded.

The table uses standard source names. Historical files keep those names under `historical/`, including the existing quarter in `FINWIREyyyyQq`. Daily files use `incremental/YYYYMMDD/<Source>_YYYYMMDD.<extension>`, such as `incremental/20170708/Customer_20170708.txt`. A quarterly FINWIRE file arrives in the daily folder of its release date. The pipeline keeps a predefined list of expected files; a delivery is complete when all of them are present, and a zero-record file means no activity.

**Change Data Capture (CDC) Rules**: Daily OLTP files are CDC extracts. Each row starts with a `CDC_FLAG` (`I` for insert, `U` for update) and a `CDC_DSN` (sequence number). Insert-only files always carry `I`; historical files omit both fields.

| Origin | Source File(s) | Format | Grain | Batch 1 | Batch 2+ | CDC Rules |
| --- | --- | --- | --- | :---: | :---: | --- |
| CMS | `CustomerMgmt.xml` | XML | One action on a customer and its accounts | Yes | No | N/A (Action-ordered) |
| OLTP | `Customer.txt` | Pipe-delimited | One changed customer | No | Yes | `I`, `U` |
| OLTP | `Account.txt` | Pipe-delimited | One changed account | No | Yes | `I`, `U` |
| OLTP Trading DB | `Trade.txt` | Pipe-delimited | One trade (state or change) | Yes | Yes | `I`, `U` |
| OLTP Trading DB | `TradeHistory.txt` | Pipe-delimited | One trade status change | Yes | No | N/A (Historical only) |
| OLTP Trading DB | `CashTransaction.txt` | Pipe-delimited | One cash movement | Yes | Stream | N/A (events after Batch 1) |
| OLTP Trading DB | `HoldingHistory.txt` | Pipe-delimited | One holding change per trade | Yes | Yes | `I` only |
| OLTP Trading DB | `WatchHistory.txt` | Pipe-delimited | One watch placement or cancellation | Yes | Yes | `I` only |
| OLTP Trading DB | `DailyMarket.txt` | Pipe-delimited | One security per day | Yes | Yes | `I` only |
| Internal HR DB | `HR.csv` | Comma-delimited | One employee | Yes | No | N/A (Full extract) |
| External Marketing | `Prospect.csv` | Comma-delimited | One prospect | Yes | Yes | N/A (Full snapshot) |
| Financial Newswire | `FINWIREyyyyQq` | Fixed-width | One company, security, or financial report | Yes | Release dates | N/A (One file per posting quarter) |
| Static Reference | `Date.txt`, `Time.txt`, `Industry.txt`, `StatusType.txt`, `TaxRate.txt`, `TradeType.txt` | Pipe-delimited | One row per code, day, or second | Yes | No | N/A (Static) |

### Load Cycle & Processing Strategy

TPC-DI defines one historical load followed by two daily batches. This project keeps the historical load, extends the daily batches up to one year, delivers FINWIRE quarterly inside the daily batches, and sends later cash movements as a continuous stream:

| Phase | Path | Coverage | Work |
| --- | --- | --- | --- |
| **Historical load (Batch 1)** | Files in `historical/` | `start_date` through `end_date` | Load reference data and full extracts, build SCD2 history, and calculate opening cash balances and holdings. |
| **Daily batches (Batch 2+)** | Files in `incremental/YYYYMMDD/` | One `data_date` per batch, up to one year | Apply the day's CDC changes (`I`, `U`), new activity, the full `Prospect.csv` snapshot, and FINWIRE on release dates. |
| **Cash stream** | Events on Kafka | Cash movements after `end_date` | Update running balances, window aggregates, and daily balances as events arrive. |

- **Calendar**: Every calendar date has a daily batch, from midnight UTC to the next midnight. `data_date` is the business date that an extract represents; extraction and arrival dates do not change it. The historical load takes inclusive `start_date` and `end_date` as run parameters.
- **Cadence**: OLTP changes except cash, market data, and `Prospect.csv` arrive daily; cash arrives only through the stream. FINWIRE arrives in the daily batch of its release date, the last day of the month after each quarter. Its file names follow the posting quarter, so Q1 results released April 30 belong in a Q2 file but remain Q1 in `Financial`. HR and reference files load once and stay static.
- **Handoff**: The stream starts from the final historical balances and never reprocesses historical movements. A cash event's date is the UTC date of `CT_DTS`; a late event updates its day and every later day.

Batch 1 requires at least one year of prices and four quarters of earnings to calculate 52-week highs and lows and P/E ratios.

## Layer 1: Data Platform

The data platform turns source data into validated tables for analytics and machine learning, with scheduled quality checks and catalog lineage. The [Layer 1 design](docs/Data-Platform.md) holds the full architecture, problem-injection rules, and rationale.

### Data Zones and Silver Layer

| # | Zone | Purpose | Grain | Storage |
| --- | --- | --- | --- | --- |
| 1 | **Staging** | Immutable raw batch files as delivered. The cash stream enters through Kafka instead. | One file | SeaweedFS `staging` bucket |
| 2 | **Bronze** | Source records and received events as delivered, including duplicates and parse failures. Preserves provenance. | One source record or one delivery | SeaweedFS `lakehouse` bucket, `bronze/` tables |
| 3 | **Silver** | Cleaned, typed, deduplicated, and conformed records. Ready for joins. | One validated business record | SeaweedFS `lakehouse` bucket, `silver/` tables |
| 4 | **Gold** | The warehouse, cash stream outputs, and ML tables. | Per business entity or metric | PostgreSQL and `lakehouse` tables |

Operational files, such as Spark event logs, use a separate `platform` bucket ([bucket rationale](docs/Data-Platform.md#62-object-storage-layout-and-dataset-scope)). Hive Metastore records lakehouse table schemas and file locations. Spark processes the tables, and Trino provides SQL access for inspection and queries.

The TPC-DI specification does not define a silver layer. The Silver layer is the project's own model, with the following traits:
- **Preserve Change Order**: Keep `CDC_FLAG`, `CDC_DSN`, `BatchID`, and `data_date`. SCD2 versioning depends on them.
- **Handle XML Nuance**: For `CustomerMgmt.xml`, preserve the distinction between omitted and empty elements until Gold versions are built.
- **Deduplicate Correctly**: Remove repeated deliveries of the same event, but keep legitimate repeated business changes (such as two updates to one trade).
- **Separate Temporal Data**: Keep event dates strictly separate from `data_date` and system arrival dates.

### Data Lineage: Source-to-Gold Mapping

| Source Records | Transformation Logic | Gold Output |
| --- | --- | --- |
| `CustomerMgmt.xml` + `Customer.txt` | Apply XML actions, then CDC rows. Add tax rates and prospect data; keep daily SCD2 versions. | `DimCustomer` |
| `CustomerMgmt.xml` + `Account.txt` | Same pattern. Resolve customer and broker keys; add a version when the customer changes. | `DimAccount` |
| `HR.csv` | Keep employees with job code 314. | `DimBroker` |
| FINWIRE (`CMP`, `SEC`) | Parse fixed-width records in posting order; build histories; link securities to companies by CIK or name. | `DimCompany`, `DimSecurity` |
| FINWIRE (`FIN`) | Copy the report year and quarter from the record; resolve the company at `PTS`; retain publication and availability metadata. | `Financial` |
| `Trade.txt` + `TradeHistory.txt` | Insert each trade, then update its status. Keys are set at creation and kept at close. | `DimTrade` |
| `CashTransaction.txt` | Sum each account's daily movements and add them to the previous balance. *(Subsequent movements go through Kafka and Flink.)* | `FactCashBalances` |
| `HoldingHistory.txt` + `DimTrade` | Copy `HH_AFTER_QTY`; take keys, date, time, and price from the changing trade. | `FactHoldings` |
| `DailyMarket.txt` + `DimSecurity` + `Financial` | Copy prices and volume; calculate 52-week high and low, P/E, and yield. | `FactMarketHistory` |
| `WatchHistory.txt` | Insert a watch on `ACTV`; set its removal date on `CNCL`. | `FactWatches` |
| `Prospect.csv` + `DimCustomer` | Insert or update prospects, calculate tags, and match active customers. | `Prospect` |

### Gold Warehouse

The Gold layer uses the TPC-DI warehouse model with ongoing FINWIRE releases. Dimensions describe business entities; facts record activity.

**SCD2 Dimension Rules**:
- **Keys**: A natural key (`CustomerID`) identifies the entity across time. A surrogate key (`SK_CustomerID`) identifies one specific version.
- **Versioning**: On a change, the current version gets `IsCurrent = 0` and `EndDate` = the new version's `EffectiveDate`. A new row is inserted with a new `SK`, `IsCurrent = 1`, and `EndDate = 9999-12-31`.
- **Collapse**: Multiple changes to one entity on the same day collapse into a single daily version.
- **Fact Joining**: Facts store the `SK` of the dimension version valid on the event's date.
- **Propagation**: A new customer version gives the customer's accounts new versions. A new company version does the same for its securities.

| Table | Type | Contents | Grain | Built From | Batch 1 | Batch 2+ |
| --- | --- | --- | --- | --- | --- | --- |
| `DimCustomer` | SCD2 Dimension | Identity, contacts, tier, tax rates, prospect data | One customer version | `CustomerMgmt.xml`, `Customer.txt`, `Prospect.csv` | Versions from XML actions | New versions from CDC |
| `DimAccount` | SCD2 Dimension | Owner, broker, tax status, status | One account version | `CustomerMgmt.xml`, `Account.txt` | Versions from XML actions | New versions from CDC and customer changes |
| `DimBroker` | SCD2 Dimension | Name, manager, branch, office, phone | One broker | `HR.csv` | One version per broker | No change |
| `DimCompany` | SCD2 Dimension | Name, industry, credit rating, CEO, address | One company version | FINWIRE `CMP` | Versions from postings | New versions from quarterly FINWIRE |
| `DimSecurity` | SCD2 Dimension | Symbol, exchange, shares, dividend, issuing company | One security version | FINWIRE `SEC` | Versions from postings | New versions from quarterly FINWIRE |
| `DimTrade` | Mutable Dimension | Type, status, quantity, prices, fees, account, security | One trade | `Trade.txt`, `TradeHistory.txt` | One row per trade | Insert new trades; update status on close |
| `FactCashBalances` | Fact | End-of-day account cash | One account per day with transactions | `CashTransaction.txt` | Daily balances | From the cash stream |
| `FactHoldings` | Fact | Quantity held after a trade | One holding change | `HoldingHistory.txt` | One row per change | One row per change |
| `FactMarketHistory` | Fact | Prices, volume, 52-week high and low, P/E, yield | One security per day | `DailyMarket.txt` | One row per day | One row per day |
| `FactWatches` | Fact | Period a customer watches a security | One watch | `WatchHistory.txt` | Watches from actions | Insert placements; set removal dates |
| `Prospect` | Current State | Agency record, marketing tags, `IsCustomer` | One prospect | `Prospect.csv` | One row per prospect | Insert new; update changed |
| `Financial` | Current State | Quarterly results | One company per quarter | FINWIRE `FIN` | One row per report | New quarterly reports |
| `DimDate`, `DimTime` | Reference | Calendar and clock attributes | One day; one second | `Date.txt`, `Time.txt` | Loaded once | No change |
| `Industry`, `StatusType`, `TaxRate`, `TradeType` | Reference | Code lookups | One code | Matching `.txt` files | Loaded once | No change |
| `DImessages` | Load Log | Status messages and data alerts | One message | The load process | Each batch | Each batch |

#### Gold Layer Entity Relationships

The ERD shows all 19 tables, their key fields, and the main entity relationships. The `Prospect` relationship is a match by name and address, not a key.

![Gold layer ERD with all 19 tables, their keys, and their relationships.](docs/diagrams/data-specification/gold-erd.png)

*Header colors: blue for dimensions, orange for facts, green for current-state tables, gray for reference tables and the load log.*

### Data Generation

The generator produces related source records using the TPC-DI specification, the official TPC-DI sample dataset, and the Databricks reference generator as references. Predictive account behavior is designed together with the cash forecast task, while keeping accounts, trades, cash, and holdings consistent.

- **Scale**: Supports small development runs and at least 100 GB of offline data.
- **Relationships**: Preserves referential integrity and consistent entity histories.
- **Load Patterns**: Produces historical data and daily changes locally. A separate delivery step sends batch files to Staging and cash events to Kafka.
- **Offline Problems**: Injects skew, schema evolution, and an additional problem such as duplicates.
- **Streaming Problems**: The delivery step injects late arrivals and an additional problem such as duplicate events.
- **Inspection**: Measures volume, cardinality, skew, schema variants, and duplicates in generated data. Measures event delays and streaming duplicates during delivery.
- **Generator Summary**: Keeps generation settings, output counts, and simulated data characteristics in one reference summary on local disk. It is never uploaded, and ingestion does not read it.

Implementation references include the [Databricks TPC-DI generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen).

### Batch Processing

Spark and dbt handle batch source parsing, cleaning, SCD2 integration, and warehouse modeling.

- **Source Parsing**: Reads the different file formats and accounts for records that cannot be parsed.
- **Cleaning and Integration**: Applies types, handles duplicates and daily changes, and validates relationships.
- **Historical Modeling**: Applies SCD2 rules to the history-tracking dimensions.
- **Warehouse Modeling**: Builds related dimensions and facts with defined grains.
- **Feature Computation**: Retains historical feature values with event and availability timestamps.
- **Baseline Comparison**: Spark task metrics, file layouts, and query plans before and after each optimization.

### Streaming Processing

After Batch 1, each new cash transaction is published to Kafka as one event with a stable event ID, and Flink processes it continuously.

| Cash Event Field | Type | Meaning |
| --- | --- | --- |
| Event ID | Project-defined identifier | Identifies the cash movement. A repeated delivery keeps the same ID. |
| `CT_CA_ID` | `NUM(11)` | Account ID |
| `CT_DTS` | `DATETIME` | Event time |
| `CT_AMT` | `SNUM(10,2)` | Signed amount (+ adds cash, - removes cash) |
| `CT_NAME` | `CHAR(100)` | Description |
| Delivery time | Kafka timestamp | When the event arrived; used to measure lateness |

| Output | Zone | Grain | Contents |
| --- | --- | --- | --- |
| Received events | Bronze | One delivery | Each event as delivered, including duplicates |
| Cash events | Silver | One cash movement | Validated events with duplicates removed |
| `FactCashBalances` | Gold | One account per day | TPC-DI daily balance: previous balance plus the day's net movement |
| Running balance | Gold | One account | Current cash after each event |
| Window aggregates | Gold | One account per window | Inflow, outflow, net movement, and count |

- **Start**: Flink initializes state from the final Batch 1 Gold balances and never reprocesses historical transactions.
- **Duplicates**: Repeated deliveries share an Event ID and are counted once.
- **Late events**: A late event changes its day's balance and every later balance.

### Orchestration and Governance

Airflow coordinates three pipelines. Each includes processing and validation stages.

| Pipeline | Scope |
| :--- | :--- |
| **DP1** | Parses source files into bronze tables and validates ingestion. |
| **DP2** | Transforms bronze into silver and gold, with validation of the resulting tables. |
| **DP3** | Computes and validates offline feature tables. |

Airflow connections and variables provide shared configuration. DataHub records the tables each pipeline reads and writes, together with validation results.

## Layer 2: Machine Learning Platform

The ML platform connects feature computation, training, serving, and document retrieval in a reproducible workflow. The [Layer 2 design](docs/ML-Platform.md) holds the full details and rationale.

### Prediction Task

The model predicts total future net cash movement for each account over a chosen horizon. The policy adds that forecast to recorded cash and flags a projected balance below the minimum configured for the account owner's customer tier.

```text
projected_cash = current_cash + forecast_net_movement
alert = projected_cash < configured_minimum[customer_tier]
```

| Data | Grain | Contents |
| --- | --- | --- |
| **Offline features** | One account per feature time | Feature history for training and backfills (`event_timestamp` and `created`) |
| **Online features** | One account | Latest feature values for low-latency scoring |
| **Label** | One account per prediction time | Sum of unique `CT_AMT` values after the prediction time, up to the forecast cutoff |
| **Tier minimums** | One tier (1, 2, 3) | Minimum cash per tier. TPC-DI supplies the tier; the project supplies the threshold amount. |
| **Forecast and alert** | One account per as-of time | Account ID, as-of time, forecast cutoff, current cash, forecast movement, projected cash, tier, minimum, shortfall, model version, policy version |

Candidate features include previous daily balance, earlier daily inflow and outflow totals, recent movement counts and amounts from Flink, and trade-type totals.

### Features and Training

- **Offline Features**: Timestamped feature history for training and backfills.
- **Online Features**: Latest eligible feature values for inference. Batch and streaming jobs can produce features for either use.
- **Point-in-Time Rule**: Training uses only values available before the prediction time, including for late records.
- **Feature Retrieval**: Uses Feast to assemble training examples.
- **Training Pipeline**: Uses Kubeflow Pipelines for data loading, splitting, training, and evaluation.
- **Run Tracking**: Uses MLflow to record model artifacts, parameters, metrics, and data versions.
- **Evaluation**: Compares a heuristic baseline, a batch-only model, and a batch-plus-streaming model on held-out time periods.

### Serving and Retrieval

- **Feature Materialization**: Updates the online feature store from offline tables and streaming jobs.
- **Model Serving**: Uses KServe to expose the trained model to applications and agents.
- **Alert Path**: Flink features go to a separate scorer, which calls the model, applies the minimum-cash policy, and exposes alerts through an API.
- **Text Pipeline**: Ingests documents, creates chunks and embeddings in Feast, and records lineage and validation in DataHub.

## Layer 3: Agent Platform

The agent platform provides governed access to project data, documents, and predictions through MCP tools and multi-agent coordination. The [Layer 3 design](docs/Agent-Platform.md) describes the architecture.

### Agent Components

- **Model Server**: Hosts the LLM and exposes its inference API.
- **Data and ML Tools**: Provides controlled MCP access to structured data, retrieved documents, and model predictions.
- **Coordinator**: Routes work between specialist agents.
- **Gateway**: Applies routing, authentication, rate limiting, and PII safety policies to model, tool, and agent requests.
- **Registry**: Publishes available agents and their interfaces.
- **Prompt Management**: Versions prompts used by deployed agents.

## Infrastructure and Operations

### Local Deployment

Docker packages services and jobs. Docker Compose groups local services by function.

- **Core Services**: Runs PostgreSQL, SeaweedFS, Hive Metastore, and Trino.
- **Batch Compute**: Runs Spark and the service used to connect dbt to Spark.
- **Streaming**: Runs Kafka and Flink.
- **Orchestration**: Runs Airflow.
- **Governance**: Runs DataHub and its supporting services.

Profiles control which groups start together. Container builds are reviewed for image size and required dependencies.

### Deployment and Service Operation

- **Infrastructure as Code**: Uses Terraform and Ansible for infrastructure provisioning and service configuration.
- **CI/CD**: Tests, builds, and deploys pipelines, streaming jobs, tools, and agents.
- **Routing and Security**: Configures gateways, authentication, HTTPS, rate limits, and centralized secrets.
- **Observability**: Collects service metrics, logs, traces, and model and agent telemetry.
- **Testing**: Checks data correctness, rerun behavior, APIs, and performance under load.

See [Infrastructure and Operations](docs/IaC.md) for container profiles, ports, memory limits, and operating commands.

## Getting Started

Run project commands from the repository directory in Ubuntu WSL. Read [Infrastructure](docs/IaC.md) for service setup and operating details.

**Runs on:** Ubuntu 24.04 on WSL 2 (Windows 11), Intel Core i7-12700H (20 threads), 24 GiB RAM ([development environment](docs/IaC.md#2-development-environment)).

1. Start the core services (PostgreSQL, SeaweedFS, Hive Metastore, and Trino).

   ```bash
   docker compose --project-directory infra up -d postgres seaweedfs hive-metastore trino
   ```

2. Initialize storage buckets and databases.

   ```bash
   docker compose --project-directory infra --profile init run --rm bootstrap
   ```

3. Verify core service integration.

   ```bash
   docker compose --project-directory infra --profile tools run --rm tools
   ```

4. Start the Spark cluster.

   ```bash
   docker compose --project-directory infra --profile compute up -d spark-master spark-worker
   ```

5. Generate a small input dataset and validate its source contracts.

   ```bash
   .venv/bin/python -m data.generator run --config data/generator/configs/small.yaml --output local/sample_run
   .venv/bin/python -m data.generator validate local/sample_run
   ```
