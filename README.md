# Brokerage Data and AI Platform

**Contents**

[Overview](#overview) | [Repository Structure](#repository-structure) | [Business Domain and Data](#business-domain-and-data) | [Layer 1: Data Platform](#layer-1-data-platform) | [Layer 2: Machine Learning Platform](#layer-2-machine-learning-platform) | [Layer 3: Agent Platform](#layer-3-agent-platform) | [Infrastructure and Operations](#infrastructure-and-operations) | [Getting Started](#getting-started)

## Overview

This project is a data and AI system for a retail brokerage, based on the TPC-DI domain. It generates related financial records, processes them through batch and streaming pipelines, and uses the resulting data for analytics, machine learning, and agent applications.

- **Layer 1 ([Data Platform](docs/Data-Platform.md))**: Turns source files and streaming cash events into validated lakehouse and warehouse tables, using SeaweedFS, Hive Metastore, Trino, PostgreSQL, Spark, dbt-postgres, Kafka, Flink, Airflow, GX Core, and DataHub.
- **Layer 2 ([Machine Learning Platform](docs/ML-Platform.md))**: Predicts whether an account closes within 30 days and applies a tier-based broker outreach policy, using Feast, Kubeflow Pipelines, MLflow, and KServe, with document retrieval for supporting brokerage information.
- **Layer 3 ([Agent Platform](docs/Agent-Platform.md))**: Connects language models to project data, retrieved documents, and model predictions through gateways, MCP tools, and agent coordination.

The sections below specify the target system. The current generator proof of concept (POC) covers the source families with simplified histories; the full business simulation and platform pipelines remain to be built. Getting Started uses the existing POC commands, not a future interface.

## Repository Structure

The target layout groups code into four platform pillars. The source department has separate generator and publisher components. The current POC remains in `data/generator/` until implementation moves it.

```text
.
├── data/                  # Layer 1: Data platform
│   ├── data_source/       # Simulated source department
│   │   ├── generator/     # Business behavior, attrition signal, source recording
│   │   └── publisher/     # File and event delivery
│   ├── spark/             # DP1, Silver, and PostgreSQL loads
│   ├── dbt/               # PostgreSQL Gold models
│   ├── flink/             # Cash landing and live processing
│   ├── airflow/           # Pipeline scheduling
│   ├── gx/                # Great Expectations checks
│   └── datahub/           # Metadata and lineage
├── ml/                    # Layer 2: feature_repo, training, kfp, serving, rag
├── agents/                # Layer 3: mcp, coordinator, gateway, prompts, registry
├── infra/                 # Containers, cloud IaC, CI/CD, security, observability
├── docs/                  # Five public system documents and source references
└── workflow/              # Engineering guides, working records, verification tools
```

Each main section below maps to one detailed document: [Data Specification](docs/Data-Specification.md), [Data Platform](docs/Data-Platform.md), [ML Platform](docs/ML-Platform.md), [Agent Platform](docs/Agent-Platform.md), and [Infrastructure](docs/IaC.md).

## Business Domain and Data

The project uses the TPC-DI retail brokerage domain and all 18 business and reference source families. The [data specification](docs/Data-Specification.md) defines source formats and integration rules. The simulated source department publishes files and cash events; the platform pulls the files and consumes the stream.

### Business Domains & Origin Systems

| Business Domain | Origin System | Business Context |
| --- | --- | --- |
| **Customers & Accounts** | CMS history and daily OLTP extracts | Customer profiles, demographics, and brokerage accounts managed by brokers |
| **Brokers** | Internal HR database | Employees with job code `314` |
| **Trading & Holdings** | OLTP trading database | Trade submission, completion or cancellation, and resulting holdings |
| **Cash** | OLTP trading database | Deposits, withdrawals, trade settlements, and dividends |
| **Market** | OLTP trading database | Daily price and volume for each security |
| **Watch Lists** | OLTP trading database | Securities that customers watch over time |
| **Prospects** | External marketing provider | Potential customers, with some overlap with existing customers |
| **Companies & Securities** | Financial newswire, FINWIRE | Companies, issued securities, and quarterly financial reports |
| **Reference Data** | Static reference files | Calendar, time, industries, statuses, tax rates, and trade types |

### Physical Source Files & Ingestion Rules

The target source contract excludes `BatchDate.txt` and benchmark-only audit files and tables. Initial files use standard names under `initial/`, such as `initial/Trade.txt`. Daily files use `daily/YYYYMMDD/<Source>_YYYYMMDD.ext`, such as `daily/20190102/Customer_20190102.txt`. FINWIRE retains `FINWIREyyyyQq` in the folder for its release date.

A pipeline-side expected-file list defines completeness. An empty expected file means no activity; a missing expected file means the delivery is incomplete. Daily OLTP rows carry `CDC_FLAG` (`I` for insert, `U` for update) and `CDC_DSN` for change order. Initial files omit these CDC fields.

| Origin | Source File(s) | Format | Grain | Initial Load | Daily Period | CDC Rules |
| --- | --- | --- | --- | :---: | :---: | --- |
| CMS | `CustomerMgmt.xml` | XML | One action on a customer and its accounts | Yes | No | Ordered actions |
| OLTP | `Customer.txt` | Pipe-delimited | One customer change | No | Daily | `I`, `U` |
| OLTP | `Account.txt` | Pipe-delimited | One account change | No | Daily | `I`, `U` |
| Trading | `Trade.txt` | Pipe-delimited | One trade state or change | Yes | Daily | `I`, `U` |
| Trading | `TradeHistory.txt` | Pipe-delimited | One trade status change | Yes | No | Initial history |
| Trading | `CashTransaction.txt` | Pipe-delimited | One cash movement | Yes | Kafka | Stable `CT_ID` across channels |
| Trading | `HoldingHistory.txt` | Pipe-delimited | One holding change | Yes | Daily | `I` |
| Trading | `WatchHistory.txt` | Pipe-delimited | One watch action | Yes | Daily | `I` |
| Trading | `DailyMarket.txt` | Pipe-delimited | One security per trading day | Yes | Daily | `I` |
| HR | `HR.csv` | Comma-delimited | One employee | Yes | No | Static extract |
| Marketing | `Prospect.csv` | Comma-delimited | One prospect | Yes | Monthly | Full snapshot |
| FINWIRE | `FINWIREyyyyQq` | Fixed-width | One company, security, or financial report record | Yes | Quarterly release | Posting order |
| Reference | `Date.txt`, `Time.txt`, `Industry.txt`, `StatusType.txt`, `TaxRate.txt`, `TradeType.txt` | Pipe-delimited | One code, day, or second | Yes | No | Static |

### Load Cycle & Processing Strategy

TPC-DI defines an initial load of historical data followed by two daily batches. This project extends the daily period to seven years and sends cash through Kafka instead of daily cash files.

| Phase | Path or channel | Reference coverage | Work |
| --- | --- | --- | --- |
| **Initial load** | `initial/` in `upstream` | `initial_start` = 2014-01-01 through `initial_end` = 2018-12-31 | Load history and references, build SCD2 versions, calculate balances and holdings |
| **Daily delivery** | `daily/YYYYMMDD/` in `upstream` | `daily_start` = 2019-01-01 through `daily_end` = 2025-12-31 | Apply changes and activity, monthly prospects, and FINWIRE on release dates |
| **Cash stream** | `brokerage.cash_transaction.raw` | Daily-period cash | Land unique movements, update live balances, build cash windows and features |

- **Run dates**: All four parameters are inclusive UTC dates. `daily_start` must be the day after `initial_end`; invalid or nonadjacent periods are rejected.
- **Calendar**: Each calendar date has a delivery, including weekends and holidays, for 2,557 dates in the reference daily period. `data_date` is the represented business date, not extraction or arrival time. Trading follows NYSE holidays, with session hours simplified to 09:30 to 16:00 UTC.
- **Cadence**: OLTP changes except cash and daily market extracts arrive each day. Prospects arrive as a full snapshot on the first day of each month; the initial snapshot represents `initial_end`. FINWIRE releases each quarter on the last day of the following month. HR and references load once.
- **FINWIRE time**: Q1 results released April 30 go in a Q2 posting file but remain Q1 inside `financial`. Reports begin five quarters before the quarter containing `initial_start`, so four released quarters are available from the first initial-period day.
- **Handoff**: Flink starts from final initial-period Gold balances and does not replay initial cash. A cash event belongs to the UTC date of `CT_DTS`; a correction affects its day and later balances.

## Layer 1: Data Platform

The data platform turns source data into validated tables for analytics and ML. The [Layer 1 design](docs/Data-Platform.md) holds the generation model, processing rules, versioning, and rationale.

### Data Zones and Silver Layer

| # | Zone | Purpose | Grain | Storage |
| --- | --- | --- | --- | --- |
| 1 | **Staging** | Immutable copy of each pulled file version | One file version | SeaweedFS `staging` bucket |
| 2 | **Bronze** | Raw source values and stream deliveries with provenance | One source record or delivery | Delta under `lakehouse/bronze/` |
| 3 | **Silver** | Typed, deduplicated, and conformed records | One business record or change | Delta under `lakehouse/silver/` |
| 4 | **Gold** | Warehouse, live cash values, and features | Per entity, event, or window | PostgreSQL `gold` schema |

Four buckets separate source ownership and recovery needs: source-owned `upstream`, platform-owned `staging`, analytical `lakehouse`, and operational `platform`. The publisher can write `upstream`; the platform can only read it. DP1 saves files as `initial/<version_id>/<source_filename>` or `daily/YYYYMMDD/<version_id>/<source_filename>` in `staging`. Each pulled file gets a version ID; retries keep it, corrections get a new one, and reruns reuse saved inputs. An ingestion record maps source keys and version information to staging paths. ETag is not guaranteed to be a content hash.

Hive Metastore resolves table names and locations. Trino provides SQL over Delta and PostgreSQL. Silver is a project layer, not a TPC-DI requirement:

- **Change order**: Preserve CDC type and sequence and keep business date distinct from event time and arrival time.
- **XML meaning**: Keep omitted and empty elements distinct until Gold applies them.
- **Deduplication**: Remove repeated deliveries without losing legitimate second updates.
- **Quality**: Keep referenced records, repair only certain values, mark uncertain fields, and quarantine unusable payloads.

### Data Lineage: Source-to-Gold Mapping

| Source Records | Transformation Logic | Gold Output |
| --- | --- | --- |
| `CustomerMgmt.xml` + `Customer.txt` | Apply actions and CDC, tax rates, and prospect matches; build daily SCD2 versions | `dim_customer` |
| `CustomerMgmt.xml` + `Account.txt` | Resolve customer and broker; propagate customer versions | `dim_account` |
| `HR.csv` | Keep job code 314 | `dim_broker` |
| FINWIRE `CMP`, `SEC` | Apply posting order and company references; propagate company versions | `dim_company`, `dim_security` |
| FINWIRE `FIN` | Keep the record's reporting period; resolve company at `PTS`; use only released reports | `financial` |
| `Trade.txt` + `TradeHistory.txt` | Insert trades and update status; retain creation-time keys | `dim_trade` |
| Initial cash file + streamed Silver cash | Load unique movements, then sum daily cash and carry balances forward | `fact_cash_transactions`, `fact_cash_balances` |
| `HoldingHistory.txt` + trades | Use `HH_AFTER_QTY` and the changing trade's keys, time, and price | `fact_holdings` |
| `DailyMarket.txt` + securities + financials | Calculate 52-week values, P/E, and yield | `fact_market_history` |
| `WatchHistory.txt` | Open on `ACTV`, close on `CNCL` | `fact_watches` |
| `Prospect.csv` + customers | Update prospects, marketing tags, and customer matches | `prospect` |

### Gold Warehouse

The warehouse contains 19 tables: TPC-DI's 19 minus `DImessages`, plus `fact_cash_transactions`. Quality results, quarantine, quality metadata, and operational load status replace the benchmark load log. Live cash and feature tables are additional outputs, not part of this count.

**SCD2 rules**:

- A natural key identifies an entity; a surrogate key identifies one version.
- A change closes the current version and inserts a new version with the new effective date.
- Multiple same-day changes collapse into one daily version.
- Facts use the dimension version valid on their event date.
- Customer changes propagate to accounts; company changes propagate to securities.

| Table | Type | Contents | Grain | Built From | Initial Load | Daily Processing |
| --- | --- | --- | --- | --- | --- | --- |
| `dim_customer` | SCD2 | Identity, contacts, tier, tax, prospect data | Customer version | XML, customer CDC, prospects | Versions from actions | New versions |
| `dim_account` | SCD2 | Owner, broker, tax status, status | Account version | XML, account CDC | Versions from actions | Changes and customer propagation |
| `dim_broker` | Dimension | Broker and office details | Broker | HR | One version | Static |
| `dim_company` | SCD2 | Company identity and attributes | Company version | FINWIRE `CMP` | Posting history | Quarterly changes |
| `dim_security` | SCD2 | Security and issuer attributes | Security version | FINWIRE `SEC` | Posting history | Changes and company propagation |
| `dim_trade` | Mutable dimension | Type, status, amounts, account, security, channel | Trade | Trades and status history | Initial state | Inserts and updates |
| `fact_cash_transactions` | Fact | Unique cash movements and related keys | Cash movement | Silver cash | Initial cash | Streamed cash loaded by DP2 |
| `fact_cash_balances` | Fact | Daily account cash | Account and day with movement | Cash movement fact | Daily balances | Built daily by dbt |
| `fact_holdings` | Fact | Quantity after a trade | Holding change | Holding history and trades | Initial history | New changes |
| `fact_market_history` | Fact | Prices, volume, 52-week values, P/E, yield | Security and day | Market, securities, financials | Initial history | New observations |
| `fact_watches` | Fact | Watch period | Watch | Watch actions | Initial history | Placements and removals |
| `prospect` | Current state | Agency records, tags, customer match | Prospect | Prospect snapshot | Initial snapshot | Monthly changes |
| `financial` | Current state | Quarterly results | Company and quarter | FINWIRE `FIN` | Released reports | Quarterly releases |
| `dim_date`, `dim_time` | Reference | Calendar and clock | Day; second | Date, time | Loaded once | Static |
| `industry`, `status_type`, `tax_rate`, `trade_type` | Reference | Lookup codes | Code | Matching source files | Loaded once | Static |

#### Gold Layer Entity Relationships

The [data specification](docs/Data-Specification.md) owns Gold relationships and integration rules. Cash movements link to account, customer, date, and time, to trades for settlements, and to securities for dividends. Prospect matching uses names and addresses rather than a source foreign key.

![Gold warehouse ERD with 19 tables and their main relationships.](docs/diagrams/data-specification/gold-erd.png)

*Gold warehouse (`gold` schema), reference model rather than a final physical schema.*

### Data Generation

The source department simulates a mid-size firm day by day. Four generation layers separate business behavior, hidden attrition signal, source recording, and delivery. The generator implements the first three; the publisher implements delivery.

- **Scale**: Factor 100 represents about 330,000 customers and 550,000 accounts at cutover, with about 8% annual net account growth. One unit targets about 1 GB of offline files over the full reference calendar. Market and reference data remain fixed; dates are separate settings.
- **Volume**: Generate the large dataset once and measure at least 100 GB of offline source files. The estimated 18 GB cash stream is separate. Each experiment processes only the scale and dates needed to show its effect.
- **Behavior**: Five hidden profiles and wealth levels produce trading and cash patterns. Completed trades reconcile with cash and holdings. About 20% of accounts use margin; no account sells securities it does not hold.
- **Attrition**: About 3% of accounts close annually. Most closures follow a hidden 1 to 8 week leaving period; look-alike outflows keep a simple rule from solving the task alone.
- **Source problems**: TPC-DI recording errors remain separate from skew and the added `T_CHANNEL` schema change, effective 2019-07-01 by default.
- **Delivery**: The clean publisher comes first. The realistic profile adds retried extract blocks, corrected and late files, late events, and duplicate cash deliveries without regenerating business data.
- **Measurement**: The local generator summary records settings, output counts, hidden traits, and measured characteristics. A separate publisher report measures delivery problems. Ingestion reads neither report.

The [generation design](docs/Data-Platform.md#2-data-generation-and-problem-injection) includes a causal chain, a worked example, parameter tables, measurement bases, expected results, and reference-generator corrections.

### Batch Processing

Spark parses files and builds Silver. It pushes the date's required Silver rows into a PostgreSQL load schema; dbt-postgres builds Gold there.

- **Parsing**: Preserve delivered text and quarantine unusable rows.
- **Cleaning**: Apply types, CDC order, deduplication, and relationships.
- **Modeling**: Build daily SCD2 history and warehouse tables with defined grains.
- **Features**: DP3 produces non-cash features; Flink produces cash features. Each feature has one producer.
- **Experiments**: Compare baseline and handled Spark jobs for skew, schema evolution, and duplicates. Compare lakehouse layouts and warehouse indexes using measured runtime and query plans.
- **Corrections**: Keep every input version and rebuild affected dates and later dependent outputs in order. Repeated writes must leave the same logical result.

### Streaming Processing

Two Flink 2.2 jobs process daily-period cash. Job 1 lands raw deliveries in Bronze, validates and deduplicates by `CT_ID`, and writes Silver and a clean topic. Job 2 consumes clean events, updates live tables, produces cash features, and sends large-withdrawal rescoring requests.

The table uses original source-field names. JSON messages use lowercase keys such as `ct_id` and `ct_amt`; the [cash contract](docs/Data-Specification.md#71-cash-streaming-kafka-and-flink) gives the mapping and wire representations.

| Source Field or Metadata | Type or Role | Meaning |
| --- | --- | --- |
| `CT_ID` | Global source sequence | Stable cash movement ID, also first field of the initial cash file |
| `CT_CA_ID` | `NUM(11)` | Account ID |
| `CT_DTS` | `DATETIME` | UTC event time |
| `CT_AMT` | Signed decimal text in JSON | Positive adds cash; negative removes cash |
| `CT_NAME` | `CHAR(100)` | Fixed description parsed into cash type and trade link |
| Delivery time | Kafka timestamp | Arrival time for lateness measurement |

| Output | Zone | Grain | Writer |
| --- | --- | --- | --- |
| Raw deliveries | Bronze | Delivery | Job 1 |
| Unique cash | Silver | Movement | Job 1 after the initial period |
| Cash movements and daily balances | Gold warehouse | Movement; account and date | dbt in DP2 |
| Live cash | Gold live | Account | Job 2 |
| Hourly windows | Gold live | Account and hour | Job 2 |
| Cash features | Feature topic to Feast | Account and feature time | Job 2 and Feast push job |

- **Start**: Job 2 initializes from final initial-period account balances using natural account IDs.
- **Windows**: Watermarks trail observed event time by 30 seconds. Allowed lateness is 10 minutes; later events enter a late-events table.
- **Recovery**: Jobs checkpoint every 30 seconds. Kafka transactions, Delta checkpoint commits, and full-total PostgreSQL upserts support repeatable recovery.
- **Reconciliation**: DP2 builds daily cash facts from Silver after stream progress passes the day boundary plus 10 minutes. A later event requires correction from its day forward; a progress marker cannot prove that all late events have arrived.

### Orchestration and Governance

Airflow 3 with `LocalExecutor` coordinates three pipelines. Each has an ingest stage and GX Core validation that reports to DataHub.

| Pipeline | Scope |
| --- | --- |
| **DP1** | Pull `upstream` to versioned `staging`, then parse Bronze |
| **DP2** | Bronze to Silver, Silver to PostgreSQL load schema, then dbt Gold |
| **DP3** | Build and validate non-cash feature tables |

Dates run in order, initially with one active run per pipeline. Independent tables can proceed within a date; a late file holds only its dependent tables. `ops.load_status` and a completeness view identify partial dates. DataHub records catalog metadata, lineage, quality results, and incidents; Hive Metastore remains the engine catalog.

## Layer 2: Machine Learning Platform

The ML platform connects feature computation, training, inference, and document retrieval. The [Layer 2 design](docs/ML-Platform.md) owns the task and feature contracts.

### Prediction Task

The model predicts whether an open account closes within 30 days. A separate policy creates an outreach task for the account's broker when closure probability reaches the customer's tier threshold. Tier 1 uses a lower probability threshold than tier 3. Changing policy thresholds does not require model retraining.

| Data | Grain | Contents |
| --- | --- | --- |
| **Offline features** | Account and feature time | Feature history for training and backfills |
| **Online features** | Account | Latest eligible feature values |
| **Label** | Account and prediction time | Whether the account closes within the next 30 days, from account status history |
| **Tier policy** | Customer tier | Probability threshold and unknown-tier behavior |
| **Outreach task** | Flagged account | Broker action based on closure risk |

Daily scoring covers all open accounts after DP3. A large withdrawal triggers rescoring for one account. A broker can also request inference through the agent. The earlier minimum-cash forecast was replaced because normal withdrawal controls already prevent spending beyond available cash or permitted borrowing.

### Features and Training

- **Non-cash features**: DP3 derives tenure, tier, trading, holdings, watches, and account changes from Gold.
- **Cash features**: Flink derives cash movement windows, external net flow, days since deposit, and current cash, then a push job sends features to Feast's offline and online stores.
- **Consistency**: Each feature has one definition and producer. Point-in-time training must exclude information not available at prediction time.
- **Feature retrieval**: Feast assembles training examples. Creation time needs explicit filtering and store support; a `created` field alone does not enforce availability.
- **Training**: Kubeflow Pipelines coordinates training and evaluation; MLflow records model artifacts, parameters, metrics, and the full training dataset version.
- **Evaluation**: Compare the model with a simple outflow rule. Comparing models with and without cash features can show the contribution of streaming inputs.

### Serving and Retrieval

- **Materialization**: Airflow materializes non-cash feature tables to Feast's online store; the push job supplies cash features.
- **Inference**: KServe exposes model inference for applications and agents.
- **Outreach**: Closure probability and tier policy determine a broker task; the model does not set the policy.
- **Text pipeline**: Ingests brokerage documents, creates chunks and embeddings, and records lineage and validation.

## Layer 3: Agent Platform

The agent platform provides controlled access to project data, documents, and predictions through MCP tools and multi-agent coordination. The [Layer 3 design](docs/Agent-Platform.md) describes the architecture.

### Agent Components

- **Model server**: Hosts the language model and exposes inference.
- **Data and ML tools**: Provide controlled access to account data, retrieved documents, and account attrition inference.
- **Coordinator**: Routes work between specialist agents.
- **Gateway**: Applies routing, authentication, rate limits, and personal-data safety policies.
- **Registry**: Publishes agents and their interfaces.
- **Prompt management**: Versions deployed prompts.

## Infrastructure and Operations

### Local Deployment

Docker packages services and jobs. Compose groups services so Layer 1 can run within the WSL memory cap. The starting Spark 3.5 and Delta 3.3 combination, Flink 2.2 with the Delta 4.4 connector, and Trino must pass a shared-table test on SeaweedFS before pipeline implementation. Spark 4 with Delta 4 is the fallback.

- **Core services**: PostgreSQL, SeaweedFS, Hive Metastore, Trino.
- **Batch compute**: Spark, with dbt-postgres operating in PostgreSQL rather than through Spark Thrift.
- **Streaming**: Kafka, Kafbat UI, and two Flink jobs.
- **Orchestration**: Airflow 3 with GX Core.
- **Governance**: DataHub and supporting services.

Target resource names use project identity `brokerage` and scoped role names. Compose generates container, network, and volume names; service keys stay bare role names. Bucket role names remain unchanged, topics use `brokerage.*`, and TPC-DI source names remain intact.

### Deployment and Service Operation

- **Infrastructure as code**: Terraform provisions infrastructure; Ansible configures services.
- **CI/CD**: Tests, builds, and deploys pipelines, jobs, tools, and agents.
- **Security**: Gateway access, HTTPS, rate limits, centralized secrets, separate service users, and parameterized SQL protect service boundaries.
- **Observability**: Metrics, logs, traces, and tool UIs support service operation; DataHub and GX report data health.
- **Testing**: Checks data correctness, recovery, safe reruns, APIs, and performance under load.

[Infrastructure and Operations](docs/IaC.md) owns service setup, resource limits, and operating details.

## Getting Started

Run commands from the repository directory in Ubuntu WSL. These commands use the existing Compose files and generator POC; they do not implement the target publisher, pipeline jobs, or renamed resource identities. The [infrastructure guide](docs/IaC.md) separates current configuration from target deployment.

**Runs on:** Ubuntu 24.04 on WSL 2 (Windows 11), Intel Core i7-12700H (20 threads), 24 GiB RAM ([development environment](docs/IaC.md#2-development-environment)).

1. Start the current core services.

   ```bash
   docker compose --project-directory infra up -d postgres seaweedfs hive-metastore trino
   ```

2. Initialize the current storage buckets and databases.

   ```bash
   docker compose --project-directory infra --profile init run --rm bootstrap
   ```

3. Run the current core service integration check.

   ```bash
   docker compose --project-directory infra --profile tools run --rm tools
   ```

4. Start the current Spark cluster.

   ```bash
   docker compose --project-directory infra --profile compute up -d spark-master spark-worker
   ```

5. Generate and validate a small POC dataset. Choose an unused output path because the runner refuses to overwrite an existing run.

   ```bash
   .venv/bin/python -m data.generator run --config data/generator/configs/small.yaml --output local/sample_run
   .venv/bin/python -m data.generator validate local/sample_run
   ```

   The POC writes its existing `Batch1` and later batch layout. Its current configuration and output do not yet implement the four-date target contract or the `initial/` and `daily/` delivery paths.
