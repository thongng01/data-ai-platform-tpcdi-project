# Data and AI Platform for TPC-DI Financial Data

This project is a data and AI system for a retail brokerage firm / securities company, modeled on the TPC-DI domain to approximate realistic financial data complexity. The architecture comprises three layers: Layer 1 (Data), Layer 2 (ML), and Layer 3 (Agent).

[Diagram: High Level System Architecture showing the three layers and data flow]

### Layer 1: Data Platform
- **Data Generation**: Produces raw source files based on an adapted subset of the TPC-DI specification, with configurable scale factors and injectable data quality problems.
- **Integration & Storage**: Ingests multi-format source data into a governed lakehouse and warehouse, progressing through landing, bronze, silver, and gold zones.
- **Processing**: Resolves data quality issues using batch processing (Spark/dbt) and continuous streaming (Flink).
- **Orchestration & Governance**: Schedules pipelines via Airflow, enforcing ingestion and validation stages, while tracking data lineage and quality metrics in a centralized catalog.

### Layer 2: ML Platform
- **Feature Store**: Serves point-in-time correct features for customers, accounts, and markets, strictly separating event timestamps from creation timestamps to prevent data leakage.
- **Model Lifecycle**: Trains, deploys, and continuously monitors predictive models using the engineered features from the Gold layer.

### Layer 3: Agent Platform
- **LLM Agents**: Executes large language model agents capable of reasoning over the firm's structured and unstructured data.
- **Tool Integration**: Connects agents to the Gold Layer and Feature Store via secure tools (e.g., Text-to-SQL) to retrieve governed context.
- **Task Execution**: Automates complex business workflows, such as prospect matching, account summarization, and market analysis.

# Repository Structure

| Directory | Purpose |
| :--- | :--- |
| `pipelines/` | Layer 1 data processing code. Contains Spark jobs and dbt models. |
| `generator/` | Source data generator. Produces raw files and injects data problems. |
| `orchestration/` | Airflow configuration. Contains DAGs, connections, and variables. |
| `infra/` | Infrastructure as code. Contains Docker Compose files and container configurations. |
| `docs/` | Documentation. Contains architecture decision records and status reports. |
| `tests/` | Test suites. Contains pipeline contract tests and generator tests.


# Business Domain and Data Specification

TPC-DI models a retail brokerage firm. The firm's functional structure consists of a **front office** and a **back office**, which is reflected in its data composition, along with a firm-wide reference source:

### 1. Front Office (Client-Facing Operations)
Generates high-volume, time-sensitive data related to direct client interaction and market activity.
* **Trading and Cash Operations**: Executed trades, trade history, and cash movements.
* **CRM and Client Portal**: Customer profiles, account details, and personal watchlists.
* **External Data Feeds**: Daily market prices, volumes, and external prospect lists.

### 2. Back Office (Internal Operational Support)
Generates lower-volume, high-accuracy data related to internal firm operations and post-trade processing.
* **Custody and Portfolio Accounting**: Historical holding quantities and post-trade settlement records.
* **Human Resources**: Employee records, licensed broker assignments, and branch office mappings.

### 3. Reference Data (Firm-Wide Golden Source)
Standalone, static, or slowly changing foundational data. This domain is not owned by the front or back office; it is a shared vocabulary consumed by all systems to ensure referential integrity.
* **Calendar and Time**: `Date.txt`, `Time.txt`
* **Classifications and Codes**: `Industry.txt`, `StatusType.txt`, `TradeType.txt`
* **Regulatory**: `TaxRate.txt`

## Data Ingestion and Processing Schedule

The system routes source files to specific processing engines based on their arrival pattern, update frequency, and execution schedule.

| Processing Engine | Source Files | Schedule / Interval | Mechanism |
| :--- | :--- | :--- | :--- |
| **Batch (Spark & dbt)** | `CustomerMgmt.xml`, `HR.csv`, `FINWIRE`, Reference Data | Initial full load; then daily/periodic batches | Processes full historical loads, static reference data, and complex XML parsing. |
| **Streaming (Flink)** | `Trade.txt` (deltas), `CashTransaction.txt` (deltas), `DailyMarket.txt` | Continuous stream / daily micro-batches | Processes incremental Change Data Capture (CDC) streams and daily market feeds. |
| **Model Consumption** | Gold Layer Star Schema | Triggered post-pipeline / scheduled daily | Feeds the Feature Store for Layer 2 and provides governed query targets for Layer 3. |

## Source Data Characteristics

The data generator produces 16 specific files. It injects intentional, configurable data quality problems to test pipeline resilience.

| Data File | Format | Injected Characteristics |
| :--- | :--- | :--- |
| `Trade.txt` | Pipe-delimited | Delta records grow in field count. One status value dominates. Whole-row duplicates exist. |
| `TradeHistory.txt` | Pipe-delimited | Contains duplicate rows. |
| `CashTransaction.txt` | Pipe-delimited | Delta records grow in field count. A few accounts dominate the volume. Duplicate rows exist. |
| `CustomerMgmt.xml` | XML action stream | Payloads arrive partially filled. Point-in-time versions accumulate without explicit deletion flags. |
| `WatchHistory.txt` | Pipe-delimited | Contains duplicate rows. |
| `FINWIRE` | Fixed-width | Unchanged company records repeat across quarters. No delimiters; record type is determined by position. |
| `Prospect.csv` | Comma-delimited | Candidates match existing customers by fuzzy name and address, not by primary key. |
| `DailyMarket.txt` | Pipe-delimited | Delta records grow in field count. Contains duplicate rows. |
| `HoldingHistory.txt` | Pipe-delimited | Contains duplicate rows. |
| `HR.csv` | Comma-delimited | Standard format, full load only. |
| Reference Files | Pipe-delimited | Standard format, full load only. |

## Gold Layer Model

The Gold layer stores the final, governed decision-support model. The system uses a star schema optimized for analytical queries and feature engineering.

* **Dimensions**: `DimCustomer`, `DimAccount`, `DimCompany`, `DimSecurity`, `DimBroker`, `DimDate`, `DimTime`.
* **Facts**: `FactCashBalances`, `FactHoldings`, `FactMarketHistory`, `FactWatches`.
* **History Logic**: The system implements Slowly Changing Dimension Type 2 (SCD2) for Customer, Account, Company, Security, and Broker. An update adds a new version. The old version receives an `enddate`. The new version receives a `startdate` and an `iscurrent` flag. Spark performs this transformation because it supports the `LAST_VALUE(... IGNORE NULLS)` window function required to carry forward partial payload updates, which Postgres lacks.

# Layer 1: Data Platform

## Data Generation
The generator produces raw source files. Scale factors control dataset size. The generator injects specific data quality problems independently.

## Batch Pipelines
Spark processes the initial historical load. Pipelines move data from the landing zone to the bronze zone, then to the silver zone. dbt transforms silver to gold, making transforms reviewable and tests declarative.

## Streaming Pipeline
Flink processes incremental deltas. The job applies watermarks and windowed aggregations, handles late data via side outputs, and writes idempotently to the lakehouse.

## Orchestration and Governance
Airflow schedules batch and streaming pipelines using cron triggers. Connections and variables reside strictly within Airflow; no DAG hardcodes hostnames or credentials. Data lineage and validation results feed the data catalog.

[Diagram: Data Flow showing movement through landing, bronze, silver, and gold zones]

# Layer 2: Machine Learning Platform

Layer 2 builds and serves predictive models using processed data.

* **Feature Store**: Stores historical features for customers, accounts, and markets. Uses event timestamps and creation timestamps to prevent data leakage.
* **Model Training and Serving**: Trains, deploys, and monitors a prediction model.
* **Problem Definition**: *[To be determined. Define the target variable, feature engineering strategy, and evaluation metrics here.]*

[Diagram: Feature Flow showing data moving from Gold to Feature Store to Model]

# Layer 3: Agent Platform

Layer 3 executes large language model agents.

* **Architecture**: *[To be determined. Define the agent framework, memory management, and tool integration here.]*
* **Problem Definition**: *[To be determined. Define the business problem, the required tools, and the expected output here.]*

# Infrastructure and Operations

## Container Profiles
Five Docker Compose files isolate resources and prevent local memory swapping.

* `compose.yaml`: Core services (Postgres, SeaweedFS, Hive Metastore, Trino).
* `compose.compute.yaml`: Spark cluster and dbt Thrift server.
* `compose.streaming.yaml`: Kafka and Flink.
* `compose.orchestration.yaml`: Airflow.
* `compose.governance.yaml`: OpenSearch and DataHub.

## Lakehouse Storage
SeaweedFS acts as the lakehouse storage. Because SeaweedFS reports empty prefixes as existing directories, managed Spark table creation fails. All lakehouse tables are external.

## Services and Ports

| Service | Port | Purpose |
| :--- | :--- | :--- |
| Trino | 8080 | SQL query engine |
| Spark Master | 8082 | Cluster management and UI |
| Airflow | 8081 | Orchestration UI |
| DataHub | 9002 | Governance UI |
| Postgres | 5432 | Gold layer and metadata storage |

[Diagram: Deployment showing Docker containers, network connections, and published ports]

# Getting Started and Prerequisites

## Prerequisites
* Docker and Docker Compose
* Windows Subsystem for Linux (WSL)
* Make

## Execution Steps
Execute all commands from within WSL. The repository must reside on the WSL filesystem to utilize the WSL Docker socket.

1. Start the core services.
   ```bash
   make up-core
2. Create the S3 buckets and Postgres databases.
   ```bash
   make bootstrap
   ```
3. Run the platform smoke tests.
   ```bash
   make verify
   ```
4. Start the Spark cluster.
   ```bash
   make up-compute
   ```
5. Run the first batch pipeline.
   ```bash
   make dp1
   ```