# Data and AI Platform for TPC-DI Financial Data

**Contents**

[Overview](#overview) | [Repository Structure](#repository-structure) | [Business Domain and Data](#business-domain-and-data) | [Layer 1: Data Platform](#layer-1-data-platform-1) | [Layer 2: Machine Learning Platform](#layer-2-machine-learning-platform-1) | [Layer 3: Agent Platform](#layer-3-agent-platform-1) | [Infrastructure and Operations](#infrastructure-and-operations) | [Getting Started](#getting-started)

## Overview

This project is a data and AI system for a retail brokerage, based on the TPC-DI domain. It generates related financial records, processes them through batch and streaming pipelines, and uses the resulting data for analytics, machine learning, and agent applications.

The system has three layers: **Data**, **ML**, and **Agent**.

### Layer 1: Data Platform

The data platform turns source data into validated tables for analytics and machine learning.

- **Data Generation**: Produces TPC-DI-based source files and events, with configurable volume and injected data problems.
- **Integration & Storage**: Stores batch files in staging and lakehouse tables on SeaweedFS. Hive Metastore records schemas and locations, Trino provides SQL access, and PostgreSQL holds the gold model (proposed).
- **Processing**: Uses Spark for batch ingestion and transformation, dbt for SQL modeling, and Kafka with Flink for streaming.
- **Orchestration & Governance**: Uses Airflow to schedule pipelines and coordinate validation and data quality using Soda. DataHub catalogs tables and records lineage and validation results.

### Layer 2: Machine Learning Platform

The ML platform forecasts future net account cash movement and applies a tier-based minimum-cash policy for the firm.

- **Feature Store**: Uses Feast to retrieve historical features for training and current features for inference.
- **Training & Tracking**: Uses Kubeflow Pipelines to run training and evaluation. MLflow records models, data references, parameters, and metrics.
- **Model Serving**: Uses KServe to expose the trained model through an inference endpoint.
- **Text Processing & Retrieval**: Ingests documents, splits them into chunks, and stores embeddings through Feast for retrieval.

### Layer 3: Agent Platform

The agent platform connects language models to project data and ML services. User task and tool contracts: open.

- **LLM Serving**: Hosts the language model and exposes its inference API.
- **Agents & Tools**: Retrieves structured data, searches documents, and calls the prediction model.
- **Routing & Access**: Uses gateways for model, MCP tool, and agent-to-agent requests, with access policies.
- **Agent Management**: Registers available agents and coordinates work between them.

## Repository Structure

```text
coursework/
├── generator/        # Source-data generator, tests, reference fixtures
├── pipelines/        # Data-processing jobs
├── dbt/              # dbt configuration and warehouse transformations
├── orchestration/    # Airflow configuration and DAGs
├── governance/       # DataHub ingestion and metadata configuration
├── infra/            # Docker Compose files, container builds, service configuration
├── tests/            # Pipeline tests
├── docs/             # Data specification, design, infrastructure, status, research
└── misc/             # Tools, temporary work, archives, reference data
```

The [documentation guide](docs/Documentation-Guide.md) maps each document to its purpose.

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

### Source Files

The project ingests all 18 TPC-DI business and reference source files, plus `BatchDate.txt`. Metadata used only for benchmark audits is excluded.

| Format | Files |
| --- | --- |
| XML | `CustomerMgmt.xml` |
| Pipe-delimited | `Customer.txt`, `Account.txt`, `Trade.txt`, `TradeHistory.txt`, `CashTransaction.txt`, `HoldingHistory.txt`, `WatchHistory.txt`, `DailyMarket.txt`, and six reference files |
| Comma-delimited | `HR.csv`, `Prospect.csv` |
| Fixed-width | `FINWIREyyyyQq`, one file per quarter |

### Load Cycle

- **Batch 1 (Historical)**: Full extracts, SCD2 history, and opening cash balances and holdings. History length: open.
- **Batch 2+ (Daily)**: One day of CDC changes, new activity, and the full prospect snapshot per batch, up to one year.
- **Cash Stream**: After Batch 1, cash transactions arrive through Kafka and Flink instead of daily files.

### Gold Warehouse

Gold keeps the TPC-DI warehouse: SCD2 dimensions for customers, accounts, brokers, companies, and securities; a trade table updated in place; and facts for cash balances, holdings, market history, and watches. The [ERD](docs/Data-Specification.md#gold-layer-entity-relationships) shows all 19 tables.

## Layer 1: Data Platform

### Data Generation

The generator produces related source records using the TPC-DI specification and existing dataset as references. Predictive account behavior is designed together with the cash forecast task, while keeping accounts, trades, cash, and holdings consistent (behavior model open).

- **Scale**: Supports small development runs and at least 100 GB of offline data.
- **Relationships**: Preserves referential integrity and consistent entity histories.
- **Load Patterns**: Produces historical data and daily changes.
- **Offline Problems**: Injects skew, schema evolution, and an additional problem such as duplicates.
- **Streaming Problems**: Injects late arrivals and an additional problem such as duplicate events.
- **Inspection**: Measures output volume, cardinality, skew, schema variants, duplicate rates, and event delays.

Implementation references include the [Databricks TPC-DI generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/main/src/tools/tpcdi_gen).

### Integration and Storage

| Zone | Purpose | Storage |
| :--- | :--- | :--- |
| **Staging** | Raw batch files, unchanged. The cash stream enters through Kafka instead. | SeaweedFS (object storage) |
| **Bronze** | Source records and received events as delivered, including duplicates and parse failures, with provenance. | SeaweedFS (lakehouse tables) |
| **Silver** | Cleaned, typed, deduplicated, and conformed records. | SeaweedFS (lakehouse tables) |
| **Gold** | The warehouse, cash stream outputs, and ML tables. | PostgreSQL (proposed) |

Hive Metastore records lakehouse table schemas and file locations. Spark processes the tables, and Trino provides SQL access for inspection and queries. Table format: open.

### Batch Processing

Spark handles batch source parsing and integration. dbt is planned for SQL modeling. Division of work between Spark, dbt, PostgreSQL, and Trino: open.

- **Source Parsing**: Reads the different file formats and accounts for records that cannot be parsed.
- **Cleaning and Integration**: Applies types, handles duplicates and daily changes, and validates relationships.
- **Historical Modeling**: Applies SCD2 rules to the history-tracking dimensions.
- **Warehouse Modeling**: Builds related dimensions and facts with defined grains.
- **Feature Computation**: Retains historical feature values with event and availability timestamps.
- **Baseline Comparison**: Spark task metrics, file layouts, and query plans before and after each optimization.

### Streaming Processing

Each cash transaction after Batch 1 is one Kafka event with a stable event ID. Flink validates and deduplicates the events and keeps account cash state.

- **Bronze**: Received events, including duplicates.
- **Silver**: Unique cash events.
- **Gold**: `FactCashBalances` daily balances, running balances, and window aggregates.

Window sizes, lateness limits, and correction rules: open. The [data specification](docs/Data-Specification.md#71-cash-streaming-kafka-and-flink) defines the event fields and outputs.

### Orchestration and Governance

Airflow coordinates three pipelines. Each includes processing and validation stages.

| Pipeline | Scope |
| :--- | :--- |
| **DP1** | Parses source files into bronze tables and validates ingestion. |
| **DP2** | Transforms bronze into silver and gold, with validation of the resulting tables. |
| **DP3** | Computes and validates offline feature tables. |

Airflow connections and variables provide shared configuration. DataHub records the tables each pipeline reads and writes, together with validation results.

## Layer 2: Machine Learning Platform

The ML platform connects feature computation, training, and serving in a reproducible workflow.

### Prediction Task

The model predicts total future net cash movement for each account over a chosen horizon. The policy adds that forecast to recorded cash and flags a projected balance below the minimum configured for the account owner's customer tier.

```text
projected_cash = current_cash + forecast_net_movement
alert = projected_cash < configured_minimum[customer_tier]
```

TPC-DI supplies tiers 1, 2, and 3; the project sets the minimum amounts. Horizon, scoring cadence, features, model, and metrics: open. Obligations and payment coverage: future work.

### Features and Training

- **Offline Features**: Timestamped feature history for training and backfills.
- **Online Features**: Latest eligible feature values for inference. Batch and streaming jobs can produce features for either use.
- **Point-in-Time Rule**: Training uses only values available before the prediction time, including for late records and corrections.
- **Feature Retrieval**: Uses Feast to assemble training examples.
- **Training Pipeline**: Uses Kubeflow Pipelines for data loading, splitting, training, and evaluation.
- **Run Tracking**: Uses MLflow to record model artifacts, parameters, metrics, and data versions.
- **Evaluation**: A simple baseline against the selected model on held-out periods and generator seeds.

### Serving and Retrieval

- **Feature Materialization**: Updates the online feature store from offline tables and streaming jobs.
- **Model Serving**: Uses KServe to expose the trained model to applications and agents.
- **Alert Path (proposed)**: Flink features go to a separate scorer, which calls the model, applies the minimum-cash policy, and keeps alerts for an API.
- **Text Pipeline**: Ingests documents, creates chunks and embeddings, and records lineage and validation.

## Layer 3: Agent Platform

The agent platform provides access to project data, documents, and predictions through defined tools. User task, expected output, and tool permissions: open.

### Agent Components

- **Model Server**: Hosts the LLM and exposes its inference API.
- **Data and ML Tools**: Provides controlled access to structured data, retrieved documents, and model predictions.
- **Coordinator**: Routes work between agents.
- **Gateway**: Applies routing and access policies to model, tool, and agent requests.
- **Registry**: Publishes available agents and their interfaces.
- **Prompt Management**: Versions prompts used by deployed agents.

Framework, language model, tool contracts, and coordination rules: open.

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

The coursework also requires two demonstrated novel ideas; the [project design](docs/Project-Design.md#coursework-requirements) records the requirements.

## Getting Started

Run project commands from the repository directory in Ubuntu WSL. These commands operate the existing platform and batch pipeline. Read [Project Status](docs/Status.md) for its limits and [Infrastructure](docs/IaC.md) for service setup.

1. Start the core services.

   ```bash
   make up-core
   ```

2. Initialize storage and databases.

   ```bash
   make bootstrap
   ```

3. Check core service integration.

   ```bash
   make verify
   ```

4. Start Spark.

   ```bash
   make up-compute
   ```

5. Generate the small input dataset. This command replaces `misc/sample_run`.

   ```bash
   make generate
   ```

6. Run the existing batch pipeline. Its default overwrite mode removes earlier silver data; see [the command's scope and limits](docs/IaC.md#1-how-to-run-it).

   ```bash
   make dp1
   ```
