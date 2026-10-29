# Data and AI Platform for TPC-DI Financial Data

**Contents**

[Overview](#overview) | [Repository Structure](#repository-structure) | [Business Domain and Data Specification](#business-domain-and-data-specification) | [Layer 1: Data Platform](#layer-1-data-platform-1) | [Layer 2: Machine Learning Platform](#layer-2-machine-learning-platform-1) | [Layer 3: Agent Platform](#layer-3-agent-platform-1) | [Infrastructure and Operations](#infrastructure-and-operations) | [Getting Started](#getting-started)

## Overview

This project is a data and AI system for a retail brokerage, based on the TPC-DI domain. It generates related financial records, processes them through batch and streaming pipelines, and uses the resulting data for analytics, machine learning, and agent applications.

The system has three layers: **Data**, **ML**, and **Agent**. This README describes the intended design. [Project Status](docs/Status.md) records what is implemented and verified. The [project design](docs/Project-Design.md) separates accepted decisions, proposals, and open parameters.

The first connected use case processes account cash movements through Kafka and Flink, maintains cash balances, and forecasts future net movement. A policy flags projected cash below a minimum configured for the customer's tier. Other TPC-DI sources retain their historical and daily batch paths.

### Layer 1: Data Platform

The data platform turns source data into validated tables for analytics and machine learning.

- **Data Generation:** Produces TPC-DI-based source files and events, with configurable volume and injected data problems.
- **Integration & Storage:** Stores batch files in staging and lakehouse tables on SeaweedFS. Hive Metastore records schemas and locations, and Trino provides SQL access. PostgreSQL is proposed for gold; the lakehouse table format remains open.
- **Processing:** Uses Spark for batch ingestion and transformation, dbt for SQL modelling, and Kafka with Flink for streaming.
- **Orchestration & Governance:** Uses Airflow to schedule pipelines and coordinate validation. DataHub catalogs tables and records lineage and validation results.

### Layer 2: Machine Learning Platform

The ML platform forecasts future net account cash movement and applies a tier-based minimum-cash policy.

- **Feature Store:** Uses Feast to retrieve historical features for training and current features for inference.
- **Training & Tracking:** Uses Kubeflow Pipelines to run training and evaluation. MLflow records models, data references, parameters, and metrics.
- **Model Serving:** Uses KServe to expose the trained model through an inference endpoint.
- **Text Processing & Retrieval:** Ingests documents, splits them into chunks, and stores embeddings through Feast for retrieval.

### Layer 3: Agent Platform

The agent platform connects language models to project data and ML services. Its user task and tool contracts remain to be defined in the project design.

- **LLM Serving:** Hosts the language model and exposes its inference API.
- **Agents & Tools:** Retrieves structured data, searches documents, and calls the prediction model.
- **Routing & Access:** Uses gateways for model, MCP tool, and agent-to-agent requests, with access policies.
- **Agent Management:** Registers available agents and coordinates work between them.


## Repository Structure

| Directory | Purpose |
| :--- | :--- |
| `generator/` | Contains the source-data generator, its tests, and reference fixtures. |
| `pipelines/` | Contains data-processing jobs. |
| `dbt/` | Holds the dbt configuration and is the location for warehouse transformations. |
| `orchestration/` | Holds Airflow configuration and is the location for pipeline DAGs. |
| `governance/` | Holds DataHub ingestion and metadata configuration. |
| `infra/` | Contains Docker Compose files, container builds, and service configuration. |
| `tests/` | Contains pipeline tests. |
| `docs/` | Contains design documents, implementation notes, and supporting evidence. |

The [documentation guide](docs/Documentation-Guide.md) links the detailed references and explains what belongs in each document.

## Business Domain and Data Specification

TPC-DI models data integration for a retail brokerage. The brokerage maintains customer accounts, executes trades, records cash and holdings, and receives company and market information.

The [TPC-DI reference](docs/TPC-DI/TPC-DI.md) explains the source records, warehouse model, historical and incremental loads, and transformation rules.

The following groups provide a functional view of that data.

### Front Office

The front office manages customer accounts and trading activity.

- **Customer Management:** Maintains customer profiles and account details.
- **Trading:** Records trades and their status changes.
- **Watchlists:** Records customers adding or removing securities from their watchlists.
- **Prospects:** Holds information about potential customers.

### Back Office

The back office maintains account records and supports operations.

- **Cash and Holdings:** Records account cash movements and changes to security holdings.
- **Human Resources:** Maintains employee and broker information.
- **Company and Security Data:** Integrates issuer details, security definitions, and financial records used to interpret trading activity.

### Shared Reference and Market Data

Shared sources provide codes, calendars, and market observations across the system.

- **Reference Codes:** Defines industries, statuses, trade types, and tax rates.
- **Calendar and Time:** Provides date and time records for analytical models.
- **Daily Market:** Provides security prices and trading volumes.

### Physical Source Data

The project includes all TPC-DI business and reference sources and excludes metadata used only for formal benchmark validation. Batch dates, change ordering, operation codes, and source provenance remain necessary for integration. Source files use several formats and include historical records and source-specific incremental loads.

| Source files | Format | Contents |
| :--- | :--- | :--- |
| `Trade.txt`, `TradeHistory.txt` | Delimited text | These files contain trades and trade status history. |
| `CashTransaction.txt`, `HoldingHistory.txt` | Delimited text | These files contain cash movements and holding changes. |
| `WatchHistory.txt`, `DailyMarket.txt` | Delimited text | These files contain watchlist changes and daily market observations. |
| `CustomerMgmt.xml` | XML | This file contains historical customer and account actions. |
| `Customer.txt`, `Account.txt` | Delimited text | These files contain incremental customer and account records. |
| `FINWIRE` files | Fixed-width text | These files contain company, security, and financial records. |
| `HR.csv`, `Prospect.csv` | CSV | These files contain employee and prospect records. |
| Reference files | Delimited text | These files contain codes, dates, and times. |

The [TPC-DI reference](docs/TPC-DI/TPC-DI.md) defines fields, layouts, identifiers, and batch differences.

### Data Relationships and Table Models

Customer, account, trade, security, and company records form a connected dataset. Accounts reference customers, trades reference accounts and securities, and securities reference companies.

The platform represents this data in two models:

- **Silver Model:** Provides consistently typed and integrated tables for downstream transformations.
- **Gold Model:** Retains the original TPC-DI warehouse as the baseline and adds analytical and feature tables.

The [project design](docs/Project-Design.md#project-data-flow) defines zone responsibilities. Detailed silver schemas, added gold tables, keys, and update rules remain open.

### Data Ingestion and Processing Schedule

- **Historical Load:** Establishes the initial dataset.
- **Incremental Batch Loads:** Follow the original daily source-specific patterns for sources that remain in batch.
- **Streaming Events:** Send subsequent CashTransaction movements through Kafka for continuous processing.
- **Feature Updates:** Refreshes historical feature tables and current values used for inference.

The initial history supplies opening cash balances. The boundary between that history and subsequent cash events must prevent the same movement from entering a balance twice.

## Layer 1: Data Platform

### Data Generation

The generator produces related source records using the TPC-DI specification and existing dataset as references. Predictive account behavior must be designed with the cash forecast task, while keeping accounts, trades, cash, and holdings consistent. The behavior model remains open.

- **Scale:** Supports small development runs and at least 100 GB of offline data.
- **Relationships:** Preserves referential integrity and consistent entity histories.
- **Load Patterns:** Produces historical data and incremental changes.
- **Offline Problems:** Injects skew, schema evolution, and an additional problem such as duplicates.
- **Streaming Problems:** Injects late arrivals and an additional problem such as duplicate events.
- **Inspection:** Measures output volume, cardinality, skew, schema variants, duplicate rates, and event delays.

Implementation references include the [Databricks TPC-DI generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/main/src/tools/tpcdi_gen).

### Integration and Storage

| Location | Responsibility | Storage |
| :--- | :--- | :--- |
| **Staging** | Holds original batch files before ingestion. Streaming enters through Kafka. | SeaweedFS stores the source files. |
| **Bronze** | Preserves one row per logical source record or received event, including duplicate deliveries and provenance. | SeaweedFS stores lakehouse tables; the table format remains open. |
| **Silver** | Holds typed, cleaned, deduplicated, and integrated records. | SeaweedFS stores lakehouse tables; the table format remains open. |
| **Gold** | Holds the original warehouse outputs and project analytical and feature tables. | PostgreSQL is proposed; physical placement remains open. |

Hive Metastore records lakehouse table schemas and file locations. Spark processes the tables, and Trino provides SQL access for inspection and queries.

### Batch Processing

Spark handles batch source parsing and integration. dbt is planned for SQL modelling. The exact division between Spark, dbt, PostgreSQL, and Trino remains open.

- **Source Parsing:** Reads the different file formats and accounts for records that cannot be parsed.
- **Cleaning and Integration:** Applies types, handles duplicates and incremental updates, and validates relationships.
- **Historical Modelling:** Applies TPC-DI's SCD Type 2 rules to the history-tracked dimensions.
- **Warehouse Modelling:** Builds related dimensions and facts with defined grains.
- **Feature Computation:** Retains historical feature values with event and availability timestamps.

Processing and storage changes must be compared with a baseline. Evidence includes Spark task metrics, file layouts, and query plans for the selected stores.

### Streaming Processing

The generator or a replay producer assigns a stable event ID before publishing each cash movement to Kafka. Repeated delivery keeps that ID. Kafka feeds bronze capture and Flink processing: bronze retains received deliveries, and Flink validates and deduplicates the movements.

| Output | Meaning |
| --- | --- |
| Silver cash events | These records contain unique, cleaned cash movements. |
| Gold `FactCashBalances` | These records retain the TPC-DI daily account balance calculation. |
| Project cash outputs | These records add running balances and selected event-time window aggregates. |

Flink uses event timestamps and watermarks to calculate window results. Late-event rules must define when results change and when separate correction is required. An older movement can change its day's balance and later balances. Windows, lateness limits, state retention, and physical sinks remain open in the [project design](docs/Project-Design.md#streaming-candidates).

### Orchestration and Governance

Airflow coordinates three pipelines. Each includes processing and validation stages.

| Pipeline | Scope |
| :--- | :--- |
| **DP1** | Parses source files into bronze tables and validates ingestion. |
| **DP2** | Transforms bronze into silver and gold, with validation of the resulting tables. |
| **DP3** | Computes and validates offline feature tables. |

Airflow connections and variables provide shared configuration. DataHub records the tables each pipeline reads and writes, together with validation results.

The [project data flow](docs/Project-Design.md#project-data-flow) connects these batch pipelines with cash streaming and feature computation.

## Layer 2: Machine Learning Platform

The ML platform connects feature computation, training, and serving in a reproducible workflow.

### Prediction Task

The model predicts total future net cash movement for each account over a chosen horizon. The policy adds that forecast to recorded cash and flags a projected balance below the minimum configured for the account owner's customer tier.

```text
projected_cash = current_cash + forecast_net_movement
alert = projected_cash < configured_minimum[customer_tier]
```

TPC-DI supplies tiers 1, 2, and 3. It does not define minimum-cash amounts or a risk order between tiers. The project must define those amounts and their rationale. Forecast horizon, scoring cadence, features, model, and metrics remain open. Obligations and payment-coverage modelling are future work.

### Features and Training

- **Offline Features:** Retain timestamped feature history for training and backfills.
- **Online Features:** Supply the latest eligible feature values for inference. Batch and streaming jobs can produce features for either use.
- **Feature Retrieval:** Uses Feast to assemble training examples with information available at the prediction time.
- **Training Pipeline:** Uses Kubeflow Pipelines for data loading, splitting, training, and evaluation.
- **Run Tracking:** Uses MLflow to record model artifacts, parameters, metrics, and data versions.

Offline and online features must share definitions and availability rules. Historical retrieval must use values available at prediction time, including the availability of late records and corrections. Training must compare a simple baseline with the selected model on held-out periods and generator seeds.

### Serving and Retrieval

- **Feature Materialization:** Updates the online feature store from offline tables and streaming jobs.
- **Model Serving:** Uses KServe to expose the trained model to applications and agents.
- **Text Pipeline:** Ingests documents, creates chunks and embeddings, and records lineage and validation.

The proposed cash-serving path sends Flink features to a separate scorer. The scorer calls the planned model service, applies the minimum-cash policy, and retains alerts for an API. The [project design](docs/Project-Design.md#features-and-serving) defines this proposal; its [open questions](docs/Project-Design.md#open-questions) include the text sources, retrieval purpose, and service contracts.

## Layer 3: Agent Platform

The agent platform provides access to project data, documents, and predictions through defined tools.

### Agent Task

The coursework scope includes an agent platform. The user task, expected output, and tool permissions require a separate design; the cash forecast does not establish an agent use case.

### Agent Components

- **Model Server:** Hosts the LLM and exposes its inference API.
- **Data and ML Tools:** Provides controlled access to structured data, retrieved documents, and model predictions.
- **Coordinator:** Routes work between agents.
- **Gateway:** Applies routing and access policies to model, tool, and agent requests.
- **Registry:** Publishes available agents and their interfaces.
- **Prompt Management:** Versions prompts used by deployed agents.

Framework, language model, tool contracts, and coordination rules remain open.

## Infrastructure and Operations

### Local Deployment

Docker packages services and jobs. Docker Compose groups local services by function.

- **Core Services:** Runs PostgreSQL, SeaweedFS, Hive Metastore, and Trino.
- **Batch Compute:** Runs Spark and the service used to connect dbt to Spark.
- **Streaming:** Runs Kafka and Flink.
- **Orchestration:** Runs Airflow.
- **Governance:** Runs DataHub and its supporting services.

Profiles control which groups start together. Container builds are reviewed for image size and required dependencies.

### Deployment and Service Operation

- **Infrastructure as Code:** Uses Terraform and Ansible for infrastructure provisioning and service configuration.
- **CI/CD:** Tests, builds, and deploys pipelines, streaming jobs, tools, and agents.
- **Routing and Security:** Configures gateways, authentication, HTTPS, rate limits, and centralized secrets.
- **Observability:** Collects service metrics, logs, traces, and model and agent telemetry.
- **Testing:** Checks data correctness, rerun behaviour, APIs, and performance under load.

Subject documents under `docs/` explain design, mechanisms, and supporting evidence. Working records hold implementation state and routine run reports. The coursework also requires two demonstrated novel ideas; their selection belongs in the [project design](docs/Project-Design.md#coursework-requirements).

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
