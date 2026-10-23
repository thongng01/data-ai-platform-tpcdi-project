# Data and AI Platform for TPC-DI Financial Data

A data and AI system for a retail brokerage firm / securities company, based on the domain, data characteristics and design of the TPC-DI benchmark. This system has three layers: a data platform (Layer 1), an ML platform (Layer 2) and an LLM and agent platform (Layer 3).

<!-- skeleton: every section below carries a placeholder comment until you
approve its final text; the comment dies when the section is filled. Earlier
drafts are parked at .scratch/study/readme-drafts.md -->

## Project overview

The system is built in three layers

### Data Layer:

- **Data generator**: generates raw source data (adapted subset of the TPC-DI specification), with configurable scale and injectable data problems;
- **Integration**: integrate all source data from different systems / format into one governed lakehouse, setting up a landing zone, then bronze, silver and gold, from raw arrival to a decision-support model;
- **Processing**: handles the source data problems, offline in Spark and streaming in Flink;
- **Orchestration**: runs pipelines in Airflow with ingestion and validation stages, and shows lineage and validation in a data catalog;
- **Storage**: lakehouse and warehouse.

### ML Layer

- **Feature tables**: serve point-in-time features over the firm's customers, accounts and markets, with event timestamps and creation timestamps;
- **Model**: trains, serves and monitors a trading-prediction model on those features.

### Agent Layer

- **Runs LLM-powered agents**

## Business domain
### Overview
TPC-DI depicts a retail brokerage firm that serves individual investors. The firm's functional structure consists of a **front office** and a **back office**, each with its own data.

The **front office** works with customers directly. From a data perspective, it spans **3** functional and supporting domains:

- **Trading and cash operations**: book orders and fills, track cash deposits and withdrawals, calculate commissions and charges, and maintain the lifecycle history of trades;
- **CRM and client portal**: maintain customer profiles, account details, and personal watchlists that evolve over time as relationships grow;
- **External data feeds**: a marketing prospect list for sales acquisition, and a market data feed that provides company fundamentals and corporate actions, plus daily prices and volumes.

The **back office** supports the operation from the inside, across **3** functional and supporting domains:

- **Custody and portfolio accounting**: track customer holdings and how each trade changes them;
- **HR**: map employees to licensed brokers and their branch offices;
- **Reference data**: provide the firm's shared vocabulary, including trade types, status codes, industry classifications, tax rates, and the calendar.

### Source data
Consists of 16 files

**Trading and cash operations**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| Trade.txt | trading and operations system (OLTP) | executed trades, one per row, with commission, charge and status | full load (Batch 1), then incremental deltas (Batches 2 and 3) | pipe-delimited, 14 fields — delta records grow to 16; one status value dominates; a few accounts carry most rows; whole-row duplicates |
| TradeHistory.txt | trading and operations system (OLTP) | the status history behind each trade | full load (Batch 1) | pipe-delimited, 3 fields — duplicate rows |
| CashTransaction.txt | trading and operations system (OLTP) | cash moving in and out of accounts | full load (Batch 1), then incremental deltas (Batches 2 and 3) | pipe-delimited, 4 fields — delta records grow to 6; the same few accounts dominate; duplicate rows |

**CRM and client portal**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| CustomerMgmt.xml | CRM system | customer and account records, kept as versions | full load (Batch 1) | one XML action stream, six action types covering creates, updates and closures — point-in-time versions of the same customer accumulate; payloads can arrive partially filled |
| WatchHistory.txt | CRM system (client portal) | personal watchlist actions as dated events | full load (Batch 1), then incremental deltas (Batches 2 and 3) | pipe-delimited, 4 fields — duplicate rows |

**External data feeds**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| FINWIRE* | financial newswire (external) | company profiles, security definitions, financial reports | full load (Batch 1) | fixed-width lines, no delimiter, three record kinds (company, security, financials) — unchanged company records repeat |
| Prospect.csv | prospect data vendor (external) | candidate customers for the sales team | full load (Batch 1) | comma-delimited — candidates match customers by name and address, not by key |
| DailyMarket.txt | market data vendor (external) | daily price and volume per security | full load (Batch 1), then incremental deltas (Batches 2 and 3) | pipe-delimited, 6 fields — delta records grow to 8; duplicate rows |

**Custody and portfolio accounting**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| HoldingHistory.txt | trading and operations system (OLTP) | holding quantities before and after each trade | full load (Batch 1), then incremental deltas (Batches 2 and 3) | pipe-delimited, 4 fields — duplicate rows |

**HR**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| HR.csv | HR system | employees, licensed brokers, branch offices | full load (Batch 1) | comma-delimited |

**Reference data**

| Data file | Source origin | Contains | Arrival pattern | Characteristics |
| :-- | :-- | :-- | :-- | :-- |
| TradeType.txt | the firm's reference data set | trade type codes | full load (Batch 1) | pipe-delimited |
| StatusType.txt | the firm's reference data set | trade status codes | full load (Batch 1) | pipe-delimited |
| Industry.txt | the firm's reference data set | industry classifications | full load (Batch 1) | pipe-delimited |
| TaxRate.txt | the firm's reference data set | national and local tax rates | full load (Batch 1) | pipe-delimited |
| Date.txt | the firm's reference data set | the calendar | full load (Batch 1) | pipe-delimited, 18 fields |
| Time.txt | the firm's reference data set | one row per second of the day | full load (Batch 1) | pipe-delimited, 86,400 rows |

## Table of contents

- [What we build](#what-we-build)
- [Business domain](#business-domain)
- [System diagram](#system-diagram)
- [Steps](#steps)
- [Repo structure](#repo-structure)
- [Quick start](#quick-start)
- [Services and ports](#services-and-ports)
- [Docs](#docs)

<!-- placeholder discipline: keep one anchor link per section, same order as
the file. The TOC routes the reader; it never duplicates content.
Requirements and Architecture entries join this list when those sections are
born. -->

## System diagram

<!-- placeholder: high-level system deployment diagram, graded in sheet 01 r2.
Rules: one diagram per named question; every box is one deployable unit; arrows
follow the data path and carry the data name as their label; arrows numbered
and described in an arrows table; separate numbering per user flow; dashed
arrows only for non-main flows. Planned boxes stay visible and say planned. -->

## Steps

<!-- placeholder: numbered step table per phase, three columns: step number,
component, what it does. Step numbers become stable IDs reused in the diagram,
in engine docs, and in the architecture arrows table. Flow letters get names
here: B for batch, S for stream, O for orchestration and governance. -->

## Repo structure

<!-- placeholder: fenced tree of the top-level directories, one line per
directory, annotated with what lives there. The tree doubles as the docs map:
a reader should be able to answer "where is X" from this section alone. -->

## Quick start

<!-- placeholder: one fenced code block per phase, in phase order, with the
compose profile each phase needs. Copy paste must get that phase running.
State memory limits where the commands can violate them. Start from the
existing make targets: up-core, bootstrap, verify, generate, dp1, test,
check-docs. -->

## Services and ports

<!-- placeholder: table of running services: service, port, purpose, and which
profile starts it. Take the values from docs/IaC.md so the two files cannot
drift without a check. -->

## Docs

<!-- placeholder: one line per doc, ordered by phase. README stays an index; it
summarises and links, it does not carry content. Sheet 01 r2 and the
Documentation row above it make this the gate for the whole sheet. -->
