# Project Data Specification

This document defines the data contracts for the retail brokerage platform. The project uses the business domain and source formats of the Transaction Processing Performance Council's data integration benchmark (TPC-DI), moves data through medallion zones, streams cash transactions, and predicts account closure within 30 days so brokers can act before customers leave.

For all details about the TPC-DI benchmark, refer to the [TPC-DI v1.1.0 Specification PDF](TPC-DI/TPC-DI_v1.1.0.pdf). Our design also draws on [Shannon Barrow's Databricks TPC-DI implementation](https://github.com/shannon-barrow/databricks-tpc-di) and the [associated Medium article](https://medium.com/dbsql-sme-engineering/benchmarking-etl-with-the-tpc-di-part-1-b581d46c44f1).

---

## 1. Business Domains and Origin Systems
The system models the data landscape of a retail brokerage. This table maps each business domain to its logical origin system and business context. In the table, CMS is the customer management system, OLTP means online transaction processing, HR means human resources, and DB means database.

*Note: Section 3 lists the physical files that these systems produce.*

| Business Domain | Origin System | Business Context |
| --- | --- | --- |
| **Customers & Accounts** | CMS (history) & OLTP (daily) | Customer profiles, demographics, and brokerage accounts managed by brokers. |
| **Brokers** | Internal HR DB | Employee records. Brokers are employees with job code `314`. |
| **Trading & Holdings** | OLTP Trading DB | Trade lifecycles (submission to completion or cancellation) and resulting security holdings. |
| **Cash Transactions** | OLTP Trading DB | Money moving into and out of accounts: deposits, withdrawals, trade settlements, and dividends. |
| **Market** | OLTP Trading DB | Daily price and volume observations for each security. |
| **Watch Lists** | OLTP Trading DB | Securities that customers watch, added and removed over time. |
| **Prospects** | External Marketing Provider | Potential customers with demographic data (some overlap with existing customers). |
| **Companies & Securities** | Financial Newswire (FINWIRE) | Companies, the securities they issue, and quarterly financial reports. |
| **Reference Data** | Static Reference Files | Calendar, time of day, industries, statuses, tax rates, and trade types. |

## 2. Load Cycle and Processing Strategy
TPC-DI defines one historical load followed by two incremental updates. This project uses an initial load for 2014-01-01 through 2018-12-31, followed by daily deliveries for 2019-01-01 through 2025-12-31. FINWIRE arrives quarterly inside daily deliveries. Cash transactions in the daily delivery period arrive only through Kafka.

### 2.1 Processing Phases

| Phase | Path | Starting State | Coverage | Work |
| --- | --- | --- | --- | --- |
| **Initial load (TPC-DI Batch 1)** | Files in `initial/` | Empty warehouse | `initial_start` through `initial_end` | Load reference data and full extracts, build type 2 slowly changing dimension (SCD2) history, and calculate historical cash balances and holdings. |
| **Daily deliveries** | Files in `daily/YYYYMMDD/` | Earlier dates processed in order | One `data_date` per delivery, from `daily_start` through `daily_end` | Apply change data capture (CDC) inserts and updates (`I`, `U`) and new activity; apply prospect snapshots monthly and FINWIRE on release dates. |
| **Cash transaction stream** | Messages on `brokerage.cash_transaction.raw` | Closing cash balances at `initial_end` | `daily_start` through `daily_end`, continuously | Update live balances and windows; supply Silver cash transaction rows for the daily warehouse build ([Section 2.4](#24-batch-to-stream-handoff)). |

The initial load needs price history for 52-week values and four released quarters of earnings for price-to-earnings ratios (P/E). FINWIRE reports start five quarters before the quarter that contains `initial_start`, so four quarters are available on the first date. The reference calendar has 1,826 initial-load dates and 2,557 daily delivery dates. The New York Stock Exchange (NYSE) calendar includes its unscheduled closures on 2018-12-05 and 2025-01-09.

### 2.2 Source Cadence
Each source follows one cadence after the initial load:
- **Daily**: `Customer.txt`, `Account.txt`, `Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`, and `DailyMarket.txt`.
- **Monthly**: A full `Prospect.csv` snapshot on the first day of each month. Customer marketing fields keep their previous values until the new snapshot arrives.
- **Quarterly, on the release date**: `FINWIREyyyyQq`. Q1 results release April 30, Q2 results July 31, Q3 results October 31, and Q4 results January 31 of the following year. The release date's delivery applies company and security postings before that day's trades and prices. File names use the posting quarter of `PTS`; each financial record keeps its reporting quarter.
- **Continuous, through Kafka**: Cash transactions. No `CashTransaction.txt` file arrives in a daily delivery.
- **Once, in the initial load**: `CustomerMgmt.xml`, `TradeHistory.txt`, `HR.csv`, and the six reference files. Later customer, account, and trade changes arrive through `Customer.txt`, `Account.txt`, and `Trade.txt`. HR and reference data stay static for the run; `dim_broker` keeps its history columns with one version per broker.

*Note: The fixed FINWIRE release schedule simulates an earnings feed, not statutory filing deadlines. Earnings announcements can precede formal filings ([SEC guidance](https://www.investor.gov/sites/default/files/readan8k.pdf)).*

### 2.3 Calendar and Dates
- **Day**: Each day runs from midnight Coordinated Universal Time (UTC) up to, but not including, the next midnight. Source timestamps without a time zone are UTC.
- **Daily delivery calendar**: Every calendar date has a daily delivery, including weekends and holidays. A source with no changes on a date still delivers its file, with zero records.
- **`data_date`**: The business date that a daily extract represents, encoded in its folder and file name. Extraction and arrival dates do not change it.
- **Run periods**: Four inclusive UTC dates: `initial_start`, `initial_end`, `daily_start`, and `daily_end`. The defaults are 2014-01-01, 2018-12-31, 2019-01-01, and 2025-12-31. `daily_start` must be the day after `initial_end`; the configuration loader rejects nonadjacent periods. FINWIRE and reference files can begin before `initial_start`. The initial `Prospect.csv` snapshot is dated `initial_end`.
- **Cash transaction date**: The UTC date of `CT_DTS`. Delivery time is a separate value that measures lateness.
- **Processing order**: Process dates in order, initially with one active run per pipeline. Independent tables can run in parallel within a date. Each table waits only for its own inputs and dependencies.

### 2.4 Batch-to-Stream Handoff
- **Split**: Cash transactions through `initial_end` arrive in `CashTransaction.txt`. From `daily_start`, they arrive only as stream messages.
- **Start state**: Flink Job 2 starts from the closing initial-load balances, keyed by natural account ID. It consumes the clean topic, not historical file rows ([handoff rules](Data-Platform.md#4-stream-processing-kafka-and-flink)).
- **Warehouse facts**: DP2 pushes Silver cash transaction rows into PostgreSQL. dbt builds `fact_cash_transactions` and then `fact_cash_balances`, with the same logic for initial-load and streamed dates. Flink does not write either warehouse fact.
- **Duplicates**: The source system assigns every cash transaction a `CT_ID` from one global sequence. Repeated deliveries keep it and count once in Silver and Gold.
- **Late messages**: Flink Job 1 writes valid late messages to Silver. Daily builds of the cash transaction and balance facts wait until its progress marker passes the end of the date plus 10 minutes. A correction to a closed date rebuilds that date and every later processed date in order; live tables remain separate from corrected warehouse facts.

## 3. Physical Source Files and Ingestion Rules
The project uses all 18 TPC-DI business and reference source families. It excludes `BatchDate.txt`, the `<name>_audit.csv` files, and the benchmark `Audit` table. Run dates define the delivery periods (Section 2.3). `CashTransaction.txt` gains `CT_ID`, and a later `Trade.txt` contract adds `T_CHANNEL`; Section 9.2 records these project changes.

The tables and schema headings use the standard source names. The naming rules below define the project file names.

**Change Data Capture (CDC) Rules**: Daily OLTP files are CDC extracts. Each row starts with a `CDC_FLAG` (`I` for insert, `U` for update) and a `CDC_DSN` (sequence number). Insert-only files always carry `I`; initial files omit both fields.

| Origin | Source File(s) | Format | Grain | Initial load | Daily delivery period | CDC Rules |
| --- | --- | --- | --- | :---: | :---: | --- |
| CMS | `CustomerMgmt.xml` | XML | One action on a customer and its accounts | Yes | No | N/A (Action-ordered) |
| OLTP | `Customer.txt` | Pipe-delimited | One changed customer | No | Yes | `I`, `U` |
| OLTP | `Account.txt` | Pipe-delimited | One changed account | No | Yes | `I`, `U` |
| OLTP Trading DB | `Trade.txt` | Pipe-delimited | One trade (state or change) | Yes | Yes | `I`, `U` |
| OLTP Trading DB | `TradeHistory.txt` | Pipe-delimited | One trade status change | Yes | No | N/A (Initial load only) |
| OLTP Trading DB | `CashTransaction.txt` | Pipe-delimited | One cash transaction | Yes | Stream | N/A (stream after initial load) |
| OLTP Trading DB | `HoldingHistory.txt` | Pipe-delimited | One holding change per trade | Yes | Yes | `I` only |
| OLTP Trading DB | `WatchHistory.txt` | Pipe-delimited | One watch placement or cancellation | Yes | Yes | `I` only |
| OLTP Trading DB | `DailyMarket.txt` | Pipe-delimited | One security per day | Yes | Yes | `I` only |
| Internal HR DB | `HR.csv` | Comma-delimited | One employee | Yes | No | N/A (Full extract) |
| External Marketing | `Prospect.csv` | Comma-delimited | One prospect | Yes | Monthly | N/A (Full snapshot) |
| Financial Newswire | `FINWIREyyyyQq` | Fixed-width | One company, security, or financial report | Yes | Release dates | N/A (One file per posting quarter) |
| Static Reference | `Date.txt`, `Time.txt`, `Industry.txt`, `StatusType.txt`, `TaxRate.txt`, `TradeType.txt` | Pipe-delimited | One row per code, day, or second | Yes | No | N/A (Static) |

**Source Naming and Delivery**:
- **Initial files**: Use `initial/<Source>` with standard names such as `CustomerMgmt.xml`, `Trade.txt`, and `Prospect.csv`. Add no date suffix. Keep the existing year and quarter in `FINWIREyyyyQq`.
- **Daily files**: Use `daily/YYYYMMDD/<Source>_YYYYMMDD.<ext>`, for example `daily/20190305/Customer_20190305.txt` and `daily/20190301/Prospect_20190301.csv`. The folder and file name carry the same `data_date`, written as plain `YYYYMMDD`. Delivery delays and retries do not change it.
- **Quarterly files**: Name `FINWIREyyyyQq` by the year and quarter of `PTS`, not the financial reporting period. For example, Q1 2026 results published April 30 belong in `FINWIRE2026Q2`; their `Year` and `Quarter` fields remain `2026` and `1`. Keep the existing name format without a daily date suffix. Publish only records released by that delivery, never future postings. Each file arrives in the daily folder of its release date, for example `daily/20260430/FINWIRE2026Q2`. That date's delivery applies it before that day's trades and prices.
- **Generation and delivery**: The generator writes source data locally. The publisher writes files to the source department's `upstream` bucket and cash transactions for the daily delivery period to Kafka. DP1 can only read `upstream`; it pulls new and changed files into `staging` before parsing them into Bronze.
- **Delivery completeness**: The platform keeps an expected-file list for each delivery: the initial set, the daily set, the monthly prospect snapshot, and FINWIRE on release dates. The source department sends no control or manifest file. A file with zero records means no activity; a missing file means the delivery is incomplete. `ops.load_status` and a completeness view show missing inputs. A late file holds back only dependent tables.
- **S3 placement**: The paths above are relative to `upstream` ([bucket rationale](Data-Platform.md#62-object-storage-layout-and-dataset-scope)). DP1 copies each pulled file version to `initial/<version_id>/<source_filename>` or `daily/YYYYMMDD/<version_id>/<source_filename>` in `staging`. S3 groups object keys by prefix, not by directories or tags. The platform holds one active dataset at a time, so keys have no dataset level.
- **File versions**: Each file version has its own `version_id`. A retried pull reuses that ID and path. A corrected source file gets a new ID; reruns and backfills reuse the selected inputs. An ingestion record maps the source key and version information to its staging path. An object-store ETag is not a guaranteed content hash.
- **Generator summary**: The generator keeps one reference summary on local disk beside the generated dataset and never uploads it. It records generation settings, output counts, and the simulated data characteristics and data errors. Ingestion does not read it ([generator rules](Data-Platform.md#2-data-generation-and-problem-injection)).

## 4. Data Zones (Medallion Architecture)
Data moves through four zones:

| # | Zone | Purpose | Grain | Storage |
| --- | --- | --- | --- | --- |
| 1 | **Staging** | The platform's immutable copy of every pulled file version. The cash transaction stream enters through Kafka instead. | One pulled file version | SeaweedFS `staging` bucket |
| 2 | **Bronze** | Source records and received messages as delivered text, including duplicates. Preserves provenance and parse failures. | One source record or one message | Delta tables in the SeaweedFS `lakehouse` bucket, under `bronze/` |
| 3 | **Silver** | Cleaned, typed, deduplicated, and conformed records. Ready for joins. | One business record, with quality metadata where needed | Delta tables in the SeaweedFS `lakehouse` bucket, under `silver/` |
| 4 | **Gold** | The warehouse (Section 6), live tables, and machine learning (ML) feature tables (Section 7). | Per business entity or metric | PostgreSQL `gold` schema |

### Versioning and History
The versioning baseline preserves inputs and business history ([rationale](Data-Platform.md#63-data-versioning-and-safe-reruns)):
- **Raw versions**: Staging and Bronze keep every pulled file version. Replaying the selected file versions rebuilds downstream tables. Stream Bronze keeps every message; Silver keeps one cash transaction per `CT_ID`.
- **Provenance and rejects**: Bronze preserves delivered values and provenance. Unusable records go to Bronze quarantine tables with their rule and reason. GX Core publishes quality results to DataHub; quality metadata and `ops.load_status` replace the benchmark's `DImessages` table.
- **Business history**: SCD2 dimensions record what was true on each date (Section 6). Delta table history records changes to Bronze and Silver.
- **Names**: Tables use `bronze.raw_*`, `silver.stg_*`, `gold.dim_*`, `gold.fact_*`, and `gold.feat_*`. Gold current-state and reference tables use snake_case names such as `prospect` and `status_type`. Platform metadata uses `_snake_case`. Original source field names stay unchanged in the source dictionaries.

### Silver Zone Design
The TPC-DI specification does not define a Silver zone. Silver is the project's own model, with the following traits:
- **Preserve Change Order**: Keep CDC type, sequence, and delivery date information. SCD2 versioning depends on the order of changes.
- **Keep XML meaning**: For `CustomerMgmt.xml`, keep omitted and empty elements distinct until Gold versions are built.
- **Deduplicate Correctly**: Remove repeated deliveries of the same event, but keep legitimate repeated business changes (such as two updates to one trade).
- **Separate Temporal Data**: Keep event dates separate from `data_date` and system arrival dates.

## 5. Data Lineage: Source-to-Gold Mapping
*This table shows high-level transformation lineage. PDF clauses 4.5 (historical) and 4.6 (daily) provide field-level rules.*

| Source Records | Transformation Logic | Gold Output |
| --- | --- | --- |
| `CustomerMgmt.xml` + `Customer.txt` | Apply XML actions, then CDC rows. Add tax rates, phone numbers (PDF 4.5.4, 4.6.4), and prospect data; keep daily SCD2 versions. | `dim_customer` |
| `CustomerMgmt.xml` + `Account.txt` | Same pattern. Resolve customer and broker keys; add a version when the customer changes. | `dim_account` |
| `HR.csv` | Keep employees with job code 314. | `dim_broker` |
| FINWIRE (`CMP`, `SEC`) | Parse fixed-width records in posting order; build histories; link securities to companies by numeric Central Index Key (CIK). | `dim_company`, `dim_security` |
| FINWIRE (`FIN`) | Copy the report year and quarter from the record; resolve the company at `PTS`; use only reports released by the as-of date. | `financial` |
| `Trade.txt` + `TradeHistory.txt` | Insert each trade, then update its status. Keys are set at creation and kept at close. | `dim_trade` |
| `CashTransaction.txt` and streamed cash transactions in Silver | Deduplicate by `CT_ID`; parse `CT_NAME`; resolve dimension keys. dbt builds the cash transaction fact, then adds each day's net amount to the prior balance. | `fact_cash_transactions`, `fact_cash_balances` |
| `HoldingHistory.txt` + `dim_trade` | Copy `HH_AFTER_QTY`; take keys, date, time, and price from the changing trade. | `fact_holdings` |
| `DailyMarket.txt` + `dim_security` + `financial` | Copy prices and volume; calculate 52-week high and low, P/E, and yield (PDF 4.5.11, 4.6.10). | `fact_market_history` |
| `WatchHistory.txt` | Insert a watch on `ACTV`; set its removal date on `CNCL`. | `fact_watches` |
| `Prospect.csv` + `dim_customer` | Insert or update prospects, calculate marketing tags (PDF 4.5.15, 4.6.14), and match active customers. | `prospect` |

## 6. Gold Warehouse
The Gold warehouse has 19 tables: the 19 TPC-DI tables minus `DImessages`, plus `fact_cash_transactions`. dbt-postgres builds them in PostgreSQL. Dimensions describe business entities; facts record activity. Gold also holds live tables written by Flink Job 2 and feature tables built by DP3. These are separate from the 19-table warehouse inventory.

**SCD2 Dimension Rules**:
- **Keys**: A natural key (`CustomerID`) identifies the entity across time. A surrogate key, or SK (`SK_CustomerID`), identifies one specific version.
- **Versioning**: On a change, the current version gets `IsCurrent = 0` and `EndDate` = the new version's `EffectiveDate`. The new version is a new row with a new `SK`, `IsCurrent = 1`, and `EndDate = 9999-12-31`.
- **Collapse**: Multiple changes to one entity on the same day collapse into a single daily version.
- **Fact Joining**: Facts store the `SK` of the dimension version valid on the event's date.

| Table | Type | Contents | Grain | Built From | Initial load | Daily delivery period |
| --- | --- | --- | --- | --- | --- | --- |
| `dim_customer` | SCD2 Dimension | Identity, contacts, tier, tax rates, prospect data | One customer version | `CustomerMgmt.xml`, `Customer.txt`, `Prospect.csv` | Versions from XML actions | New versions from CDC |
| `dim_account` | SCD2 Dimension | Owner, broker, tax status, status | One account version | `CustomerMgmt.xml`, `Account.txt` | Versions from XML actions | New versions from CDC and customer changes |
| `dim_broker` | SCD2 Dimension | Name, manager, branch, office, phone | One broker | `HR.csv` | One version per broker | No change |
| `dim_company` | SCD2 Dimension | Name, industry, credit rating, CEO, address | One company version | FINWIRE `CMP` | Versions from postings | New versions from quarterly FINWIRE |
| `dim_security` | SCD2 Dimension | Symbol, exchange, shares, dividend, issuing company | One security version | FINWIRE `SEC` | Versions from postings | New versions from quarterly FINWIRE |
| `dim_trade` | Mutable Dimension | Type, status, quantity, prices, fees, account, security, channel | One trade | `Trade.txt`, `TradeHistory.txt` | One row per trade; no recorded channel | Insert new trades; update status on close |
| `fact_cash_transactions` | Fact | Cash transactions and related dimension keys | One cash transaction, keyed by `CT_ID` | Silver cash transactions from the initial file and Kafka | All initial-load cash transactions | Cash transactions of closed dates, built daily by dbt |
| `fact_cash_balances` | Fact | End-of-day account cash balance | One account per day with cash transactions | `fact_cash_transactions` | Daily balances | Daily balances, built by dbt |
| `fact_holdings` | Fact | Quantity held after a trade | One holding change | `HoldingHistory.txt` | One row per change | One row per change |
| `fact_market_history` | Fact | Prices, volume, 52-week high and low, P/E, yield | One security per day | `DailyMarket.txt` | One row per day | One row per day |
| `fact_watches` | Fact | Period a customer watches a security | One watch | `WatchHistory.txt` | Watches from actions | Insert placements; set removal dates |
| `prospect` | Current State | Agency record, marketing tags, `IsCustomer` | One prospect | `Prospect.csv` | One row per prospect | Insert new; update changed |
| `financial` | Current State | Quarterly results | One company per quarter | FINWIRE `FIN` | One row per report | New quarterly reports |
| `dim_date`, `dim_time` | Reference | Calendar and clock attributes | One day; one second | `Date.txt`, `Time.txt` | Loaded once | No change |
| `industry`, `status_type`, `tax_rate`, `trade_type` | Reference | Code lookups | One code | Matching `.txt` files | Loaded once | No change |

`fact_cash_transactions` links each cash transaction to account, customer, date, and time dimensions. Trade settlements also link to `dim_trade`; dividends link to `dim_security`. It is partitioned by date. `dim_trade.channel` is nullable because older source rows do not record a channel. `fact_market_history` retains P/E, 52-week values, and yield.

### Gold Entity Relationships
The entity relationship diagram (ERD) shows all 19 warehouse tables in the `gold` schema and their main relationships. Table labels omit the namespace because the whole view belongs to one schema. Existing fields use the TPC-DI reference names. The cash transaction fact shows its identity and dimension relationships, not a full physical schema; `ID` denotes a logical identifier. The `prospect` relationship is a match by name and address, not a key.

![Gold ERD with all 19 tables, their keys, and their relationships.](diagrams/data-specification/gold-erd.png)

*Gold warehouse (`gold` schema), reference model rather than a final physical schema. Header colors: blue for dimensions, orange for facts, green for current-state tables, gray for reference tables.*

## 7. Cash Transaction Stream and ML
After the initial load, cash transactions arrive only through Kafka. Flink Job 1 writes Bronze and Silver and publishes clean messages. Job 2 consumes the clean topic, updates live tables, produces cash-flow features, and requests rescoring after large withdrawals. Cash-flow features and batch features support the 30-day account attrition model.

### 7.1 Cash Transaction Stream (Kafka and Flink)
Cash transactions after the initial load arrive as Kafka messages and follow the handoff rules in [Section 2.4](#24-batch-to-stream-handoff). The table maps original source fields to the lowercase JSON keys in the [cash transaction message contract](Data-Platform.md#topics-and-cash-transaction-contract). Source file types remain in Section 9.2; JSON amounts use decimal text rather than floating-point numbers. Messages keep this key order: `ct_id`, `ct_ca_id`, `ct_dts`, `ct_amt`, `ct_name`, `schema_version`.

| Source Field or Metadata | JSON Key or Kafka Metadata | Representation and Meaning |
| --- | --- | --- |
| `CT_ID` | `ct_id` | Integer global cash transaction sequence, shared with the initial file. Repeated deliveries keep the same ID. |
| `CT_CA_ID` | `ct_ca_id` | Integer account ID |
| `CT_DTS` | `ct_dts` | UTC event timestamp as ISO 8601 text |
| `CT_AMT` | `ct_amt` | Signed decimal text; positive adds cash and negative removes cash |
| `CT_NAME` | `ct_name` | Fixed description as text |
| Message contract version | `schema_version` | Integer schema version |
| Delivery time | Kafka record timestamp, outside the JSON payload | When the message was delivered; used to measure lateness |

| Output | Zone | Grain | Contents |
| --- | --- | --- | --- |
| Received messages | Bronze | One message | Each message as delivered, including duplicates, with its Kafka partition, offset, and delivery time |
| Streamed cash transactions | Silver | One cash transaction | Validated cash transactions, deduplicated by `CT_ID`, with parsed `cash_type` and `trade_id` |
| `fact_cash_transactions` | Gold | One cash transaction | dbt's daily cash transaction fact from Silver |
| `fact_cash_balances` | Gold | One account per day with cash transactions | dbt's daily balance: previous balance plus the day's net amount |
| Running balance | Gold | One account | Flink Job 2's current cash balance after each cash transaction |
| Hourly windows | Gold | One account per hour | Inflow, outflow, net amount, and count per `cash_type` |

### 7.2 Account Attrition and Broker Outreach
The model predicts whether an open account closes within 30 days ([ML Platform](ML-Platform.md)). A separate outreach policy sends the broker an outreach task when the closure probability reaches the customer's tier threshold. Tier 1 has a lower tier threshold than tier 3. Changing tier thresholds does not require retraining the model.

| Data | Grain | Contents |
| --- | --- | --- |
| **Batch features** | One account per feature time | DP3 produces them from Gold; Airflow materializes them to Feast's online store. |
| **Cash-flow features** | One account per feature time | Flink Job 2 produces them and sends them through the features topic to a Feast push job for the online and offline stores. |
| **Label** | One open account per prediction time | Closure within the following 30 days, observed in `Account.txt` and `dim_account`. Hidden state is not a model input. |
| **Outreach policy** | One customer tier | A tier threshold that decides when the broker gets an outreach task. |
| **Prediction** | One account per prediction time | Probability of closure within 30 days. Daily scoring covers all open accounts after DP3; a large withdrawal triggers rescoring for one account. |

Each feature has one definition and one producer. Features have a business time and an availability time. Training must not use information from after the prediction time. A prediction whose 30-day observation period has not ended has no complete label; it is not a negative example. MLflow stores each training dataset version with its model, so later feature corrections do not change the recorded training inputs.

## 8. Core Integration Rules

### Temporal Rules
- **Record Dates vs. `data_date`**: Historical rows use their own dates. Daily customer and account changes use `data_date`. Streamed cash transactions use the UTC date of `CT_DTS`.
- **Trade Times**: `PNDG` sets the creation time (`SBMT` for market orders `TMB` and `TMS`). `CMPT` or `CNCL` sets the close time.
- **SCD2 Collapse**: Several changes to one entity on one day become one version. The latest value wins, by timestamp and then file order.
- **Propagation**: A new customer version gives the customer's accounts new versions. A new company version does the same for its securities.
- **Financial periods**: Populate `financial.FI_YEAR`, `FI_QTR`, and `FI_QTR_START_DATE` from the `FIN` record, never from the file name. Resolve `SK_CompanyID` against the company version valid at `PTS` ([PDF clause 4.5.14](TPC-DI/TPC-DI_v1.1.0.pdf)). Company and security versions use their own posting dates, not the financial quarter's start or end.
- **Financial availability**: P/E calculations and ML features must not use a report before its release. A report released April 30 cannot supply earnings to an April 15 as-of calculation. Follow natural company identity across SCD2 versions when combining quarterly earnings; reports need not share one `SK_CompanyID`.
- **Source dependencies**: Make new company and security identities available before normal trade and market activity refers to them. A table waits only for its own complete inputs and dependencies. Corrections rebuild the affected dates and every later processed date, in order. Do not substitute current dimension values for historical ones.

### Historical Actions (XML)
In `CustomerMgmt.xml`, updates are partial and contextual:
- An **omitted** property retains its existing value.
- An **explicitly empty** element (for example `<C_M_NAME/>`) sets the value to `NULL`.
- One action can create or change several accounts of its customer.
- `INACT` and `CLOSEACCT` add a version with `Status = 'INACTIVE'`. Nothing is deleted, and the new version is current (`IsCurrent = 1`).

### Trade Lifecycle Updates
`dim_trade` is updated in place. Historical status events come from `TradeHistory.txt`. Daily updates use `Trade.txt`. Creating the trade sets its dimension keys; later close updates keep them. TPC-DI calls these keys `SK_CustomerID`, `SK_AccountID`, `SK_BrokerID`, `SK_CompanyID`, and `SK_SecurityID` in the reference dictionary.

### Fact Rules
- **Cash balances**: No opening balance exists, so each account starts at zero. Carry the balance by the natural account ID, because the account's surrogate key changes with each new version (PDF 4.5.9, 4.6.8).
- **Holdings**: `HH_AFTER_QTY` is the quantity after the change. Copy it; summing it counts holdings more than once (PDF 4.5.10, 4.6.9).
- **Watch removal**: A `CNCL` row closes the watch that its `ACTV` row opened, even when the customer or security has a newer version since then (PDF 4.5.12, 4.6.11).

## 9. Physical Structures and Schema Reference
The source dictionaries define parsing order and source formats. The Gold dictionaries preserve the TPC-DI fields, types, and restrictions as mapping references, not complete project physical schemas. Destination table labels use snake_case; reference field names remain unchanged. Project changes are stated beside the relevant dictionary.

### 9.1 General Parsing Rules
- **Delimited Files**: `.txt` uses pipe (`|`), `.csv` uses comma (`,`). No header rows. An empty field represents `NULL` where allowed.
- **CDC (Change Data Capture)**: Daily OLTP files start with `CDC_FLAG` (`I` insert, `U` update) and `CDC_DSN` (sequence number). Initial files omit both fields.
- **Date and Time Formats**: Default is `YYYY-MM-DD` and `YYYY-MM-DD HH:MM:SS`. FINWIRE uses `YYYYMMDD` and `YYYYMMDD-HHMMSS`. `CustomerMgmt.xml` uses `YYYY-MM-DDTHH:MM:SS`. Interpret source timestamps without a time zone as UTC; keep their source text formats.
- **FINWIRE Records**: Characters 1 to 15 are `PTS`; characters 16 to 18 are the record type (`CMP`, `SEC`, `FIN`). Record lengths: `CMP` 543, `SEC` 170, `FIN` 196 characters. Records use numeric CIKs and include full trailing padding.
- **XML Null vs. Omitted**: Parsing keeps an omitted element distinct from an explicitly empty one, such as `<C_M_NAME/>`. [Historical Actions](#historical-actions-xml) defines what each means.

### 9.2 Source File Schemas

Fields follow file order, with the types, restrictions, and descriptions of PDF clause 2.2, plus the two stated project changes: `CT_ID` and `T_CHANNEL`. Daily OLTP files start with `CDC_FLAG` and `CDC_DSN`; initial files omit them. Bronze stores delivered fields as text; Silver converts types.

<details>
<summary>Data Types</summary>

| Type | Definition | Used In | Meaning |
| --- | --- | --- | --- |
| `BOOLEAN` | `0` or `1` | Both | False or true |
| `CHAR(n)` | Up to n single-byte characters | Both | Text |
| `DATE` | `YYYY-MM-DD` | Both | Date |
| `DATETIME` | `YYYY-MM-DD HH:MM:SS` | Both | Timestamp; no time zone |
| `TIME` | Time of day to 1 ms | Warehouse | Time |
| `NUM(m[,n])` | Unsigned, m digits, n after the point | Both | Exact number |
| `SNUM(m[,n])` | Signed, m digits, n after the point | Both | Exact signed number |
| `IDENT_T` | NUM(11) | Both | Identifier from the OLTP system |
| `SK_T` | NUM(11) | Warehouse | Surrogate key of a dimension row |
| `TRADE_T` | NUM(15) | Both | Trade identifier |
| `CDC_FLAG_T` | CHAR(1) | Source | `I`, `U`, or `D`; each file narrows the set |
| `CDC_DSN_T` | NUM(12) | Source | Change sequence number within a file |
| `S_QTY_T` | NUM(6) | Source | Share quantity of one trade |
| `S_COUNT_T` | NUM(12) | Both | Aggregate share count |
| `S_PRICE_T` | SNUM(8,2) | Both | Share price |
| `VALUE_T` | SNUM(10,2) | Both | Single transaction or security value |
| `BALANCE_T` | SNUM(12,2) | Both | Aggregate account or transaction value |
| `FIN_AGG_T` | SNUM(15,2) | Warehouse | Aggregate financial value |

</details>

<details>
<summary>Customer & Account Sources (CustomerMgmt, Customer, Account)</summary>

#### CustomerMgmt.xml

XML, initial load only. Actions in time order; namespace `http://www.tpc.org/tpc-di`. An update carries only the identifiers and the changed properties.

```xml
<TPCDI:Action ActionType="CLOSEACCT" ActionTS="2007-07-16T00:29:01">
  <Customer C_ID="6">
    <Account CA_ID="6" />
  </Customer>
</TPCDI:Action>
```

| Action Type | Meaning |
| --- | --- |
| `NEW` | New customer with one or more new accounts |
| `ADDACCT` | Exactly one new account for an existing customer |
| `UPDCUST` | Changes to an existing customer |
| `UPDACCT` | Changes to exactly one existing account |
| `CLOSEACCT` | Closes exactly one account |
| `INACT` | Makes a customer and its active accounts inactive |

| Field | Element | Type | Restrictions | Description |
| --- | --- | --- | --- | --- |
| `ActionType` | Action | `CHAR(9)` | ’NEW’, ‘ADDACCT’, ‘UPDCUST’, ‘UPDACCT’, ‘CLOSEACCT’, ‘INACT’ | Action type; see the table above. |
| `ActionTS` | Action | `CHAR` | Not empty | Date and time of the action as YYYY-MM-DDTHH:MM:SS. Note the ‘T’ is a literal value. |
| `C_ID` | Customer | `IDENT_T` | Not empty, Required | Customer identifier |
| `C_TAX_ID` | Customer | `CHAR(20)` | Not empty, Required on ‘NEW’ | Customer’s tax identifier |
| `C_GNDR` | Customer | `CHAR(1)` | Not empty | Gender of the customer |
| `C_TIER` | Customer | `NUM(1)` | Not empty | Customer tier |
| `C_DOB` | Customer | `DATE` | Not empty, Required on ‘NEW’ | Customer’s date of birth as YYYY-MM-DD |
| `C_L_NAME` | Name | `CHAR(25)` | Not empty, Required on ‘NEW’ | Customer's last name. |
| `C_F_NAME` | Name | `CHAR(20)` | Not empty, Required on ‘NEW’ | Customer's first name. |
| `C_M_NAME` | Name | `CHAR(1)` |  | Customer's middle initial |
| `C_ADLINE1` | Address | `CHAR(80)` | Not empty, Required on ‘NEW’ | Address Line 1 |
| `C_ADLINE2` | Address | `CHAR(80)` |  | Address Line 2 |
| `C_ZIPCODE` | Address | `CHAR(12)` | Not empty, Required on ‘NEW’ | Zip or postal code |
| `C_CITY` | Address | `CHAR(25)` | Not empty, Required on ‘NEW’ | City |
| `C_STATE_PROV` | Address | `CHAR(20)` | Not empty, Required on ‘NEW’ | State or province |
| `C_CTRY` | Address | `CHAR(24)` |  | Country |
| `C_PRIM_EMAIL` | ContactInfo | `CHAR(50)` |  | Customer's primary e-mail address |
| `C_ALT_EMAIL` | ContactInfo | `CHAR(50)` |  | Customer's alternate e-mail address |
| `C_PHONE_1` | ContactInfo | `PhoneNumber*` |  | Customer’s primary phone number |
| `C_PHONE_2` | ContactInfo | `PhoneNumber*` |  | Customer’s secondary phone number |
| `C_PHONE_3` | ContactInfo | `PhoneNumber*` |  | Customer’s third phone number |
| `C_LCL_TX_ID` | TaxInfo | `CHAR(4)` |  | Customer's local tax rate |
| `C_NAT_TX_ID` | TaxInfo | `CHAR(4)` |  | Customer's national tax rate |
| `CA_ID` | Account | `IDENT_T` | Not empty, Required | Customer account identifier |
| `CA_TAX_ST` | Account | `NUM(1)` | Not empty, Required on ‘NEW’ | Tax status of this account |
| `CA_B_ID` | Account | `IDENT_T` | Not empty, Required on ‘NEW’ | Identifier of the managing broker |
| `CA_NAME` | Account | `CHAR(50)` |  | Name of customer account |

*Phone numbers*: `C_PHONE_1` to `C_PHONE_3` use the `PhoneNumber` type, with these child elements:

| Field | Type | Description |
| --- | --- | --- |
| `C_CTRY_CODE` | `CHAR(3)` | Country code |
| `C_AREA_CODE` | `CHAR(3)` | Area code |
| `C_LOCAL` | `CHAR(10)` | Local number |
| `C_EXT` | `CHAR(5)` | Extension |

#### Customer.txt

Pipe-delimited, daily deliveries only, ordered by `CDC_DSN`.

```text
I|6440|4728|031-80-9744|ACTV|Kempffer|Tandie|||3|1933-04-19|16132 Corsicana Road||85775|Overland Park|DC|United States of America|||880-2336|||266|978-6267||||||Tandie.Kempffer@gawab.com|Tandie.Kempffer@mail2world.com|DE2|MN1
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’ or ‘U’ | Denotes insert or update |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `C_ID` | `IDENT_T` | Not NULL | Customer identifier |
| `C_TAX_ID` | `CHAR(20)` | Not NULL | Customer’s tax identifier |
| `C_ST_ID` | `CHAR(4)` | ‘ACTV’ or ‘INAC’ | Customer status type identifier |
| `C_L_NAME` | `CHAR(25)` | Not NULL | Primary Customer's last name. |
| `C_F_NAME` | `CHAR(20)` | Not NULL | Primary Customer's first name. |
| `C_M_NAME` | `CHAR(1)` |  | Primary Customer's middle initial |
| `C_GNDR` | `CHAR(1)` |  | Gender of the primary customer |
| `C_TIER` | `NUM(1)` |  | Customer tier |
| `C_DOB` | `DATE` | Not NULL | Customer’s date of birth, as YYYY-MM-DD. |
| `C_ADLINE1` | `CHAR(80)` | Not NULL | Address Line 1 |
| `C_ADLINE2` | `CHAR(80)` |  | Address Line 2 |
| `C_ZIPCODE` | `CHAR(12)` | Not NULL | Zip or postal code |
| `C_CITY` | `CHAR(25)` | Not NULL | City |
| `C_STATE_PROV` | `CHAR(20)` | Not NULL | State or province |
| `C_CTRY` | `CHAR(24)` |  | Country |
| `C_CTRY_1` | `CHAR(3)` |  | Country code for Customer's phone 1. |
| `C_AREA_1` | `CHAR(3)` |  | Area code for customer’s phone 1. |
| `C_LOCAL_1` | `CHAR(10)` |  | Local number for customer’s phone 1. |
| `C_EXT_1` | `CHAR(5)` |  | Extension number for Customer’s phone 1. |
| `C_CTRY_2` | `CHAR(3)` |  | Country code for Customer's phone 2. |
| `C_AREA_2` | `CHAR(3)` |  | Area code for Customer’s phone 2. |
| `C_LOCAL_2` | `CHAR(10)` |  | Local number for Customer’s phone 2. |
| `C_EXT_2` | `CHAR(5)` |  | Extension number for Customer’s phone 2. |
| `C_CTRY_3` | `CHAR(3)` |  | Country code for Customer's phone 3. |
| `C_AREA_3` | `CHAR(3)` |  | Area code for Customer’s phone 3. |
| `C_LOCAL_3` | `CHAR(10)` |  | Local number for Customer’s phone 3. |
| `C_EXT_3` | `CHAR(5)` |  | Extension number for Customer’s phone 3. |
| `C_EMAIL_1` | `CHAR(50)` |  | Customer's e-mail address 1. |
| `C_EMAIL_2` | `CHAR(50)` |  | Customer's e-mail address 2. |
| `C_LCL_TX_ID` | `CHAR(4)` | Not NULL | Customer's local tax rate |
| `C_NAT_TX_ID` | `CHAR(4)` | Not NULL | Customer's national tax rate |

#### Account.txt

Pipe-delimited, daily deliveries only, ordered by `CDC_DSN`.

```text
I|12860|9008|11473|4728||1|ACTV
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’ or ‘U’ | Denotes insert or update |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `CA_ID` | `IDENT_T` | Not NULL | Customer account identifier |
| `CA_B_ID` | `IDENT_T` | Not NULL | Identifier of the managing broker |
| `CA_C_ID` | `IDENT_T` | Not NULL | Owning customer identifier |
| `CA_NAME` | `CHAR(50)` |  | Name of customer account |
| `CA_TAX_ST` | `NUM(1)` | 0, 1 or 2 | Tax status of this account |
| `CA_ST_ID` | `CHAR(4)` | ‘ACTV’ or ‘INAC’ | Customer status type identifier |

</details>

<details>
<summary>Trading & Activity Sources (Trade, TradeHistory, CashTransaction, HoldingHistory, WatchHistory)</summary>

#### Trade.txt

Pipe-delimited. The initial load is ordered by `T_ID`; daily deliveries by `CDC_DSN`.

*Initial load:*

```text
0|2012-07-07 00:02:34|CMPT|TMB|0|AAAAAAAAAAAACQP|2939|9.57|0|3160|10.02|58.95|27.31|1611.19
```

*Daily delivery:*

```text
U|1488405|372101|2017-07-08 00:57:49|SBMT|TLS|0|AAAAAAAAAAAABFV|5225|4.06|428|990||||
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’, ‘U’ | Denotes insert, update |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `T_ID` | `TRADE_T` | Not Null | Trade identifier. |
| `T_DTS` | `DATETIME` | Not Null | Date and time of trade. |
| `T_ST_ID` | `CHAR(4)` | Not Null | Status type identifier |
| `T_TT_ID` | `CHAR(3)` | Not Null | Trade type identifier |
| `T_IS_CASH` | `BOOLEAN` | ‘0’ or ’1’ | Account type: cash (‘1’) or margin (‘0’). The project keeps it constant for all trades of an account; TPC-DI allows it to vary by trade. |
| `T_S_SYMB` | `CHAR(15)` | Not Null | Security symbol of the security |
| `T_QTY` | `S_QTY_T` | > 0 | Quantity of securities traded. |
| `T_BID_PRICE` | `S_PRICE_T` | > 0 | The requested unit price. |
| `T_CA_ID` | `IDENT_T` | Not Null | Customer account identifier. |
| `T_EXEC_NAME` | `CHAR(49)` | Not Null | Name of the person executing the trade. |
| `T_TRADE_PRICE` | `S_PRICE_T` | Null except in CMPT records, then > 0 | Unit price at which the security was traded. |
| `T_CHRG` | `VALUE_T` | Null except in CMPT records, then >= 0 | Fee charged for placing this trade request. |
| `T_COMM` | `VALUE_T` | Null except in CMPT records, then >= 0 | Commission earned on this trade |
| `T_TAX` | `VALUE_T` | Null except in CMPT records, then >= 0 | Amount of tax due on this trade |
| `T_CHANNEL` | Text | `WEB`, `MOBILE`, `BROKER` | Project addition at the end of version 2 daily rows, from the configured date, 2019-07-01 by default. Version 1 has 14 business fields; version 2 has 15. Daily rows also have the two CDC fields. |

The initial load always uses version 1, even if its period reaches the configured channel date. A daily file uses its delivery business date to select the contract, not the trade's original `T_DTS`. An update to an earlier trade therefore carries `T_CHANNEL` when its daily contract requires it.

DP1 checks field count against the dated contract. The first version 2 write adds the channel column to Bronze through Delta schema merge. Silver maps both versions to one schema, and `dim_trade.channel` stays NULL when the source did not record it. Rows that match neither contract go to quarantine.

#### TradeHistory.txt

Pipe-delimited, initial load only, ordered by `TH_T_ID`.

```text
0|2012-07-07 00:01:13|SBMT
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TH_T_ID` | `TRADE_T` | Not Null | Trade identifier. Corresponds to T_ID in the Trade.txt file |
| `TH_DTS` | `DATETIME` | Not Null | When the trade history was updated. |
| `TH_ST_ID` | `CHAR(4)` | Not Null | Status type identifier. |

#### CashTransaction.txt

Pipe-delimited, initial load only; rows omit `CDC_FLAG` and `CDC_DSN`. The project adds `CT_ID` as the first field. Later cash transactions arrive as stream messages with the same identity (Section 7.1).

```text
81234567|1001|2018-12-11 08:10:42|-400.00|Withdrawal
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CT_ID` | Source-assigned integer | Unique cash transaction ID; duplicates keep it | Project addition: one global source-system sequence, also carried in stream messages. |
| `CT_CA_ID` | `IDENT_T` | Not Null | Customer account identifier |
| `CT_DTS` | `DATETIME` | Not Null | Time of the cash transaction |
| `CT_AMT` | `VALUE_T` | Not Null | Signed cash transaction amount |
| `CT_NAME` | `CHAR(100)` | Not Null | Fixed forms: `Trade settlement <trade_id>`, `Deposit`, `Withdrawal`, or `Dividend <symbol>`. DP2 and Flink Job 1 use one shared parser to derive `cash_type` and `trade_id`. |

#### HoldingHistory.txt

Pipe-delimited. Daily deliveries are ordered by `CDC_DSN`.

```text
0|0|0|2939
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’ | Denotes insert |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `HH_H_T_ID` | `TRADE_T` | Not Null | Trade Identifier of the trade that originally created the holding row. |
| `HH_T_ID` | `TRADE_T` | Not Null | Trade Identifier of the current trade |
| `HH_BEFORE_QTY` | `S_QTY_T` | Not Null | Quantity of this security held before the modifying trade. |
| `HH_AFTER_QTY` | `S_QTY_T` | Not Null | Quantity of this security held after the modifying trade. |

#### WatchHistory.txt

Pipe-delimited. The initial load is ordered by `W_DTS`; daily deliveries by `CDC_DSN`.

```text
17|AAAAAAAAAAAAAJR|2012-07-07 00:03:44|ACTV
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’ | Rows are only added |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `W_C_ID` | `IDENT_T` | Not Null | Customer identifier |
| `W_S_SYMB` | `CHAR(15)` | Not Null | Symbol of the security to watch |
| `W_DTS` | `DATETIME` | Not Null | Date and Time Stamp for the action |
| `W_ACTION` | `CHAR(4)` | ‘ACTV’ or ‘CNCL’ | Whether activating or canceling the watch |

</details>

<details>
<summary>Market & Company Sources (FINWIRE, DailyMarket)</summary>

#### FINWIRE

Fixed-width, used in the initial load and in daily deliveries on release dates. One file per posting quarter (`FINWIRE2003Q1`); records in `PTS` order, types mixed. Text pads right with spaces, numbers pad left with spaces, CIKs pad left with zeroes; an empty field is all spaces. Records include full trailing padding and use numeric CIKs, not company names.

**CMP** (Company, 543 characters)

```text
19670101-154842CMPkkhSvCsfgbDUvsiZWtPQwjkYPwKrfTUFMGMCWCBXBcOcTkaLSudO        0000000000ACTVDRA   1886072219918 Rogers North                                                                                                                                              35233       Long Beach               NC                  United States of AmericaEllebracht                                     CUpESkLIQ LBcTnhDEOCLVhfDjOopGKRBxRYGboIrPFcDxMSGkskSwAJdAPWSAPTf T
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `PTS` | `CHAR(15)` | Not empty | Posting date & time as YYYYMMDD-HHMMSS |
| `RecType` | `CHAR(3)` | Not empty | “CMP” |
| `CompanyName` | `CHAR(60)` | Not empty | Name of the company |
| `CIK` | `CHAR(10)` | Not empty | Company identification code from SEC |
| `Status` | `CHAR(4)` | Not empty | ‘ACTV’ for Active company, ‘INAC’ for inactive |
| `IndustryID` | `CHAR(2)` | Not empty | Code for industry segment |
| `SPrating` | `CHAR(4)` | Not empty | S&P rating |
| `FoundingDate` | `CHAR(8)` |  | A date as YYYYMMDD |
| `AddrLine1` | `CHAR(80)` | Not empty | Mailing address |
| `AddrLine2` | `CHAR(80)` |  | Mailing address |
| `PostalCode` | `CHAR(12)` | Not empty | Mailing address |
| `City` | `CHAR(25)` | Not empty | Mailing address |
| `StateProvince` | `CHAR(20)` | Not empty | Mailing address |
| `Country` | `CHAR(24)` |  | Mailing address |
| `CEOname` | `CHAR(46)` | Not empty | Name of company CEO |
| `Description` | `CHAR(150)` | Not empty | Description of the company |

**SEC** (Security, 170 characters with a numeric CIK)

```text
19670427-161251SECAAAAAAAAAAAAAAACOMMONACTVDiIPnSJHqplAHTPKTGIhStBRNgEpVZYuRSRDlerURQYDcFPMaJ                    NASDAQ652346051    1905061019660302        1.850000000005
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `PTS` | `CHAR(15)` | Not empty | Posting date & time as YYYYMMDD-HHMMSS |
| `RecType` | `CHAR(3)` | Not empty | “SEC” |
| `Symbol` | `CHAR(15)` | Not empty | Security symbol |
| `IssueType` | `CHAR(6)` | Not empty | Issue type |
| `Status` | `CHAR(4)` | ‘ACTV’ | Securities remain active; no inactive versions |
| `Name` | `CHAR(70)` | Not empty | Security name |
| `ExID` | `CHAR(6)` | Not empty | ID of the exchange the security is traded on |
| `ShOut` | `CHAR(13)` | Not empty | Number of shares outstanding |
| `FirstTradeDate` | `CHAR(8)` | Not empty | Date of first trade as YYYYMMDD |
| `FirstTradeExchg` | `CHAR(8)` | Not empty | Date of first trade on exchange as YYYYMMDD |
| `Dividend` | `CHAR(12)` | Not empty | Dividend as VALUE_T |
| `CoNameOrCIK` | `CHAR(10)` | Not empty; digits only | Company CIK number |

**FIN** (Financial, 196 characters with a numeric CIK)

```text
19670401-063705FIN196721967040119670401    9288741181.53    2284183075.45       13.18       12.44        0.25     805819257.50  135871569610.30    4339544932.47    173262702    1836200100000000068
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `PTS` | `CHAR(15)` | Not empty | Posting date & time as YYYYMMDD-HHMMSS |
| `RecType` | `CHAR(3)` | Not empty | “FIN” |
| `Year` | `CHAR(4)` | Not empty | Year of the quarter end. |
| `Quarter` | `CHAR(1)` | Not empty | Quarter number: valid values are ‘1’, ‘2’, ‘3’, ‘4’ |
| `QtrStartDate` | `CHAR(8)` | Not empty | Start date of quarter, as YYYYMMDD |
| `PostingDate` | `CHAR(8)` | Not empty | Posting date of quarterly report as YYYYMMDD |
| `Revenue` | `CHAR(17)` | Not empty | Reported revenue for the quarter |
| `Earnings` | `CHAR(17)` | Not empty | Net earnings reported for the quarter |
| `EPS` | `CHAR(12)` | Not empty | Basic earnings per share for the quarter |
| `DilutedEPS` | `CHAR(12)` | Not empty | Diluted earnings per share for the quarter |
| `Margin` | `CHAR(12)` | Not empty | Profit divided by revenues for the quarter |
| `Inventory` | `CHAR(17)` | Not empty | Value of inventory on hand at end of quarter |
| `Assets` | `CHAR(17)` | Not empty | Value of total assets at the end of quarter |
| `Liabilities` | `CHAR(17)` | Not empty | Value of total liabilities at the end of quarter |
| `ShOut` | `CHAR(13)` | Not empty | Average number of shares outstanding |
| `DilutedShOut` | `CHAR(13)` | Not empty | Average number of shares outstanding (diluted) |
| `CoNameOrCIK` | `CHAR(10)` | Not empty; digits only | Company CIK number |

#### DailyMarket.txt

Pipe-delimited. Daily deliveries are ordered by `CDC_DSN`.

```text
2015-07-06|AAAAAAAAAAAABOY|242.93|284.42|185.08|111904727
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `CDC_FLAG` | `CDC_FLAG_T` | ‘I’ | Denotes insert |
| `CDC_DSN` | `CDC_DSN_T` | Not NULL | Database Sequence Number |
| `DM_DATE` | `DATE` | Not Null | Date of last completed trading day. |
| `DM_S_SYMB` | `CHAR(15)` | Not Null | Security symbol of the security |
| `DM_CLOSE` | `S_PRICE_T` | Not Null | Closing price of the security on this day. |
| `DM_HIGH` | `S_PRICE_T` | Not Null | Highest price for the security on this day. |
| `DM_LOW` | `S_PRICE_T` | Not Null | Lowest price for the security on this day. |
| `DM_VOL` | `S_COUNT_T` | Not Null | Volume of the security on this day. |

</details>

<details>
<summary>Reference, HR & Prospect Sources</summary>

#### HR.csv

Comma-delimited, initial load only, ordered by `EmployeeID`.

```text
0,211,Ozkan,Douglas,,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,(726) 088-3331
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `EmployeeID` | `IDENT_T` | Not NULL | ID of employee |
| `ManagerID` | `IDENT_T` | Not NULL | ID of employee’s manager |
| `EmployeeFirstName` | `CHAR(30)` | Not NULL | First name |
| `EmployeeLastName` | `CHAR(30)` | Not NULL | Last name |
| `EmployeeMI` | `CHAR(1)` |  | Middle initial |
| `EmployeeJobCode` | `NUM(3)` |  | Numeric job code |
| `EmployeeBranch` | `CHAR(30)` |  | Facility in which employee has office |
| `EmployeeOffice` | `CHAR(10)` |  | Office number or description |
| `EmployeePhone` | `CHAR(14)` |  | Employee phone number |

#### Prospect.csv

Comma-delimited, full snapshot at `initial_end`, then on the first day of each month.

```text
PEL0,PELLAND,Netti,,f,21847 olympia street,,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,,3,W,20,760,O,Brink's,,1058868
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `AgencyID` | `CHAR(30)` | Not NULL | Unique identifier from agency |
| `LastName` | `CHAR(30)` | Not NULL | Last name |
| `FirstName` | `CHAR(30)` | Not NULL | First name |
| `MiddleInitial` | `CHAR(1)` |  | Middle initial |
| `Gender` | `CHAR(1)` |  | ‘M’ or ‘F’ or ‘U’ |
| `AddressLine1` | `CHAR(80)` |  | Postal address |
| `AddressLine2` | `CHAR(80)` |  | Postal address |
| `PostalCode` | `CHAR(12)` |  | Postal code |
| `City` | `CHAR(25)` | Not NULL | City |
| `State` | `CHAR(20)` | Not NULL | State or province |
| `Country` | `CHAR(24)` |  | Postal country |
| `Phone` | `CHAR(30)` |  | Telephone number |
| `Income` | `NUM(9)` |  | Annual income |
| `NumberCars` | `NUM(2)` |  | Cars owned |
| `NumberChildren` | `NUM(2)` |  | Dependent children |
| `MaritalStatus` | `CHAR(1)` |  | ‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’ |
| `Age` | `NUM(3)` |  | Current age |
| `CreditRating` | `NUM(4)` |  | Numeric rating |
| `OwnOrRentFlag` | `CHAR(1)` |  | ‘O’ or ‘R’ or ‘U’ |
| `Employer` | `CHAR(30)` |  | Name of employer |
| `NumberCreditCards` | `NUM(2)` |  | Credit cards |
| `NetWorth` | `NUM(12)` |  | Estimated total net worth |

#### Date.txt

Pipe-delimited, initial load only.

```text
19500101|1950-01-01|January 1, 1950|1950|1950|19501|1950 Q1|19501|1950 January|19501|1950-W1|7|Sunday|1950|1950|19503|1950 Q3|true
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_DateID` | `IDENT_T` | Not NULL | Surrogate key for the date |
| `DateValue` | `CHAR(20)` | Not NULL | The date as text, e.g. “2004-07-07” |
| `DateDesc` | `CHAR(20)` | Not NULL | The date Month Day, YYYY, e.g. July 7, 2004 |
| `CalendarYearID` | `NUM(4)` | Not NULL | Year number as a number |
| `CalendarYearDesc` | `CHAR(20)` | Not NULL | Year number as text |
| `CalendarQtrID` | `NUM(5)` | Not NULL | Quarter as a number, e.g. 20042 |
| `CalendarQtrDesc` | `CHAR(20)` | Not NULL | Quarter as text, e.g. “2004 Q2” |
| `CalendarMonthID` | `NUM(6)` | Not NULL | Month as a number, e.g. 20047 |
| `CalendarMonthDesc` | `CHAR(20)` | Not NULL | Month as text, e.g. “2004 July” |
| `CalendarWeekID` | `NUM(6)` | Not NULL | Week as a number, e.g. 200428 |
| `CalendarWeekDesc` | `CHAR(20)` | Not NULL | Week as text, e.g. “2004-W28” |
| `DayOfWeekNum` | `NUM(1)` | Not NULL | Day of week as a number, e.g. 3 |
| `DayOfWeekDesc` | `CHAR(10)` | Not NULL | Day of week as text, e.g. “Wednesday” |
| `FiscalYearID` | `NUM(4)` | Not NULL | Fiscal year as a number, e.g. 2005 |
| `FiscalYearDesc` | `CHAR(20)` | Not NULL | Fiscal year as text, e.g. “2005” |
| `FiscalQtrID` | `NUM(5)` | Not NULL | Fiscal quarter as a number, e.g. 20051 |
| `FiscalQtrDesc` | `CHAR(20)` | Not NULL | Fiscal quarter as text, e.g. “2005 Q1” |
| `HolidayFlag` | `BOOLEAN` |  | Indicates holidays |

#### Time.txt

Pipe-delimited, initial load only.

```text
000000|00:00:00|00|00|00|00:00|00|00:00:00|false|false
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_TimeID` | `IDENT_T` | Not NULL | Surrogate key for the time |
| `TimeValue` | `CHAR(20)` | Not NULL | The time as text, e.g. “01:23:45” |
| `HourID` | `NUM(2)` | Not NULL | Hour number as a number, e.g. 01 |
| `HourDesc` | `CHAR(20)` | Not NULL | Hour number as text, e.g. “01” |
| `MinuteID` | `NUM(2)` | Not NULL | Minute as a number, e.g. 23 |
| `MinuteDesc` | `CHAR(20)` | Not NULL | Minute as text, e.g. “01:23” |
| `SecondID` | `NUM(2)` | Not NULL | Second as a number, e.g. 45 |
| `SecondDesc` | `CHAR(20)` | Not NULL | Second as text, e.g. “01:23:45” |
| `MarketHoursFlag` | `BOOLEAN` |  | Indicates a time during market hours |
| `OfficeHoursFlag` | `BOOLEAN` |  | Indicates a time during office hours |

#### Industry.txt

Pipe-delimited, initial load only.

```text
AA|Misc. Capital Goods|FN
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `IN_ID` | `CHAR(2)` | Not NULL | Industry code |
| `IN_NAME` | `CHAR(50)` | Not NULL | Industry description |
| `IN_SC_ID` | `CHAR(4)` | Not NULL | Sector identifier |

#### StatusType.txt

Pipe-delimited, initial load only.

```text
ACTV|Active
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `ST_ID` | `CHAR(4)` | Not NULL | Status code |
| `ST_NAME` | `CHAR(10)` | Not NULL | Status description |

#### TaxRate.txt

Pipe-delimited, initial load only.

```text
US1|U.S. Income Tax Bracket for the poor|0.15000
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TX_ID` | `CHAR(4)` | Not NULL | Tax rate code |
| `TX_NAME` | `CHAR(50)` | Not NULL | Tax rate description |
| `TX_RATE` | `NUM(6,5)` | Not NULL | Tax rate |

#### TradeType.txt

Pipe-delimited, initial load only.

```text
TMB|Market Buy|0|1
```

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TT_ID` | `CHAR(3)` | Not NULL | Trade type code |
| `TT_NAME` | `CHAR(12)` | Not NULL | Trade type description |
| `TT_IS_SELL` | `NUM(1)` | Not NULL | Flag indicating a sale |
| `TT_IS_MRKT` | `NUM(1)` | Not NULL | Flag indicating a market order |

</details>

### 9.3 Warehouse (Gold) Schemas

The following dictionaries retain the TPC-DI field order, types, restrictions, and descriptions from PDF clause 3.2. `BatchID` is benchmark reference metadata, not a specified project run-metadata contract. Target tables use the snake_case names below. `dim_trade` also has nullable `channel`; the added cash transaction fact follows the logical contract in Section 6.

<details>
<summary>SCD2 Dimension Pattern & Common History Fields</summary>

All SCD2 dimensions (`dim_customer`, `dim_account`, `dim_broker`, `dim_company`, `dim_security`) end with these fields:

| Field | Type | Description |
| --- | --- | --- |
| `IsCurrent` | `BOOLEAN` | True for the current version |
| `BatchID` | `NUM(5)` | Batch that inserted this version |
| `EffectiveDate` | `DATE` | First date this version applies |
| `EndDate` | `DATE` | Exclusive end date; `9999-12-31` for the current version |

</details>

<details>
<summary>Customer & Account Tables (dim_customer, dim_account, dim_broker, prospect)</summary>

#### dim_customer

SCD2 dimension. One row per customer version.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_CustomerID` | `SK_T` | Not NULL | Surrogate key for CustomerID |
| `CustomerID` | `IDENT_T` | Not NULL | Customer identifier |
| `TaxID` | `CHAR(20)` | Not NULL | Customer’s tax identifier |
| `Status` | `CHAR(10)` | Not NULL | Customer status type |
| `LastName` | `CHAR(30)` | Not NULL | Customer's last name. |
| `FirstName` | `CHAR(30)` | Not NULL | Customer's first name. |
| `MiddleInitial` | `CHAR(1)` |  | Customer's middle name initial |
| `Gender` | `CHAR(1)` |  | Gender of the customer |
| `Tier` | `NUM(1)` |  | Customer tier |
| `DOB` | `DATE` | Not NULL | Customer’s date of birth. |
| `AddressLine1` | `CHAR(80)` | Not NULL | Address Line 1 |
| `AddressLine2` | `CHAR(80)` |  | Address Line 2 |
| `PostalCode` | `CHAR(12)` | Not NULL | Zip or Postal Code |
| `City` | `CHAR(25)` | Not NULL | City |
| `StateProv` | `CHAR(20)` | Not NULL | State or Province |
| `Country` | `CHAR(24)` |  | Country |
| `Phone1` | `CHAR(30)` |  | Phone number 1 |
| `Phone2` | `CHAR(30)` |  | Phone number 2 |
| `Phone3` | `CHAR(30)` |  | Phone number 3 |
| `Email1` | `CHAR(50)` |  | Email address 1 |
| `Email2` | `CHAR(50)` |  | Email address 2 |
| `NationalTaxRateDesc` | `CHAR(50)` |  | National Tax rate description |
| `NationalTaxRate` | `NUM(6,5)` |  | National Tax rate |
| `LocalTaxRateDesc` | `CHAR(50)` |  | Local Tax rate description |
| `LocalTaxRate` | `NUM(6,5)` |  | Local Tax rate |
| `AgencyID` | `CHAR(30)` |  | Agency identifier |
| `CreditRating` | `NUM(5)` |  | Credit rating |
| `NetWorth` | `SNUM(10)` |  | Net worth |
| `MarketingNameplate` | `CHAR(100)` |  | Marketing nameplate |
| `IsCurrent` | `BOOLEAN` | Not NULL | True if this is the current record |
| `BatchID` | `NUM(5)` | Not NULL | Batch ID when this record was inserted |
| `EffectiveDate` | `DATE` | Not NULL | Beginning of date range when this record was the current record |
| `EndDate` | `DATE` | Not NULL | Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31. |

#### dim_account

SCD2 dimension. One row per account version.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_AccountID` | `SK_T` | Not NULL | Surrogate key for AccountID |
| `AccountID` | `IDENT_T` | Not NULL | Customer account identifier |
| `SK_BrokerID` | `SK_T` | Not NULL | Surrogate key of managing broker |
| `SK_CustomerID` | `SK_T` | Not NULL | Surrogate key of customer |
| `Status` | `CHAR(10)` | Not NULL | Account status, active or closed |
| `AccountDesc` | `CHAR(50)` |  | Name of customer account |
| `TaxStatus` | `NUM(1)` | 0, 1 or 2 | Tax status of this account |
| `IsCurrent` | `BOOLEAN` | Not NULL | True if this is the current record |
| `BatchID` | `NUM(5)` | Not NULL | Batch ID when this record was inserted |
| `EffectiveDate` | `DATE` | Not NULL | Beginning of date range when this record was the current record |
| `EndDate` | `DATE` | Not NULL | Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31. |

#### dim_broker

SCD2 dimension structure with one initial version per broker. The generator simulates no later HR updates; keep the existing history columns.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_BrokerID` | `SK_T` | Not NULL | Surrogate key for broker |
| `BrokerID` | `IDENT_T` | Not NULL | Natural key for broker |
| `ManagerID` | `IDENT_T` |  | Natural key for manager’s HR record |
| `FirstName` | `CHAR(50)` | Not NULL | First name |
| `LastName` | `CHAR(50)` | Not NULL | Last Name |
| `MiddleInitial` | `CHAR(1)` |  | Middle initial |
| `Branch` | `CHAR(50)` |  | Facility in which employee has office |
| `Office` | `CHAR(50)` |  | Office number or description |
| `Phone` | `CHAR(14)` |  | Employee phone number |
| `IsCurrent` | `BOOLEAN` | Not NULL | True if this is the current record |
| `BatchID` | `NUM(5)` | Not NULL | Batch ID when this record was inserted |
| `EffectiveDate` | `DATE` | Not NULL | Beginning of date range when this record was the current record |
| `EndDate` | `DATE` | Not NULL | Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31. |

#### prospect

Current state, not SCD2. One row per prospect.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `AgencyID` | `CHAR(30)` | Not NULL | Unique identifier from agency |
| `SK_RecordDateID` | `SK_T` | Not NULL | Last date this prospect appeared in input |
| `SK_UpdateDateID` | `SK_T` | Not NULL | Latest change date for this prospect |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was last modified |
| `IsCustomer` | `BOOLEAN` | Not NULL | True if this person is also in dim_customer, else False |
| `LastName` | `CHAR(30)` | Not NULL | Last name |
| `FirstName` | `CHAR(30)` | Not NULL | First name |
| `MiddleInitial` | `CHAR(1)` |  | Middle initial |
| `Gender` | `CHAR(1)` |  | M / F / U |
| `AddressLine1` | `CHAR(80)` |  | Postal address |
| `AddressLine2` | `CHAR(80)` |  | Postal address |
| `PostalCode` | `CHAR(12)` |  | Postal code |
| `City` | `CHAR(25)` | Not NULL | City |
| `State` | `CHAR(20)` | Not NULL | State or province |
| `Country` | `CHAR(24)` |  | Postal country |
| `Phone` | `CHAR(30)` |  | Telephone number |
| `Income` | `NUM(9)` |  | Annual income |
| `NumberCars` | `NUM(2)` |  | Cars owned |
| `NumberChildren` | `NUM(2)` |  | Dependent children |
| `MaritalStatus` | `CHAR(1)` |  | S / M / D / W / U |
| `Age` | `NUM(3)` |  | Current age |
| `CreditRating` | `NUM(4)` |  | Numeric rating |
| `OwnOrRentFlag` | `CHAR(1)` |  | O / R / U |
| `Employer` | `CHAR(30)` |  | Name of employer |
| `NumberCreditCards` | `NUM(2)` |  | Credit cards |
| `NetWorth` | `NUM(12)` |  | Estimated total net worth |
| `MarketingNameplate` | `CHAR(100)` |  | For marketing purposes |

</details>

<details>
<summary>Company & Market Tables (dim_company, dim_security, financial, fact_market_history)</summary>

#### dim_company

SCD2 dimension. One row per company version.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_CompanyID` | `SK_T` | Not NULL | Surrogate key for CompanyID |
| `CompanyID` | `IDENT_T` | Not NULL | Company identifier (CIK number) |
| `Status` | `CHAR(10)` | Not NULL | Company status |
| `Name` | `CHAR(60)` | Not NULL | Company name |
| `Industry` | `CHAR(50)` | Not NULL | Company’s industry |
| `SPrating` | `CHAR(4)` |  | Standard & Poor company’s rating |
| `isLowGrade` | `BOOLEAN` |  | True if this company is low grade |
| `CEO` | `CHAR(100)` | Not NULL | CEO name |
| `AddressLine1` | `CHAR(80)` |  | Address Line 1 |
| `AddressLine2` | `CHAR(80)` |  | Address Line 2 |
| `PostalCode` | `CHAR(12)` | Not NULL | Zip or postal code |
| `City` | `CHAR(25)` | Not NULL | City |
| `StateProv` | `CHAR(20)` | Not NULL | State or Province |
| `Country` | `CHAR(24)` |  | Country |
| `Description` | `CHAR(150)` | Not NULL | Company description |
| `FoundingDate` | `DATE` |  | Date the company was founded |
| `IsCurrent` | `BOOLEAN` | Not NULL | True if this is the current record |
| `BatchID` | `NUM(5)` | Not NULL | Batch ID when this record was inserted |
| `EffectiveDate` | `DATE` | Not NULL | Beginning of date range when this record was the current record |
| `EndDate` | `DATE` | Not NULL | Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31. |

#### dim_security

SCD2 dimension. One row per security version.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_SecurityID` | `SK_T` | Not NULL | Surrogate key for Symbol |
| `Symbol` | `CHAR(15)` | Not NULL | Identifies security on “ticker” |
| `Issue` | `CHAR(6)` | Not NULL | Issue type |
| `Status` | `CHAR(10)` | Not NULL | Status type |
| `Name` | `CHAR(70)` | Not NULL | Security name |
| `ExchangeID` | `CHAR(6)` | Not NULL | Exchange the security is traded on |
| `SK_CompanyID` | `SK_T` | Not NULL | Company issuing security |
| `SharesOutstanding` | `S_COUNT_T` | Not NULL | Shares outstanding |
| `FirstTrade` | `DATE` | Not NULL | Date of first trade |
| `FirstTradeOnExchange` | `DATE` | Not NULL | Date of first trade on this exchange |
| `Dividend` | `VALUE_T` | Not NULL | Annual dividend per share |
| `IsCurrent` | `BOOLEAN` | Not NULL | True if this is the current record |
| `BatchID` | `NUM(5)` | Not NULL | Batch ID when this record was inserted |
| `EffectiveDate` | `DATE` | Not NULL | Beginning of date range when this record was the current record |
| `EndDate` | `DATE` | Not NULL | Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31. |

#### financial

Current state, not SCD2. One row per company per reporting quarter; retain earlier quarters when a new report arrives. The report's period comes from its fields, not the FINWIRE file quarter. Resolve the company version at `PTS`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_CompanyID` | `IDENT_T` | Not NULL | Company SK. |
| `FI_YEAR` | `NUM(4)` | Not NULL | Year of the quarter end. |
| `FI_QTR` | `NUM(1)` | Not NULL | Quarter number that the financial information is for: valid values 1, 2, 3, 4. |
| `FI_QTR_START_DATE` | `DATE` | Not NULL | Start date of quarter. |
| `FI_REVENUE` | `SNUM(15,2)` | Not NULL | Reported revenue for the quarter. |
| `FI_NET_EARN` | `SNUM(15,2)` | Not NULL | Net earnings reported for the quarter. |
| `FI_BASIC_EPS` | `SNUM(10,2)` | Not NULL | Basic earnings per share for the quarter. |
| `FI_DILUT_EPS` | `SNUM(10,2)` | Not NULL | Diluted earnings per share for the quarter. |
| `FI_MARGIN` | `SNUM(10,2)` | Not NULL | Profit divided by revenues for the quarter. |
| `FI_INVENTORY` | `SNUM(15,2)` | Not NULL | Value of inventory on hand at the end of quarter. |
| `FI_ASSETS` | `SNUM(15,2)` | Not NULL | Value of total assets at the end of the quarter. |
| `FI_LIABILITY` | `SNUM(15,2)` | Not NULL | Value of total liabilities at the end of the quarter. |
| `FI_OUT_BASIC` | `SNUM(12)` | Not NULL | Average number of shares outstanding (basic). |
| `FI_OUT_DILUT` | `SNUM(12)` | Not NULL | Average number of shares outstanding (diluted). |

#### fact_market_history

Fact. One row per security per day.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_SecurityID` | `SK_T` | Not Null | Surrogate key for SecurityID |
| `SK_CompanyID` | `SK_T` | Not Null | Surrogate key for CompanyID |
| `SK_DateID` | `SK_T` | Not Null | Surrogate key for the date |
| `PERatio` | `NUM(10,2)` |  | Price to earnings per share ratio |
| `Yield` | `NUM(5,2)` | Not Null | Dividend to price ratio, as a percentage |
| `FiftyTwoWeekHigh` | `NUM(8,2)` | Not Null | Security highest price in last 52 weeks from this day |
| `SK_FiftyTwoWeekHighDate` | `SK_T` | Not Null | Earliest date on which the 52 week high price was set |
| `FiftyTwoWeekLow` | `NUM(8,2)` | Not Null | Security lowest price in last 52 weeks from this day |
| `SK_FiftyTwoWeekLowDate` | `SK_T` | Not Null | Earliest date on which the 52 week low price was set |
| `ClosePrice` | `NUM(8,2)` | Not Null | Security closing price on this day |
| `DayHigh` | `NUM(8,2)` | Not Null | Highest price for the security on this day |
| `DayLow` | `NUM(8,2)` | Not Null | Lowest price for the security on this day |
| `Volume` | `NUM(12)` | Not Null | Trading volume of the security on this day |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was inserted |

</details>

<details>
<summary>Trading & Activity Tables (dim_trade, fact_cash_balances, fact_holdings, fact_watches)</summary>

#### dim_trade

Updated in place, not SCD2. One row per trade.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TradeID` | `IDENT_T` | Not NULL | Trade identifier |
| `SK_BrokerID` | `SK_T` |  | Surrogate key for BrokerID |
| `SK_CreateDateID` | `SK_T` | Not NULL | Surrogate key for date created |
| `SK_CreateTimeID` | `SK_T` | Not NULL | Surrogate key for time created |
| `SK_CloseDateID` | `SK_T` |  | Surrogate key for date closed |
| `SK_CloseTimeID` | `SK_T` |  | Surrogate key for time closed |
| `Status` | `CHAR(10)` | Not NULL | Trade status |
| `Type` | `CHAR(12)` | Not NULL | Trade type |
| `CashFlag` | `BOOLEAN` | Not NULL | Is this trade a cash (1) or margin (0) trade? |
| `SK_SecurityID` | `SK_T` | Not NULL | Surrogate key for SecurityID |
| `SK_CompanyID` | `SK_T` | Not NULL | Surrogate key for CompanyID |
| `Quantity` | `NUM(6,0)` | Not NULL | Quantity of securities traded. |
| `BidPrice` | `NUM(8,2)` | Not NULL | The requested unit price. |
| `SK_CustomerID` | `SK_T` | Not NULL | Surrogate key for CustomerID |
| `SK_AccountID` | `SK_T` | Not NULL | Surrogate key for AccountID |
| `ExecutedBy` | `CHAR(64)` | Not NULL | Name of person executing the trade. |
| `TradePrice` | `NUM(8,2)` |  | Unit price at which the security was traded. |
| `Fee` | `NUM(10,2)` |  | Fee charged for placing this trade request |
| `Commission` | `NUM(10,2)` |  | Commission earned on this trade |
| `Tax` | `NUM(10,2)` |  | Amount of tax due on this trade |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was inserted |

#### fact_cash_balances

Fact. One row per account per day with cash transactions.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_CustomerID` | `SK_T` | Not Null | Surrogate key for CustomerID |
| `SK_AccountID` | `SK_T` | Not Null | Surrogate key for AccountID |
| `SK_DateID` | `SK_T` | Not Null | Surrogate key for the date |
| `Cash` | `SNUM(15,2)` | Not Null | Cash balance for the account after applying changes for this day |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was inserted |

#### fact_holdings

Fact. One row per holding change.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TradeID` | `IDENT_T` | Not NULL | Key for Original Trade Identifier |
| `CurrentTradeID` | `IDENT_T` | Not Null | Key for the current trade |
| `SK_CustomerID` | `SK_T` | Not NULL | Surrogate key for Customer Identifier |
| `SK_AccountID` | `SK_T` | Not NULL | Surrogate key for Account Identifier |
| `SK_SecurityID` | `SK_T` | Not NULL | Surrogate key for Security Identifier |
| `SK_CompanyID` | `SK_T` | Not NULL | Surrogate key for Company Identifier |
| `SK_DateID` | `SK_T` | Not NULL | Surrogate key for the date associated with the current trade |
| `SK_TimeID` | `SK_T` | Not NULL | Surrogate key for the time associated with the current trade |
| `CurrentPrice` | `S_PRICE_T` | > 0 | Unit price of this security for the current trade |
| `CurrentHolding` | `SNUM(6)` | Not NULL | Quantity of a security held after the current trade. The value can be a positive or negative integer |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was inserted |

#### fact_watches

Fact. One row per watch, from placement to removal.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_CustomerID` | `SK_T` | Not NULL | Customer associated with watch list |
| `SK_SecurityID` | `SK_T` | Not NULL | Security listed on watch list |
| `SK_DateID_DatePlaced` | `SK_T` | Not NULL | Date the watch list item was added |
| `SK_DateID_DateRemoved` | `SK_T` |  | Date the watch list item was removed |
| `BatchID` | `NUM(5)` | Not Null | Batch ID when this record was inserted |

</details>

<details>
<summary>Reference Tables (dim_date, dim_time, industry, status_type, tax_rate, trade_type)</summary>

#### dim_date

Loaded once from `Date.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_DateID` | `SK_T` | Not NULL | Surrogate key for the date |
| `DateValue` | `DATE` | Not NULL | The date stored appropriately for doing comparisons in the Data Warehouse |
| `DateDesc` | `CHAR(20)` | Not NULL | The date in full written form, e.g. “July 7, 2004” |
| `CalendarYearID` | `NUM(4)` | Not NULL | Year number as a number |
| `CalendarYearDesc` | `CHAR(20)` | Not NULL | Year number as text |
| `CalendarQtrID` | `NUM(5)` | Not NULL | Quarter as a number, e.g. 20042 |
| `CalendarQtrDesc` | `CHAR(20)` | Not NULL | Quarter as text, e.g. “2004 Q2” |
| `CalendarMonthID` | `NUM(6)` | Not NULL | Month as a number, e.g. 20047 |
| `CalendarMonthDesc` | `CHAR(20)` | Not NULL | Month as text, e.g. “2004 July” |
| `CalendarWeekID` | `NUM(6)` | Not NULL | Week as a number, e.g. 200428 |
| `CalendarWeekDesc` | `CHAR(20)` | Not NULL | Week as text, e.g. “2004-W28” |
| `DayOfWeekNum` | `NUM(1)` | Not NULL | Day of week as a number, e.g. 3 |
| `DayOfWeekDesc` | `CHAR(10)` | Not NULL | Day of week as text, e.g. “Wednesday” |
| `FiscalYearID` | `NUM(4)` | Not NULL | Fiscal year as a number, e.g. 2005 |
| `FiscalYearDesc` | `CHAR(20)` | Not NULL | Fiscal year as text, e.g. “2005” |
| `FiscalQtrID` | `NUM(5)` | Not NULL | Fiscal quarter as a number, e.g. 20051 |
| `FiscalQtrDesc` | `CHAR(20)` | Not NULL | Fiscal quarter as text, e.g. “2005 Q1” |
| `HolidayFlag` | `BOOLEAN` |  | Indicates holidays |

#### dim_time

Loaded once from `Time.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `SK_TimeID` | `SK_T` | Not NULL | Surrogate key for the time |
| `TimeValue` | `TIME` | Not NULL | The time stored appropriately for doing comparisons in the Data Warehouse |
| `HourID` | `NUM(2)` | Not NULL | Hour number as a number, e.g. 01 |
| `HourDesc` | `CHAR(20)` | Not NULL | Hour number as text, e.g. “01” |
| `MinuteID` | `NUM(2)` | Not NULL | Minute as a number, e.g. 23 |
| `MinuteDesc` | `CHAR(20)` | Not NULL | Minute as text, e.g. “01:23” |
| `SecondID` | `NUM(2)` | Not NULL | Second as a number, e.g. 45 |
| `SecondDesc` | `CHAR(20)` | Not NULL | Second as text, e.g. “01:23:45” |
| `MarketHoursFlag` | `BOOLEAN` |  | Indicates a time during market hours |
| `OfficeHoursFlag` | `BOOLEAN` |  | Indicates a time during office hours |

#### industry

Loaded once from `Industry.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `IN_ID` | `CHAR(2)` | Not NULL | Industry code |
| `IN_NAME` | `CHAR(50)` | Not NULL | Industry description |
| `IN_SC_ID` | `CHAR(4)` | Not NULL | Sector identifier |

#### status_type

Loaded once from `StatusType.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `ST_ID` | `CHAR(4)` | Not NULL | Status code |
| `ST_NAME` | `CHAR(10)` | Not NULL | Status description |

#### tax_rate

Loaded once from `TaxRate.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TX_ID` | `CHAR(4)` | Not NULL | Tax rate code |
| `TX_NAME` | `CHAR(50)` | Not NULL | Tax rate description |
| `TX_RATE` | `NUM(6,5)` | Not NULL | Tax rate |

#### trade_type

Loaded once from `TradeType.txt`.

| Field | Type | Restrictions | Description |
| --- | --- | --- | --- |
| `TT_ID` | `CHAR(3)` | Not NULL | Trade type code |
| `TT_NAME` | `CHAR(12)` | Not NULL | Trade type description |
| `TT_IS_SELL` | `NUM(1)` | Not NULL | Flag indicating a sale |
| `TT_IS_MRKT` | `NUM(1)` | Not NULL | Flag indicating a market order |

</details>

## 10. Differences from TPC-DI

The project keeps TPC-DI's business domain, source formats, and warehouse model, and changes the parts below. The last three rows correct mistakes in the specification itself. Clause numbers refer to the [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf).

| Area | TPC-DI | This project | Reason |
| --- | --- | --- | --- |
| Source data | Generated by DIGen at a TPC-DI scale factor | Generated by the project's generator at its own scale factor; the TPC-DI scale factor 3 sample is about factor 0.3 | The generator simulates customer behavior and an attrition signal for Layer 2 ([Data Platform](Data-Platform.md#2-data-generation-and-problem-injection)) |
| Load cycle | One historical load and two incremental updates (1.2.2) | Initial load from 2014-01-01 to 2018-12-31, then one daily delivery per calendar day to 2025-12-31 | Years of daily processing exercise reruns, late files, and the stream ([Section 2](#2-load-cycle-and-processing-strategy)) |
| Delivery layout | `Batch1/` to `Batch3/` folders, each with `BatchDate.txt` (2.3, 2.2.2.2) | `initial/` and `daily/YYYYMMDD/` paths; the path gives the data date | One folder per delivery date ([Section 3](#3-physical-source-files-and-ingestion-rules)) |
| Audit data | `<name>_audit.csv` files and the `Audit` table (2.2.2.20, 3.2.19, 7.4, 8.2) | Excluded | They serve only official benchmark scoring |
| Prospects | A full `Prospect.csv` in every batch (1.2.2.3) | A full snapshot on the first day of each month; the initial snapshot represents `initial_end` | Monthly marketing-list cadence ([Section 2.2](#22-source-cadence)) |
| FINWIRE | Quarterly files in the historical load only (1.3.5) | Also in daily deliveries: each quarter is released on the last day of the following month | Company and financial changes continue after cutover ([Section 2.2](#22-source-cadence)) |
| Cash transactions | `CashTransaction.txt` in every batch, with no ID field (2.2.2.3) | `CT_ID` as the first field; in the daily delivery period, cash transactions arrive only through Kafka | The stream needs a stable ID for deduplication ([Section 7.1](#71-cash-transaction-stream-kafka-and-flink)) |
| `CT_NAME` | Free text, such as "Cash from sale of DuPont stock" (2.2.2.3) | Fixed forms: `Trade settlement <trade_id>`, `Deposit`, `Withdrawal`, and `Dividend <symbol>` | DP2 and Flink derive the cash transaction type and trade link from it ([Section 9.2](#92-source-file-schemas)) |
| `Trade.txt` format | One format (2.2.2.17) | `T_CHANNEL` added as a 15th business field from 2019-07-01; the initial load keeps the original format | Evidence for schema evolution handling ([Data Platform](Data-Platform.md#schema-evolution-and-duplicate-rules)) |
| `T_IS_CASH` | Can differ between trades of one account (2.2.2.17) | The same for all trades of an account | An account is either a cash account or a margin account |
| Warehouse tables | 19 tables, including `DImessages` (3.2) | 19 tables: `DImessages` removed, `fact_cash_transactions` added, and `fact_cash_balances` built from it; `dim_trade` gains a nullable `channel` | Cash transactions need their own fact for the stream and the cash-flow features ([Section 6](#6-gold-warehouse)) |
| Data errors | Alerts in `DImessages` (3.2.8, 4.4.3) | Quality metadata, quarantine, GX results in DataHub, and `ops.load_status`; a value is repaired only when the correct value is certain | Bad records stay traceable, and no value is invented ([Data Platform](Data-Platform.md#data-errors-and-quarantine)) |
| Table names | Mixed case, such as `DimCustomer` (3.2) | snake_case with role prefixes, such as `gold.dim_customer`; field names keep the TPC-DI names | Rubric naming rule; no quoted identifiers in SQL ([Section 4](#4-data-zones-medallion-architecture)) |
| Warehouse trade ID | `TradeID` is `NUM(11)` (3.2.7.1) | Stored with 15 digits | Mistake in the specification: source `T_ID` is `NUM(15)` (2.2.2.17) |
| Table 4.3 | Names fields that do not exist, such as `SK_TimeID` in `FactCashBalances` | Follow the table definitions in clause 3.2 | Mistake in the specification |
| Account version lookup | Clause 4.5.1.3 matches versions with `ActionTS <= EndDate`, an inclusive end date | Follow the general SCD2 rule in clause 4.4.1, where a version ends when the next one starts | Mistake in the specification: an inclusive end date matches two versions on the change date |
