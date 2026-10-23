# TPC-DI Overview (cloned repo + primary sources)

Status note (2026-10-18): two recommendations here are superseded by the locked decision in Domain and entity model: keep all tables (overrides the §8 subset advice), and 100GB is in scope as a config-driven stretch (overrides the "do not attempt 100GB" remarks in §1 and §8). Row counts, formats, and lineage below remain accurate (re-verified against sf_3 files).

Repo root: `coursework/tpcdi/`. Primary spec: TPC-DI v1.1.0 PDF (obsolete workload) at https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DI_v1.1.0.pdf; TPC-DI page https://www.tpc.org/tpcdi/default5.asp; Poess et al., "TPC-DI: The First Industry Benchmark for Data Integration", VLDB 2014 (https://www.vldb.org/pvldb/vol7/p1367-poess.pdf).

## 1. What TPC-DI is

- **Purpose**: benchmark for **data integration (DI/ETL)**. It extracts from an OLTP system plus other sources (HR, financial newswire, prospect lists, reference data), transforms into a unified warehouse model, and loads a decision-support warehouse. Source/destination models, transformations and implementation rules represent modern DI requirements (TPC-DI page).
- **Target systems**: ETL/data-integration tools and data warehouses; **not** OLTP throughput. Metric is DI elapsed time at a given scale factor (spec §2).
- **Scale factor (SF)**: determines cardinalities of source files; row counts scale roughly linearly (`rows_F(SF) = SF * R_F`), and source data size scales with SF (Poess et al., Table 3 / Fig. 4). No fixed published list of valid SFs is enforced in the spec; the sponsor chooses SF. This repo ships `sf_3` demo data and `dags/constants.py` sets `SF = 'sf5'` (demo naming, not TPC publication sizes).
- **Data volume**: at `sf_3`, Batch1 has ~390,978 trades, 982,180 trade-history rows, 361,856 cash transactions, 362,342 holding-history rows, 1,282,768 daily-market rows, 900,042 watch-history rows (`sf_3/output_data/Batch1/*.txt` line counts). 100 GB+ scale factors are out of scope for a one-week coursework demo.

## 2. End-to-end flow

Flow is **source files -> staging tables -> master (warehouse) tables**; there is no downstream consumer in this repo.

- Airflow DAG `tpc_di_historical_load_v2` (`dags/tpc_di_historical_load.py`): create staging + master schemas; load source files into staging; transform into master; includes customer-mgmt XML->JSON conversion.
- **Batch1 (initial/historical load)**: full reload of all source files; `BatchDate.txt` = `2017-07-07` (`sf_3/output_data/Batch1/BatchDate.txt`).
- **Batch2 (incremental)**: delta files with `I`/`U` prefixes for Trade, CashTransaction, HoldingHistory, DailyMarket, WatchHistory, Customer, Account; `BatchDate.txt` = `2017-07-08`.
- **Batch3 (incremental)**: same delta pattern; `BatchDate.txt` = `2017-07-09`.
- This repo's DAG implements only the **Batch1 historical load**; incremental batches are available as data but not wired into a DAG.

## 3. Source systems and file formats

| Source file | Format | Staging table | Notes |
|---|---|---|---|
| `CustomerMgmt.xml` | XML actions (NEW/UPDCUST/INACT) | `staging.customer_management` -> `staging.customer_historical` | SCD changes; `staging/staging_customer_historical_table.sql` carries forward last values; `dags/customer_mgmt_helpers.py` converts XML to JSON |
| `Trade.txt` | pipe-delimited | `staging.trade` | trade header; Batch2/3 use `I`/`U` prefixes |
| `TradeHistory.txt` | pipe-delimited | `staging.trade_history` | trade status history |
| `CashTransaction.txt` | pipe-delimited | `staging.cash_transaction` | account cash movements |
| `DailyMarket.txt` | pipe-delimited (repo) | `staging.daily_market` | daily price/volume |
| `HoldingHistory.txt` | pipe-delimited | `staging.holding_history` | before/after quantities |
| `WatchHistory.txt` | pipe-delimited | `staging.watch_history` | customer watch list actions |
| `HR.csv` | CSV | `staging.hr` | employees -> brokers |
| `FINWIRE*` | fixed-width (CMP/SEC/FIN records) | `staging.finwire` -> `finwire_cmp_record` / `finwire_sec_record` / `finwire_fin_record` | company/security/financials (`staging/staging_finwire_*_table.sql`) |
| `Prospect.csv` | CSV | `staging.prospect` | prospect list |
| `Account.txt` (Batch2/3) | pipe-delimited with I/U | (not in Batch1 staging schema) | incremental account changes |
| `Customer.txt` (Batch2/3) | pipe-delimited with I/U | (not in Batch1 staging schema) | incremental customer changes |
| `TaxRate.txt`, `Industry.txt`, `StatusType.txt`, `TradeType.txt` | pipe-delimited | `staging.tax_rate`, `industry`, `status_type`, `trade_type` | reference data |
| `Date.txt`, `Time.txt` | pipe-delimited | `staging.dim_date`, `dim_time` | reference date/time |

Staging schemas: `staging/data_sources_schema.sql`, `staging/staging_reference_tables_schema.sql`, `staging/staging_finwire_*_table.sql`, `staging/cluster_staging/staging_cluster_daily_market.sql`.

## 4. Target model (master schema)

Source: `master/master_schema.sql`. SCD2 dimensions have `IsCurrent`, `EffectiveDate`, `EndDate`.

| Table | Type | Grain / notes | SCD |
|---|---|---|---|
| `DimAccount` | Dim | one account version | SCD2 |
| `DimBroker` | Dim | one broker version | SCD2 |
| `DimCompany` | Dim | one company version | SCD2 |
| `DimCustomer` | Dim | one customer version | SCD2 |
| `DimSecurity` | Dim | one security version | SCD2 |
| `DimDate` | Dim (ref) | one calendar date | none |
| `DimTime` | Dim (ref) | one second-of-day | none |
| `DimTrade` | Dim | one trade (fact-like dim) | none (BatchID) |
| `DimMessages` | Dim/audit | integration messages | none |
| `FactCashBalances` | Fact | customer/account/date | none |
| `FactHoldings` | Fact | trade/holding snapshot | none |
| `FactMarketHistory` | Fact | security/company/date | none |
| `FactWatches` | Fact | customer/security watch interval | none |
| `Prospect` | Dim/bridge | prospect attributes by record/update date | none |
| `Financial` | Fact/ref | company/year/quarter financials | none |
| `TradeType`, `Industry`, `StatusType`, `TaxRate` | Reference | code lists | none |
| `Audit` | Audit | benchmark row-count checks | none |

Surrogate keys (SK_*): `SK_AccountID`, `SK_BrokerID`, `SK_CompanyID`, `SK_CustomerID`, `SK_SecurityID`, plus `SK_DateID`, `SK_TimeID`, `SK_*DateID` in facts. Natural keys: `AccountID`, `BrokerID` (HR employee), `CompanyID` (CIK), `CustomerID`, `Symbol`, `TradeID`.

FK relationships (ERD-style):
- `DimAccount.SK_BrokerID -> DimBroker.SK_BrokerID`; `SK_CustomerID -> DimCustomer.SK_CustomerID`
- `DimSecurity.SK_CompanyID -> DimCompany.SK_CompanyID`
- `DimTrade.SK_BrokerID -> DimBroker`; `SK_CreateDateID/SK_CloseDateID -> DimDate`; `SK_CreateTimeID/SK_CloseTimeID -> DimTime`; `SK_SecurityID -> DimSecurity`; `SK_CompanyID -> DimCompany`; `SK_CustomerID -> DimCustomer`; `SK_AccountID -> DimAccount`
- `FactCashBalances.SK_CustomerID -> DimCustomer`; `SK_AccountID -> DimAccount`; `SK_DateID -> DimDate`
- `FactHoldings.TradeID/CurrentTradeID -> DimTrade`; `SK_CustomerID -> DimCustomer`; `SK_AccountID -> DimAccount`; `SK_SecurityID -> DimSecurity`; `SK_CompanyID -> DimCompany`; `SK_DateID -> DimDate`; `SK_TimeID -> DimTime`
- `FactMarketHistory.SK_SecurityID -> DimSecurity`; `SK_CompanyID -> DimCompany`; `SK_DateID -> DimDate`; `SK_FiftyTwoWeekHighDate/SK_FiftyTwoWeekLowDate -> DimDate`
- `FactWatches.SK_CustomerID -> DimCustomer`; `SK_SecurityID -> DimSecurity`; `SK_DateID_DatePlaced/DateRemoved -> DimDate`
- `Financial.SK_CompanyID -> DimCompany`
- `DimCustomer` tax rates joined to `TaxRate` (NationalTaxID/LocalTaxID -> `TaxRate.TX_ID`)
- `DimCompany` Status/Industry joined to `StatusType`/`Industry`

## 5. Lineage (source -> staging -> master)

Staging tables (from `staging/data_sources_schema.sql` + `staging_reference_tables_schema.sql`): `batch_date`, `cash_transaction`, `daily_market`, `holding_history`, `trade`, `trade_history`, `watch_history`, `prospect`, `hr`, `customer_historical`, `customer_management`, `finwire`, `finwire_cmp_record`, `finwire_sec_record`, `finwire_fin_record`, `dim_date`, `dim_time`, `industry`, `status_type`, `tax_rate`, `trade_type`, (plus `audit`).

Transform lineage:
- `staging.hr` -> `master.DimBroker` (`master/create_dim_tables/master_populate_dimbroker.sql`)
- `staging.finwire` -> `finwire_cmp_record` -> `master.DimCompany`; -> `finwire_sec_record` -> `master.DimSecurity` (joins `DimCompany`); -> `finwire_fin_record` -> `master.Financial` (joins `DimCompany`)
- `staging.customer_management` -> `staging.customer_historical` -> `master.DimCustomer` (joins `TaxRate`, `prospect`)
- `customer_management` accounts -> `master.DimAccount` (joins `DimBroker`, `DimCustomer`)
- `staging.trade` + `staging.trade_history` -> `master.DimTrade` (joins `DimAccount`, `DimSecurity`, `TradeType`, `DimDate/Time`)
- `staging.cash_transaction` -> `master.FactCashBalances` (joins `DimAccount`)
- `staging.holding_history` -> `master.FactHoldings` (joins `DimTrade`)
- `staging.daily_market` -> `master.FactMarketHistory` (joins `DimSecurity`, `Financial`)
- `staging.watch_history` -> `master.FactWatches` (joins `DimCustomer`, `DimSecurity`)
- `staging.prospect` -> `master.Prospect` (joins `customer_historical`, `DimDate`, `batch_date`)
- `master.DimTrade` + `master.FactMarketHistory` -> `master.DimMessages`
- Reference files -> master reference tables as-is; `staging.audit` -> `master.Audit`

## 6. Data characteristics

- **Row counts (sf_3, Batch1)**: Trade 390,978; TradeHistory 982,180; CashTransaction 361,856; HoldingHistory 362,342; DailyMarket 1,282,768; WatchHistory 900,042.
- **Audit deltas (Batch1)**: `DimCustomer`: NEW 4,728, UPDCUST 1,284, INACT 428; `DimAccount`: ADDACCT 4,280, CLOSEACCT 1,284, UPDACCT 2,568; `DimTrade`: records 390,978, canceled 22,780, invalid commission 38, invalid charge 25 (`sf_3/output_data/Batch1/*_audit.csv`).
- **Date ranges**: DailyMarket 2015-07-06..2017-07-07 (Batch1); FINWIRE covers 1967..2017 quarters; incremental batches dated 2017-07-08/09. CustomerMgmt actions run 2007..2017.
- **Change frequencies**: SCD2 customer/account/company/security changes via NEW/UPDCUST/INACT and FINWIRE CMP/SEC updates; trade updates (SBMT->CMPT/Canceled); late-arriving incremental batches (Batch2/3) replayed as micro-batches.
- **Duplicates/potential issues**: CustomerMgmt uses carry-forward (LAST_VALUE) to materialize versions; FINWIRE may repeat unchanged CMP records (repo keeps each as a new current row, not deduplicating); natural-key joins (Prospect to Customer by name/address) can create many-to-one matches.

## 7. Mapping to ETL/data-platform rubric (coursework)

- **Offline batch**: TPC-DI Batch1 maps directly to Spark batch pipelines (bronze->silver->gold) and Airflow orchestration; staging/master schemas are the warehouse model.
- **Streaming**: Batch2/3 (small deltas with I/U) can be replayed as a stream/micro-batch in Flink; provides incremental updates and late data.
- **SCD2 dims**: DimCustomer/Account/Company/Security/Broker satisfy SCD2 requirement (IsCurrent/EffectiveDate/EndDate).
- **Facts**: FactCashBalances, FactHoldings, FactMarketHistory, FactWatches provide measures and FKs to dims.
- **Feature tables**: can be derived (e.g., customer cash/holdings aggregates) with `event_timestamp` + `created` columns.
- **Missing for streaming ML**: no native streaming source (files only); no per-event timestamps in some sources (batch date only); spec is batch-oriented; incremental "stream" must be simulated by replaying Batch2/3.

## 8. Recommended subset for this coursework (Layers 1&2, one week, demo scale)

Keep (minimum viable, satisfies rubric):
- **Batch1 only** at `sf_3` (demo scale; ~1.3M largest file, manageable).
- **Dims**: DimCustomer (SCD2), DimAccount (SCD2), DimCompany (SCD2), DimSecurity (SCD2), DimBroker (SCD2), DimDate, DimTime.
- **Facts**: FactCashBalances, FactHoldings, FactMarketHistory, FactWatches.
- **Refs**: TaxRate, Industry, StatusType, TradeType.
- **Sources**: CustomerMgmt.xml, Trade.txt, TradeHistory.txt, CashTransaction.txt, HoldingHistory.txt, DailyMarket.txt, WatchHistory.txt, HR.csv, FINWIRE (CMP/SEC/FIN), Prospect.csv, Date/Time.
- **Audit**: keep `master.Audit` for row-count validation.

Omit/reduce:
- Omit full FINWIRE 1967-2017 history; keep recent quarters or a subset for DimCompany/Security/Financial.
- Omit or sample Prospect matching complexity if time-constrained (keep schema).
- Omit DimMessages unless needed for audit lineage.
- Do not attempt 100 GB scale factors.

Changes to make:
- Implement SCD2 **merge/upsert** (expire old rows) instead of inserting all versions as current.
- Split pipelines into DP1 raw->bronze, DP2 bronze->gold, DP3 offline feature tables; add validation.
- Replay Batch2/3 through Kafka/Flink as incremental stream; implement late data handling.
- Add DataHub governance and feature store (Feast) materialization.
- Map to rubric: docker/compose, Spark batch, Flink streaming, Airflow (3 pipelines), SCD2 dims, feature tables, data quality.

Citations: `master/master_schema.sql`, `staging/data_sources_schema.sql`, `staging/staging_reference_tables_schema.sql`, `staging/staging_finwire_*_table.sql`, `staging/staging_customer_historical_table.sql`, `master/create_dim_tables/*.sql`, `master/populate_fact_tables/*.sql`, `dags/tpc_di_historical_load.py`, `dags/constants.py`, `sf_3/output_data/Batch{1,2,3}/BatchDate.txt`, `sf_3/output_data/Batch1/*_audit.csv`, TPC-DI page/spec PDF, Poess et al. VLDB 2014.
