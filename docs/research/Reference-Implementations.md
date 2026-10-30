# Reference implementations

Research note, 2026-10-27. It records what an existing TPC-DI implementation shows and where it stops. TPC-DI facts are in the [data specification](../Data-Specification.md). The Databricks generator is assessed in [Generator feasibility](Generator-Feasibility.md).

## AlaaAlmutawa/TPC-DI

The local checkout of [AlaaAlmutawa/TPC-DI at commit 0702dd9](https://github.com/AlaaAlmutawa/TPC-DI/tree/0702dd9bba87e13a33280412a47dca2d5257dbba) holds the sample data and a historical-load implementation with Airflow, Google Cloud Storage, and BigQuery. Its `staging` and `master` namespaces belong to that implementation; TPC-DI does not require them. Its files show what it declares. They do not show that its pipelines ran or produced a correct warehouse. This checkout is separate from the Databricks generator. Start with its `misc/tpcdi/README.md` and the historical DAG `misc/tpcdi/dags/tpc_di_historical_load.py`.

| Question | Where to look |
|---|---|
| What raw data exists? | `misc/tpcdi/sf_3/output_data/Batch1`, `Batch2`, `Batch3`; raw examples appear beside the [source file schemas](../Data-Specification.md#92-source-file-schemas) |
| What target columns does it declare? | `misc/tpcdi/master/master_schema.sql` |
| How does it stage source files? | `misc/tpcdi/staging/data_sources_schema.sql`, `misc/tpcdi/staging/staging_reference_tables_schema.sql` |
| How does it rebuild XML customer state? | `misc/tpcdi/staging/staging_customer_historical_table.sql`, `misc/tpcdi/staging/convert_xml_to_json.py`, `misc/tpcdi/dags/customer_mgmt_helpers.py` |
| How does it parse FINWIRE? | `misc/tpcdi/staging/staging_finwire_{cmp,sec,fin}_record_table.sql` |
| How does it build dimensions and financial reports? | `misc/tpcdi/master/create_dim_tables/` |
| How does it build facts, prospects, and messages? | `misc/tpcdi/master/populate_fact_tables/`, for example `master_populate_factcashbalances.sql` and `master_populate_factmarkethistory.sql` |
| What cloud path does the DAG expect? | `misc/tpcdi/dags/constants.py` |

Observed limits:

1. **Incremental loads**: the DAGs cover historical-load variants. Batch2 and Batch3 files exist, but no incremental pipeline is shown to run. `constants.py` sets `SF='sf5'`, which does not describe the local `sf_3` directory.
2. **Message table name**: the specification uses `DImessages`; the DDL in `master_schema.sql` creates `DimMessages`.
3. **Company history**: the company SQL in `master_populate_dimcompany.sql` sets `IsCurrent` to true and `EndDate` to 9999-12-31 for every CMP record. It does not expire earlier versions or skip unchanged content, although its comment repeats the specification's rule. Comments and SCD columns do not prove SCD2 behavior.
4. **Surrogate keys**: the same query builds a key from a formatted date and the CIK. TPC-DI requires unique version keys but does not prescribe this construction.
5. **Broker history**: the broker SQL in `master_populate_dimbroker.sql` selects job code 314 and emits current rows from the HR snapshot. This matches the absence of broker updates. It is not evidence of broker change handling.
6. **Constraints**: the DDL declares no primary keys or foreign keys. Uniqueness and relationships need explicit checks.
7. **Trade staging**: the staging schema calls the history timestamp `TRADE_T`, where the specification uses `TH_DTS`. It models `Trade` without the CDC prefix, so it cannot parse the incremental layout unchanged. The DimTrade SQL in `master_populate_dimtrade.sql` derives creation and close times from historical Trade and TradeHistory. It shows intended historical processing only.

These are specific observations, not a complete audit.
