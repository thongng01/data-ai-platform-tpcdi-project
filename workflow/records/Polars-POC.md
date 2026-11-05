# Polars Generator POC Evidence

The owner authorized an autonomous fresh Polars proof of concept (POC) on 2026-10-29, with code quality and declarative configuration as priorities. The POC code moved to [`data/data_source/generator/`](../../data/data_source/generator/README.md) on 2026-11-05, where the target generator builds on it. Because that guide now describes the target design, the POC's own rules are kept in [POC Generation Rules](#poc-generation-rules) below. On 2026-11-04 the owner decided to extend this POC into the production generator ([Data Platform](../../docs/Data-Platform.md#2-data-generation-and-problem-injection)).

## Scope and Acceptance

The experiment covers the TPC-DI business and reference source families, historical and daily file patterns, and a connected customer, account, trade, cash transaction, and holding history. The generator uses a simplified analytical schedule. It does not implement the complete Databricks scheduler or claim DIGen equivalence.

The checks require source-format validity, correct temporal references, exact cash reconciliation, continuous holding quantities, globally ordered exports, identical source bytes across execution batch sizes, and bounded generation and merge state. Memory and temporary disk must be measured, not inferred from the batch setting. Failure must preserve partial evidence without publishing a completed dataset.

## Measured Runs

Five final configurations ran sequentially on the local WSL development machine. WSL reported 20 logical CPUs and about 15.5 GiB of RAM. Each generator worker used four Polars threads. These are development measurements, not timings from an isolated benchmark machine. The largest validation overlapped a short test run.

| Run | Initial customers | Historical days | Customers per batch | Source records | Source MiB | Peak worker RSS MiB | Generation and export seconds | Validation seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small | 200 | 28 | 128 | 226,828 | 9.71 | 104.61 | 4.00 | 3.17 |
| Medium | 2,000 | 56 | 128 | 2,869,945 | 122.58 | 111.87 | 55.28 | 34.81 |
| Medium with wider batches | 2,000 | 56 | 2,048 | 2,869,945 | 122.58 | 122.05 | 8.67 | 37.77 |
| Longer history | 2,000 | 112 | 2,048 | 5,639,222 | 239.06 | 121.50 | 16.85 | 71.92 |
| Larger population | 8,000 | 112 | 2,048 | 22,292,201 | 964.09 | 125.16 | 73.88 | 297.77 |

The largest run wrote 1,010,923,475 source bytes and stayed below its 256 MiB worker budget. Linux reported peak RSS of 131,235,840 bytes. Sampled peak disk use, including sources, sorting files, and the temporary validation database, was about 2,055.71 MiB. Disk was sampled every two seconds, so this is not an exact instantaneous peak.

The medium pair produced identical bytes in all 39 source files. Increasing the customer batch from 128 to 2,048 reduced generation and export time by about 6.4 times and increased peak RSS by about 10.2 MiB. The wider batches amortize repeated Polars operations and create fewer sorting runs. This comparison does not establish an advantage over DuckDB or Spark.

Doubling history at the wider batch size left peak RSS nearly unchanged. Increasing population fourfold then raised it by about 3.7 MiB. The largest generated frame passed to export had 11,816 rows. A merge opened at most 16 input runs, and the largest retained run-path count was 227. These measurements support the bounded design for this workload; they do not prove a bound for a different scheduler or arbitrary configuration.

The final reports, without the generated source files, are in the local folder `local/archive/polars-poc-final-benchmarks/`. `summary.json` records the comparison. Each run contains its configuration, file inventory and hashes, validation results, phase times, resource supervision, and implementation file hashes. Every final run's implementation hashes matched the code at completion. Earlier discovery runs were not used for the final comparison and are not kept.

## Correctness and Failure Checks

The final suite passed 50 tests. It checked configuration errors, seed stability, exact source bytes across batch sizes and thread counts, all six XML action types, cross-batch trade transitions and holdings, source money limits, reference fixtures, duplicate export keys, preserved existing outputs, and retained partial output after a resource-budget failure. Deliberately corrupted cash amounts, holding continuity, and future execution values caused validation failures.

Twelve format tests used the repository's committed SF3 source fixtures. The XML test used the specification's XSD with lxml 6.0.2. The specification text has typographic quotes around one `maxOccurs` attribute, so the test schema `data/generator/tests/CustomerMgmt.xsd` uses straight quotes there. lxml was installed only under `local/tmp/polars-poc/qa-deps` for this check.

Each final run passed 25 source and relationship checks. The validator parses the exported files and builds a temporary SQLite database. It checks effective account lifetimes, latest trade state, legal status transitions, exact cash amounts, holding origins and quantity continuity, financial and company references, watch state, market price bounds, entity counts, and source hashes. It does not substitute generator-internal tables for the source files.

An independent DuckDB 1.5.6 scan matched the largest run's cash transaction row count and signed cent total for all three batches. The evidence is `independent_cash_check.json` in the reports folder, beside the helper `check_cash_with_duckdb.py`. The first attempt used DuckDB's `64MB` memory setting, default CSV buffers, and two threads; a CSV buffer allocation exceeded that limit. One thread and an explicit 1 MiB CSV buffer completed under the same setting. This setting controls DuckDB memory, not whole-process RSS.

## Interpretation and Remaining Work

The POC demonstrates a complete connected source-format experiment with bounded Polars work, globally ordered exports, declarative settings, and repeatable output. It does not establish a complete port of the Databricks generator. Its analytical schedule deliberately replaces the general customer scheduler and multi-lot trading behavior. [POC Generation Rules](#poc-generation-rules) lists the simplified rules and omitted cases.

Validation took about four times as long as generation and export on the largest run. A 100 GB run therefore needs a separate runtime and disk plan, especially for full relationship validation. No 100 GB run was attempted. No forecast model, predictive behavior assessment, fault injection, or downstream pipeline integration was performed.

The production generator extends this code. It must decide which simplified rules survive, add the business behavior, attrition signal, data errors, and schema versions of the generation model, and keep the memory and repeatability gates. Transport problems, such as duplicates and late delivery, belong to the separate publisher.

## POC Generation Rules

Copied from the POC generator guide on 2026-11-05, before that guide was replaced by the target design.

The source families are covered at small scale, but the workload is deliberately narrower than DIGen. The following rules are POC choices and do not establish the project's final business simulation.

- Each initial customer has two accounts. Selected customers receive a third account or customer and account updates. Other selected accounts close, and selected customers become inactive. The historical XML includes all six action types. Inactivation closes that customer's remaining accounts. Incremental batches introduce new customers and accounts and update selected existing records.
- Each eligible account starts a configured number of trade cycles per day. A cycle contains a buy and, if that buy completes, a subsequent sell. A buy completes or cancels on the next day. A sell completes one day later. Some trades therefore cross the historical-to-incremental boundary or remain unfinished at the end of the run.
- Each cycle owns one holding lot. Its buy changes quantity from zero to the bought quantity. Its sell changes that quantity back to zero. The holding origin is the completed buy for the same account and security. The POC does not implement multiple-lot allocation, partial fills, or borrowing.
- Trades in cash accounts produce signed cash transactions at completion. Buys subtract executed value and fees. Sells add executed value minus fees. The POC uses a 25-cent charge, a 50-cent commission, and zero transaction tax. Margin trades change holdings without producing these cash transactions.
- An unfinished or canceled trade has empty execution price, charge, commission, and tax fields. Historical snapshots contain only the state known before the batch cutoff. Incremental files contain ordered insert and update transitions.
- Financial records cover four completed quarters. Companies and securities start before those reports. FINWIRE records use quarterly files ordered by posting timestamp. The generator uses CIK references; it does not exercise company-name references or company and security version changes.
- Every active customer activates and cancels one watch per day. Daily market prices cover generated execution prices. The POC treats every calendar day as an activity day; it does not model exchange holidays. Fiscal dates equal calendar dates, and holiday flags are zero.
- Prospect snapshots contain matching identities for current and future customers. The reference vocabulary is intentionally small: one industry and two tax rates. Time covers all 86,400 seconds; Date covers the financial and activity history.

The cash transaction description (`CT_NAME`) contains `Trade <ID>` for POC reconciliation. This is a project lineage convention, not a TPC-DI transaction identifier. The target adds the accepted `CT_ID` cash transaction identity and replaces this description with fixed `CT_NAME` forms defined in [Data Specification](../../docs/Data-Specification.md).

Population and date settings replace the reference's scale-factor formulas. The POC has no asserted mapping to an official scale factor. The target adds behavior profiles, an attrition signal, data errors, and schema evolution to the generator. The publisher adds duplicates and late delivery. Dense daily trade cycles in this POC do not establish a useful attrition signal.
