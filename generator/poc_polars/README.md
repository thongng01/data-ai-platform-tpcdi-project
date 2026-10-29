# Polars source generator POC

This POC generates connected TPC-DI source files in one local Python worker. It tests whether bounded Polars calculations, explicit histories, and disk-based ordering can replace the Databricks execution layer. The existing generator remains separate.

The source contracts come from [TPC-DI 1.1.0](../../docs/TPC-DI/TPC-DI_v1.1.0.pdf), clause 2.2. The [Databricks reference](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen) supplies the dependency pattern, ID-derived values, shared trade calculations, and analytical scheduling approach. This is a selective reimplementation. It does not reproduce the full reference scheduler, DIGen distributions, or official benchmark bytes.

## Run the POC

Run these commands from the coursework root in WSL. The checked environment uses Python 3.14. Python 3.11 or later is required by the code; other versions have not been tested. The [requirements](requirements.txt) pin the packages used by this POC. They are already present in the workspace virtual environment.

```bash
.venv/bin/python -m generator.poc_polars run \
  --config generator/poc_polars/configs/small.yaml \
  --output misc/tmp/polars-poc/my-run

.venv/bin/python -m generator.poc_polars validate misc/tmp/polars-poc/my-run

.venv/bin/python -m pytest generator/poc_polars/tests -q \
  --basetemp=misc/tmp/polars-poc/pytest-local
```

The optional XML Schema test uses `lxml==6.0.2`. The recorded full test pass loaded it from the isolated QA directory `misc/tmp/polars-poc/qa-deps`; no package was added to the shared virtual environment. To reproduce that check:

```bash
.venv/bin/python -m pip install --target misc/tmp/polars-poc/qa-deps lxml==6.0.2
PYTHONPATH=misc/tmp/polars-poc/qa-deps:. .venv/bin/python -m pytest \
  generator/poc_polars/tests -q --basetemp=misc/tmp/polars-poc/pytest-with-xsd
```

Without lxml, pytest skips this one test. The test reads the specification's XSD and changes the typographic quotes around `maxOccurs` to XML quotes in memory. It does not change the source specification.

Choose a new output path for each run. The runner refuses to overwrite an existing output or partial directory. Tests recreate their dedicated pytest temporary directory; do not use a directory containing work you need to preserve as `--basetemp`.

The runner first creates `my-run.partial`. A successful run generates and validates the files, writes a manifest, then renames the directory to `my-run`. A failed run retains its log and partial files. Failure does not produce a completed dataset. Automatic resume is not implemented.

| Output | Contents |
| --- | --- |
| `sources/Batch1/` | Contains historical business and reference files. |
| `sources/Batch2/` and later batches | Contain daily incremental activity, customer/account changes, prospect snapshots, and batch dates. |
| `config.json` | Records the exact configuration used. |
| `manifest.json` | Records source counts, byte sizes, hashes, runtime versions, phase times, peak RSS, and validation results. |
| `validation.json` | Records relationship checks and measured totals. |
| `supervision.json` | Records sampled process memory, disk usage, and elapsed time. |
| `worker.log` | Records execution stages and any exception. |
| `runs/` | Holds intermediate sorted records. Successful export removes these records. |

Only `sources/` is the source-data staging root. Reports and intermediate records are outside it. Source byte totals exclude metadata and temporary files. Benchmark audit files are not generated.

## Definitions and implementation

The [run configuration](configs/small.yaml) controls population sizes, history length, seed, cash/cancellation probabilities, batch size, merge fan-in, threads, and resource budgets. YAML values cannot contain executable expressions. Unknown keys, duplicate keys, invalid types, and invalid ranges fail before a worker starts.

The [source definitions](definitions/sources.yaml) specify field order, source types, nullability, delimiters, file names, and CDC support. The [FINWIRE definitions](definitions/finwire.yaml) specify field widths and padding. These files describe formats. They do not contain scheduling algorithms or arbitrary expression strings.

| Module | Responsibility |
| --- | --- |
| `config.py` | Loads and validates configuration. |
| `random_values.py` | Derives repeatable integer values from a seed, stable ID, and field name, and formats exact integer cents. |
| `generation.py` | Routes records to source files and calculates account availability. |
| `customers.py` | Builds customer/account actions, CDC records, and prospect snapshots. |
| `reference.py` | Builds reference tables, FINWIRE, and daily market records. |
| `activity.py` | Derives trades, status changes, cash, holdings, and watches. |
| `sorting.py` | Writes sorted runs, compacts them, and merges them with bounded open-file counts. |
| `validation.py` | Parses the exported source files and checks relationships using a temporary SQLite database. SQLite does not generate source data. |
| `runner.py` | Supervises the worker, records evidence, and publishes a validated run directory. |

## Generation rules

The source families are covered at small scale, but the workload is deliberately narrower than DIGen. The following rules are POC choices and do not establish the project's final business simulation.

- Each initial customer has two accounts. Selected customers receive a third account or customer/account updates. Other selected accounts close, and selected customers become inactive. The historical XML includes all six action types. Inactivation closes that customer's remaining accounts. Incremental batches introduce new customers/accounts and update selected existing records.
- Each eligible account starts a configured number of trade cycles per day. A cycle contains a buy and, if that buy completes, a subsequent sell. A buy completes or cancels on the next day. A sell completes one day later. Some trades therefore cross the historical/incremental boundary or remain unfinished at the end of the run.
- Each cycle owns one holding lot. Its buy changes quantity from zero to the bought quantity. Its sell changes that quantity back to zero. The holding origin is the completed buy for the same account and security. The POC does not implement multiple-lot allocation, partial fills, or borrowing.
- Cash trades produce signed cash movements at completion. Buys subtract executed value and fees. Sells add executed value minus fees. The POC uses a 25-cent charge, a 50-cent commission, and zero transaction tax. Margin trades change holdings without producing these cash movements.
- An unfinished or canceled trade has empty execution price, charge, commission, and tax fields. Historical snapshots contain only the state known before the batch cutoff. Incremental files contain ordered insert/update transitions.
- Financial records cover four completed quarters. Companies and securities start before those reports. FINWIRE records use quarterly files ordered by posting timestamp. The POC uses CIK references; it does not exercise company-name references or company/security version changes.
- Every active customer activates and cancels one watch per day. Daily market prices cover generated execution prices. The POC treats every calendar day as an activity day; it does not model exchange holidays. Fiscal dates equal calendar dates, and holiday flags are zero.
- Prospect snapshots contain matching identities for current and future customers. The reference vocabulary is intentionally small: one industry and two tax rates. Time covers all 86,400 seconds; Date covers the financial and activity history.

The cash description contains `Trade <ID>` for POC reconciliation. This is a project lineage convention, not a TPC-DI transaction identifier. The eventual stream still needs its separate event-ID contract.

Population and date settings replace the reference's scale-factor formulas. The POC has no asserted mapping to an official scale factor. Predictive account behavior, skew, schema evolution, injected duplicates, and late delivery remain subsequent work. Dense daily trade cycles do not establish a useful forecasting task.

## Memory and ordering

Activity generation processes one origin day and a bounded customer group at a time. Polars expands only that group's accounts and cycles. The generator derives a cycle's lifecycle and holding changes from stable IDs, so it does not retain a dictionary of every historical trade or holding. All related events for a cycle are computed together, then routed to their historical or incremental outputs.

This calculation replaces the reference's general schedule with a simpler analytical schedule. It is why history can grow without a corresponding in-memory position ledger. A future model with partial fills, interacting lots, or discretionary actions must introduce and budget the required state explicitly.

Each export batch is sorted in memory and written as a temporary run. Runs compact through levels as they arrive. Each level retains fewer paths than `merge_fan_in`; path metadata therefore grows logarithmically with the number of runs per output. A merge opens at most `merge_fan_in` input files. The final merge assigns CDC sequence numbers in event order. Duplicate sort keys fail the export.

This trades temporary disk and extra reads/writes for bounded sorting memory. It preserves global ordering across customer groups. The implementation does not collect the complete generated dataset into a Polars DataFrame.

The supervisor samples worker RSS every 0.1 seconds and disk use every two seconds. It stops the worker if a sampled limit or the wall-time limit is exceeded. These checks are not kernel-enforced limits; a rapid allocation can exceed the threshold between samples. Batch bounds and limited concurrency remain necessary. The manifest also records Linux peak RSS, which can capture a peak missed by sampling.

## Check repeatability and scale

Generate the same semantic configuration with different `customer_batch_size`, `merge_fan_in`, or `threads` values, then compare the source files:

```bash
.venv/bin/python -m generator.poc_polars compare \
  misc/tmp/polars-poc/first-run misc/tmp/polars-poc/second-run
```

Configuration changes to population, dates, probabilities, or seed describe a different dataset. Runtime-only changes must preserve source bytes. Random values use a named-field BLAKE2 salt and a fixed unsigned 64-bit mixing algorithm. They do not use Polars' version-dependent hash function or a random stream whose position depends on batch size.

The [medium](configs/medium.yaml) and [medium-wide](configs/medium-wide.yaml) configurations describe the same dataset with different customer batch sizes. The [long-history](configs/long-history.yaml) configuration doubles history at the wider batch size. The [large](configs/large.yaml) configuration then increases the population fourfold and sets a 256 MiB worker RSS budget. Run them sequentially when measuring memory:

```bash
.venv/bin/python -m generator.poc_polars.benchmark \
  --output misc/tmp/polars-poc/my-benchmarks \
  --configs generator/poc_polars/configs/medium.yaml \
  generator/poc_polars/configs/medium-wide.yaml \
  generator/poc_polars/configs/long-history.yaml \
  generator/poc_polars/configs/large.yaml
```

The benchmark writes a summary after every successful run and compares actual source bytes when two configurations describe the same dataset. Source bytes, peak RSS, maximum generation frame rows, retained run paths, merge inputs, and phase times answer different questions. Peak RSS is not inferred from output size. The larger workload can take several minutes because validation reads all records and checks their relationships.

The [working record](../../docs/records/Polars-POC.md) holds measured results and their limits. A successful bounded pilot does not establish that a 100 GB run will finish within a given time or disk budget.
