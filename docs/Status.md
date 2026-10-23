# Status

Where the project stands, what is decided, and what comes next. Written for
someone picking this up cold.

Terms are in [`../GLOSSARY.md`](../GLOSSARY.md). Design is in
[`Architecture.md`](Architecture.md). Infrastructure is in
[`IaC.md`](IaC.md). Work tracking is in the
[map](../.scratch/coursework/map.md).

---

## Summary

We are building a data platform and an ML platform for a brokerage, graded on
data engineering fundamentals: a generator that produces deliberately broken
data, a lakehouse that survives it, a warehouse that models it, streaming that
handles lateness, and an ML system on top.

**Built and running:** the generator, the core platform (SeaweedFS, Hive
Metastore, Trino, Postgres), the Spark cluster, and DP1, which moves a batch
from the landing bucket through bronze into silver.

**Built and passing tests:** 55 contract tests over the schema, deduplication
and variant-splitting rules. They cover the failures that produce plausible
wrong numbers rather than errors: a column list chosen once instead of per row, a
schema split that emits every line twice, a business key naming a column a
batch does not carry, and a key narrower than the record.

**Scaffolded but not built:** DP2 with dbt, DP3's feature tables, the Airflow
DAGs, the Kafka and Flink streaming path, Soda validation and DataHub
governance. Each has a written and tested compose file and a folder with a
README saying what goes in it.

The blocking unknown from the last session is resolved. Trino reads SeaweedFS.
The full loop is proven end to end: Spark writes Parquet into SeaweedFS,
registers the table in the Hive Metastore, and Trino reads it back.

---

## Run it

```bash
make up-core      # postgres, seaweedfs, hive-metastore, trino
make bootstrap    # create the buckets and databases
make verify       # 8 smoke tests, all passing
make up-compute   # the Spark cluster
make dp1          # landing -> bronze -> silver
make test         # both test suites, 86 tests
make check-docs   # broken links, stale phrases, claimed-but-missing paths
```

`make help` lists the rest. Everything runs in WSL.

---

## Decisions made

| Decision | Where |
|---|---|
| We build our own generator and do not wrap benchbox | [adr/0002](adr/0002-build-our-own-generator.md) |
| SeaweedFS is the lakehouse store, lab MinIO is the fallback | [adr/0001](adr/0001-seaweedfs-as-lakehouse-object-store.md) |
| Generated characteristics are measured from output, not from config | [adr/0003](adr/0003-measure-generated-characteristics.md) |
| Keep all TPC-DI tables, SCD2 on Customer, Account, Security, Company | map, ticket 01 |
| dbt-spark owns silver to gold, Spark owns raw to bronze to silver, Airflow orchestrates | map |
| Postgres holds gold for ML and serving | map |
| **Trino reads SeaweedFS.** Proved, not assumed | [R13](../.scratch/coursework/research/R13-trino-seaweedfs-spike.md) |
| **Every lakehouse table is external.** SeaweedFS reports an empty prefix as an existing directory, so a managed CREATE TABLE always fails | [IaC.md 7.2](IaC.md#72-every-lakehouse-table-is-external) |
| **HiveServer2 lives in its own profile.** It takes every core of the worker and starves batch jobs | [IaC.md 8.2](IaC.md#82-a-hiveserver2-takes-the-whole-worker) |
| **A path write is followed by `MSCK REPAIR TABLE`.** A write is not a table write until the metastore knows | [IaC.md 8.1](IaC.md#81-a-path-write-is-not-a-table-write) |
| **One Postgres instance, four databases.** Separate databases, not separate containers | [IaC.md 7.4](IaC.md#74-one-postgres-instance-four-databases) |
| Five compose files gated by profiles, loaded together via `COMPOSE_FILE` | [IaC.md 3](IaC.md#3-the-compose-files) |

---

## What is built

### The generator

In `generator/`. 31 tests pass.

It emits Batch1, the full historical load, plus Batch2 and Batch3 as incremental
deltas, in the TPC-DI byte format: CRLF, no header rows, mixed delimiters,
empty string means NULL.

Tables: Trade, TradeHistory, CashTransaction, HoldingHistory, DailyMarket,
WatchHistory, CustomerMgmt.xml, the reference tables, FINWIRE.

Data problems inject per run: skew, duplicates split into offline and streaming
rates, schema evolution, late arrivals, out-of-order events.

The format contract is checked against real TPC-DI bytes committed as a 4 KB
fixture, so the suite runs on a fresh clone with nothing installed.

Gaps, listed in [`../generator/README.md`](../generator/README.md): `HR.csv`,
`Prospect.csv`, the `Customer.txt` and `Account.txt` deltas, a price walk
instead of uniform random values, and wiring the lateness and out-of-order
rates into the Trade writer.

### The platform

In `infra/`. `make verify` passes 8 of 8.

| Service | Image | State |
|---|---|---|
| postgres | `postgres:16-alpine` | running, 4 databases |
| seaweedfs | `chrislusf/seaweedfs:latest` | running, S3 gateway verified |
| hive-metastore | `starburstdata/hive:3.1.2-e.18` | running |
| trino | `trinodb/trino:476` | running, reads SeaweedFS |
| spark-master, spark-worker | `edai/spark:3.5.3` | running |
| spark-thrift | `edai/spark:3.5.3` | `dbt` profile, not in the default set |

### DP1: landing to bronze to silver

In `pipelines/dp1/`. `make dp1` runs it end to end.

Three steps:

| Step | Module | What it does |
|---|---|---|
| publish | `publish_landing.py` | copies the generator's output into the landing bucket, standing in for another team's storage |
| ingest | `landing_to_bronze.py` | parses each source file into a `raw_` table, unmodified, with four provenance columns |
| transform | `bronze_to_silver.py` | casts types, removes duplicates, partitions on event time |

Measured on Batch1 at scale 0.02, seed 20261018, 9.7 MB of source files across
10 tables. Bronze row counts match the source files' line counts exactly, which
is the check that proves the parser neither drops nor invents a row.

| Table | Source lines = Bronze | Silver | Duplicates removed | Rate | Partitions |
|---|---|---|---|---|---|
| `stg_trade` | 7,980 | 7,820 | 160 | 2.01% | 1,777 |
| `stg_cash_transaction` | 7,390 | 7,237 | 153 | 2.07% | 1,773 |
| `stg_daily_market` | 26,176 | 25,655 | 521 | 1.99% | 514 |
| `stg_watch_history` | 18,367 | 18,001 | 366 | 1.99% | 1,800 |
| `stg_holding_history` | 7,355 | 7,247 | 108 | 1.47% | none, no event time |
| `stg_trade_history` | 20,023 | 19,644 | 379 | 1.89% | 1,800 |
| `stg_status_type` | 6 | 6 | 0 | 0% | none |
| `stg_trade_type` | 5 | 5 | 0 | 0% | none |
| `stg_industry` | 28 | 28 | 0 | 0% | none |
| `stg_tax_rate` | 6 | 6 | 0 | 0% | none |

The measured rates land on the generator's configured `duplicate_rate_offline`
of 2 percent. That agreement is the evidence. An earlier version of this table
showed rates of 51, 62 and 58 percent, all wrong, and the only reason it was
noticed was comparing these numbers against the manifest's own injection rates.

The silver counts were read back through Trino, not taken from the job's own
report:

```sql
SELECT 'stg_trade' AS table_name, count(*) AS rows, count(DISTINCT dt) AS partitions
FROM hive.silver.stg_trade
```

That query is at `misc/q/silver_counts.sql`, and it is the whole proof chain:
the generator wrote the files, the job moved them through three zones, Spark
wrote the Parquet, and Trino read it back over the shared metastore. The job's
count and Trino's count agree on every table. They did not agree on the first
run of this review, and finding out why is recorded in
[`IaC.md` section 8.10](IaC.md#810-overwrite-is-not-idempotent-on-a-partitioned-table).

Three things DP1 handles that a naive reader does not:

- **Schema evolution.** Batch1 Trade has 14 fields, Batch2 has 16. The column
  list is chosen per row from that row's own field count, not once per file.
- **Business identity.** A trade's identity is its history id plus its status,
  so a delta batch's submitted-then-completed pair is one trade. In the
  historical load there is no history id and the row id is the identity; using
  the delta key there collapses 15,960 trades into 5 rows.
- **Text with spaces.** The parser splits on the delimiter. It never splits on
  whitespace.

### Two problems we have not solved yet

**Small files.** DP1 writes roughly one Parquet file per date partition, so
`stg_trade_history` produces 1,800 files of roughly 5 KB each and the write takes
minutes. The query is correct and the counts are right, but a scan opens 1,800
file handles. This is the rubric's storage-optimisation item, measured rather
than hypothesised. Coalescing before the write, or `maxRecordsPerFile`, is the
next thing to try.

**Spark shutdown.** Closing a SparkSession uploads the event log, and against
SeaweedFS that close blocks past a minute. The job now leaves on a 90 second
deadline and reports `stopped_cleanly: false` when it does, so a completed run
is never recorded as a hung one. The underlying slowness is not fixed.
Recorded in [`IaC.md` section 8.11](IaC.md#811-a-job-that-finished-its-work-still-hung-and-reported-nothing).

---

## What is not built

| Area | State | Next |
|---|---|---|
| DP2, dbt-spark, silver to gold | compose ready under `--profile dbt`, `dbt/` empty | write `dbt/dbt_project.yml`, `profiles.yml`, the `stg_`/`int_`/`dim_`/`fact_` models |
| DP3, offline feature table | not started | `f_customer_total_orders_90d` from silver, with `event_timestamp` and `created` |
| Airflow DAGs | compose ready, Connections and Variables seeded, no DAGs | three DAGs, each with an ingest stage and a validate stage |
| Kafka and Flink | compose ready under `--profile stream`, no job | PyFlink job: tumbling windows, watermarks, allowed lateness, side output |
| Soda validation | not started | check files per zone, run as the validate stage of each DAG |
| DataHub governance | compose ready under `--profile governance`, `governance/` empty | ingest bronze, silver and gold; emit a `DataProcessInstance` per DAG run so lineage ties a table to the pipeline that made it |

Each of those directories exists with a README saying what goes in it. The
compose files bind-mount them, and Docker creates a missing bind source as an
empty root-owned directory rather than failing, so `dbt/` was in exactly that
state until this review: created by Docker, owned by root, empty, and the
containers would have mounted nothing without saying anything.

---

## Open questions, in the order they block work

1. **Can dbt-spark reach the cluster?** dbt needs HiveServer2, which takes the
   whole worker. Options: run DP2 alone with `--profile compute,dbt`; or use
   `method: session` and accept that dbt no longer runs on the cluster. Decide
   before writing the first model.
2. **How does SCD2 land?** Postgres has no `LAST_VALUE(... IGNORE NULLS)`, so
   the carry-forward has to run in Spark and Postgres stores the finished
   `valid_from`/`valid_to`/`is_current` rows. Settled in principle, still needs
   the schema design in ticket 03.
3. **How does dbt do incremental without `MERGE`?** `insert_overwrite` or append
   per batch. Neither is written yet.
4. **How is volume reported?** The rubric asks for 100 GB. The answer stays a
   scale factor, a documented path and a measured demo run. Do not claim
   100 GB without measuring it.
5. **Is a RAG and embeddings pipeline possible?** TPC-DI carries almost no free
   text. FINWIRE narratives, CustomerMgmt XML and prospect names are the only
   candidates. Undecided, and it belongs to Layer 2.

---

## Findings that change how you plan

These came out of building the platform. The full list, with explanations, is
in [`IaC.md` section 8](IaC.md#8-findings-that-cost-time).

- **A path write is not a table write.** Parquet under `dt=` directories is
  invisible to a query until `MSCK REPAIR TABLE` runs.
- **An unfiltered union of two schema variants doubles a file.** DP1 did this
  and bronze came out at 100 percent duplication against an injected 2 percent.
- **A deduplication key narrower than the record deletes real rows.** Watch
  history keyed on three of its four columns removed 62 percent of the table.
- **SeaweedFS maps each S3 bucket to a collection**, and a collection needs a
  volume. Out of volumes, writes fail with an `InternalError` that reads like a
  gateway fault.
- **HiveServer2 takes every core of the Spark worker**, whatever
  `spark.executor.cores` says, and a job that cannot get an executor hangs with
  no stack trace.
- **`overwrite` is not idempotent** on a partitioned table. Files outside the
  partitions a run wrote survive and a reader counts them.
- **SeaweedFS reports an empty prefix as an existing directory**, so Spark's
  managed-table location check always fires. Every lakehouse table is external.
- **An off-by-one bind mount does not fail.** Docker creates the missing source
  as an empty directory, so the mount works and the file inside is not there.
- **`spark-class` does not read `spark-defaults.conf`.** Only `spark-submit`
  does.

Five were found in DP1 itself, all correctness bugs in a pipeline that ran to
completion and reported success. What found them was cross-checking measured
values against independent sources rather than against golden values. See
[`IaC.md` section 8](IaC.md#8-findings-that-cost-time).

These came out of research and predate the build:

- The schema drifts between batches, on purpose.
- A trade's business identity is not its row id.
- Value skew is not automatically a slow query.
- Text fields contain spaces.
- The customer file does not match the published spec, deliberately.
- Late arrival and out-of-order are different problems.
- Postgres lacks `LAST_VALUE(... IGNORE NULLS)`.

---

## Two corrections made along the way

Recorded so they do not get repeated.

**sf_3 is a format reference, not our dataset.** An early research pass measured
sf_3's skew baselines, row counts and duplicate rates as if we would ship it. We
generate our own data, so those numbers describe a file we never use. Only the
format contract and the rubric wording carried forward.

**Both generator references were the wrong shape.** benchbox emits finished
warehouse tables and skips the ETL, so it produces the answer rather than the
inputs. The other is not a TPC-DI generator at all.

---

## Housekeeping

The repo held a second direction: a TPC-DS retail schema and a 330 MB DuckDB
database from a local `dbt-tpcdi` build. TPC-DS was explored first and dropped.
Those files are in `misc/abandoned-tpcds/`.

The git repo has one commit, `9f3b127`, holding the whole project as it stood.
Identity is set repo-local, so `user.name` and `user.email` both resolve and
commits work. Nothing has been pushed and no remote is configured.

`infra/.env` is committed on purpose. It holds local development defaults with
no production credential, and Docker Compose reads it at parse time, so a fresh
clone has no port map and no service config without it.

Environment: one Python, the `uv`-created venv at the repo root, holding
`pytest`, `datagen`, `benchbox` and `boto3`. Do not run things on Windows
Python, which has only `pip`. Run commands from the `Makefile`.

**Layout note.** R12 recommends one Python package at `src/coursework/`, with
the generator moved into it. That move has not happened: `generator/` still
holds `datagen`, and pipeline code lives under `pipelines/`. It is a mechanical
move and should be done before the ML work adds a third importer.

---

## Next

1. Write the dbt project and get silver to gold working. It is the largest
   remaining rubric block and it unblocks DP3.
2. Write the Airflow DAGs, because the rubric scores ingest *and* validate
   stages for all three pipelines.
3. Build the Flink job. Ten rubric points, and nothing exists yet.
4. Move `datagen` to `src/coursework/datagen` per R12, while the blast radius is
   still two directories.