# Architecture

How the system fits together, from the top down. Section 1 is the shape.
Section 7 is the wiring. Read section 1 if you only want the picture.

Terms are in [`../GLOSSARY.md`](../GLOSSARY.md). Decisions are in
[`adr/`](adr/).

---

## 1. The shape

We generate brokerage data, move it through three lakehouse zones, model it into
a warehouse, then train and serve a model.

```
  datagen                      Kafka                    Flink
     |                           |                        |
     v                           |                        v
 +---------+   DP1    +------------------+  DP2   +-------------+
 | landing | -------> | bronze -> silver | -----> |    gold    |
 +---------+  Spark   +------------------+ dbt    +-------------+
                    SeaweedFS (S3)               Postgres  +  SeaweedFS
                                                              |
                                            DP3  <------------+
                                     feature tables -> Feast
                                                              |
                                        MLflow <- train <- Kubeflow
                                                              |
                                                     FastAPI <- KServe
```

Each numbered arrow is one deployable unit:

1. **Generate.** `datagen` writes files as if a source system produced them.
2. **DP1, Spark.** Landing to bronze to silver. Raw ingest, then cleaning and typing.
3. **DP2, dbt-spark.** Silver to gold. Dimensional modelling, tests, docs.
4. **Governance, DataHub.** Assets, lineage, and validation results for all three pipelines.
5. **Stream.** Incremental batches replay into Kafka, Flink aggregates them, results go to Feast.
6. **DP3.** Offline feature tables materialise into the Feast online store.
7. **ML.** Kubeflow trains, MLflow records, FastAPI serves.

Solid arrows are the main data path. Dashed arrows are secondary reads, such as
governance and serving.

---

## 2. Zones

| Zone | Contents | Written by | Purpose |
|---|---|---|---|
| **landing** | Raw generated files, unmodified | `datagen` | Stands in for another team's systems, so ingest starts from files |
| **bronze** | Source data as received, Parquet | Spark | The record of what actually arrived |
| **silver** | Cleaned, typed, deduplicated entities | Spark | One row per real-world thing |
| **gold** | Dimensions, facts, feature tables | dbt-spark | Queryable and trainable |
| **online store** | Materialised features | Feast | Low latency read at inference |

Naming: `raw_` in bronze, `stg_` in silver, `dim_` / `fact_` / `obt_` / `feat_`
in gold.

Landing and the lakehouse are different buckets, not different prefixes in one
bucket. The rule "nothing writes into landing except the generator" then holds
at the storage layer rather than by convention. Landing keys are
`s3://landing/tpcdi/<batch>/`, lakehouse keys are
`s3://lakehouse/<zone>/<table>/`.

Landing, bronze and silver partition nothing. Landing is a flat directory per
batch. Bronze keeps every arrival column, including which file and which batch
a row came from, because bronze is the audit trail. Silver partitions on the
event timestamp's date, never on the batch id, because a trade from 2016
delivered in a 2017 batch belongs to 2016.

---

## 3. Storage

**Lakehouse: SeaweedFS over its S3 API, queried by Trino.** Chosen over MinIO,
which is what the course lab uses. Reasoning is in
[`adr/0001`](adr/0001-seaweedfs-as-lakehouse-object-store.md), configuration is in
[`IaC.md`](IaC.md), and the measurements are in
[`../.scratch/coursework/research/R13-trino-seaweedfs-spike.md`](../.scratch/coursework/research/R13-trino-seaweedfs-spike.md).

Trino documents AWS S3 and MinIO as the S3-compatible stores it tests against.
SeaweedFS is neither, so the combination was an assumption for two sessions.
It is now measured: `make verify` writes Parquet with pyarrow, registers it in
the metastore, and reads it back through Trino. The fallback to lab MinIO was
never needed and is not wired in.

Two SeaweedFS behaviours shape the design and are recorded in
[`IaC.md` section 7](IaC.md#7-why-these-choices):

- An empty prefix reads as an existing directory, so Spark's managed-table
  location check always fires. Every lakehouse table is external.
- Each S3 bucket is a collection, and a collection needs a volume. Out of
  volumes, writes fail with an error that reads like a gateway fault.

**Warehouse: Postgres holds gold.** One instance, four databases: `warehouse`,
`metastore`, `airflow`, `datahub`. The warehouse database is the source of
truth for ML and serving. It is separate from the metastore database so a gold
table can never collide with a catalog table.

Partition discovery is the mechanism that bites. New files landing in an already
registered partition appear automatically. A new `dt=…/` directory does not. It
needs `sync_partition_metadata` or `MSCK REPAIR`. Our pipelines ingest repeatedly,
so this is a recurring step, and DP1 measures it: 100 rows written into 2
partitions read back as 0 rows until the repair runs.

---

## 4. Processing

**Spark, DP1. Built.** Batch ingest and cleaning, landing to bronze to silver.
Three steps: publish the generator's output into the landing bucket, parse each
source file into a `raw_` table without interpreting it, then type, deduplicate
and partition into `stg_`. Code in `pipelines/dp1/`, run with `make dp1`.

**dbt-spark, DP2. Designed.** Silver to gold: models, tests, generated docs.
This is one of our two novel ideas, because it makes transforms reviewable and
tests declarable instead of buried in job code. dbt-spark cannot connect to a
Spark master URL, so DP2 needs a HiveServer2, and that decision is open. See
`docs/Status.md`.

**Flink, streaming. Designed.** Consumes replayed incremental batches, applies
watermarks and windowed aggregation, handles late data through side outputs,
writes idempotently.

**Airflow. Designed.** Schedules DP1, DP2, DP3, and the generator. Cron
triggers, not file sensors. Connections and Variables live in Airflow so no DAG
hardcodes a hostname, a bucket or a password.

### SCD2 needs Spark, not Postgres

TPC-DI sends updates as partial payloads. An update carries only the fields that
changed. Reconstructing version history therefore requires a carry-forward, and
Postgres cannot do it because it lacks `LAST_VALUE(... IGNORE NULLS)`.

Resolution: run the SCD2 transform in Spark, which supports that function, then
write the finished `valid_from` / `valid_to` / `is_current` rows into Postgres
as a plain table. Postgres stores the result. Ticket 03 confirms this.

### dbt incremental cannot merge

SeaweedFS has no `MERGE`, and Trino is not a dbt write target. So dbt's default
incremental strategy does not work. We use `insert_overwrite` or append per
batch, which suits dated batches. Ticket 08 confirms this.

---

## 5. The generator

The generator is the centre of this project. Running it and its format contract
are in [`../generator/README.md`](../generator/README.md). Why we built it
rather than reused one is in [`adr/0002`](adr/0002-build-our-own-generator.md).

Three points shape everything else:

- **The format contract lives in one module**, `generator/src/datagen/emit.py`.
  CRLF everywhere, no header rows, mixed delimiters across file families, empty
  string means NULL. Tests check it against real TPC-DI bytes committed as a
  4KB fixture.
- **Data problems are configurable per run** and inject independently. The same
  code produces clean data and broken data. Clean data matters because the
  rubric wants before and after comparisons.
- **Characteristics are measured from the output**, not read from the config.
  `inspect.py` computes skew, cardinality, duplicate rate, schema drift, and
  volume from the bytes produced. See
  [`adr/0003`](adr/0003-measure-generated-characteristics.md).

---

## 6. ML platform

Not started. Tickets 05 and 06. The plan: Feast for features, a Kubeflow
training pipeline running load, split, train, eval, MLflow for tracking and the
registry, FastAPI for serving.

---

## 7. What exists and what is planned

| Area | State |
|---|---|
| Generator: trade, cash, market, watch, holding, history, reference tables, FINWIRE, CustomerMgmt XML | Works, 31 tests |
| Generator characteristics inspector | Works, tested |
| Platform: SeaweedFS, Hive Metastore, Trino, Postgres | Running, 8 of 8 smoke tests pass |
| Spark cluster | Running |
| DP1: landing to bronze to silver | Running, verified through Trino |
| DP2: dbt-spark, silver to gold | Compose ready, no dbt project |
| DP3: offline feature table | Not started |
| Airflow DAGs | Compose ready, no DAGs |
| Kafka and Flink streaming | Compose ready, no job |
| Soda validation, DataHub governance | Compose ready, no checks, no recipes |
| ML platform | Not started, tickets 05 and 06 |

`[`docs/Status.md`](Status.md) has the measured numbers and the next step for
each. `docs/IaC.md` has the service inventory and the findings that cost time.

The repo previously held a second direction: a TPC-DS retail schema and a 330MB
DuckDB database from a local `dbt-tpcdi` build. TPC-DS was explored first and
dropped. Those files are in `misc/abandoned-tpcds/`.