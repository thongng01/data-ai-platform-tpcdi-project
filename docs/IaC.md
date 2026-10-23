# Infrastructure as Code

How the platform is put together, what each container is for, and what the
measured numbers are. Everything here was run on the machine described in
[Environment](#environment); the figures are from those runs, not estimates.

Terms are in [`../GLOSSARY.md`](../GLOSSARY.md). The system's shape is in
[`Architecture.md`](Architecture.md). What is built and what is not is in
[`Status.md`](Status.md).

---

## Contents

1. [How to run it](#1-how-to-run-it)
2. [Environment](#2-environment)
3. [The compose files](#3-the-compose-files)
4. [Services](#4-services)
5. [Ports](#5-ports)
6. [Memory budget](#6-memory-budget)
7. [Why these choices](#7-why-these-choices)
8. [Findings that cost time](#8-findings-that-cost-time)
9. [What is not built yet](#9-what-is-not-built-yet)

---

## 1. How to run it

Everything runs in WSL. Do not run any of it from Windows: the repository is on
a WSL filesystem and the containers need WSL's Docker socket.

```bash
make up-core       # postgres, seaweedfs, hive-metastore, trino
make bootstrap     # create the buckets and databases
make verify        # 8 smoke tests against the running platform
make up-compute    # the Spark cluster
make dp1           # landing -> bronze -> silver
```

`make help` lists every target. Each one is a thin wrapper around
`docker compose`, so reading the target tells you what it runs.

The four commands above are the whole proof of the platform: `make verify`
checks that SeaweedFS, the metastore and Trino work together, and `make dp1`
runs a real batch through all three lakehouse zones.

---

## 2. Environment

| Property | Value |
|---|---|
| Host OS | WSL2, Linux 6.6 kernel |
| Docker Engine | 29.3.1, Compose v5.1.1 |
| CPU | 20 cores |
| RAM | 15 GB total, ~4 GB swap |
| Disk | 935 GB free on the WSL root |
| Python (host) | `uv` venv at the repo root, WSL Python 3.14 |

15 GB is the binding constraint. Twelve containers do not fit in it, so every
service has a memory limit and the heavy ones sit behind compose profiles.

---

## 3. The compose files

Five files, all in `infra/`, all loaded for every `docker compose` command
because `infra/.env` sets `COMPOSE_FILE`. Profiles decide what starts, not
which file is named.

| File | Contents | Profiles |
|---|---|---|
| `compose.yaml` | postgres, seaweedfs, hive-metastore, trino, bootstrap, tools | none for the four services, `init`, `tools` |
| `compose.compute.yaml` | spark master, worker, Thrift server, one-shot submit | `compute`, `dbt` |
| `compose.streaming.yaml` | kafka, topic init, Flink job manager and task manager | `stream` |
| `compose.orchestration.yaml` | Airflow init, scheduler, webserver, worker | `airflow` |
| `compose.governance.yaml` | OpenSearch, DataHub GMS, frontend, upgrade, ingest | `governance` |

One network, `edai-net`. Services reach each other by compose service name.

A service that belongs to two profiles and also has a `depends_on` breaks the
other profile: Compose filters the dependencies by profile and then fails to
resolve them. The one-shot runners (`spark-jobs`, `datahub-ingest`,
`flink-submit`) therefore carry no `depends_on`, and `spark-submit` fails
immediately and clearly when the cluster is not there.

---

## 4. Services

### 4.1 Storage

**postgres:16-alpine.** One instance, four logical databases: `warehouse`,
`metastore`, `airflow`, `datahub`. Separate databases rather than separate
containers, because a metastore table and a gold table must never collide on a
name, and four Postgres containers would cost about a gigabyte to achieve the
same thing.

`shared_buffers` is 512 MB and `default_statistics_target` is raised to 200.
The second one matters for the warehouse: the gold layer is queried with
predicates on `dt` and on surrogate keys, and the default sample of 100 rows
picks a poor plan for a table with tens of thousands of partitions.

**chrislusf/seaweedfs.** One container running `weed server`, which is the
master, a volume server, a filer and the S3 gateway in a single process. Four
containers would cost roughly 1 GB more.

Two settings are load-bearing:

- `-volume.max=0` lets the volume count follow free disk space. With a fixed
  count the server silently runs out: SeaweedFS maps each S3 bucket to a
  *collection*, a new collection needs a new volume, and once the cap is reached
  every write to the newest bucket fails. See
  [Finding 8.3](#83-the-object-store-runs-out-of-volumes).
- Credentials come from a mounted IAM config rather than from flags, because
  without one SeaweedFS generates a random identity at first start and every
  client would have to read it back out of the container.

**starburstdata/hive:3.1.2-e.18.** Hive Metastore 3.1 on the `metastore`
database. The image's entrypoint blocks on `test -v` for twenty-three
variables, eleven of them Azure ones this project never uses. All must be set;
empty strings satisfy the check. The image also ignores `HIVE_CUSTOM_CONF_DIR`
and edits its own `hive-site.xml`, so `SERVICE_OPTS` is the only supported way
to add a property.

Hive 3.1 rather than 4.x because Spark 3.5 bundles a Hive 2.3 client, and 3.1 is
the version that client talks to without version negotiation.

**trinodb/trino:476.** Query only. It is never in the ETL path, so a Trino
failure cannot break ingest. The S3 block in
[`trino/catalog/hive.properties`](../infra/trino/catalog/hive.properties) sets
`s3.exclusive-create=false`, which Trino's docs do not mention but SeaweedFS
requires: Trino sends `If-None-Match: *` on writes by default and SeaweedFS
answers 501.

### 4.2 Compute

**edai/spark:3.5.3.** Built from `apache/spark:3.5.3` with `hadoop-aws` and the
AWS SDK added, because Spark ships no S3 client. The jars are fetched in a
builder stage and copied into the runtime stage, so curl, the apt cache and the
download scratch space stay out of the shipped image.

A standalone cluster: `spark-master` plus one `spark-worker` with 6 cores and
5 GB. The Spark UI is on port 8082, which is what the rubric asks for as
evidence when it wants a screenshot of a skewed stage.

`spark-thrift` is HiveServer2, the endpoint dbt-spark connects through. It
lives in its own `dbt` profile, not beside the cluster, for the reason in
[Finding 8.2](#82-a-hiveserver2-takes-the-whole-worker).

### 4.3 Streaming

**confluentinc/cp-kafka:7.7.1** in KRaft mode: one broker that is also the
controller, no ZooKeeper. Two advertised listeners, one for containers and one
for the host, because a client that receives the wrong one connects and then
fails to fetch partitions, which looks like a broker fault and is not one.

**apache/flink:1.20.1.** Job manager plus one task manager with 4 slots.

### 4.4 Orchestration

**apache/airflow:2.10.5**, extended locally with PySpark, dbt-spark and the
DataHub SDK. Metadata in the `airflow` database. LocalExecutor.

Connections and Variables are seeded by `airflow-init` from
`orchestration/connections.yaml` and `orchestration/variables.yaml`, which is
how the rubric's "all connections and variables inside Airflow" is met: no DAG
hardcodes a hostname, a bucket or a password.

### 4.5 Governance

**acryldata/datahub-gms** on the `datahub` database, with
**opensearchproject/opensearch:2.19.3** as its search backend. Its metadata
change log runs through the Kafka broker from the streaming profile.

---

## 5. Ports

The course labs reuse 8080, 8081, 8082 and 9092 across stacks, which is why
only one lab can run at a time. This project takes a single, non-overlapping
map so a reviewer can have the labs and this up at once where memory allows.

| Port | Service | What it is |
|---|---|---|
| 5432 | postgres | warehouse, metastore, Airflow and DataHub databases |
| 8333 | seaweedfs | S3 API |
| 8080 | seaweedfs | volume server UI |
| 8888 | seaweedfs | filer UI |
| 9333 | seaweedfs | master UI |
| 9083 | hive-metastore | Thrift |
| 8081 | trino | coordinator and UI |
| 7077 | spark-master | cluster manager |
| 8082 | spark-master | Spark UI |
| 8083 | spark-worker | Spark executor UI |
| 10001 | spark-thrift | HiveServer2, dbt only |
| 9092 | kafka | broker, host side |
| 8084 | jobmanager | Flink job manager UI |
| 8085 | airflow | webserver |
| 9002 | datahub | frontend |
| 8086 | datahub-gms | GMS API |

Two hosts are worth knowing. `make list-ports` prints the resolved map from the
fully rendered compose configuration and flags any collision, because a
collision stops the stack starting and a prose table goes stale the moment
someone edits a port.

`8081` is Trino here and the Spark worker UI on the labs' l10 stack. Running
both at once needs a remap; nothing else overlaps with the labs.

---

## 6. Memory budget

Every limit below is set in the compose files, not left to the JVM's default
container detection. Without a limit, a JVM sized for a 15 GB host takes all of
it and the kernel kills something else.

| Service | Limit | Notes |
|---|---|---|
| postgres | 1536 MB | `shared_buffers` 512 MB |
| seaweedfs | 1536 MB | one process for four roles |
| hive-metastore | 1280 MB | heap 768 MB |
| trino | 2560 MB | heap 1 GB, coordinator and worker together |
| spark-master | 1024 MB | |
| spark-worker | 5 GB | 6 cores, 3 GB executor memory |
| spark-thrift | 2 GB | `dbt` profile only |
| kafka | 1536 MB | heap 768 MB |
| flink job manager | 1280 MB | process size 900 MB |
| flink task manager | 2048 MB | process size 1600 MB |
| airflow (x4) | 1 GB each | init, scheduler, webserver, worker 2 GB |
| opensearch | 2048 MB | heap 1 GB |
| datahub gms | 1536 MB | |

**Core only:** 6.8 GB. **With compute:** 12.8 GB. **With governance as well:**
over 15 GB, which is why `docs/Status.md` says to stop compute before starting
governance.

`make list-ports` and `misc/tools/check_memory.sh` print these numbers from the
rendered compose configuration, so this table can be checked rather than
trusted. It was wrong twice: the totals were estimated before the limits were
written down, and one service had a different limit from the one quoted here.

The Hive Metastore heap is the one number worth raising if the tables grow.
Everything else is sized to fit, not to perform.

---

## 7. Why these choices

### 7.1 Five compose files rather than one

One file with twelve services means `docker compose up -d` starts all of them
and the machine swaps. Splitting by role and gating each with a profile means
the floor is always runnable, and the reviewer can bring up exactly the part
they want to look at.

### 7.2 Every lakehouse table is external

Spark refuses to create a *managed* table whose location already exists.
SeaweedFS's S3 gateway reports an empty prefix as an existing directory, so a
managed `CREATE TABLE` fails with `LOCATION_ALREADY_EXISTS` on a path that lists
as empty. Declaring the location skips the check.

This is not a workaround with a cost. External tables state their location
explicitly, which is what a lakehouse wants, and dbt's `location_root` produces
the same shape.

### 7.3 Hive DDL rather than the DataSource V2 syntax

`USING parquet` requires partition columns to be declared in the column list,
which is the opposite of the Hive convention, and it writes table metadata in a
shape Trino describes differently. `PARTITIONED BY` plus `STORED AS PARQUET`
writes the SerDe and file-format metadata Trino's Hive connector expects.

### 7.4 One Postgres instance, four databases

See section 4.1. The alternative, a container per database, costs about a
gigabyte and buys nothing: the isolation that matters is the database
boundary, not the process boundary.

---

## 8. Findings that cost time

These are the ones that cost the most hours. They are here so the next person
does not pay for them twice, and they are the evidence for the design choices
above.

### 8.1 A path write is not a table write

Writing Parquet to `s3a://…/silver/stg_trade` creates `dt=…/` directories on
the object store. The metastore has never heard of them, so
`SELECT count(*)` returns 0 while `ls` shows the files.

Every partitioned write in this project is followed by
`MSCK REPAIR TABLE`. This is confirmed, measured behaviour: the wiring check
writes 100 rows into 2 partitions, reads 0 back, repairs, and reads 100.

The alternative, writing with `INSERT INTO`, registers partitions as a side
effect. That is cleaner when the table already exists and does not work when
the pipeline owns the location, which is this project's case.

### 8.2 A HiveServer2 takes the whole worker

dbt-spark cannot talk to a Spark master URL. Its connection methods are
thrift, http, livy, odbc and session, and only thrift fits a standalone cluster.

With HiveServer2 running, the master's application list shows it holding every
core of the worker, whatever `--conf spark.executor.cores` says. A batch job
then waits forever, logging `Initial job has not accepted any resources` every
15 seconds and never a stack trace, because nothing has failed.

`spark-thrift` therefore sits in a `dbt` profile. `make up-compute` gives the
cluster entirely to DP1 and DP3; `make up-dbt` adds HiveServer2 for a DP2 run.
The alternative, dbt's `method: session`, needs no Thrift server at all and is
the right choice on a smaller machine, at the cost of dbt no longer running on
the shared cluster.

When a job hangs with no resources, `make spark-state` shows what the master
thinks is running.

### 8.3 The object store runs out of volumes

`docker compose run` against the landing bucket failed with
`InternalError ... reached max retries`. The S3 client reads that as a gateway
fault.

SeaweedFS's log says otherwise: `failed to find writable volumes for
collection:landing`. SeaweedFS maps each S3 bucket to a collection, a new
collection needs a new volume, and `-volume.max=8` had been reached. Fixed with
`-volume.max=0`, which derives the count from free disk space.

`misc/tools/s3_probe.py` is the two-line check that separates "the bucket does
not exist", "the credentials are wrong" and "the store is full".

### 8.4 `PYSPARK_SUBMIT_ARGS` corrupts every spark-submit

The Apache Spark image does not set it, and setting it looks harmless. But
`spark-submit` *appends* it to every submission, so it injects
`pyspark-shell` into a batch job's argument list. The symptom is a job that
registers with the master requesting zero cores and then waits forever.

It belongs to the `pyspark` REPL launcher only. It is commented out in
[`docker/spark/Dockerfile`](../infra/docker/spark/Dockerfile).

### 8.5 An off-by-one bind mount looks like a working mount

Every compose file lives in `infra/`, so a sibling path is `../<name>` and a
path inside `infra/` is `./<name>`. Getting it wrong does not fail: Docker
creates the missing source as an empty root-owned directory and mounts that.
The symptom is a config file that is mounted but ignored, so Spark falls back
to a Derby metastore and a local warehouse without saying so.

`misc/` and `infra/docker/*/` were both wrong at one point during this build.

### 8.6 `spark-class` does not read `spark-defaults.conf`

The Apache Spark image's entrypoint only understands `driver` and `executor`,
which are its Kubernetes modes; anything else falls through to a bare `exec`.
Running `spark-class org.apache.spark.deploy.master.Master` starts a master
that has read none of the configuration.

`spark-class` builds a classpath and nothing else. Only `spark-submit` turns
`spark-defaults.conf` into system properties. So the cluster runs through
`/opt/spark/sbin/start-master.sh` and friends, which wrap `spark-submit`.

### 8.7 S3 has no directories, and Spark checks for one

`spark.eventLog.dir` is asked whether it is a directory before Spark starts
writing, and an empty prefix is not a directory. `make bootstrap` drops a
zero-byte `_keep` object under every prefix the platform writes to.

### 8.8 An unfiltered union doubles an evolved file

DP1 splits a file whose schema changed into a historical half and a delta half,
because two column lists of different lengths cannot come out of one `select`.
The first version selected both halves from the same frame and unioned them
without filtering either by field count. Every line was therefore emitted twice,
once under each schema.

It did not look broken. Where the business key covers every column,
deduplication collapsed the twin and the row count came out right. The only
symptom was a bronze count of 15,960 against a source file of 7,980 lines, which
is 100 percent duplication against a generator configured for 2 percent. It was
caught by reading the generator's manifest and comparing its injection rates
against the measured counts, not by looking at the pipeline.

The fix filters each branch by field count. The guard is
`variants.validate_split`, which raises if the two branches do not account for
every line, and the job now logs the per-branch counts:

```
Trade: 7980 rows, branches {'historical': 7980, 'delta': 0, 'unmatched': 0}
```

`unmatched` is the count of rows whose field count matches neither declared
shape. It is zero everywhere now. When it is not zero, the source schema changed
in a way nobody declared, and that should be a loud failure rather than a row
padded or truncated into the nearest shape.

The pure arithmetic lives in `pipelines/dp1/variants.py`, with no Spark import,
so `tests/test_schema_variant_split.py` checks it in a second instead of
starting a cluster.

### 8.9 A business key narrower than the record deletes real rows

Two tables carried a partial deduplication key. Watch history keyed on customer,
ticker and action. Trade history keyed on business id and status. Both removed
far more than the injected 2 percent: 62 percent and 58 percent respectively.

A customer watching one ticker twice on different minutes is two real events. A
trade has many history rows by design. A key that stops short of the whole
record only answers "do these rows agree on some columns", not "is this the same
record delivered twice", and nothing in the output distinguishes the two cases.

Every table now keys on the whole record by default. Trade is the only
exception, because its two shapes genuinely disagree about what identifies it:
the history id exists only from Batch2, and one trade is delivered twice there,
once submitted and once completed.

### 8.10 `overwrite` is not idempotent on a partitioned table

Spark's dynamic partition overwrite mode replaces only the partitions that
receive data in the run. Anything else under the table location survives. A
file written by an earlier run that partitioned differently, a
`.spark-staging-*` tree from a killed job, or a stray file at the table root all
stay, and a reader that lists the directory counts them.

The symptom is small and confusing: the job reports 7,820 rows and Trino counts
7,821, reproducibly. The extra row was a 268 KB `part-00000` file at the table
root, left by an earlier run that wrote the table unpartitioned because of a
separate bug.

DP1 clears the silver location before writing when the mode is `overwrite`. A
batch job owns its output location, so clearing it first is the cheap way to
make a re-run mean the same thing twice. `append` keeps what is there, which is
the mode that should be used when the location is shared.

### 8.11 A job that finished its work still hung, and reported nothing

Closing a SparkSession uploads the event log to object storage, and that close
blocked for minutes against SeaweedFS. The run had written all ten silver
tables. It then sat in `spark.stop()` indefinitely, so the run report was never
printed and the process never exited. Under Airflow that is a task that times
out rather than a task that succeeded, with no error and no partial output to
look at.

`stop_session` in `pipelines/dp1/main.py` runs the stop on its own thread and
leaves the process on a 90 second deadline whether or not it finished. The run
report is written and flushed first, so the numbers survive even when the close
does not. The report carries `stopped_cleanly`, so a run that left on the
deadline is visible as one rather than passing silently.

This is worth separating from the others: the work was correct and complete,
and the failure was entirely in the reporting of it. A pipeline whose success is
only visible after an unbounded cleanup is a pipeline that will eventually be
recorded as a failure.

This is worth separating from the others: the work was correct and complete,
and the failure was entirely in the reporting of it. A pipeline whose success is
only visible after an unbounded cleanup is a pipeline that will eventually be
recorded as a failure.

Findings 8.8, 8.9, 8.10 and 8.11 came out of a documentation review rather than
out of building, and all four were bugs in a pipeline that ran to completion and
reported success. What found them was cross-checking measured values against
independent sources: the generator's configured injection rates, the source
file's line count, and the query engine's count. A number nobody compares
against anything else is a number nobody notices is wrong.

That is why `make check-docs` exists, and why
`tests/test_schema_variant_split.py` compares a bronze row count against the
source file's line count rather than against a golden value.

---

## 9. What is not built yet

This document covers what exists. The compose files for streaming,
orchestration and governance are written and parse, but the code behind them is
not written. [`Status.md`](Status.md) has the current state and the next step
for each.

| Area | State |
|---|---|
| Generator | built and tested, 31 tests |
| DP1, Spark, landing to bronze to silver | built and running |
| Pipeline contract tests | built, 28 tests |
| Core platform, Trino over SeaweedFS | built and verified |
| DP2, dbt-spark, silver to gold | compose ready, `dbt/` empty |
| DP3, offline feature table | not started |
| Airflow DAGs | compose ready, `orchestration/` holds the Connections and Variables but no DAG |
| Kafka and Flink streaming | compose ready, no job |
| Soda validation | not started |
| DataHub governance | compose ready, `governance/` empty |

The design for each is in `.scratch/coursework/issues/` and
`.scratch/coursework/map.md`. Each empty directory carries a README saying what
goes in it, because a bind-mounted directory that Docker silently created is
worse than one that is honestly empty.