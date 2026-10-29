# Infrastructure as Code

This reference describes the Compose configuration, service connections, and operating commands. It distinguishes configured behavior from runtime evidence. Design choices are in [Project Design](Project-Design.md), implementation state is in [Status](Status.md), and earlier trials are in the [infrastructure evidence record](records/Infrastructure-Evidence.md).

## 1. How to run it

Run commands from the repository root in WSL, where the project filesystem and Docker socket are available. The [Makefile](../Makefile) defines these commands.

| Command | Effect and prerequisites |
|---|---|
| `make up-core` | Starts PostgreSQL, SeaweedFS, Hive Metastore, and Trino. Required local images must be available or buildable. |
| `make bootstrap` | Creates the configured buckets, database objects, and prefix markers against the core services. |
| `make verify` | Runs the platform smoke checks. A pass checks those operations, not every pipeline or optional service. |
| `make up-compute` | Starts the Spark master and worker. |
| `make generate` | Regenerates `misc/sample_run` with the existing generator at scale 0.02. It deletes the prior output first. |
| `make dp1 BATCH=Batch1` | Stops the Thrift server, publishes local source files, and runs the existing landing-to-bronze-to-silver job. It requires generated input and the core and compute services. |
| `make up-dbt` | Starts the Spark master, worker, and Thrift server. |
| `make spark-state` | Reads the Spark master's workers and applications. |
| `make list-ports` | Prints published ports from resolved Compose configuration and checks for duplicates within that configuration. |
| `make stop` | Stops containers and retains data. |
| `make down` | Removes containers and retains named volumes. |

`make help` lists the available targets. `make up-stream`, `make up-airflow`, and `make up-governance` are not defined. The optional profiles have incomplete build inputs; see [Status](Status.md#known-defects) before attempting to run them. `make images` also includes the incomplete governance build.

The accepted DP1 boundary is source to bronze. The existing `make dp1` command continues to silver and can delete earlier silver batches in its default overwrite mode. It does not implement the revised pipeline boundary.

## 2. Environment

The commands assume Linux tools and Docker inside WSL. Host resources, installed versions, and free disk space can change. The [earlier environment snapshot](records/Infrastructure-Evidence.md#environment-snapshot) records the machine used for the original runs.

Container image settings are in `infra/.env`. Compose reads that file to substitute variables; a container receives only the values its service passes through. The file contains committed local development defaults. SeaweedFS uses the mutable `latest` tag, so a later pull can change behavior without a configuration edit.

## 3. The compose files

`infra/.env` sets `COMPOSE_FILE` to five files. The Makefile runs Compose from `infra/`, and profiles select the optional services.

| File | Services | Profiles |
|---|---|---|
| `compose.yaml` | PostgreSQL, SeaweedFS, Hive Metastore, Trino, bootstrap, and tools. | The four core services have no profile. The runners use `init` and `tools`. |
| `compose.compute.yaml` | Spark master, worker, Thrift server, and submit runner. | `compute` and `dbt`. |
| `compose.streaming.yaml` | Kafka, topic initialization, Flink job manager, task manager, and submit runner. | `stream`. |
| `compose.orchestration.yaml` | Airflow initialization, scheduler, webserver, and a worker definition. | `airflow`. |
| `compose.governance.yaml` | OpenSearch, DataHub upgrade, GMS, frontend, and ingestion runner. | `governance`. |

Services share `edai-net` and resolve each other by service name. If a selected service depends on a service excluded by profiles, Compose can reject the configuration. `spark-thrift` requires the compute profile for its master. `spark-jobs` and `datahub-ingest` have no `depends_on`; their callers must arrange service readiness. `flink-submit` does depend on its job manager.

## 4. Services

### 4.1 Storage

PostgreSQL uses `postgres:16-alpine`. Bootstrap creates `warehouse`, `metastore`, `airflow`, and `datahub` in one instance. `shared_buffers=512MB` sets shared buffer capacity. `default_statistics_target=200` increases the detail available to the planner and the cost of collecting statistics. It is not a fixed sample of 200 rows. The presence of a `warehouse` database does not settle the proposed gold placement.

SeaweedFS runs master, volume server, filer, and S3 gateway through one `weed server` command. The mounted `infra/seaweedfs/s3.json` defines the S3 identities. `-volume.max=0` derives the volume limit from available disk capacity; `-volume.minFreeSpacePercent=5` reserves free space. Volume allocation failures can surface as S3 retries; see [section 8.3](#83-the-object-store-runs-out-of-volumes).

Hive Metastore uses `starburstdata/hive:3.1.2-e.18` and PostgreSQL's `metastore` database. The service provides the image's required environment variables, including empty values for unused cloud settings. `SERVICE_OPTS` supplies JVM and metastore properties. This image's earlier startup behavior is recorded in the [evidence record](records/Infrastructure-Evidence.md#image-and-storage-trials).

Trino uses `trinodb/trino:476`. Its Hive catalog reads table metadata from Hive Metastore and objects from SeaweedFS. The existing batch job does not use Trino to ingest data. The [Hive catalog configuration](../infra/trino/catalog/hive.properties) disables exclusive create with `s3.exclusive-create=false`; the earlier SeaweedFS run rejected conditional writes with HTTP 501.

### 4.2 Compute

`edai/spark:3.5.3` extends `apache/spark:3.5.3` with Hadoop's S3A client and the AWS SDK. A build stage downloads the jars; the runtime stage copies them and installs the job dependencies.

The standalone cluster has one master and one worker. The worker advertises six cores and 5 GB of memory. The master UI is on host port 8082. Stage and task evidence comes from an application's UI; the master UI alone does not show skewed task durations.

`spark-thrift` exposes HiveServer2 on port 10001 for a possible dbt-spark connection. Its separate `dbt` profile lets batch jobs run without that long-lived Spark application. Total application cores and executor cores are different limits; see [section 8.2](#82-a-hiveserver2-takes-the-whole-worker).

### 4.3 Streaming

Kafka uses `confluentinc/cp-kafka:7.7.1` with a combined KRaft broker and controller. Container clients receive `kafka:29092`; host clients receive `localhost:9092`. If a client cannot resolve the advertised address, it can reach the initial broker address but fail later requests.

The topic initializer declares `tpcdi.trade` and `tpcdi.cash_transaction`, each with three partitions, replication factor one, and 24-hour retention. These are existing configuration values. CashTransaction is the selected project stream; topic configuration does not define its event contract or recovery policy.

The Flink configuration names local image `edai/flink:1.20.1`, one job manager, and one task manager with four slots. It requests checkpoints every 30 seconds and `EXACTLY_ONCE` mode. End-to-end delivery guarantees also depend on the source, sink, checkpoint storage, and restart behavior. These settings alone do not establish those guarantees.

### 4.4 Orchestration

The Airflow configuration names local image `edai/airflow:2.10.5` and selects `LocalExecutor`. It mounts DAGs, connection settings, and variables from `orchestration/`. Initialization attempts to import those settings. Both import commands allow failure with `|| true`, so a successful initialization exit does not prove that the imports succeeded.

LocalExecutor runs task processes from the scheduler. The additional `airflow-worker` service with a `worker` command is inconsistent with that execution model. The missing image build and this configuration need resolution before the profile can be treated as an operating Airflow deployment.

### 4.5 Governance

The DataHub definitions use version `v1.5.0.6` for upgrade, GMS, and frontend images. OpenSearch uses `2.19.3`. The configuration points DataHub at PostgreSQL and the Kafka service in the stream profile. Enabling only the governance profile does not start Kafka.

The ingestion runner mounts `governance/` and names a recipe path. A service definition does not establish that a recipe, dataset lineage, or validation publication exists. See [Status](Status.md#what-exists).

## 5. Ports

These are the defaults in `infra/.env`. Other stacks can use the same host ports. `make list-ports` checks this project's definitions, not every process listening on the host.

| Host port | Service endpoint |
|---|---|
| 5432 | PostgreSQL. |
| 8333 | SeaweedFS S3 API. |
| 8080 | SeaweedFS volume server. |
| 8888 | SeaweedFS filer. |
| 9333 | SeaweedFS master. |
| 9083 | Hive Metastore Thrift. |
| 8081 | Trino coordinator. |
| 7077 | Spark cluster manager. |
| 8082 | Spark master UI. |
| 8083 | Spark worker UI. |
| 10001 | Spark Thrift server. |
| 9092 | Kafka host listener. |
| 8084 | Flink job manager UI. |
| 8085 | Airflow webserver. |
| 9002 | DataHub frontend. |
| 8086 | DataHub GMS API. |

## 6. Memory budget

Compose declares the limits below. They are limits, not measured use or a guarantee that simultaneous workloads fit. JVM heap is only part of a process's memory. One-shot runners such as `spark-jobs`, `bootstrap`, and `datahub-ingest` have no explicit memory limit in these files.

| Service | Configured limit | Related setting |
|---|---|---|
| PostgreSQL | 1536M | Shared buffers are 512 MB. |
| SeaweedFS | 1536M | One process provides four roles. |
| Hive Metastore | 1280M | Maximum heap is 768 MB. |
| Trino | 2560M | Maximum heap is 1 GB. |
| Spark master | 1024M | The master schedules applications. |
| Spark worker | 5G | The worker advertises six cores and 5 GB. |
| Spark Thrift | 2G | The service uses the `dbt` profile. |
| Kafka | 1536M | Maximum heap is 768 MB. |
| Flink job manager | 1280M | Configured process memory is 900 MB. |
| Flink task manager | 2048M | Configured process memory is 1600 MB. |
| Airflow init, scheduler, webserver | 1G each | Init is a one-shot process. |
| Airflow worker definition | 2G | Its role conflicts with LocalExecutor. |
| OpenSearch | 2048M | Maximum heap is 1024 MB. |
| DataHub upgrade, frontend | 1G each | Upgrade is a one-shot process. |
| DataHub GMS | 1536M | Kafka and OpenSearch add separate costs. |

`bash misc/tools/check_memory.sh` reads the resolved core and compute limits. It does not measure runtime memory. Leave capacity for the operating system, Docker, drivers, and concurrent one-shot tasks when selecting profiles.

## 7. Why these choices

### 7.1 Five compose files rather than one

Files group services by role. Profiles control which optional services start. Splitting files alone does not control memory use because `COMPOSE_FILE` loads all five files.

### 7.2 Every lakehouse table is external

The earlier build used external Hive tables with explicit object locations. In that run, Spark rejected a managed table location with `LOCATION_ALREADY_EXISTS` even when the SeaweedFS prefix listed as empty. External table metadata avoids that managed-location check. The pipeline must then manage object retention, replacement, and partition registration explicitly.

### 7.3 Hive DDL rather than the DataSource V2 syntax

The existing tables use `PARTITIONED BY` and `STORED AS PARQUET` so Hive Metastore stores the SerDe and file-format metadata used by Trino's Hive connector. Spark's `USING parquet` syntax has different partition declaration rules. Any replacement DDL or table format needs a read/write compatibility check across the selected engines.

### 7.4 One Postgres instance, four databases

Separate databases keep each application's tables in a separate namespace while sharing one process and buffer pool. They still share memory, connections, storage, and failure exposure. This configuration reduces local service overhead but does not provide process or resource isolation. PostgreSQL gold storage remains a proposal.

## 8. Operating constraints

These operating notes explain failure mechanisms found in the earlier build. Trial values and their limits are in the [infrastructure evidence record](records/Infrastructure-Evidence.md#earlier-trials).

### 8.1 A path write is not a table write

A Parquet path write can create partition directories without registering them in Hive Metastore. A query engine that reads registered partitions can then return no rows. The existing jobs call `MSCK REPAIR TABLE` after partitioned writes. A table-aware insert can register partitions as part of its write; the correct choice depends on who owns the table and its location.

### 8.2 A HiveServer2 takes the whole worker

A long-lived Thrift server can occupy all worker cores and leave a batch application waiting with `Initial job has not accepted any resources`. Setting `spark.executor.cores` limits each executor, not the total application's cores. Inspect application resource requests with `make spark-state`.

The project separates the Thrift server into the `dbt` profile, and `make dp1` stops it before submission. dbt-spark can use a Thrift endpoint; it does not accept a Spark master URL as a Thrift endpoint. Session mode starts a PySpark session, whose Spark configuration determines its execution target. It does not inherently require local execution.

### 8.3 The object store runs out of volumes

SeaweedFS can report `failed to find writable volumes for collection:landing` while the S3 client reports retries or `InternalError`. A collection needs writable volume capacity. The current command uses `-volume.max=0` after an earlier fixed cap was reached. Check both the S3 response and server logs; increasing a volume count cannot fix exhausted disk space. `misc/tools/s3_probe.py` provides a request-level check.

### 8.4 PySpark launcher arguments

The earlier build associated a malformed launch and zero-core request with `PYSPARK_SUBMIT_ARGS`. The current Spark image leaves it unset. This variable belongs to PySpark's launcher; its presence is not evidence that every direct `spark-submit` invocation appends it. Inspect the actual launch arguments when diagnosing a resource request.

### 8.5 An off-by-one bind mount looks like a working mount

Relative bind paths resolve against the base Compose file directory, `infra/`. A repository sibling is therefore `../pipelines`, not `../../pipelines`. Short bind syntax can create a missing host directory and mount an empty path. Inspect the resolved configuration and source path when expected code or settings are absent. The current Flink submit mount has this path defect, recorded in [Status](Status.md#known-defects).

### 8.6 `spark-class` does not read `spark-defaults.conf`

The cluster configuration uses `start-master.sh`, `start-slave.sh`, and `start-thriftserver.sh`. These scripts start different kinds of processes. Master and worker daemons use daemon settings and command arguments; a Spark application uses submission settings. Do not assume that every start script wraps `spark-submit` or that application defaults configure the master daemon. `make spark-conf` prints the configuration resolved by a submitted job.

### 8.7 S3 has no directories, and Spark checks for one

S3 stores object keys rather than directories. Filesystem clients infer directories from prefixes and marker objects. Bootstrap creates `_keep` objects under the configured output prefixes because the earlier Spark event-log initialization required a directory-like prefix.

### 8.8 An unfiltered union doubles an evolved file

Parsing every source row under two schemas and unioning both results duplicates the input. Filter each branch by its declared field count before the union, then account for matched and unmatched source rows. The existing pure `variants.validate_split` helper checks arithmetic, but it does not prove that the Spark parser measured unmatched rows correctly. The current parser counts them after replacing the original frame, so unknown shapes can disappear without a warning.

### 8.9 A business key narrower than the record deletes real rows

A deduplication key must distinguish repeated delivery from separate business events. Watch actions at different times and trade status changes are separate records even when other fields agree. The existing job uses whole-record keys for most tables. Incremental Trade still needs review against the specification: `T_ID` identifies the trade, and each CDC record represents a change. A generated column name or a sample field position does not define benchmark identity.

### 8.10 Partition replacement and reruns

Dynamic partition overwrite replaces the partitions touched by the write. Files outside those partitions can remain, including files left by an earlier layout. The existing silver job deletes its whole output location before overwrite. That removes stray files but also removes valid earlier batches. This is a known data-loss defect, not a general rerun strategy. An append also needs duplicate and retry rules; sharing a location does not make append safe by itself.

### 8.11 Shutdown deadlines and run reports

The earlier run blocked during Spark shutdown while closing the event log against SeaweedFS. `stop_session` now waits up to 90 seconds on a separate thread. The caller writes the report after that wait, including `stopped_cleanly`. If cleanup does not finish, the code signals the parent JVM and exits directly.

The report's `ok` field records whether processing raised an exception. It is separate from clean shutdown and does not prove business correctness. If the report remains only in the container's default `/tmp` path, removing the container removes that file; retain stdout or configure a writable persistent report path.
