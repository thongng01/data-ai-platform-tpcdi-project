# Infrastructure as Code

This reference describes the Compose configuration, service connections, resource limits, and operating commands. Layer 1 storage and pipeline design choices are in [Data Platform](Data-Platform.md).

## 1. How to Run It

Run commands directly from the repository root in WSL, where the project filesystem and Docker socket are available. Passing `--project-directory infra` tells Docker Compose to read `infra/.env` (which sets `COMPOSE_FILE` across all five Compose files) and resolve relative paths against `infra/`.

| Command | Effect and Prerequisites |
|---|---|
| `docker compose --project-directory infra up -d postgres seaweedfs hive-metastore trino` | Starts PostgreSQL, SeaweedFS, Hive Metastore, and Trino. |
| `docker compose --project-directory infra --profile init run --rm bootstrap` | Creates the configured buckets, databases, and prefix markers against the core services. |
| `docker compose --project-directory infra --profile tools run --rm tools` | Runs the platform smoke checks across Postgres, SeaweedFS, Hive Metastore, and Trino. |
| `docker compose --project-directory infra --profile compute up -d spark-master spark-worker` | Starts the Spark master and worker. |
| `docker compose --project-directory infra --profile compute --profile dbt up -d spark-master spark-worker spark-thrift` | Starts the Spark master, worker, and HiveServer2 Thrift endpoint for dbt. |
| `.venv/bin/python -m data.generator run --config data/generator/configs/small.yaml --output local/sample_run` | Generates a validated TPC-DI dataset in `local/sample_run`. |
| `curl -fsS http://localhost:8082/json/` | Reads the Spark standalone master's registered workers and applications JSON state. |
| `docker compose --project-directory infra config` | Prints the fully resolved Compose configuration across active profiles. |
| `docker compose --project-directory infra stop` | Stops running containers and retains data. |
| `docker compose --project-directory infra down --remove-orphans` | Removes containers and retains named volumes (add `--volumes` to remove data volumes). |

## 2. Environment

The commands assume Linux tools and Docker inside WSL. Host resources, installed versions, and free disk space can change.

Container image settings are in `infra/.env`. Compose reads that file to substitute variables; a container receives only the values its service passes through. The file contains committed local development defaults. SeaweedFS uses the mutable `latest` tag, so a later pull can change behavior without a configuration edit.

## 3. The Compose Files

`infra/.env` sets `COMPOSE_FILE` to five files. Running `docker compose --project-directory infra` loads `infra/.env`, and `--profile` flags select the optional service groups.

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

PostgreSQL uses `postgres:16-alpine`. Bootstrap creates `warehouse`, `metastore`, `airflow`, and `datahub` in one instance. `shared_buffers=512MB` sets shared buffer capacity. `default_statistics_target=200` increases the detail available to the planner and the cost of collecting statistics. It is not a fixed sample of 200 rows.

SeaweedFS runs master, volume server, filer, and S3 gateway through one `weed server` command. The mounted `infra/seaweedfs/s3.json` defines the S3 identities. `-volume.max=0` derives the volume limit from available disk capacity; `-volume.minFreeSpacePercent=5` reserves free space. Volume allocation failures can surface as S3 retries; see [section 8.3](#83-the-object-store-runs-out-of-volumes).

Hive Metastore uses `starburstdata/hive:3.1.2-e.18` and PostgreSQL's `metastore` database. The service provides the image's required environment variables, including empty values for unused cloud settings. `SERVICE_OPTS` supplies JVM and metastore properties.

Trino uses `trinodb/trino:476`. Its Hive catalog reads table metadata from Hive Metastore and objects from SeaweedFS. The [Hive catalog configuration](../infra/trino/catalog/hive.properties) disables exclusive create with `s3.exclusive-create=false` because SeaweedFS rejects conditional writes with HTTP 501.

### 4.2 Compute

`edai/spark:3.5.3` extends `apache/spark:3.5.3` with Hadoop's S3A client and the AWS SDK. A build stage downloads the jars; the runtime stage copies them and installs the job dependencies.

The standalone cluster has one master and one worker. The worker advertises six cores and 5 GB of memory. The master UI is on host port 8082. Stage and task evidence comes from an application's UI; the master UI alone does not show skewed task durations.

`spark-thrift` exposes HiveServer2 on port 10001 for a possible dbt-spark connection. Its separate `dbt` profile lets batch jobs run without that long-lived Spark application. Total application cores and executor cores are different limits; see [section 8.2](#82-a-hiveserver2-takes-the-whole-worker).

### 4.3 Streaming

Kafka uses `confluentinc/cp-kafka:7.7.1` with a combined KRaft broker and controller. Container clients receive `kafka:29092`; host clients receive `localhost:9092`. If a client cannot resolve the advertised address, it can reach the initial broker address but fail later requests.

The topic initializer declares `tpcdi.trade` and `tpcdi.cash_transaction`, each with three partitions, replication factor one, and 24-hour retention. These are existing configuration values. CashTransaction is the selected project stream; topic configuration does not define its event contract or recovery policy.

The Flink configuration names local image `edai/flink:1.20.1`, one job manager, and one task manager with four slots. It requests checkpoints every 30 seconds and `EXACTLY_ONCE` mode. End-to-end delivery guarantees also depend on the source, sink, checkpoint storage, and restart behavior. These settings alone do not establish those guarantees.

### 4.4 Orchestration

The Airflow configuration names local image `edai/airflow:2.10.5`, selects `LocalExecutor`, and mounts DAGs, connection settings, and variables from `data/airflow/`.

### 4.5 Governance

The DataHub definitions use version `v1.5.0.6` for upgrade, GMS, and frontend images. OpenSearch uses `2.19.3`. The configuration points DataHub at PostgreSQL and the Kafka service in the stream profile. Enabling only the governance profile does not start Kafka.

The ingestion runner mounts `data/datahub/` and names a recipe path. A service definition alone does not establish that a recipe, dataset lineage, or validation publication exists.

## 5. Ports

These are the defaults in `infra/.env`. Other stacks on the host can use the same ports.

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

## 6. Memory Budget

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

Leave capacity for the operating system, Docker, drivers, and concurrent one-shot tasks when selecting profiles.

## 7. Why These Choices

### 7.1 Five Compose Files Rather Than One

Files group services by role. Profiles control which optional services start. Splitting files alone does not control memory use because `COMPOSE_FILE` loads all five files.

### 7.2 Every Lakehouse Table Is External

The earlier build used external Hive tables with explicit object locations. In that run, Spark rejected a managed table location with `LOCATION_ALREADY_EXISTS` even when the SeaweedFS prefix listed as empty. External table metadata avoids that managed-location check. The pipeline must then manage object retention, replacement, and partition registration explicitly.

### 7.3 Hive DDL Rather Than the DataSource V2 Syntax

Hive DDL (`PARTITIONED BY` and `STORED AS PARQUET`) stores the SerDe and file-format metadata used by Trino's Hive connector, whereas Spark's `USING parquet` syntax has different partition declaration rules. Any table format choice requires verifying both read and write compatibility across Spark, dbt, and Trino.

### 7.4 One Postgres Instance, Four Databases

Separate databases keep each application's tables in a separate namespace while sharing one process and buffer pool. They still share memory, connections, storage, and failure exposure. This configuration reduces local service overhead but does not provide process or resource isolation.

## 8. Operating Constraints

These operating notes document service behaviors that constrain pipeline and storage configuration.

### 8.1 A Path Write Is Not a Table Write

A Parquet path write can create partition directories without registering them in Hive Metastore, causing a query engine that reads registered partitions to return no rows. Register partitions through a catalog write, `MSCK REPAIR TABLE`, or a transactional table format (Delta Lake or Iceberg).

### 8.2 A HiveServer2 Takes the Whole Worker

A long-lived Thrift server can occupy all worker cores and leave a batch application waiting with `Initial job has not accepted any resources`. Setting `spark.executor.cores` limits each executor, not the total application's cores (`spark.cores.max`). Inspect application resource requests on the Spark master UI (`http://localhost:8082/json/`).

The project separates the Thrift server into the `dbt` profile so batch jobs can run without contention. dbt-spark can connect through the Thrift endpoint or run in session mode.

### 8.3 The Object Store Runs Out of Volumes

Each SeaweedFS bucket is a separate collection with its own volumes. With default settings, a new collection can allocate up to 7 volumes at once, each with a 30 GiB size limit (`-master.volumeSizeLimitMB`, default 30720), while `-volume.max=0` derives the total volume limit from remaining free disk space divided by that size limit. Set the volume growth count to 1 and use a smaller volume size limit so multiple buckets (`staging`, `lakehouse`, `platform`) do not exhaust volume slots ([SeaweedFS guidance](https://github.com/seaweedfs/seaweedfs/wiki/Optimization-for-Many-Small-Buckets)).

### 8.4 PySpark Launcher Arguments

Leave `PYSPARK_SUBMIT_ARGS` unset in the Spark container image unless explicitly needed; PySpark's launcher uses it, and an unintended value can override submission resource flags.

### 8.5 Relative Bind Mounts in Docker Compose

Relative bind paths resolve against the base Compose file directory (`infra/`). A repository sibling is therefore `../data`, not `../../data`. Short bind syntax creates a missing host directory and mounts an empty path if the relative depth is off by one.

### 8.6 `spark-class` Does Not Read `spark-defaults.conf`

The cluster configuration uses `start-master.sh`, `start-slave.sh`, and `start-thriftserver.sh`. Master and worker daemons use daemon settings and command arguments, whereas a Spark application reads `spark-defaults.conf` during `spark-submit`.

### 8.7 S3 Prefix Markers for Spark Event Logs

S3 stores object keys rather than directories. Bootstrap creates `_keep` marker objects under configured output prefixes because Spark's event-log writer checks for a directory-like prefix at startup.
