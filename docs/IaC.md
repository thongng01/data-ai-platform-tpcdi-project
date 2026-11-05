# Infrastructure as Code

This reference defines the target infrastructure, resource naming, storage boundary, and operating constraints. The checked-in Compose files predate parts of this design; the commands below inspect that configuration or run the existing generator proof of concept (POC). They do not deploy the target stack. [Data Platform](Data-Platform.md) owns the pipeline design.

## 1. How to Run It

Run project commands from the repository root in WSL, where the project filesystem and Docker socket are available. Passing `--project-directory infra` tells Docker Compose to read `infra/.env` and resolve relative paths against `infra/`. The target build starts with the version compatibility test, then updates Compose, then builds the generator and publisher from a small workload. Build the clean publisher profile before the realistic profile.

| Command | Effect and Prerequisites |
|---|---|
| `docker compose --project-directory infra config` | Prints the checked-in Compose configuration across active profiles, not the target design in this document. |
| `.venv/bin/python -m data.generator run --config data/generator/configs/small.yaml --output local/sample_run` | Runs the existing generator POC and its validation in `local/sample_run`. It does not implement the target day-by-day simulation or publisher. |
| `curl -fsS http://localhost:8082/json/` | Reads the Spark master's workers and applications when that service is running. |
| `docker compose --project-directory infra stop` | Stops running containers and retains data. |
| `docker compose --project-directory infra down --remove-orphans` | Removes containers and retains named volumes. Adding `--volumes` removes those volumes and their data. |

## 2. Development Environment

**Runs on:** Ubuntu 24.04 on WSL 2 (Windows 11), Intel Core i7-12700H (20 threads), 24 GiB RAM.

Measurements in these documents come from this machine unless a section states otherwise. Installed versions can change. `C:\Users\XWIND-PC\.wslconfig` caps WSL memory at 24 GiB with 8 GiB of swap. It sets no processor cap. The machine has no NVIDIA GPU and lacks the AVX-512 and AMX CPU instructions used by llm-d's reference setups.

Container image settings are in `infra/.env`. Compose reads that file to substitute variables; a container receives only the values its service passes through. The file contains committed local development defaults, not production credentials. SeaweedFS currently uses the mutable `latest` tag, so a later pull can change behavior without a configuration edit.

## 3. The Compose Files

The five Compose files group local Layer 1 services by role. Profiles select which groups run together because all services cannot fit within the WSL memory cap.

| File | Target Services | Profiles |
|---|---|---|
| `compose.yaml` | PostgreSQL, SeaweedFS, Hive Metastore, Trino, bootstrap, and tools | Core services; `init` and `tools` runners |
| `compose.compute.yaml` | Spark master, worker, and submit runner | `compute`; no Spark Thrift service or `dbt` profile |
| `compose.streaming.yaml` | Kafka, topic initialization, Kafbat UI, Flink job manager, task manager, and submit runner | `stream` |
| `compose.orchestration.yaml` | Airflow 3 initialization, API and scheduling components with `LocalExecutor` | `airflow`; no separate executor worker |
| `compose.governance.yaml` | OpenSearch, DataHub upgrade, GMS, frontend, and ingestion runner | `governance` |

dbt-postgres and GX Core run from the Airflow image; they are not separate services. DataHub also uses Kafka.

The project identity is `brokerage`. Use that identity for resources that need a project scope, such as local image repositories. Service keys use bare role names, such as `postgres`, `trino`, and `kafka`. Compose generates container, network, and volume names from its project and role keys; the target does not set `container_name`. Internal clients use service names rather than generated container names. Compose scope prevents container-name collisions between deployments without changing service DNS. `brokerage` describes the domain; a longer `brokerage-platform` prefix adds little, and a fictional firm name is not needed. Buckets keep their role names: `upstream`, `staging`, `lakehouse`, and `platform`. Kafka topics use `brokerage.*`. TPC-DI remains the source specification's proper name, and actual source filenames and field names do not change.

Service callers must wait for their dependencies to be ready. Governance also needs Kafka; selecting only a governance profile does not by itself make Kafka available. Image builds use multiple stages where this keeps build tools out of runtime images, and image size comparisons supply the course evidence.

Terraform provisions cloud resources, and Ansible configures services by role. CI/CD tests, builds, and deploys the RAG pipeline, MCP servers and data agent, coordinator agent, and streaming feature jobs. System prompts remain versioned outside agent code. These requirements do not select a host or cluster for Layers 2 and 3.

## 4. Services

### 4.1 Storage

One PostgreSQL instance separates the `warehouse`, `metastore`, `airflow`, and `datahub` databases. The warehouse contains `load`, `gold`, and `ops` schemas. Spark sends the Silver rows needed by Gold into `load`, and dbt-postgres builds Gold. `ops` holds load status and stream progress. Gold exists once, in PostgreSQL.

SeaweedFS provides the S3 API. The publisher has an identity that writes `upstream`; the platform identity can only read `upstream` and writes its own storage. This enforces the source department boundary rather than allowing the publisher to alter platform copies.

| Bucket | Contents | Writer |
|---|---|---|
| `upstream` | Source deliveries under `initial/` and `daily/YYYYMMDD/` | Publisher |
| `staging` | Immutable copy of each pulled file version | DP1 |
| `lakehouse` | Bronze and Silver Delta tables under `bronze/` and `silver/` | Spark and Flink, each on its assigned tables |
| `platform` | Spark event logs, Flink checkpoints, and MLflow artifacts | Platform services |

Source files use `initial/<Source>` and `daily/YYYYMMDD/<Source>_YYYYMMDD.ext`; FINWIRE keeps its quarter filenames. Staging uses `initial/<version_id>/<source_filename>` and `daily/YYYYMMDD/<version_id>/<source_filename>`. DP1 allocates a version ID per pulled file. A retry uses the same ID and path; a corrected source file gets a new ID. Reruns and backfills reuse the recorded inputs. An ingestion record maps the source key and version information to the staging path. An ETag is source metadata, not a guaranteed content hash.

Hive Metastore resolves table names, schemas, and locations for Spark and Trino. Trino uses a Delta Lake catalog for the lakehouse and a PostgreSQL catalog for warehouse queries. DataHub collects metadata and lineage; it does not replace the query-time catalog or each system's access controls.

### 4.2 Compute

Start with Spark 3.5 and Delta 3.3, plus Flink 2.2 and the Delta 4.4 Flink connector. The first build gate is an end-to-end compatibility test: Flink writes a Delta table on SeaweedFS, then Spark and Trino read it. Spark 4 with Delta 4 is the fallback if the starting combination fails. These versions are a starting choice that the test must confirm, not a claim of compatibility already proved.

The Spark image includes Delta, the PostgreSQL JDBC driver, S3 access dependencies, and DataHub's Spark agent. The standalone master schedules work on the worker. The master UI shows applications and resource allocation; stage and task evidence comes from each application's UI.

Gold uses dbt-postgres rather than dbt-spark, so the target has no HiveServer2 Thrift service. dbt runs SQL against PostgreSQL after Spark moves the required Silver rows into its load schema.

### 4.3 Streaming

Kafka runs as a local combined KRaft broker and controller. Container clients use `kafka:29092`; host clients use `localhost:9092`. If the advertised address cannot be resolved, a client can reach the first broker address but fail later requests. Kafbat UI shows topics, messages, and consumer lag.

| Topic | Contents | Retention |
|---|---|---|
| `brokerage.cash_transaction.raw` | All cash deliveries, including duplicates; source-owned | Whole active dataset |
| `brokerage.cash_transaction.clean` | Deduplicated events with `cash_type` and `trade_id` | Whole active dataset |
| `brokerage.account_cash.features` | Cash feature rows for the Feast push job | 7 days |
| `brokerage.account.rescore` | Requests for the scoring worker | 7 days |

Each topic has four partitions and uses account ID as its key. JSON messages use a repository schema contract, decimal text for amounts, and the delivery time as the Kafka timestamp. The platform holds one active dataset. Switching datasets or publisher profiles clears the buckets, topics, and Flink state; it is not a routine rerun.

Flink 2.2 uses the Python DataStream API with Kafka, JDBC, and Delta connectors. Two jobs run at parallelism 2, using four TaskManager slots. Job 1 writes raw deliveries to Bronze, deduplicates `CT_ID` within 24 hours per account, and writes Silver and the clean topic. Job 2 reads clean events and writes live totals, event-time windows, features, and rescoring requests. The [Data Platform](Data-Platform.md) specifies their processing rules.

Checkpoints run every 30 seconds, and jobs restart from the last checkpoint. Kafka transactions protect clean-topic writes. End-to-end guarantees also depend on connector behavior and checkpoint recovery; setting `EXACTLY_ONCE` alone does not establish them. PostgreSQL live-table upserts write full totals, not increments, so replayed writes stay correct. Checkpoints use `platform`, and the raw and clean topics support replay.

### 4.4 Orchestration

Airflow 3 uses `LocalExecutor`. Its image includes GX Core, dbt-postgres, and DataHub's Airflow plugin. DP1 pulls files into staging and Bronze. DP2 builds Silver, loads PostgreSQL, and runs dbt. DP3 builds non-cash features. Each pipeline has ingest and GX validate stages that report to DataHub.

Process dates in order, with one active run per pipeline at first. Independent tables can proceed within a date when their own inputs and dependencies are complete. A late file holds only the tables that need it. `ops.load_status` and a completeness view identify missing inputs; dashboards label partial dates. A correction rebuilds the affected dates and every later processed date in order, not all Gold history.

### 4.5 Governance

DataHub catalogs datasets, lineage, and GX quality results. Integration paths are its Airflow plugin, Spark agent, dbt ingestion, GX plugin, and lineage declared when Flink jobs are deployed. OpenSearch supports the local DataHub services. A service definition is not proof that a recipe, lineage event, or quality result exists.

The security baseline uses Vault for secrets, separate database users per service, marked personal-data columns, and parameterized SQL in the Web API and MCP tools. Gateways enforce login and rate limits, and the Web API uses HTTPS. Ingress hides internal service endpoints and applies access controls. Broker-only row access, personal-data masking for analysts, and full audit logs are not part of this baseline.

## 5. Ports

These are the checked-in local defaults in `infra/.env` for roles retained in the target. They are not a published API contract or proof that a service is running. Other stacks on the host can use the same ports.

| Host port | Service endpoint |
|---|---|
| 5432 | PostgreSQL |
| 8333 | SeaweedFS S3 API |
| 8080 | SeaweedFS volume server |
| 8888 | SeaweedFS filer |
| 9333 | SeaweedFS master |
| 9083 | Hive Metastore Thrift |
| 8081 | Trino coordinator |
| 7077 | Spark cluster manager |
| 8082 | Spark master UI |
| 8083 | Spark worker UI |
| 9092 | Kafka host listener |
| 8084 | Flink job manager UI |
| 8085 | Airflow web endpoint |
| 9002 | DataHub frontend |
| 8086 | DataHub GMS API |

## 6. Memory Budget

The current Compose limits provide the planning baseline below. They are ceilings, not measured use or a guarantee that workloads fit together. JVM heap is only part of process memory. Target changes remove Spark Thrift and the conflicting Airflow worker; these rows do not assign budgets to new services.

| Service | Current limit | Related setting |
|---|---|---|
| PostgreSQL | 1536M | Shared buffers are 512 MB |
| SeaweedFS | 1536M | One process provides four roles |
| Hive Metastore | 1280M | Maximum heap is 768 MB |
| Trino | 2560M | Maximum heap is 1 GB |
| Spark master | 1024M | Schedules applications |
| Spark worker | 5G | Advertises six cores and 5 GB |
| Kafka | 1536M | Maximum heap is 768 MB |
| Flink job manager | 1280M | Configured process memory is 900 MB |
| Flink task manager | 2048M | Configured process memory is 1600 MB |
| Airflow init, scheduler, webserver | 1G each | Init is a one-shot process |
| OpenSearch | 2048M | Maximum heap is 1024 MB |
| DataHub upgrade, frontend | 1G each | Upgrade is a one-shot process |
| DataHub GMS | 1536M | Kafka and OpenSearch add separate costs |

The current long-running Layer 1 definitions total about 26 GiB before target removals and exclude the conflicting Airflow worker. Run services in groups and leave space for Linux, Docker, drivers, and one-shot tasks. Current Spark defaults give each task only about 180 MiB of execution memory; larger experiments need a larger worker budget, about 10 to 12 cores and 8 to 10 GiB, rather than all service groups running together.

At factor 100, the estimated peak disk demand is about 400 to 500 GB if every copy is present: generated files, upstream and staging files, raw and clean topics, Bronze and Silver, Gold, and Spark spill. Removing generated files after publication removes one copy. These are capacity estimates, not measured platform storage use. [Data Platform](Data-Platform.md) defines the 100 GB offline generation requirement and how workloads scale by environment.

## 7. Why These Choices

### 7.1 Five Compose Files Rather Than One

Files group services by role. Profiles control which optional services start. Splitting files alone does not control memory because Compose loads all files named in `COMPOSE_FILE`.

### 7.2 Explicit Table Locations and Retained Inputs

Delta tables use explicit lakehouse locations. Staging retains pulled file versions, Bronze retains raw deliveries, Delta keeps table history, and Gold dimensions keep SCD2 history. These serve different questions: the original file explains what arrived, table history shows a past lakehouse state, and SCD2 records business versions. Reruns reuse recorded staging inputs rather than pulling another copy.

### 7.3 Delta Compatibility Before Pipeline Work

Spark, Flink, and Trino share Delta tables on SeaweedFS. A successful write by one engine does not prove that another engine can read the table or its enabled features. The compatibility test comes first because a connector mismatch would otherwise affect every pipeline built on that format. Plain Parquet and the earlier Hive SerDe DDL are not the target table contract.

### 7.4 One Postgres Instance, Four Databases

Separate databases keep each application's tables in a separate namespace while sharing one process and buffer pool. They still share memory, connections, storage, and failure exposure. This configuration reduces local service overhead but does not provide process or resource isolation.

## 8. Operating Constraints

Monitoring covers service resources, Web API behavior, LLM token counts and latency, time to first token, personal-data filter blocks, and agent and MCP tool calls and failures. Observability dashboards cover metrics, logs, and traces; tool UIs remain useful for detailed diagnosis. Stream checks include Kafka lag, duplicate counts, late events, and quarantine counts. DataHub and GX report data health.

Verification includes mocked Web API tests with more than 90% coverage, equivalence partitioning and boundary value analysis, pull-request differential mutation tests above 80%, property-based idempotency tests, and load-test reports against the Web API service-level targets. A/B rollout compares agent configurations and LLM versions. These are verification requirements, not completed test results.

### 8.1 A Path Write Is Not a Table Write

Writing objects under a path does not by itself create a catalog table. Register each Delta table and verify it through the engine that consumes it. Replacing files outside the Delta transaction log can make the log and data disagree. Spark and Flink must not write the same Delta table at the same time on SeaweedFS.

### 8.2 Long-Lived Applications Consume Worker Capacity

A long-lived Spark application can occupy the worker's cores and leave a batch application waiting with `Initial job has not accepted any resources`. `spark.executor.cores` limits each executor, not total application cores; `spark.cores.max` limits the application. Inspect resource requests on the master UI. The target removes Spark Thrift because Gold uses dbt-postgres.

### 8.3 The Object Store Runs Out of Volumes

Each SeaweedFS bucket is a separate collection with its own volumes. With default settings, a new collection can allocate up to 7 volumes at once, each with a 30 GiB size limit (`-master.volumeSizeLimitMB`, default 30720). `-volume.max=0` derives the volume limit from free disk space divided by that size. A growth count of 1 and a smaller volume size limit let the four buckets share available slots ([SeaweedFS guidance](https://github.com/seaweedfs/seaweedfs/wiki/Optimization-for-Many-Small-Buckets)). Volume allocation failures can appear as S3 retries.

### 8.4 PySpark Launcher Arguments

Leave `PYSPARK_SUBMIT_ARGS` unset in the Spark container image unless needed. PySpark's launcher uses it, and an unintended value can override submission resource flags.

### 8.5 Relative Bind Mounts in Docker Compose

Relative bind paths resolve against the base Compose file directory (`infra/`). A repository sibling is therefore `../data`, not `../../data`. Short bind syntax creates a missing host directory and mounts an empty path if the relative depth is wrong.

### 8.6 `spark-class` Does Not Read `spark-defaults.conf`

Master and worker daemons use daemon settings and command arguments. Spark applications read `spark-defaults.conf` during submission. A setting in that file cannot fix a daemon resource limit by itself.

### 8.7 S3 Prefix Markers for Spark Event Logs

S3 stores object keys rather than directories. Spark's event-log writer checks for a directory-like prefix at startup. Bootstrap creates `_keep` marker objects under the configured event-log prefixes to satisfy that check.
