# Engineering Workflow, Repository Structure, and Documentation System

This directory (`workflow/`) holds the engineering workflow, working-state tracker, authoring guides, and verification tools for the repository. It is designed as a self-contained, reusable system that separates **product documentation** (`docs/`), **platform code** (`data/`, `ml/`, `agents/`, `infra/`), **engineering process and tooling** (`workflow/`), and **gitignored local scratch space** (`local/`).

---

## 1. Documentation and Workspace Separation

Repositories that combine multi-layer engineering, iterative design, and coding agents degrade when three different kinds of artifacts are mixed together:
1. **Product documentation** (what the system is, how it works, why its architecture was chosen, and evidence that it works).
2. **Workflow and meta state** (what is currently open or undecided, what is next to do, how documents and diagrams are authored, and scripts that lint or render them).
3. **Local scratch and reference dumps** (temporary outputs, old draft backups, external reference checkouts, and local sample datasets).

### Three-Space Separation

| Space | Git Status | Audience | Contents |
| --- | --- | --- | --- |
| **`docs/`** | Tracked | Engineers, reviewers, and users of the system ([Root README](../README.md)) | **100% product documentation**: [Data Specification](../docs/Data-Specification.md), [Data Platform](../docs/Data-Platform.md), [ML Platform](../docs/ML-Platform.md), [Agent Platform](../docs/Agent-Platform.md), [Infrastructure](../docs/IaC.md), and their diagrams. |
| **`workflow/`** | Tracked | Coding agents and developers ([AGENTS.md](../AGENTS.md)) | **Reusable workflow, working state, and tools**: [Status](Status.md), [Documentation Guide](Documentation-Guide.md), [Design Method](Design-Method.md), [Diagram Guide](Diagram-Guide.md), [Diagram Examples](Diagram-Examples.md), working [records](records/Polars-POC.md), and [tools](tools/). |
| **`local/`** | Ignored (`.gitignore`) | Local machine only | **Throwaway and external local files**: `tmp/`, `archive/`, `course/`, `tpcdi/`, `generator-feasibility/`, and `sample_run/`. Never appears on GitHub or in `README.md`. |

### Key Documentation Rules
- **Separate OLTP workflow state from permanent system definitions**: Working state (open decisions, proposals, known prototype defects, and next steps) changes every session and lives exclusively in [Status](Status.md). Public system documents in `docs/` describe the system in the present tense with zero `(open)` markers or `Status.md` links.
- **Keep design rationale inline as permanent engineering documentation**: Unlike ephemeral todo state, **why** an architectural choice was made (constraints, candidates considered, selection criteria, and trade-offs) is permanent engineering knowledge. It lives in a concise **Design Decisions and Rationale** section inside each `docs/` file rather than in a separate multi-thousand-line catch-all file.
- **Map `README.md` 1-to-1 to `docs/`**: `README.md` summarizes the system across five main sections, and each section links to one dedicated document in `docs/`.

---

## 2. Repository Code Structure (Four-Pillar Monorepo)

The codebase is organized by **platform pillar** (`data/`, `ml/`, `agents/`, `infra/`), aligning code directories 1-to-1 with the system architecture and `docs/`:

| Pillar | Matching Docs | Contents | Orchestrator & Catalog |
| --- | --- | --- | --- |
| **[`data/`](../data/README.md)** | [Data Specification](../docs/Data-Specification.md), [Data Platform](../docs/Data-Platform.md) | `generator/`, `spark/`, `dbt/`, `flink/`, `airflow/`, `soda/`, `datahub/` | **Airflow** + **Soda** + **DataHub** (orchestrates and governs all structured and RAG data pipelines) |
| **[`ml/`](../ml/README.md)** | [ML Platform](../docs/ML-Platform.md) | `feature_repo/`, `training/`, `kfp/`, `serving/`, `rag/` | **Kubeflow Pipelines** + **MLflow** + **KServe** (feature views, training, registry, and online scoring) |
| **[`agents/`](../agents/README.md)** | [Agent Platform](../docs/Agent-Platform.md) | `mcp/`, `coordinator/`, `gateway/`, `prompts/`, `registry/` | **Coordinator Agent** + **Gateway** + **Agent Registry** (LLM serving, MCP tools, and multi-agent routing) |
| **[`infra/`](../docs/IaC.md)** | [Infrastructure](../docs/IaC.md) | Compose profiles, Dockerfiles, K8s, Terraform, Ansible, Jenkins, Security, Observability | Shared service configs, cloud provisioning, CI/CD pipelines, NGINX Ingress, Vault, Prometheus, Grafana, and Jaeger |

### Pragmatic Monorepo Boundaries and Evolution Rules
1. **Thin DAGs in `data/airflow/dags/`**: Airflow schedules data pipelines across the platform (`DP1-DP3`, feature sync, and the `RAG Data Pipeline`). DAG files stay 20-line trigger and validation wrappers; domain and ML implementation code stays in `data/spark/`, `data/dbt/`, `ml/rag/`, and `ml/feature_repo/`.
2. **Co-located tests per component**: Because `data/generator` (Polars), `data/spark` (PySpark), `ml/` (Feast and XGBoost), and `agents/mcp` (FastAPI) have distinct dependency trees, unit, property (`hypothesis`), and mutation (`mutmut`) tests live inside each component (`data/generator/tests/`, `ml/serving/tests/`, `agents/mcp/tests/`), while stack-wide smoke checks live in `infra/`.
3. **One-way shared code extraction (`packages/`) when needed**: Pillars never import directly from each other's internal folders; runtime communication crosses storage contracts (Gold tables, Feast views, Kafka topics) or network APIs (HTTP, gRPC, MCP). If multiple pillars later share Python schemas or SDKs, extract them into a top-level `packages/` workspace imported one-way by `data/`, `ml/`, and `agents/`.
4. **Vertical domain slicing as use cases multiply**: When a pillar grows from one use case (cash forecast) to several (margin risk, anomaly detection), pivot that pillar's subfolders from horizontal tool groupings to vertical use-case slices (`ml/<use_case>/`).

---

## 3. Unified Naming Conventions

| Scope | Rule | Pattern & Examples | Reason |
| --- | --- | --- | --- |
| **Directories & Python modules** | `snake_case` for Python packages; `kebab-case` for containers and Kubernetes manifests | `data/generator/`, `ml/feature_repo/`, `infra/k8s/` | Python imports require valid identifiers; Docker and Kubernetes use `kebab-case`. |
| **Markdown documents** | `Title-Case-With-Hyphens.md` (`README.md` for folder index) | `Data-Specification.md`, `Data-Platform.md`, `ML-Platform.md` | Distinguishes documentation from code and configuration files. |
| **S3 Buckets** | Lowercase single-word role nouns | `staging`, `lakehouse`, `platform` | Separates immutable raw files (`staging`), lakehouse tables (`lakehouse`), and operational logs (`platform`). |
| **Medallion Schemas & Tables** | `<zone>.<prefix>_<entity>` (`snake_case`) | - Bronze: `bronze.raw_<source>` (`raw_trade`, `raw_customer_mgmt`, `raw_cash_transaction`)<br>- Silver: `silver.stg_<entity>` (`stg_trade`, `stg_customer`, `stg_cash_transaction`)<br>- Gold Dimensions: `gold.dim_<entity>` (`dim_customer`, `dim_account`, `dim_security`)<br>- Gold Facts: `gold.fact_<event>` (`fact_cash_balances`, `fact_holdings`, `fact_market_history`)<br>- Gold Current and Reference: `gold.<table_name>` (`prospect`, `financial`, `industry`, `tax_rate`, `di_messages`)<br>- Gold Features: `gold.feat_<entity>_<window>` (`feat_account_cash_daily`) | Matches coursework rubric row `r38` (`raw_`, `stg_`, `dim_`, `fact_`, `feat_`), avoids mixed-case SQL quoting across Spark, dbt, Trino, and PostgreSQL, and makes every table's zone and role self-identifying in DataHub. |
| **Pipeline Metadata Columns** | Leading underscore `_snake_case` | `_data_date`, `_source_file`, `_record_num`, `_ingested_at`, `_batch_id` | Separates platform provenance columns from TPC-DI business columns so they never collide. |
| **Feast Feature Columns** | Standard Feast timestamp names | `event_timestamp` (business time), `created` (availability time) | Required by rubric row `r36` and Feast point-in-time joins. |
| **Kafka Topics** | `<domain>.<entity>.<stage>` | `tpcdi.cash_transaction.raw`, `tpcdi.account_cash.features` | Distinguishes raw event streams from derived streaming feature topics. |
| **Airflow DAG IDs** | `<layer_or_pipe>_<action>` | `dp1_staging_to_bronze`, `dp2_bronze_to_gold`, `dp3_gold_to_features`, `ml_feast_materialize`, `ml_rag_ingest` | Groups DAGs cleanly by pipeline (`dp1..dp3`, `ml_*`) in Airflow and DataHub lineage. |
| **Containers & Network** | `edai-<service>` container name; short service key on `edai-net` | Service `trino` -> container `edai-trino` | Short DNS names between containers; unambiguous container names on the host. |

---

## 4. Contents of `workflow/` and `local/`

### Tracked Workflow Files (`workflow/`)

| Path | Purpose |
| --- | --- |
| [Status.md](Status.md) | Working state: current position, accepted decisions, open decisions, next steps, what exists, and known defects. |
| [Documentation-Guide.md](Documentation-Guide.md) | Blueprint for `README.md` and `docs/`, inline decision-record format, writing style, and evidence rules. |
| [Design-Method.md](Design-Method.md) | Six-step engineering decision method (frame, map, research, generate options, evaluate, record). |
| [Diagram-Guide.md](Diagram-Guide.md) | Visual language and workflow for editable draw.io diagrams and Mermaid ERDs. |
| [Diagram-Examples.md](Diagram-Examples.md) | Worked visual examples rendered from `workflow/diagrams/style-examples/`. |
| [records/](records/Polars-POC.md) | Intermediate benchmark and POC records during development. Merged into `docs/` once production code is built. |
| `tools/check_docs.py` | Checks all tracked and new Markdown files for broken links, heading anchors, line endings, and banned words. |
| [tools/diagrams/](tools/diagrams/README.md) | Shared draw.io and Mermaid ERD validation and PNG export tool (`diagram.py`, `erd.py`, `theme.json`). |

### Ignored Local Workspace (`local/`)

| Path | Purpose |
| --- | --- |
| `local/tmp/` | One-off scripts, logs, diagram export previews, and intermediate outputs. |
| `local/archive/` | Historical session logs, handoffs, earlier v0 prototype code (`prototype-v0/`), and reference notes. |
| `local/course/` | Local coursework rubric (`rubric.xlsx`), proposal, and domain examples. |
| `local/tpcdi/` | Upstream TPC-DI checkout and SF3 reference dataset. |
| `local/generator-feasibility/` | Pinned Databricks TPC-DI generator reference checkout. |
| `local/sample_run/` | Local generated dataset. |

---

## 5. Design Decisions and Rationale for This System

This section records the engineering criteria, decisions, and rejected alternatives behind the repository, documentation, and workflow architecture established for the project.

### Criteria
- **Zero state pollution**: Ephemeral session backlog state must never appear inside permanent system documentation.
- **1-to-1 navigability**: A reader moving from `README.md` to `docs/` to the codebase should see the same architectural boundaries at every level.
- **Tool compatibility**: Every tool (`dbt`, `Airflow`, `Feast`, `PySpark`, `FastAPI`, `pytest`) must have a valid, self-contained project root without `sys.path` or multi-mount hacks.
- **Clean dependency direction**: Subsystems must communicate through explicit data or storage contracts or network APIs rather than reaching into each other's internal folders.
- **Portability**: The workflow, authoring guides, and verification tools must be reusable in future projects without dragging project-specific code or local scratch files along.

### Selected Architecture Decisions

| Decision | Selected Choice | Main Reason |
| --- | --- | --- |
| **Workspace separation** | Three top-level spaces: `docs/` (product docs), `workflow/` (process, status, guides, tools), and `local/` (gitignored scratch, archives, reference data) | Eliminates the `misc/` junk drawer, keeps `docs/` 100% about the brokerage platform, and makes `workflow/` + `AGENTS.md` portable to future repositories. |
| **Workflow state vs. design rationale** | Put ephemeral state (`open`, `proposed`, `deferred`, defects, next step) only in `workflow/Status.md`; keep permanent **Design Decisions & Rationale** inline inside each `docs/` file | Prevents `README.md` and `docs/` from reading like an unfinished todo list, while avoiding a separate 2,000-line `Project-Design.md` catch-all file. |
| **Public documentation structure** | `README.md` + 5 cohesive engineering docs in `docs/` (`Data-Specification`, `Data-Platform`, `ML-Platform`, `Agent-Platform`, `IaC`) | Maps 1-to-1 to `README.md`'s five sections and frames the project as one unified engineering system rather than 26 fragmented rubric-row files. |
| **Codebase root layout (Variant A2)** | Four platform pillars: `data/`, `ml/`, `agents/`, `infra/` | Matches the platform layers and `docs/` 1-to-1 while keeping each tool (`data/dbt`, `ml/feature_repo`, `agents/mcp`) in a self-contained subfolder. |
| **Orchestrator & catalog placement** | Keep unified `airflow/`, `soda/`, and `datahub/` inside `data/` with thin DAG wrappers; `ml/` uses `kfp/` + `MLflow`; `agents/` uses `coordinator/` + `gateway/` + `registry/` | Airflow, Soda, and DataHub only orchestrate and govern data pipelines (`DP1-DP3`, feature sync, and the `RAG Data Pipeline`) and share one `common.py` helper and DAG bag. |
| **Test organization** | Co-locate unit, property (`hypothesis`), and mutation (`mutmut`) tests inside each component (with stack smoke checks in `infra/`), rather than a single root `tests/` folder | `data/generator` (Polars), `data/spark` (PySpark), `ml/` (XGBoost and Feast), and `agents/mcp` (FastAPI) have different Python dependencies and CI pipelines. |
| **Direct CLI over `Makefile` wrappers** | Run `docker compose --project-directory infra ...`, `pytest`, and Python modules directly without a root `Makefile` | Keeps the real underlying Docker, Compose, Python, and tool flags visible so the developer learns and controls the exact commands. |
| **Schema & table naming** | Enforce `snake_case` with zone and role prefixes (`bronze.raw_*`, `silver.stg_*`, `gold.dim_*`, `gold.fact_*`, `gold.feat_*`) and `_snake_case` metadata columns | Satisfies rubric row `r38`, avoids mixed-case SQL quoting across Spark, dbt, Trino, and PostgreSQL, and prevents provenance columns from colliding with TPC-DI fields. |

<details>
<summary>Alternatives Considered</summary>

| Area | Alternative | Why Not Selected |
| --- | --- | --- |
| **Workspace separation** | Keep `Status.md`, `Documentation-Guide.md`, and `Diagram-Guide.md` at the root of `docs/` | Half of `docs/` becomes internal process and agent meta files rather than system documentation. |
| **Workspace separation** | Keep `misc/` for both tracked tools (`misc/tools/`) and ignored dumps (`misc/tmp/`, `misc/archive/`) | Forces a "miscellaneous" folder into Git and `README.md`'s Repository Structure tree. |
| **Public docs structure** | Keep a separate `docs/Project-Design.md` for all decisions, constraints, and requirements across all layers | Grows into a multi-thousand-line ledger and splits every layer's design across `README.md`, `Data-Specification.md`, `Project-Design.md`, and the layer doc. |
| **Public docs structure** | Split `docs/` into 25+ small files mapped 1-to-1 to rubric rows (as in `card-fraud-platform`) | Makes the repository read like a homework checklist rather than a cohesive engineering system. |
| **Codebase root layout** | Tool-root folders at repo root (`generator/`, `pipelines/`, `dbt/`, `orchestration/`, `governance/`, ...) | Explodes into 15+ flat root folders once Layer 2 ML and Layer 3 Agent components are added. |
| **Codebase root layout** | Group all of Layer 1 under `pipelines/` | Misnames warehouse SQL modeling (`dbt`), metadata catalog (`datahub`), and quality contracts (`soda`) as "pipelines". |
| **Codebase root layout** | Workload-type monorepo (`services/`, `jobs/`, `libs/`, `infra/`) | Mixes Layer 1, Layer 2, and Layer 3 code inside `jobs/` and `services/`, hiding the 3-layer platform architecture. |
| **Airflow & DataHub placement** | Split DAGs into `data/dags/` and `ml/dags/` | Breaks shared Python imports (`common.py` for Soda validation and DataHub assertion and lineage publishing) and requires multiple DAG-bag mounts. |
| **Airflow & DataHub placement** | Add a 5th root folder `platform/` for `airflow/`, `soda/`, and `datahub/` | Adds an extra root folder for tools that only ever orchestrate and govern data pipelines (since ML training runs on Kubeflow Pipelines). |
| **Table naming** | Keep TPC-DI mixed-case names (`DimCustomer`, `FactCashBalances`, `SK_CustomerID`) in Gold | Requires quoting identifiers in PostgreSQL, Trino, and dbt and does not follow the rubric `r38` prefix convention (`raw_`, `stg_`, `dim_`, `fact_`, `feat_`). |

</details>

---

## 6. Verification Commands and Reuse

Run the documentation, diagram, and generator checks from the repository root in WSL:

```bash
.venv/bin/python workflow/tools/check_docs.py
.venv/bin/python -m pytest data/generator/tests -q --basetemp=local/tmp/pytest-generator
.venv/bin/python -m unittest discover -s workflow/tools/diagrams -p 'test_*.py' -v
```

To adopt this workflow and repository system in a new project:
1. Copy `AGENTS.md` and `workflow/` (excluding project-specific `workflow/Status.md` content and `workflow/records/`).
2. Add `/local/`, `/workflow/tools/diagrams/drawio/`, and `/workflow/tools/diagrams/node_modules/` to `.gitignore`.
3. Create the top-level pillar directories (`data/`, `ml/`, `agents/`, `infra/`, `docs/`) and update the section map in `workflow/Documentation-Guide.md`.
