# Documentation Guide

The [root README](../README.md) introduces the whole system. Five engineering documents in `docs/` hold the detailed specifications, architecture, rationale, and verification evidence for each major part of the platform. Working state and authoring guides live in [`workflow/`](README.md) and stay separate from the public system documentation. To resume work, start with [Status](Status.md).

---

## 1. Core Principles

- **System-first framing**: `README.md` and the five documents in `docs/` describe the platform as a coherent engineering system in the present tense.
- **No ephemeral workflow state in public docs**: Open questions, `(open)` or `(proposed)` markers, todo lists, known defects in earlier prototypes, and links to `Status.md` belong only in [Status](Status.md), never in `README.md` or `docs/`.
- **1-to-1 mapping from `README.md` to `docs/`**: Each of the five main body sections of `README.md` (`Business Domain and Data`, `Layer 1: Data Platform`, `Layer 2: Machine Learning Platform`, `Layer 3: Agent Platform`, and `Infrastructure and Operations`) maps to one dedicated document in `docs/`.
- **Inline design rationale instead of a monster catch-all**: Design choices, candidate comparisons, and engineering constraints live inside the document for that layer or subsystem, not in a separate catch-all design ledger. We work through the full exploration and trade-off analysis in conversation using the [design method](Design-Method.md), then record the distilled criteria, candidate comparison, and decision in the owning document.
- **Buildable, right-sized scope**: Define domain, data, and layer contracts at a direct, concrete level (~60 to 100 lines per layer design before implementation evidence), avoiding speculative mechanisms that neither the system nor the coursework rubric requires.

---

## 2. Document Map

### Public System Documentation (`docs/`, Linked from `README.md`)

| `README.md` Section | Detailed Document | Purpose |
| --- | --- | --- |
| *(Whole-system entry point)* | [Root README](../README.md) | System overview, domain summary, numbered 3-layer architecture and deployment diagrams, repository structure, quick start, and links to the five detail documents. |
| `Business Domain and Data` | [Data Specification](../docs/Data-Specification.md) | TPC-DI business domains, load cycle and delivery rules, Medallion schema design (`raw_`, `stg_`, `dim_`, `fact_`, `feat_`), Bronze, Silver, and Gold ERDs, core integration rules, and collapsed field schemas. |
| `Layer 1: Data Platform` | [Data Platform](../docs/Data-Platform.md) | Generator design and measured source-data characteristics, Spark and dbt-postgres batch pipelines (`DP1-DP3`), Flink processing of the cash transaction stream, storage optimization, Airflow and GX Core validation, DataHub governance, Layer 1 novel ideas, and Layer 1 design rationale. |
| `Layer 2: Machine Learning Platform` | [ML Platform](../docs/ML-Platform.md) | Account attrition prediction and tier-based outreach policy, Feast offline and online feature store, Kubeflow training pipeline, MLflow tracking, KServe inference and outreach scoring, RAG data pipeline, and Layer 2 design rationale. |
| `Layer 3: Agent Platform` | [Agent Platform](../docs/Agent-Platform.md) | Brokerage agent workflow, LLM inference serving and benchmarks, gateway routing and security policies, MCP tools, coordinator and specialist agents, registry, fleet scaling, A/B testing, Layer 3 novel ideas, and Layer 3 design rationale. |
| `Infrastructure and Operations` | [Infrastructure and Operations](../docs/IaC.md) | Docker Compose profiles and image optimization, cloud IaC (Terraform and Ansible), CI/CD pipelines and prompt management, ingress routing, TLS, centralized secrets, observability, and verification and load testing. |
| *(Shared reference)* | [Glossary](../docs/Glossary.md) | Project terms: one definition per concept and the words to avoid for it. |

### Internal Workbench and Authoring Guides (`workflow/`, Not Linked in `README.md`)

| Document | Purpose |
| --- | --- |
| [Workflow Overview](README.md) | Architecture and rationale of the `docs/`, `workflow/`, and `local/` repository system. |
| [Status](Status.md) | Working state: current position, accepted decisions, open decisions, next steps, what exists, and known defects. |
| [Polars POC record](records/Polars-POC.md) | Working benchmark record for the Polars generator proof of concept (50 tests, 1.01 GB validated output at 125.16 MiB peak resident memory). |
| [Design method](Design-Method.md) | How we frame a design problem, map its attributes, research references, compare options, balance simplicity against need, and record the choice. |
| [Diagram guide](Diagram-Guide.md) | How to create, revise, and export draw.io diagrams and Mermaid ERDs. |
| [Diagram examples](Diagram-Examples.md) | Worked examples applying the diagram guide: a source flow, a deployment, and an ERD. |
| [Diagram tools](tools/diagrams/README.md) | CLI instructions for `diagram.py` and `erd.py`. |
| [TPC-DI specification](../docs/TPC-DI/TPC-DI_v1.1.0.pdf) | The benchmark's official definitions. `Data-Specification.md` summarizes the parts this project uses. |
| [Coursework brief](../docs/course/coursework_proposal.md) | The course objective and three-layer scope. `docs/course/rubric.xlsx` is the grading rubric that the row and cell references in this guide point to, `docs/course/domain/` holds the course's sample e-commerce designs, and `docs/course/dump_xlsx.py` dumps the rubric to text. |

---

## 3. Section Blueprint for Public System Documents

### 3.1 `README.md`
Follows the coursework README rules (`rubric.xlsx` cells `A34`, `A91`, `A98`):
- **Overview**: One short section introducing the retail brokerage platform and how Layers 1, 2, and 3 connect around batch integration, a real-time cash transaction stream, account attrition prediction, and agent workflows. A role-based overview image explains the main handoffs; it does not replace the deployable-unit system view below.
- **Business Domain and Source Data**: Compact summary table mapping source domains, origin systems, cadences, and injected data problems, linking to `docs/Data-Specification.md`.
- **System Architecture and Layer Flows**:
  - High-level architecture diagram where every box is a deployable unit and arrows show numbered data flows (`1..n` per flow, colored by flow type, with dashed arrows limited to secondary control flows).
  - Concise step-by-step summary tables for **Layer 1**, **Layer 2**, and **Layer 3**, linking to `docs/Data-Platform.md`, `docs/ML-Platform.md`, and `docs/Agent-Platform.md`.
- **Infrastructure, Operations, and Deployment**: Deployment diagram and summary of local profiles, cloud IaC, CI/CD, security, observability, and testing, linking to `docs/IaC.md`.
- **Repository Structure and Getting Started**: ASCII directory tree and phase-by-phase commands to run the system.

### 3.2 `docs/Data-Specification.md`
Covers the cross-layer data contract and schema design (`rubric.xlsx` rows `34-38`):
1. **Business Domains and Origin Systems**: The 9 TPC-DI domains and their logical origin systems.
2. **Load Cycle and Source Delivery**: Initial load (`initial/`, `initial_start` and `initial_end` run parameters), daily deliveries (`daily/YYYYMMDD/<Source>_YYYYMMDD.<ext>`, `daily_start` and `daily_end`, UTC `data_date`), the continuous Kafka cash transaction stream from `daily_start`, and the 18 source file families.
3. **Medallion Zones and Naming Conventions**: Zone definitions (`staging` retained file versions, `bronze.raw_*` 1:1 records with provenance, `silver.stg_*` cleaned and conformed records, `gold.dim_*`, `fact_*`, and `feat_*` warehouse, live, and feature tables; `upstream` is the source department's bucket, not a zone) plus a complete table inventory (grain, keys, partitioning, writer).
4. **Zone Entity Relationship Diagrams**: ERDs for **Bronze**, **Silver**, and **Gold** (including SCD2 validity columns on dimensions and `event_timestamp` + `created` on `feat_*` tables).
5. **Core Integration Rules**: Source-to-Gold lineage, SCD2 versioning and propagation, XML omitted versus empty elements, trade lifecycle updates, and batch-to-stream cash balance handoff.
6. **Physical Structures and Schema Reference**: General parsing rules and collapsed `<details>` field dictionaries for all source files and Gold tables.
7. **Differences from TPC-DI**: One table of every project change and every correction to the specification, each with its reason and a link to the section that explains it.

### 3.3 `docs/Data-Platform.md` (Layer 1)
Covers Layer 1 architecture, pipelines, optimizations, and evidence (`rubric.xlsx` rows `5-33`, `39-40`):
1. **Platform Architecture and Medallion Zones**: Storage zones, buckets (`upstream`, `staging`, `lakehouse`, `platform`), and retention baseline.
2. **Data Generation and Problem Injection**: Four generation stages (business behavior, attrition signal, source recording, delivery), bounded-memory execution, a separate source publisher, scale-factor calibration, and measured data characteristics. The full generated dataset must reach at least 100 GB of offline files; streamed cash transactions are counted separately. Distinguish estimates, configured problem rates, and measured results.
3. **Batch Processing and Storage Optimization (Spark and dbt-postgres)**: `DP1` (upstream to staging to Bronze), `DP2` (Bronze to Silver and Gold), and `DP3` (Gold to batch features), baseline-versus-treated Spark optimization comparisons with Spark UI screenshots for **skew**, **schema evolution**, and **deduplication**, plus Lakehouse (compaction, partitioning, sorting) and Warehouse (indexing) storage optimizations.
4. **Stream Processing (Kafka and Flink)**: Cash transaction contract, bootstrap from the initial load's Gold balances, and baseline-versus-treated Flink comparisons with Flink UI screenshots for **late arrivals**, **deduplication**, and **event-time window processing** (with window code and explanation).
5. **Orchestration, Quality, and Governance (Airflow, GX Core, and DataHub)**: Shared Airflow Connections and Variables, `DP1` to `DP3` DAGs with Ingest and Validate stages, DataHub cataloged tables across zones, pipeline-to-table lineage, published validation assertions, and Layer 1 novel ideas.
6. **Layer 1 Design Decisions and Rationale**: Distilled criteria, candidate comparisons, and choices for delivery rules, bucket layout, versioning baseline, and streaming source selection.

### 3.4 `docs/ML-Platform.md` (Layer 2)
Covers Layer 2 feature engineering, training, serving, and RAG (`rubric.xlsx` rows `44-50`):
1. **Prediction Task and Outreach Policy**: Account closure within 30 days, tier-based broker outreach thresholds, labels from account closures, and synthetic leaving behavior. Keep the prediction separate from the policy and measure how early a useful warning arrives.
2. **Feature Store and Data Contracts (Feast)**: Offline feature tables (`feat_*`) and streaming features, Airflow incremental materialization pipeline (offline to online store), and streaming feature push jobs (to offline and online stores).
3. **Training and Experiment Tracking (Kubeflow Pipelines and MLflow)**: Multi-step Kubeflow training pipeline (offline retrieval, split, baseline versus main model training, evaluation) and MLflow tracking (parameters, metrics, data versions, and `latest` or `production` model tags).
4. **Model Serving and Scoring (KServe)**: The inference service and accepted daily, withdrawal-triggered, and agent-request scoring paths.
5. **Document Retrieval Pipeline (RAG)**: Airflow text ingestion, chunking, embedding generation, storage in Feast, and DataHub lineage and validation. Add document sources only after the owner accepts them.
6. **Layer 2 Design Decisions and Rationale**: Distilled criteria, candidate comparisons, and choices for the ML target, policy separation, point-in-time cutoffs, and serving topology.

### 3.5 `docs/Agent-Platform.md` (Layer 3)
Covers Layer 3 LLM serving, gateways, MCP tools, and agents (`rubric.xlsx` rows `53-63`, `88-89`, `92-93`):
1. **Agent Use Case and MCP Tool Plan**: User workflow, MCP tools (structured account and trade data, ML account-attrition inference, and RAG document retrieval), and an end-to-end interaction trace.
2. **LLM Inference Platform**: Model server deployment, custom model configuration, global model config, and inference benchmark and optimization results.
3. **Gateways and Access Policies**: Unified gateway for LLM, MCP, and A2A traffic, authentication, rate limiting, PII safety filtering, and per-agent tool permissions.
4. **Agent Coordination, Registry, and Fleet Scaling**: Coordinator agent and two specialist agents, agent registry publication, multi-replica autoscaling, and warm-up fleet architecture.
5. **A/B Testing and Layer 3 Design Rationale**: Traffic-split evaluation across agent model configs and LLM versions, two demonstrated Layer 3 novel ideas, and gateway architecture rationale.

### 3.6 `docs/IaC.md` (Infrastructure and Operations)
Covers cross-cutting infrastructure, CI/CD, security, observability, and testing (`rubric.xlsx` rows `3-4`, `64-90`):
1. **Local Compose Profiles and Container Optimization**: Service profiles, port and memory budgets, multi-stage Dockerfiles, and before-and-after image size comparisons.
2. **Cloud Infrastructure as Code (Terraform and Ansible)**: Modular Terraform cloud provisioning and role-based Ansible service configuration.
3. **CI/CD and Prompt Management**: Automated pipelines (testing, building, and deploying the RAG pipeline, MCP servers and data agent, coordinator agent, and streaming feature jobs), credential management, and externalized system prompts.
4. **External Routing, Edge Security, and Secrets**: Ingress routing hiding internal UIs (metrics, logs, traces, agent chat, and registry), Basic Auth, rate limiting, custom domain with HTTPS for the Web API, and centralized secret management in Vault.
5. **Observability**: Metrics, logs, and distributed traces covering the Web API, compute resources, LLM inference (tokens, latency, TTFT, PII blocks), and agent and MCP tool calls and failures.
6. **Verification and Load Testing**: Web API unit tests (>90% coverage with mocks and fixtures), equivalence partitioning and boundary value analysis (EP/BVA), PR differential mutation testing (>80% score), property-based idempotency tests, and Web API load testing SLA reports.

---

## 4. Recording Design Decisions

We separate **how we analyze a problem** from **how we write it in the document**:
1. **Explore in conversation**: Use the [design method](Design-Method.md) to frame the question, map attributes, check primary sources, and walk options through concrete failure scenarios so the engineering trade-offs are clear.
2. **Write concisely in the owning document**:
   - **Main sections**: State how the system works directly, without debating alternatives inline.
   - **Design Decisions & Rationale section**: Group the layer's key choices at the end of the document using:
     - **Criteria**: 3 to 5 bold-label bullets stating what the choice had to satisfy.
     - **Decision table**: `Decision | Selected Choice | Main Reason`.
     - **Alternatives (`<details>` block)**: A compact table of the 2 to 4 serious contenders and why they were not selected, plus what would trigger revisiting the choice.
   - **Ownership**: Only choices accepted by the owner are written into the system design as settled decisions. Unresolved options stay in [Status](Status.md) until decided.

---

## 5. Style, Evidence, and Records

### Writing Style
- **Prose and tables**: Open each section with a short cause-and-effect explanation (1 to 3 sentences or a short paragraph where a mechanism needs context), followed by a reference table or bold-label bullets (`**Label**: description`).
- **Tables**: Main carrier of schemas, mappings, configurations, and comparisons. Keep cells concise; put identifiers, codes, and file paths in backticks.
- **Detail**: Use collapsed `<details>` blocks for long field dictionaries, raw samples, and secondary candidate tables so the main page stays scannable.
- **Consistency**: Use one term per concept across all files (for example, "daily delivery", not also "daily batch" or "increment"), active voice, American English, and true-alternative slashes at most once per sentence.
- **Terms**: Use the terms in the [Glossary](../docs/Glossary.md), one term per concept. Add or change a glossary entry in the same change that settles the term.
- **Checks**: Run `.venv/bin/python workflow/tools/check_docs.py` after moving, renaming, or editing Markdown files to verify links, anchors, line endings, and banned words.

### Evidence and Working Records
- **Rubric evidence**: Place configurations, input scales, before-and-after metrics, and annotated UI screenshots directly inside the owning layer document (`Data-Platform.md`, `ML-Platform.md`, `Agent-Platform.md`, or `IaC.md`) beside the claim they prove.
- **Working records (`workflow/records/`)**: Store intermediate development benchmarks, POC measurements (such as `Polars-POC.md`), and temporary design drafts here, linked from `Status.md`. When a component is built and measured, copy the final evidence into the owning layer document and archive or remove the working record. Delete a design draft only after its accepted decisions and rationale are in the owning documents and its unresolved items remain in Status.
- **Status (`workflow/Status.md`)**: Keep only current position, accepted and open decisions, next steps, what exists, and known defects. Move older session logs to `local/archive/`.
