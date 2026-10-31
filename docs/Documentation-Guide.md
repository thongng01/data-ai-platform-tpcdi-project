# Documentation Guide

The [root README](../README.md) introduces the system. The documents below hold the detail. To resume work, start with [Status](Status.md).

## Documents

| Document | Purpose |
| --- | --- |
| [Data specification](Data-Specification.md) | The project's data: business domains and origin systems, load cycle, source files, data zones and silver rules, gold warehouse and ERD, cash stream data, ML data, transformations, and field schemas. |
| [Project design](Project-Design.md) | Coursework requirements, decisions and rationale, engineering constraints, and open questions. |
| [Infrastructure](IaC.md) | How the services run, what they need, and which service mechanisms constrain the design. |
| [Status](Status.md) | Current position, open decisions, next step, what exists, and known defects. |
| [Polars generator evidence](records/Polars-POC.md) | What the generator POC established about correctness, repeatability, memory, and scale. Working record. |
| [Diagram guide](Diagram-Guide.md) | How to create, revise, and export diagrams: draw.io files and Mermaid ERDs. |
| [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf) | The benchmark's full definitions. The data specification summarizes the parts this project uses. |

## Style

Match the README and the data specification:
- **Sections**: One-line lead-in, then a table or bold-label bullets.
- **Tables**: Main carrier of reference values. Short label cells; codes, fields, and files in backticks.
- **Bullets**: `**Label**: short description`. Fragments are fine.
- **Headings**: Title Case; numbered sections in reference documents.
- **Notes**: One italic `*Note:*` line per exception.
- **Open items**: Inline marker such as "(length open)" or an Open column; the full list in one place.
- **Detail**: Collapsed `<details>` blocks in the same style, instead of a longer main text.
- **Framing**: The project as it is, not a list of changes to a baseline.
- **Terms**: One term per concept in every document, for example "daily batch", not also "increment" or "incremental update".
- **Slashes**: True alternatives only, once per sentence.
- **Spelling**: American English.
- **Cut**: Sentences that fit any project, talk about the document itself, or answer a doubt nobody had.
- **Done**: One style from start to end, `<details>` blocks included; facts checked; no growth in length without a reason.

## What Belongs in the README

The README gives the whole system in brief:
- **Overview**: The project, its three layers, and each layer's components.
- **Data**: A short summary of the data specification: domains, sources, zones, and load cycle. Same terms and groupings as the specification.
- **Layers**: Each layer section opens with its purpose, then its main flow or responsibility.
- **Structure and Setup**: Repository structure as an ASCII tree, deployment, and the shortest setup path.
- **Links**: Detail documents and Status.

Keep discussion, progress, session updates, and test inventories out of the README.

## Where Details Go

| Subject | README | Dedicated document |
| --- | --- | --- |
| **Data** | Domains, sources, load cycle, zones, Gold warehouse and ERD, stream and ML tables | `Data-Specification.md`: cross-layer data contract, full field schemas, and edge cases |
| **Decisions & Rubric** | Main choices | `Project-Design.md`: coursework requirements, decisions and rationale, constraints, open questions |
| **Layer 1: Data Platform** | Zones, generation, DP1 to DP3 batch, Flink stream, orchestration and governance | `Data-Platform.md` (when built): generator, Spark/dbt, Flink, Airflow, Soda, DataHub, and UI optimization evidence |
| **Layer 2: ML Platform** | Prediction task, feature store, training, serving, and RAG | `ML-Platform.md` (when built): synthetic behavior, Feast, KFP, MLflow, KServe, alert scorer, and RAG pipeline |
| **Layer 3: Agent Platform** | Agent components and scope | `Agent-Platform.md` (when built): user task, LLM serving, MCP tools, gateway policies, and agent coordination |
| **Infrastructure & Ops** | Compose profiles, IaC, CI/CD, security, observability, startup | `IaC.md`: service configuration, ports, memory limits, Terraform/Ansible, and operating commands |
| **Progress** | Link to Status | `Status.md`: current position, open decisions, next steps, what exists, and known defects |

One document can cover related subjects until it becomes hard to navigate. Moving, renaming, splitting, and merging documents is normal; update incoming links in the same change and run `make check-docs`.

## Evidence and Records

- **Evidence**: Configuration, input and scale, observed result, and limits, next to the claim it supports. Screenshots go beside their explanation. Rubric evidence, such as Spark and Flink UI screenshots and baseline-versus-treated results, belongs in the layer document that makes the claim.
- **Working records**: Measured results from development runs, such as benchmarks, test results, and run reports, go in `docs/records/`, linked from Status. Raw command output and logs go in `misc/tmp/`. A record stays while the code or decision it measures is current. When that changes, copy any result that still supports a claim into the owning document, then archive the record to `misc/archive/` if a later phase may need it, or remove it.
- **Status**: Current state only. Older entries move to `misc/archive/`.
- **Research notes**: Evidence for one open choice, in `docs/research/`, linked from the design that uses it. A research note decides nothing. When the choice is made, integrate the conclusions into the owning document, then archive or remove the note.

## Course Requirements

The course rubric asks for a summary README that links to detailed documents under `docs/`, and for screenshots embedded with explanations.

The README must introduce the business domain, include a table of contents and the repository structure, and show the overall deployment. Diagram components should be deployable units, with described and numbered flows.
