# Documentation Guide

The [root README](../README.md) introduces the system. The documents below hold the detail. To resume work, start with [Status](Status.md).

## Documents

| Document | Purpose |
| --- | --- |
| [Data specification](Data-Specification.md) | The project's data: business domains and origin systems, load cycle, source files, data zones and silver rules, gold warehouse and ERD, cash stream data, ML data, transformations, and field schemas. |
| [Project design](Project-Design.md) | Requirements, decisions with their rationale, and open questions. |
| [Infrastructure](IaC.md) | How the services run, what they need, and which service mechanisms constrain the design. |
| [Status](Status.md) | Current position, open decisions, next step, what exists, and known defects. |
| [Infrastructure evidence](records/Infrastructure-Evidence.md) | Conditions, measurements, and source inspections behind the earlier build findings. Working record. |
| [Polars generator evidence](records/Polars-POC.md) | What the generator POC established about correctness, repeatability, memory, and scale. Working record. |
| [Diagram guide](Diagram-Guide.md) | How to create, revise, and export draw.io diagrams. ERDs use native Mermaid instead. |
| [Research notes](research/) | Evidence for design choices, such as [generator feasibility](research/Generator-Feasibility.md) and [streaming candidates](research/Streaming-Candidates.md). |
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
| **Data** | Domains, sources, zones, load cycle in brief | Data specification: schemas, grains, transformations, stream and ML data |
| **Decisions** | Main choices | Project design: requirements, decisions, rationale, open questions |
| **Generation** | What is generated; configurable problems | Generator documentation, once its design is chosen; research notes until then |
| **Batch processing** | DP1, DP2, DP3 in brief | Pipeline documentation: tasks, validation, reruns, lineage, measurements |
| **Streaming** | Selected events, processor, outputs | Data specification for the event data; project design for the choices |
| **Machine learning** | Prediction task, main components | Data specification for features and labels; project design for the choices |
| **Agents** | Platform scope | Agent design, once the user task is chosen |
| **Infrastructure** | Deployment shape, basic startup | Infrastructure document and its evidence records |
| **Progress** | Link to Status | Status |

One document can cover related subjects until it becomes hard to navigate. Moving, renaming, splitting, and merging documents is normal; update incoming links in the same change and run `make check-docs`.

## Evidence and Records

- **Evidence**: Configuration, input and scale, observed result, and limits, next to the claim it supports. Screenshots go beside their explanation.
- **Working records**: Routine command output, test results, and run reports go in `docs/records/`, linked from Status.
- **Status**: Current state only. Older entries move to `misc/archive/`.
- **Research notes**: Evidence for one choice, linked from the design that uses it. A research note decides nothing.

## Course Requirements

The local [rubric](course/rubric.xlsx), sheet `edai`, cells A34 and A98, asks for a summary README that links to detailed documents under `docs/`, and for screenshots embedded with explanations.

Cell A98 requires the README to introduce the business domain, include a table of contents and the repository structure, and show the overall deployment. Diagram components should be deployable units, with described and numbered flows.

The course files are local reference material and are ignored by Git.
