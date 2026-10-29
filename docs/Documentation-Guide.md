# Documentation guide

The [root README](../README.md) introduces the system. The documents below explain the source domain, the design, the infrastructure, and the current state in more detail. To resume work, start with [Current position](Status.md#current-position).

## Documents

| Document | Question it answers |
| --- | --- |
| [TPC-DI guide](TPC-DI/TPC-DI.md) | What data does TPC-DI supply, and what warehouse does it define? One page explains source and warehouse data in equal detail through tables, diagrams, and examples. Complete fields and detailed rules follow the main explanation. The `TPC-DI/` folder also holds the editable diagrams, renders, specification, and design paper. It holds benchmark facts only. |
| [Project design](Project-Design.md) | What must the project deliver, what have we decided, and what is still open? |
| [Infrastructure](IaC.md) | How do the services run, what resources do they need, and which service mechanisms constrain the design? |
| [Status](Status.md) | What exists, what is broken, what happened in each session, and how does a new session resume? |
| [Infrastructure evidence](records/Infrastructure-Evidence.md) | Which conditions, measurements, and source inspections support the earlier build findings? This is a working record. |
| [Polars generator evidence](records/Polars-POC.md) | What did the fresh generator POC establish about source correctness, repeatability, memory, and scale, and what remains unimplemented? |
| [Diagram guide](Diagram-Guide.md) | How do we create, revise, and export project diagrams with consistent visual meanings and readable Markdown views? The [examples](Diagram-Examples.md) show source flows, deployment scopes, and field relationships. |
| [Research notes](research/) | What evidence supports a design choice? See [generator feasibility](research/Generator-Feasibility.md), [streaming candidates](research/Streaming-Candidates.md), and [reference implementations](research/Reference-Implementations.md). |

## What belongs in the root README?

Keep the information needed to understand the whole system before choosing a part to inspect:

- Describe the brokerage domain briefly, using the front-office, back-office, and shared-data groups.
- State each layer's purpose and name the components and their roles.
- Show the overall deployment and data flow.
- Summarize the source families, storage zones, and pipeline responsibilities.
- Explain the repository structure and provide the shortest useful setup path.
- Link to the relevant detail and to project status.

The overview gives the whole system in brief. A later layer section should add its main flow or responsibility, rather than repeat the same tool descriptions.

Keep the README focused on the intended system. Summarize accepted choices and label material proposals. Link to the project design for unresolved parameters and to Status for implementation limits. Keep discussion prompts, progress estimates, session updates, and routine test inventories in working records.

Preserve the README's broad structure, short technical prose, functional labels, and tables. Each layer starts with its purpose. The business domain uses front-office, back-office, and shared reference and market groups. A qualification belongs beside the claim it limits. Evidence that explains a mechanism belongs with that subject; a separate evaluation section is not required.

## Where should the details go?

| Subject | Keep in the README | Put in the dedicated document |
| --- | --- | --- |
| TPC-DI | Explain the domain and main source groups. | Explain record meanings, source layouts, target grains, transformations, history, and benchmark rules in `TPC-DI/TPC-DI.md`. Keep project choices out of it. |
| Requirements and decisions | Name the main choices. | Record coursework requirements, decisions, considerations, and open questions in `Project-Design.md`. |
| Source generation | State what is generated and which problems are configurable. | Explain the generator model, relationships, distributions, configuration, and validation once its design is chosen. `research/Generator-Feasibility.md` supplies the research. |
| Storage and modelling | State what staging, bronze, silver, and gold contain. | Define keys, grains, silver/gold ERDs, SCD rules, update handling, and rejected-record handling in the modelling design. |
| Batch processing | Summarize DP1, DP2, and DP3. | Explain tasks, validation gates, retries, reruns, lineage, and baseline/optimization measurements in the pipeline documentation. |
| Streaming | Name the selected events, processor, and output. | Explain event identity, time, windows, lateness, replay, state, and sinks. `research/Streaming-Candidates.md` supplies source evidence and candidate comparisons; `Project-Design.md` records the cash selection. |
| Machine learning | State the prediction task and main components. | Define labels, features, time cutoffs, splits, evaluation, training, and serving in the ML design. |
| Agents | State the platform scope and any selected user task and tools. | Define agent/tool contracts, retrieval, policies, prompts, coordination, and tests in the agent design. |
| Infrastructure and operations | Show the deployment shape and basic startup. | Keep configuration, resource requirements, security, observability, operational commands, and evidence that explains service behavior in the relevant operations documents. Put routine run reports in working records. |
| Progress | Link to status. | Record implemented work, known defects, verification dates, the session log, and resume notes in `Status.md`. |

The future document subjects above are a placement guide, not an instruction to create every file now. One document can cover related work until it becomes difficult to navigate.

## How to write a useful detail document

Start with the question or component it explains. Describe the normal data flow, inputs, outputs, and rules before exceptional cases. Put the relevant diagram or example beside the explanation.

Distinguish three kinds of statement:

- A source fact cites the specification, code, or inspected data that establishes it.
- A project decision states what we chose and why.
- A proposed design or open question stays visibly unresolved until we agree on it.

For evidence that explains a mechanism or supports a design choice, record the relevant configuration, input and scale, observed result, and limits of the claim. Put screenshots next to the claim they support. Keep a useful baseline and treatment comparison with the component it explains.

Put routine command output, repeated test results, session summaries, and implementation inventories in Status or working records under `docs/records/` linked from Status. Subject documents can link to those records without copying their logs. A screenshot directory without explanations does not tell a reviewer what worked.

Research notes go in `research/`. Each one records evidence for a choice and links to the design that uses it. Preserve dated experimental findings, inputs, methods, and limits needed to assess that evidence. A research note does not decide anything.

## Course requirements behind this split

The local [rubric](course/rubric.xlsx), sheet `edai`, cells A34 and A98, asks for a summary README that links to detailed documents under `docs/`. It also asks for screenshots to be embedded with explanations rather than left as an unexplained image collection.

Cell A98 requires the README to introduce the business domain, include a table of contents and repository structure, and show the overall deployment. Diagram components should be deployable units; arrows should describe and number the flows. These requirements determine the README's coverage, not a fixed length.

The course files are local reference material and are currently ignored by Git. The cell references above identify the source for readers with access to that workbook.
