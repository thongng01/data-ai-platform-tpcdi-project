# Project design

This document explains:
- how the project uses TPC-DI and what changes for batch processing, streaming, and ML.

<!-- The [data specification](Data-Specification.md) explains the original sources, transformations, and warehouse.  -->

<!-- [Status](Status.md) records implementation and session progress. -->

**Decided** means accepted. **Proposed** means shortlisted but not accepted. **Open** means the choice has not been made. **Deferred** means future work outside the current goal.

| Section | Contents |
| --- | --- |
| [Scope](#scope) | Purpose and source coverage. |
| [Selected use case](#selected-use-case) | Cash-flow forecasting and the tier-based minimum-cash policy. |
| [Project data flow](#project-data-flow) | How batch, streaming, features, and ML connect. |
| [Changes from TPC-DI](#from-tpc-di-to-the-project) | Original behavior, project choices, and reasons. |
| [Streaming candidates](#streaming-candidates) | Candidate records, selection criteria, and the cash decision. |
| [ML candidates](#ml-candidates) | Candidate outcomes, selection criteria, and the forecast decision. |
| [Features and serving](#features-and-serving) | Offline training, online inference, and alert delivery. |
| [Future work](#future-work) | Obligations and payment coverage beyond the first version. |
| [Coursework requirements](#coursework-requirements) | Required scale, data problems, pipelines, and evidence. |
| [Decisions so far](#decisions-so-far) | Accepted choices and existing proposals. |
| [Implementation considerations](#design-considerations-by-area) | Storage, history, and generator constraints. |
| [Open questions](#open-questions) | Remaining choices and their dependencies. |
| [Research notes](#research-notes) | Evidence supporting the design discussion. |

## Scope

The project builds a data platform, an ML platform, and an agent platform for the TPC-DI retail brokerage. The [root README](../README.md) presents the system. Delivery milestones and implementation progress are recorded in [Status](Status.md).

**TPC-DI scope**

The source scope is decided: use all TPC-DI business and reference sources and their structures. Exclude metadata used only for formal benchmark validation.

The agreed baseline follows TPC-DI's source definitions, source-to-target rules, and daily batch cycle. CashTransaction is the selected streaming source. Its delivery and processing change, while its cash-movement meaning and daily balance calculation remain the baseline. Silver tables, stream event metadata, ML features, and a tier-based cash policy are project additions. The selected ML task is cash-flow forecasting. Forecast timing, policy amounts, and cash correction rules still need definition.

The 2026-10-30 revision extends the fixed three-batch benchmark into a historical load followed by a configurable daily sequence. The [data specification](Data-Specification.md#2-load-cycle--processing-strategy) retains historical loading followed by daily batches, potentially up to one year. Historical duration remains open. Each daily batch keeps its own batch date and identity. Longer-run FINWIRE, HR, and reference refresh rules also remain open.

## Selected use case

**Decided:** forecast future net cash movement for each account and flag a projected balance below a configured minimum. The minimum depends on the account owner's customer tier. The alert supports account review under our internal cash policy.

```text
projected_cash = current_cash + forecast_net_movement
minimum_cash = configured_buffer[customer_tier]
alert = projected_cash < minimum_cash
```

The model forecasts movement over a chosen future period. The policy rule compares the projected balance at the end of that period with the minimum. The forecast horizon, scoring cadence, and minimum amounts are open.

For example, recorded cash of 10,000 and forecast net movement of -3,000 give projected cash of 7,000. A configured minimum of 8,000 produces an alert with a projected shortfall of 1,000. These numbers illustrate the rule; they are not selected thresholds.

| Input | Where it comes from | Role |
| --- | --- | --- |
| Current cash | Opening balance plus accepted cash movements. | Provides the observed starting balance. |
| Forecast net movement | The project's ML model. | Estimates total signed movement during the future period. |
| Account owner | `DimAccount.SK_CustomerID` links to `DimCustomer`. | Identifies which customer's tier applies to the account. |
| Customer tier | `DimCustomer.Tier`, with valid categories 1, 2, and 3. | Selects a minimum from the policy configuration. Use the customer version valid at prediction time. |
| Minimum cash | Project configuration keyed by tier. | Sets the alert threshold. TPC-DI defines neither these amounts nor a credit-risk ordering of tiers. |

The policy uses existing account/customer relationships and adds configuration. It does not require a new business-source table. Threshold rationale, policy versioning, and handling of missing or invalid tiers remain open.

The alert concerns the balance at the forecast cutoff. It does not establish that the account can cover every earlier payment, or that it meets regulatory liquidity or margin requirements. An obligation schedule is [future work](#future-work).

## Project data flow

**Decided:** staging is for batch files; bronze preserves source records in table form; silver holds cleaned records; gold holds warehouse outputs and derived features. The [data specification](Data-Specification.md#4-data-zones-medallion-architecture) defines each zone and the silver rules.

For XML, the logical record boundary and nested representation need to be stated explicitly. One source record is not necessarily one physical line.

| Route | Processing | Result |
| --- | --- | --- |
| Historical batch | Staging to bronze, then silver and gold. | Establish the warehouse history, reference data, and opening cash balances. |
| Subsequent batch loads | Apply the original daily source-specific patterns for the sources that remain in batch. | Maintain the related dimensions and warehouse tables. |
| Cash stream | Publish cash movements to Kafka, capture the received events in bronze, and use Flink to validate, deduplicate, and calculate results. | Silver cash events, gold cash balances, and selected window aggregates. |
| Offline feature computation | Read retained silver/gold history. | Timestamped feature history for training, backfills, and feature materialization. |
| ML training | Train from historical features and the later observed net cash movement. | An offline-trained cash-flow model. |
| Online scoring and alerts | Use eligible features to forecast net movement, calculate projected cash, and apply the tier-based minimum. | Retained forecasts and alert state, exposed through a project API or dashboard. The serving topology is proposed below. |

The [data specification](Data-Specification.md#71-cash-streaming-kafka-and-flink) defines the cash event and its bronze, silver, and gold outputs.

DP1 remains source-to-bronze ingestion, DP2 remains bronze-to-silver/gold integration, and DP3 remains offline feature computation. The exact streaming jobs, sink layout, and division of work between Spark and Flink still need definition.

### How the paths stay consistent

1. Define the cutoff between the initial load and subsequent cash events. Initialize cash state from the completed history and load the account/customer lookup data.
2. Assign a stable event ID before publishing a cash movement. Repeated delivery retains that ID. Bronze preserves deliveries; silver retains the deduplicated business events.
3. Give each gold cash result one writer during normal operation. A batch replay used for comparison must not add the same movements to the live balance again.
4. Retain the original balance calculation: prior balance plus the day's net cash movement. Define the stream's daily cutoff and dimension lookup timing against TPC-DI's batch-date rules.
5. Specify late-event corrections. An older cash movement can affect its day's balance and later balances. Compare the stream with a batch calculation over the same events and cutoffs.

CashTransaction has no standalone transaction ID. Its existing account ID, timestamp, and signed amount are sufficient for windowed cash movement; the added event ID supports replay and duplicate handling. A running balance also needs the opening balance.

## From TPC-DI to the project

| Topic | Original TPC-DI | Project choice or proposal | Reason | State |
| --- | --- | --- | --- | --- |
| Source coverage | Defines brokerage, external, and reference files. | Use all business and reference sources and their structures. | Retain an existing source model with defined relationships. | Decided |
| Benchmark validation metadata | Supplies audit observations and formal validation procedures. | Exclude metadata used only for formal benchmark validation. | The project is coursework, not an official benchmark result. | Decided |
| Operational metadata | Uses batch dates, CDC operation/order, and integration messages. | Keep their meaning through the original fields or project equivalents; retain source provenance. | Updates, recovery, reconciliation, and quality reporting need this information. | Required for the selected processing; representation open |
| Load cycle | One historical load and two daily incremental batches. | Keep the daily batch patterns for batch sources; process subsequent cash movements as a stream. The generated duration remains open. | Preserve the defined integration rules while adding continuous cash processing. | Baseline decided; cash timing details open |
| Warehouse and transformations | Defines tables, relationships, history, calculations, and update rules. | Keep these as the baseline; implement cash processing in Flink and add silver/feature tables. | Gives the implementation a defined expected result. | Baseline decided; extensions explicit |
| Streaming | Supplies cash movements as historical and daily insert-only files. | Use CashTransaction as the stream, with bronze capture, silver events, gold balances, and window aggregates. | Cash movements are timestamped actions with signed amounts and a defined balance calculation. | Decided; windows and sink details open |
| Data problems | Defines particular source irregularities and handling rules. | Add measured skew, schema evolution, and controlled delivery/data faults. | Meet the coursework requirements with baseline and treatment evidence. | Required; exact cases open |
| ML | Defines no prediction target or feature-serving workflow. | Forecast future net account cash movement from historical and recent features. | Use the cash pipeline for account cash monitoring. | Target family decided; horizon, features, and model open |
| Minimum-cash policy | Supplies customer tiers and account/customer relationships, but no minimum balances. | Configure a minimum per tier and flag projected cash below it. | Give the forecast a defined use without an obligation dataset. | Decided; amounts and policy details open |
| Predictive behavior | Supplies synthetic integration data, with no guarantee of signal for our target. | Design documented account behavior together with the forecast task. | Make the learning problem intentional and keep related records consistent. | Decided; behavior model open |

Keeping the original file patterns preserves several integration cases: CMS history followed by customer/account CDC, trade state plus historical status changes, insert-only activity, and repeated prospect snapshots. Normalized internal records can make processing consistent without requiring every source to emit CDC.

Generating later company, security, financial, or broker changes would extend the original workload. If selected, that extension needs its own generation and update rules. New intraday market observations would also require generation; replaying daily observations does not supply them.

## Streaming candidates

**Cash movements are selected.** The alternatives remain listed for comparison.

| Candidate | Source records | Possible output |
| --- | --- | --- |
| Cash movements | `CashTransaction.txt`. | Windowed inflows/outflows and running account balances. |
| Trade activity | Historical trade status changes and incremental trade CDC. | Submission/completion/cancellation counts, traded quantities, lifecycle durations, and recent account activity. |
| Watch activity | `WatchHistory.txt`. | Additions/removals and active watches per customer/security. |
| Holding changes | `HoldingHistory.txt` enriched from trades. | Holding updates and before/after continuity checks. |
| Daily market observations | `DailyMarket.txt`. | Rolling daily price and volume measures. |
| Customer/account changes | `Customer.txt` and `Account.txt`. | Current lookup state for enriching activity events. |

### Streaming selection criteria

| Criterion | What to compare | CashTransaction |
| --- | --- | --- |
| Event meaning | Does a record describe an action, an observation, or an update to an existing entity? | A cash movement. Native incremental rows are inserts only. |
| Time resolution | Is there a timestamp suitable for the computation, or must another source supply it? | Each movement has `CT_DTS`. |
| Identity and replay | Can we distinguish another event from repeated delivery of the same event? | Account ID identifies the account, not the event. Add a stable event ID before publication. |
| Dependencies and state | Which joins, earlier records, and state are needed before computing a result? | Windowed movement uses account ID, time, and amount directly. Balances need opening state; gold also needs dimension lookups. |
| Useful outputs | Does the computation serve a defined warehouse or feature use? | Inflow/outflow windows, net movement, daily balances, and recent account features. |
| Measurable correctness | Can duplicate and late-event effects be checked against a reference result? | Duplicates change sums; late arrivals change windows and balances. A batch calculation provides a reference for the same events and cutoffs. |
| Implementation scope | How much new event reconstruction and business logic is required? | The movement fields and daily balance rule already exist. Event identity, continuous state, and correction behavior are project work. |

**Why cash was selected:** each record describes a cash movement, and incremental records are inserts only. Account ID, timestamp, and signed amount are already present. We can calculate per-account inflows, outflows, and net movement without reconstructing a trade lifecycle first. The original target defines how those movements become daily balances. The missing event ID is a detail to add, not the reason the source fits streaming.

The source constraints explain the differences:

- **Trades**: distinguish a trade from its changes. Count a completion once. Historical status rows do not contain every earlier trade attribute; final execution values must not appear in earlier events.
- **Cash**: retain a stable change identity because the source has no standalone transaction ID. A running balance also needs prior state.
- **Watches**: apply activation and cancellation in the correct order to derive active state.
- **Holdings**: obtain time and account/security context from the modifying trade. Successive after-quantities must not be summed.
- **Daily market**: the source has daily resolution. Kafka delivery does not create observed intraday prices.
- **Customer/account CDC**: this is an alternative stream for lookup updates. Cash is the only selected stream; the other sources retain their batch paths.

For the cash stream, define event identity, event time, delivery time, ordering, initial state, windows, lateness, and output updates. The [streaming research note](research/Streaming-Candidates.md) gives the source evidence and replay procedure. Window sizes and lateness limits have not been selected.

## ML candidates

Cash-flow forecasting is selected. The alternatives and criteria below record why. Generator behavior and ML suitability must be designed together from the start.

| Candidate | Prediction entity | Outcome | Fit with the selected cash stream |
| --- | --- | --- | --- |
| Cash-flow forecast | Account. | Future net cash movement. | Selected. Recent cash amounts and activity can update the forecast used by the minimum-cash rule. |
| Low-cash prediction | Account. | Whether a future balance meets a defined low-cash condition. | A direct classifier is an alternative to forecasting an amount and applying a rule. The label would need sufficient low-cash and normal examples. |
| Unusual cash activity | Account or activity window. | Whether activity belongs to a defined unusual behavior pattern. | Uses recent amounts and counts, but needs explicit synthetic ground truth. It would not establish fraud. |
| Trading-activity prediction | Account. | Future completed-trade count or whether at least one trade completes. | Uses trade history, with recent cash as a possible feature. It needs aligned trade features and labels; another stream is not required. |
| Limit-trade completion | Submitted limit trade. | Whether it completes within a defined horizon. | Depends more on order and security information. Generation must link submission, completion/cancellation, cash, and holdings. TPC-DI supplies no exchange order book. |
| Market-volume forecast | Security. | Volume from the next observed trading day's market record. | Mainly uses daily market history. Cash records have no security identifier. |
| Prospect conversion | Prospect. | Whether it becomes a customer during a future period. | Needs prospect snapshots before conversion and later customer creation. Prospects without accounts have no cash events. |

### Selection criteria

| Criterion | What to establish |
| --- | --- |
| Defined output | The prediction answers a useful question and has an observable label. It does not merely repeat a calculation whose inputs are already known. |
| Use of the stream | Recent cash events add information beyond historical features. Measure that contribution. |
| Feature availability | Reconstruct inputs as they were available before prediction. Later outcomes must not enter earlier features. |
| Coherent generation | Define the behaviors that produce features and outcomes. Keep cash, trades, accounts, holdings, and related records consistent. |
| Evaluation | Compare with a simple baseline using a time-based split and suitable metrics. Check new periods and generator seeds. |
| Scope | Count the business rules, source history, and processing needed beyond the agreed cash pipeline. |

### Why cash-flow forecasting was selected

It uses the selected cash stream, retained cash history, and original balance calculation. The tier policy connects the forecast to a specific output: an account alert for review. The policy can change without redefining the amount the model predicts.

Trading activity remains a useful alternative. It needs trade feature and label calculations aligned with the cash history. A binary activity target could be easier to model than a cash amount: signed cash movements can cancel, and large rare movements can dominate forecast errors. The cash choice reduces additional pipeline work; it does not guarantee an easier learning problem.

Both tasks need coherent account and trade behavior. Trade-related cash cannot be generated as unrelated random amounts. Optional trade features can summarize the static `TradeType` categories: market buy/sell, limit buy/sell, and stop loss. `T_IS_CASH` / `CashFlag` describes financing for a trade, not an account class or minimum balance. CashTransaction has no trade ID for an exact cash-to-trade join, so these features would be aggregated by account and time.

### Generation and evaluation

**Decided:** design predictive behavior with the generator. Account activity profiles could influence movement frequency and size over time, with documented randomness. The exact behavior model is open. Keep latent generator parameters, future state, and outcome labels out of prediction inputs. Inject delivery duplicates and lateness separately from business behavior.

The [data specification](Data-Specification.md#72-ml-forecasting--alerts) defines the label: unique signed cash movements after prediction time and through the forecast cutoff.

The proposed evaluation compares:

1. A simple forecast based on historical cash movement.
2. An ML model using historical batch features.
3. The same model setup with recent streaming features added.

Use the same prediction cutoffs, horizons, and test periods. Measure forecast error separately from missed shortfalls, false alerts, and warning lead time under the selected policy. Recent features are useful only if the comparison supports that claim. Metrics, acceptance thresholds, and treatment of forecast uncertainty remain open.

Results describe the documented synthetic behavior. Real brokerage use would need validation with real data.

## Features and serving

### Offline and online features

The [data specification](Data-Specification.md#72-ml-forecasting--alerts) defines the offline and online feature data, candidate features, and the availability rule. Offline and online describe how feature values are stored and retrieved; batch and streaming describe how they are computed. Both computations can feed both stores, and Feast is the planned retrieval layer.

The account and customer lookup must respect effective dates and availability. Since these sources remain batch-loaded, online scoring can only use the versions already received; lookup freshness and correction handling need definition.

### Proposed serving flow

The following component layout is proposed for the selected use case.

| Step | Component | Responsibility |
| --- | --- | --- |
| 1 | Flink | Update cash state, write silver/gold results, and publish recent feature values with timestamps. |
| 2 | Scoring worker | Select an eligible feature snapshot on an update or defined cadence, then request a forecast. |
| 3 | Model service | Return forecast net cash movement using a model trained offline. KServe is the planned serving component. |
| 4 | Policy and alert service | Resolve the tier and configured minimum, calculate projected cash, and retain the forecast and alert state. |
| 5 | API or dashboard | Expose the latest result and its timestamps for account review. |

The worker and alert API can be one application. Keep model calls outside cash ingestion so slow inference delays predictions without blocking incoming cash events. Online inference updates forecasts from eligible features; it does not retrain the model on each event.

The [data specification](Data-Specification.md#72-ml-forecasting--alerts) lists the fields of a forecast and alert result. The exact API and storage schema are open.

Alert writes need retry-safe identities. Late cash corrections or newly available dimension changes can revise or clear an alert. Preserve enough version and time information to explain the earlier result. Scoring cadence, latency targets, update triggers, and correction behavior still need definition.

## Future work

**Deferred:** add obligations after the cash forecast and tier-buffer alert work.

TPC-DI supplies cash balances, holdings, daily prices, and a cash/margin flag per trade. It does not define an account-level payment schedule, settlement ledger, borrowed balances, interest, collateral rules, or margin requirements. `Financial.FI_LIABILITY` is an issuing company's liability, and `FI_MARGIN` is company profit divided by revenue. The [specification PDF](TPC-DI/TPC-DI_v1.1.0.pdf), clause 2.2.2.17 and table 3.2.17, defines those company fields.

A future synthetic obligation schedule would need an obligation ID, account ID, amount, due time, known-at time, outstanding status, and a link to the eventual cash event. Its fact/dimension layout and relationships are future design work. Adding the table also requires consistent generation, status history, and payment matching.

If known payments are subtracted explicitly, the calculation would be:

```text
projected_cash = current_cash + forecast_other_net_movement - known_due_payments
```

The forecast and training labels must then exclude the payments subtracted separately. If the model forecasts total net movement, do not subtract those payments again. Only obligations known at prediction time can enter the calculation.

A balance at the forecast cutoff cannot prove that every earlier payment was covered. Payment coverage needs forecasts at the due times or a cash-path model. Borrowing and margin-call rules would require further source data and policy definitions.

## Coursework requirements

These Layer 1 requirements come from the course rubric. The text below gives their meaning in English.

### Generated data

| Requirement | Evidence the rubric asks for |
|---|---|
| Generate at least 100 GB of offline data. Use more if 100 GB does not show the effect of an optimization | Data volume |
| Simulate skew | Which columns are skewed, and in what proportion |
| Simulate schema evolution | The schema before and after the change |
| Simulate another offline problem, for example a 2% duplicate rate | The rate of the problem |
| Store the generated data so that bronze ingestion can read it later, for example in MinIO or PostgreSQL that acts as another department's system | |
| Simulate late arrivals in streaming data | Which data arrives late, and how late |
| Simulate another streaming problem, for example a 1.5% duplicate rate | The rate of the problem |

The rubric also asks for the high-cardinality columns of the offline data, measured as distinct values divided by total values.

### Processing jobs

| Engine | Requirements |
|---|---|
| Spark | A baseline job without optimization; handling of skew, schema evolution, and the other offline problem, each with an explanation; integration into the data pipelines |
| Flink | A baseline job without optimization; handling of late arrivals and the other streaming problem, each with an explanation; window processing |

Spark evidence covers each step from the baseline, Spark UI screenshots that show the problem, and the Airflow integration. Each handling explanation states the skew or problem in the data, the symptom in the UI, the methods tried, and the gain over the baseline. Flink evidence gives the same account with the Flink UI, plus the window code and an explanation of what it does. The streaming pipeline later feeds the feature store.

### What each requirement means

This table is the project's reading of the rubric. The rubric names the problems. The definitions and failure modes come from how Spark and Flink process data.

| Problem | Definition | What fails without handling | Evidence of handling |
|---|---|---|---|
| Volume | At least 100 GB of saved source data | Fixed overheads dominate small runs, so an optimization shows little effect | Measured bytes and row counts of the saved files |
| Skew | A few values of a key hold a large share of the rows. The key must be one that Spark partitions, joins, or groups by | One task receives most rows of a shuffle. The stage waits for that task, and the task can spill to disk or run out of memory | Value distribution of the key; task durations and shuffle sizes in the Spark UI, maximum against median; run time before and after |
| Schema evolution | The source schema changes between files or batches: a column is added, renamed, or removed, or its type changes | A reader with a fixed schema fails, or it drops or nulls the changed column without an error | Schema before and after; which files carry each schema; the read result after handling |
| Offline duplicates, the rubric example | The same record appears more than once in the saved data | Counts and sums increase. A duplicate change can create an extra SCD2 version | The duplicate definition; the injected rate; counts before and after deduplication |
| Late arrival | An event arrives long after its event time. Flink treats it as late when its timestamp is behind the current watermark, so its window may already be closed | The event is dropped, or a window result changes after Flink emitted it | Which events are late; the delay distribution; window results with and without handling |
| Streaming duplicates, the rubric example | The same event is delivered more than once | A window counts the event twice | The event identity; the injected rate; window results before and after |
| Window processing | Flink groups events into windows by event time | No windowed output exists for the feature store | The window code and its explanation |

### Constraints that follow

1. The offline problems must exist in the saved source data. The generator creates them, and its configuration records their parameters, so the characteristics document can report them.
2. The streaming problems need event time and a delivery process with controlled delay and duplication. Lateness is a property of delivery, so the delivery process must record which events it delayed and by how much.
3. Each handling claim needs a baseline run and a treated run on the same data, with UI evidence.
4. Skew counts only if it lies on a key that a measured Spark job partitions, joins, or groups by.
5. The 2% and 1.5% duplicate rates are examples. Another problem is acceptable if it has a measured rate and a handling step.
6. 100 GB is a minimum. The rubric asks for more if 100 GB does not show the effect.

### Other requirements

Other Layer 1 requirements depend on the choices above: lakehouse and warehouse optimization, three Airflow pipelines DP1 to DP3 with ingest and validate stages, DataHub lineage and validation, and table diagrams for each zone. The rubric also asks for SCD2 history, timestamped features, and two demonstrated novel ideas. The later layers require Feast, Kubeflow, MLflow, KServe, text retrieval (RAG), agents, and operations. This document does not define those parts yet.

The [documentation guide](Documentation-Guide.md#course-requirements) states the rubric's documentation rules.

## Decisions so far

This table distinguishes accepted design choices from proposals and deferred work.

| Subject | State | Content |
|---|---|---|
| Domain | Decided | The TPC-DI retail brokerage. |
| Source coverage | Decided | Include all TPC-DI business and reference sources, using their structures as the baseline. Exclude metadata used only for formal benchmark validation. CashTransaction is selected for streaming, with cash-flow forecasting and a tier-based cash alert as the first ML use case. |
| Storage zones | Decided | Staging holds batch files only. Bronze preserves source records or received events 1:1 in table form, including duplicates and provenance. Silver holds cleaned, typed, integrated records. Gold holds warehouse outputs and features. Logical record boundaries, retention, parsing failures, and recovery still need detail. |
| Pipelines | Decided | DP1 moves source data to bronze. DP2 moves bronze to silver and gold. DP3 computes offline features. Each pipeline has an ingest stage and a validate stage. |
| Object store | Decided | Store lakehouse zones in SeaweedFS through its S3 API. Register tables in Hive Metastore and query them with Trino. The [infrastructure reference](IaC.md#7-why-these-choices) describes the storage constraints. |
| Gold warehouse | Proposed | Store gold outputs in PostgreSQL for ML and serving. Physical placement and the division of work between Spark, dbt, PostgreSQL, and Trino depend on the gold model. |
| Generator approach | Decided | Use the TPC-DI specification and a reference generator. Generate coherent relationships and histories, configurable scale and distributions, and deliberate data problems. Use readable JSON or YAML configuration. The SF3 sample provides examples of record layouts and values. |
| Generator engine | Open | The owner authorized an autonomous fresh Polars POC based on the Databricks logic and TPC-DI specification. This selects Polars for the experiment, not the production generator. Code quality, declarative configuration, and measured memory behavior are requirements. See [Generator](#generator). |
| Generated characteristics | Decided | Report skew, cardinality, duplicate rate, schema variants, and volume as measured from the produced files, not from configured rates. A configured rate describes intent: it stays at 2 percent even if an injector stops working. Every injected problem therefore needs a measurable signature. |
| Cash streaming | Decided | CashTransaction is the first stream. Use Kafka and Flink to retain cash events and produce balances and window results. Add a stable event identity before publication. Direct generation versus timed replay, window sizes, cutoffs, and sink mechanics remain open. |
| TPC-DI baseline | Decided | Retain source meanings, source-to-target rules, and historical-plus-daily processing except for explicit project changes. Extend the daily sequence beyond the benchmark's two daily batches; duration remains open. Cash streaming changes delivery and execution. Silver schemas and ML features are additions. |
| ML target | Decided | Forecast future net account cash movement. Add it to recorded cash to obtain projected cash. The horizon, model, features, and evaluation thresholds remain open. |
| Cash policy | Decided | Flag projected account cash below a configured minimum keyed by the customer's tier. TPC-DI supplies the category, while the project supplies the policy. Amounts, rationale, versioning, and invalid-tier handling remain open. |
| ML generation | Decided | Design predictive behavior with the generator from the start. Document added synthetic behavior and preserve related records. The behavior model is open. |
| Serving topology | Proposed | Flink supplies cash/features; a separate worker calls the model service and applies the policy; an API exposes retained alert state. Keep inference outside cash ingestion. |
| Obligations | Deferred | A synthetic obligation schedule and payment-coverage model are future work. The first version uses forecast total movement and the tier-based minimum. |
| Document organization | Partly decided | The owner wants a full project data specification based on TPC-DI, with the README derived from it. The specification holds the data definitions: sources, zones and silver rules, the gold warehouse, cash stream data, and ML data. This document records choices and rationale, [Status](Status.md) records progress, and `research/` records evidence. The specification's editorial result awaits review. |

## Design considerations by area

These constraints guide storage, history handling, and generation. [Status](Status.md) records implementation gaps and measurements.

### Storage and zones

| Concern | Design rule or option |
| --- | --- |
| Write access | Separate staging and lakehouse buckets are proposed. Independent policies need distinct writer credentials to restrict changes to original files. |
| Partition dates | Keep event dates distinct from arrival and batch dates. If a query treats arrival partitions as event-date partitions, it can miss older events delivered later. Event-date partitioning is an option for event-date queries; check both query access and file sizes. |
| Table locations | Use explicit locations for external tables. This avoids managed-table creation failures when SeaweedFS reports the target prefix as an existing directory ([storage behavior](IaC.md#72-every-lakehouse-table-is-external)). |
| Partition discovery | For Hive-registered Parquet tables, register new partitions through a catalog write or a partition-repair operation. Writing a new `dt=` directory alone does not make it visible to Trino ([partition registration](IaC.md#81-a-path-write-is-not-a-table-write)). |
| Updates | Choose the table format with the silver/gold update rules. Plain Parquet does not provide transactional `MERGE`; it needs explicit append or partition-replacement logic. The dbt-spark `merge` strategy needs a supported format such as Delta Lake, Iceberg, or Hudi. |

### History and SCD2

Historical customer and account actions carry only changed fields. Each new version needs the previous value of every omitted field. An explicitly empty element clears a value to NULL. If the parser converts omitted and empty fields to the same NULL value, carry-forward can restore a value that should have been cleared. Preserve that distinction before constructing versions. The [historical action rules](Data-Specification.md#historical-actions-xml) define the source behavior.

Spark can carry values forward with `last(column, ignorenulls=True)` over an ordered window, provided that explicit clears are handled separately. PostgreSQL 16 has no `IGNORE NULLS` for window functions and needs another method, such as grouping changes. Building versions in Spark and storing them in PostgreSQL is proposed.

TPC-DI defines daily versioning and the propagation of customer changes into accounts and company changes into securities. The silver/gold model must implement these [history rules](Data-Specification.md#temporal-rules) or state each project change.

### Generator

The owner authorized an autonomous fresh Polars generator POC. The production engine choice remains open. The [POC guide](../generator/poc_polars/README.md) defines its scope and commands; the [working record](records/Polars-POC.md) holds measurements and verification results. The [feasibility report](research/Generator-Feasibility.md) describes the Databricks generation expressions, dataset dependencies, compatibility findings, and required corrections. Its earlier Spark recommendation remains research evidence.

Polars is recommended for the POC because its column expressions fit the reference's PySpark transformations and its Python interface fits the NumPy customer scheduler and configurable generation rules. Generate bounded batches, retain state across batch boundaries, and persist shared intermediate records. Stable IDs and random values must not depend on batch size or execution order. Streaming execution alone does not establish bounded memory for every operation.

DuckDB is the alternative if large joins, sorts, window calculations, and persistent intermediate tables dominate the implementation. It can spill supported operations to disk, but some queries can still exceed memory. Both engines need explicit customer/account histories, coherent trade/cash/holding relationships, and exporters for XML, delimited files, and fixed-width FINWIRE. No comparative generator benchmark has been run. The POC must measure correctness, peak memory, temporary disk, and output throughput before a scale decision.

Before reuse, check noncompleted trade amounts, account/security holding continuity, FINWIRE packaging and ordering, and native error coverage against the specification. Validate a coherent baseline before adding coursework faults. Keep native source irregularities, synthetic business behavior, and injected delivery/data faults separately configurable and measurable.

### Streaming

CashTransaction is selected; the [candidate comparison](#streaming-candidates) records why. Direct publishing from the generator and a timed replay are delivery options. Delay, reordering, and duplicate delivery must be injected and measured separately from the business history. The [research note](research/Streaming-Candidates.md) describes event identity, timestamps, and replay checks.

### Machine learning

The [selected use case](#selected-use-case) defines the target and policy. Apply the [generation and evaluation](#generation-and-evaluation) checks before selecting a model. The model, feature set, horizon, and serving implementation remain open.

## Open questions

| Question | Depends on | Current state |
|---|---|---|
| How does each data problem appear in TPC-DI data? | The requirements above and the [data specification](Data-Specification.md) | The source scope is decided; fault definitions and rates remain open. [From TPC-DI to the project](#from-tpc-di-to-the-project) separates original behavior from extensions. Example: incremental Trade rows carry two CDC fields that historical rows lack. Whether this native layout difference counts as the rubric's schema evolution is part of the question. |
| How does cash enter the stream and become final results? | Cash gold rules, feature definitions, and delivery behavior | Direct generation versus timed replay, event-ID format, bootstrap boundary, ordering, windows, lateness, batch-date mapping, lookups, and daily finalization remain open. |
| What forecast horizon, prediction cutoff, and scoring cadence do we use? | The account-review use case and available history | Future net cash movement is selected. Define the interval and timing before finalizing labels and features. |
| What minimum does each tier require under our policy? | Policy rationale and generated balance/movement distributions | The tier mapping is accepted. Amounts, versioning, and missing/invalid-tier behavior remain open. |
| Which features, generator behavior, model, and metrics do we use? | Forecast timing and coherent account/trade/cash history | Design them together. Compare historical-only and recent-feature models on held-out periods and seeds. |
| How are forecasts and alerts served and corrected? | Feature availability, late events, dimension freshness, and policy versions | The separate scoring worker and API are proposed. Define result identity, latency, retries, and revision/clearing behavior. |
| Which text sources and retrieval task does the platform serve? | The coursework's text-processing requirements and a defined user task | Select document sources, retrieval purpose, storage, and service contracts separately from the cash forecast. |
| What does the agent platform do for its user? | A defined task, available data/ML services, and access rules | Specify expected outputs, tool permissions, model/framework, and coordination rules before designing agent workflows. |
| What are the silver and gold models? | Original transformation rules and chosen feature definitions | The original warehouse is the gold baseline. Silver schemas, added feature tables, physical layout, and cash sink mechanics remain open. |
| How do Spark, dbt, PostgreSQL, and Trino divide the work? | The gold model | The division of transformations, storage, and query serving remains open. |
| Which table format do the lakehouse tables use? | Update handling in silver and gold | Compare update, replay, and connector requirements using the [storage constraints](#storage-and-zones). |
| How do batch reruns and retries behave? | The pipeline design | TPC-DI requires Batch2 to start from the Batch1 result. Rerun and recovery rules are a project choice. |
| Which scale factor gives 100 GB? | The generator engine, duration, and source distribution | Measure bytes from a pilot run, estimate the required scale, then measure the full output. |

## Research notes

Research notes hold evidence for a design choice. They do not decide anything.

| Note | Question it answers | Scope |
|---|---|---|
| [Generator feasibility](research/Generator-Feasibility.md) | Should the project adapt the Databricks TPC-DI generator to Spark, or write a Polars version? | Source inspection, compatibility-probe evidence, and correction requirements. |
| [Streaming candidates](research/Streaming-Candidates.md) | Which TPC-DI records could become stream events, and how could a replay work? | Source semantics, replay constraints, and a proposed correctness check. |
| [Reference implementations](research/Reference-Implementations.md) | What does the AlaaAlmutawa TPC-DI implementation show, and where does it stop? | Specific observations, not a complete audit. |
