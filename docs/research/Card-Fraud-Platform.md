# Card Fraud Platform: engineering references

The [Card Fraud Platform](https://github.com/cvinhnguyen/card-fraud-platform/tree/cc34515304815774a5dad6f796683b46c27f93ac) provides examples for engineering questions in this project. Links below pin the inspected source to commit `cc34515304815774a5dad6f796683b46c27f93ac`. The comparison records source inspection on 2026-10-27; its services and benchmarks were not run here. These are possible references for the [project design](../Project-Design.md), not adopted designs.

## Connecting the layers around a task

The [project overview](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/README.md#business-domain) connects authorization events, delayed fraud labels, a scoring decision, and card-support questions. Each layer has a specific consumer. For our design, use this as a prompt to name the user, prediction or question, required data, and resulting action. It supplies a way to constrain the brokerage extensions without adopting the card domain.

## Batch recovery, validation, and publication

The export load appends dates newer than the maximum date already stored. The separate stream-file load uses a Spark checkpoint to remember consumed files, including files added to older event-date folders. These are different recovery mechanisms because the source delivery rules differ. See [export loading](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/spark/ingest_bronze.py#L79-L114).

DP1 writes the nightly export to a lakeFS branch. The pipeline commits that branch, checks the data, merges it into the published branch, and then triggers DP2. A failed data check prevents publication of that export. The [DAG](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/airflow/dags/dp1_source_to_bronze.py#L37-L60) also declares lineage for the lakeFS merge because a Spark listener cannot observe this API operation. DP2 declares its source and target datasets explicitly where automatic MERGE lineage is incomplete. See [DP2 lineage](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/airflow/dags/dp2_bronze_to_gold.py#L24-L29).

Validation results are saved before a separate task publishes them to DataHub and raises an error for failed checks. This preserves information needed to diagnose a rejected batch. See [validation and reporting](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/airflow/dags/common.py#L99-L127).

For our pipelines, the useful design questions are what a retry reads, what it may overwrite, when output becomes visible, and what blocks the next stage. We can answer these before choosing lakeFS or another publication mechanism. The branch protects the export table; the DAG's snapshot and stream-file writes occur outside that branch, so it does not make the whole bronze load atomic.

One source assumption needs special care: the maximum-date check skips a corrected file or newly delivered folder whose export date is older than the recorded maximum. Reusing that algorithm requires an explicit ordered, immutable delivery contract or a separate correction path.

## Performance comparisons that preserve meaning

The [join benchmark](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/spark/skew_join.py#L29-L78) compares baseline, salting, AQE, and broadcast over the same join. It controls Spark settings and reads task duration and shuffle size from the Spark API. Its [runner](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/spark/skew_join.py#L92-L125) performs a warm-up and reports the median of three timed runs by default. A no-op sink forces computation while excluding output-file writes from the timing.

The [deduplication comparison](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/spark/scale_dedup.py#L38-L47) changes how duplicate rows are selected while retaining the same latest-record rule. It records runtime and spill, checks unique output identifiers and successful location mapping, and prints a checksum for comparison. See [output checks](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/spark/scale_dedup.py#L81-L96).

For our coursework, this gives a useful experiment: identify a costly operation, predict the failure mechanism, compare alternative implementations on fixed input, and explain the change using both output checks and execution metrics. This teaches why an optimization works and when it stops being useful.

The equality checks need strengthening before reuse as proof of correctness. The join benchmark does not compare outputs. The deduplication checksum includes only identifier, creation time, and amount; equal totals cannot establish equality of every field or temporal relationship. A small exact comparison and explicit domain invariants can cover those gaps before large runs.

## Small service interfaces and failure tests

The scoring API separates [feature retrieval and model prediction interfaces](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/services/scoring_api/app/clients.py#L20-L27), [decision policies](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/services/scoring_api/app/policy.py#L8-L37), and [request orchestration](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/services/scoring_api/app/scorer.py#L24-L60). The scorer receives its dependencies through its constructor. Tests can therefore supply fake clients that fail or respond slowly and check fallback decisions without starting Feast, Redis, or KServe. See [failure tests](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/services/scoring_api/tests/test_scorer.py#L17-L42).

This is a useful software-design reference for our eventual inference API or agent tools: separate the business decision from network clients, define the inputs and outputs of each dependency, and test behaviour when a dependency fails. The lesson is the boundary and its contract. The specific card-payment fallback policy belongs to that application. Tests with the real dependencies remain necessary to check integration.

## Event time and independent expected results

The [Flink checker](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/flink/check_windows.py#L64-L89) computes expected counts from unique source event identifiers and event-time buckets. It compares those counts with baseline, lateness-handling, and deduplicating jobs. The expected result exists independently of Flink. For the selected cash stream, an independent calculation can establish how delayed and repeated movements change sums and balances.

The checker takes the maximum emitted count as the final count because updates only add events. A cash total can decrease, so that method cannot identify its final update. The [job](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/pipelines/flink/auth_windows.py#L102-L127) also uses a bounded, single-partition replay. Live traffic and multiple partitions require separate checks.

The [feature-store design](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/docs/feature_store.md#train--serve-consistency) reasons about which values exist at prediction time. Its one-millisecond offset and 60-day label maturity follow its own data contract. Our label availability, event time, and processing delay need their own definitions; replaying the same computation alone does not prove historical availability.

## Reusable computation behind orchestration

The [Kubeflow components](https://github.com/cvinhnguyen/card-fraud-platform/blob/cc34515304815774a5dad6f796683b46c27f93ac/ml/pipelines/train_fraud_model.py#L26-L85) call a shared Python library. The notebook uses the same data, training, and evaluation classes. We can use this separation to understand and exercise a computation before adding scheduling, containers, or pipeline wiring. It also keeps changes to core logic in one place.
