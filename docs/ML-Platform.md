# Layer 2: Machine Learning Platform

The machine learning platform predicts whether a brokerage account will close within 30 days. A separate tier-based policy creates a broker outreach task from that probability. This target design uses Feast for offline and online features, Kubeflow Pipelines for training, MLflow for tracking and data versioning, KServe for inference, and a document retrieval pipeline for Layer 3 agents.

---

## 1. Prediction Task and Outreach Policy

The model predicts account closure within 30 days of the prediction time. The account's broker receives an outreach task when the probability reaches the threshold for the customer's tier. Tier 1, the wealthiest group, has a lower threshold than tier 3. The policy is separate from the model, so changing a threshold needs no retraining.

```text
closure_probability = model(account_features)
threshold           = configured_threshold[customer_tier]
outreach_required   = closure_probability >= threshold
```

| Data | Source | Role |
| --- | --- | --- |
| **Account closure** | `Account.txt` and `gold.dim_account` | Observed outcome for the 30-day label |
| **Account owner and tier** | `gold.dim_account` and `gold.dim_customer` | Select the customer's tier at prediction time and the account's broker |
| **Closure probability** | Layer 2 model | Estimates whether an open account will close within 30 days |
| **Tier threshold** | Outreach policy | Converts a probability into an action without changing the label |
| **Current cash balance and activity** | Gold and Flink cash-flow features | Provide observed behavior, not a minimum-cash requirement |

### Synthetic Account Behavior in the Generator

The generator separates business behavior, the hidden attrition signal, source recording, and delivery. About 3% of accounts close each year. Closure risk depends on behavior profile, tenure, tier, and recent losses. About 80% of closures follow a hidden leaving period of 1 to 8 weeks: deposits and buys stop, trading slows, holdings are sold in steps, and cash leaves. About 2% of accounts per year show similar outflows but stay. Some closures have no warning.

Hidden profiles, leaving periods, and look-alikes appear only in the generator summary, never in model features. The generator keeps trades, holdings, and cash transactions consistent. Cash withdrawals cannot exceed available cash; margin accounts may borrow within their limit. The publisher adds delivery problems separately. [Data Platform](Data-Platform.md) defines the generation and delivery model.

---

## 2. Feature Store and Data Contracts (Feast)

Each feature has one definition and one producer. This keeps training and serving values consistent while batch and stream producers run on different schedules.

| Data Asset | Producer | Contents and Path |
| --- | --- | --- |
| **Batch features (`gold.feat_*`)** | DP3 from Gold | Tenure, tier, trading activity, holdings and value, canceled watches, and recent account updates; Airflow materializes these features into the online store |
| **Cash-flow features** | Flink Job 2 | Withdrawals, deposits, and sale settlements over 1, 7, and 30 days; external net flow; days since the last deposit; current cash balance |
| **Cash-flow feature delivery** | Feast push job | Reads `brokerage.account_cash.features` and pushes rows into the offline and online stores |
| **Online features** | Feast | Latest feature values for daily scoring, triggered rescoring, and agent requests |
| **Training label** | Account closure history | Whether an account open at prediction time closes within the next 30 days |

In the daily delivery period, cash transactions reach the platform only through Kafka. Flink Job 2 therefore produces the cash-flow features; DP3 does not calculate a second version from daily files. Daily scoring uses these features after DP3. The live account view and large-withdrawal trigger also need fresh stream results.

### Point-in-Time Correctness and Materialization

- **Time cutoff**: Feast's default point-in-time join filters on `event_timestamp` and returns the newest `created` value. It does not automatically enforce `created <= prediction_time`. The `filter_by_created_timestamp=True` option adds that filter only for offline stores that support it.
- **Label availability**: A prediction time without a complete 30-day outcome window has no label yet, not a negative label. Hidden generator state and future outcomes are not input features.
- **Batch materialization**: An Airflow pipeline incrementally materializes the batch features into the online store after daily processing.
- **Streaming push**: A separate push job consumes cash-flow feature rows and writes both stores, so a slow Feast call does not block Flink.
- **Corrections**: A rerun replaces the affected feature rows. MLflow versions each training run's full dataset with its model because mutable feature tables alone cannot reproduce an earlier training run.

---

## 3. Training and Experiment Tracking (Kubeflow Pipelines and MLflow)

### Kubeflow Training Pipeline

The training workflow runs as a multi-step Kubeflow Pipeline:
1. **Data retrieval**: Joins account closure labels with Feast offline feature views at each prediction time.
2. **Time-based split**: Separates training, validation, and test periods in time and keeps future outcomes out of input features.
3. **Model training**: Trains the attrition model and compares it with a simple rule. A comparison with and without cash-flow features can measure the value of those inputs.
4. **Evaluation**: Compares predictions and broker outreach results on held-out outcomes.

Before platform pipelines are built, a small generator sample checks whether the synthetic signal supports the task. A quick model must beat the rule "no deposit and net outflow over 30 days," while the rule's precision stays below about 30%. This is a generator signal check, not a selected production model or deployment threshold.

### MLflow Tracking and Registry

Each training run records its parameters, metrics, model artifacts, and full training dataset version in MLflow. The registry supports the course-required `latest` or `production` model tags. Dataset versions stay linked to models even when corrections replace the source feature rows.

---

## 4. Model Serving and Outreach Scoring (KServe)

KServe supplies the course-required inference service using the `model-inside-image` pattern. Model inference stays outside Flink's ingestion path for cash transactions. Flink publishes feature rows and rescoring requests to separate topics, so a slow feature store or scoring worker cannot stop stream processing.

| Scoring Path | When | Scope |
| --- | --- | --- |
| **Daily scoring** | After DP3 | All open accounts |
| **Triggered rescoring** | When Flink Job 2 detects a large withdrawal | One account, through `brokerage.account.rescore` and a scoring worker |
| **Agent request** | When a broker asks about an account | One account, through the inference service exposed to the Layer 3 tool |

Every path reads features defined once in Feast. The model returns a closure probability; the outreach policy applies the tier threshold. The [Agent Platform](Agent-Platform.md) combines this result with account data and retrieved documents.

---

## 5. Document Retrieval Pipeline (RAG)

An Airflow pipeline prepares brokerage documents for retrieval-augmented generation (RAG) by Layer 3 agents:
1. **Ingestion and chunking**: Reads documents from storage and splits them into chunks with source metadata.
2. **Embedding and Feast storage**: Generates embedding vectors and stores chunks and vectors in Feast for retrieval.
3. **Governance**: Validates the pipeline outputs and publishes lineage and quality results to DataHub.

---

## 6. Layer 2 Design Decisions and Rationale

The 30-day horizon assumes routine broker outreach with follow-up, not emergency contact. A 3-day or 7-day horizon risks too little time to act. A 14-day horizon fits a shorter response process but leaves less time for follow-up. The 30-day prediction horizon does not guarantee 30 days of warning: evaluate warning time from the first flag to closure.

- **Business action**: A prediction must support broker outreach before an observed account closure.
- **Observed label**: Account status supplies the outcome without an invented fraud label or payment obligation.
- **Batch and stream inputs**: The task must use both offline account history and streaming cash-flow features.
- **Policy flexibility**: Tier thresholds must change without retraining the model.
- **Ingestion isolation**: Slow inference must not stop the cash transaction stream.

| Decision | Selected Choice | Main Reason |
| --- | --- | --- |
| **Primary ML task** | Account closure within 30 days | Gives brokers time to contact accounts at risk; closures already appear in the source data |
| **Outreach policy** | Tier-based probability thresholds, separate from the model | Supports different outreach priorities without changing labels |
| **Evaluation** | Model against a simple rule; optional comparison with and without cash-flow features | Tests whether the model improves on a rule and whether cash-flow features help |
| **Feature ownership** | DP3 for batch features; Flink Job 2 for cash-flow features | Avoids two producers with different calculations for the same feature |
| **Stream isolation** | Separate topics for feature delivery and rescoring | Keeps Feast and model latency off the Flink processing path |
| **Training reproducibility** | Full training dataset version linked to the model in MLflow | Feature corrections replace rows and cannot preserve each past training input by themselves |

<details>
<summary>Alternative ML Tasks Considered</summary>

| Candidate | Why Not Selected |
| --- | --- |
| **Minimum-cash alert** | A brokerage rejects a withdrawal above available cash. The customer sells first, so the forecast has no defined harm for an alert to prevent. |
| **External net flow forecast** | Changes the target but leaves the same weak business action. |
| **Fraudulent withdrawal** | Requires an invented label that the source data does not supply. |
| **Margin-call warning** | Covers only margin accounts and needs margin requirements based on holdings value. |
| **Next-week trading activity** | Has a smaller business cost than account closure. |

</details>
