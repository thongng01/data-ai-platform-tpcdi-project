# Layer 2: Machine Learning Platform

The machine learning platform forecasts future net cash movement for each brokerage account, applies a tier-based minimum-cash alert policy, manages offline and online features in Feast, runs reproducible training pipelines in Kubeflow Pipelines with MLflow tracking, serves models through KServe, and ingests brokerage documents into a Feast vector store for retrieval.

---

## 1. Prediction Task and Minimum-Cash Policy

The model predicts total signed net cash movement (`forecast_net_movement`) for an account over a future horizon. The policy adds that forecast to the account's current cash balance and flags an alert when projected cash falls below the minimum configured for the account owner's customer tier:

```text
projected_cash = current_cash + forecast_net_movement
minimum_cash   = configured_minimum[customer_tier]
alert          = projected_cash < minimum_cash
```

For example, an account with `current_cash = 10,000` and `forecast_net_movement = -3,000` has `projected_cash = 7,000`. If the owner's customer tier requires `minimum_cash = 8,000`, the policy raises an alert with a projected shortfall of `1,000`.

| Data | Source | Role |
| --- | --- | --- |
| **Current cash** | Batch 1 opening balance plus accepted cash movements from Flink | Observed starting cash balance at prediction time |
| **Forecast net movement** | Layer 2 regression model served on KServe | Predicted sum of signed `CT_AMT` movements over the forecast horizon |
| **Account owner & tier** | `DimAccount.SK_CustomerID` -> `DimCustomer.Tier` (Tiers `1`, `2`, `3`) | Selects the applicable cash threshold using the customer version valid at prediction time |
| **Configured minimum** | Policy configuration keyed by `customer_tier` | Sets the minimum required balance per tier without hardcoding thresholds into training labels |
| **Forecast and alert record** | Scoring and alert service | Stores account ID, as-of timestamp, forecast cutoff, current cash, forecast movement, projected cash, tier, minimum, shortfall, model version, and policy version |

### Synthetic Account Behavior in the Generator
Because standard TPC-DI random cash movements have no intentional predictive signal, the generator models account activity profiles that link historical trading and cash patterns and recent cash velocity to future net cash movement while keeping `Trade`, `CashTransaction`, and `HoldingHistory` consistent. Latent generator parameters and future outcomes are excluded from feature tables, and delivery faults (lateness and duplicates) are injected separately.

---

## 2. Feature Store and Data Contracts (Feast)

Offline and online stores share feature definitions while serving different access patterns:

| Data Asset | Grain | Contents |
| --- | --- | --- |
| **Offline features (`gold.feat_*`)** | One account per feature timestamp | Historical batch features (prior daily balances, rolling inflow and outflow totals, trade-type summaries) and archived streaming window features, each carrying `event_timestamp` and `created` columns |
| **Online features (Feast online store)** | One account | Latest eligible batch and streaming feature values for low-latency scoring |
| **Training label** | One account per prediction time | Sum of unique business `CT_AMT` values after the prediction timestamp up to the forecast cutoff |

### Point-in-Time Correctness and Materialization
- **No future leakage**: Training datasets are assembled with Feast point-in-time joins (`event_timestamp <= prediction_time` and `created <= prediction_time`), ensuring late-arriving records and unclosed future windows never leak into historical features. An incomplete future horizon has no label yet rather than a zero-movement label.
- **Batch materialization**: An Airflow DAG incrementally materializes the latest offline `feat_*` rows into the Feast online store after each daily batch.
- **Streaming push**: Streaming feature jobs push Flink's windowed account cash aggregates (inflow, outflow, net movement, and transaction count) into both the Feast online store and the offline store.

---

## 3. Training and Experiment Tracking (Kubeflow Pipelines and MLflow)

### Kubeflow Training Pipeline
The training workflow runs as a multi-step Kubeflow Pipeline:
1. **Data retrieval**: Builds the training dataset by joining historical account labels against Feast offline feature views as of each prediction cutoff.
2. **Time-based split**: Splits examples chronologically into training, validation, and held-out test periods to prevent temporal leakage across accounts.
3. **Model training**: Trains a heuristic or linear baseline alongside the primary gradient-boosted regression model under two feature sets (batch features only vs. batch + streaming Flink features).
4. **Evaluation and gate**: Evaluates regression error (MAE and RMSE on net cash movement) alongside policy-level alert recall, precision, and warning lead time on the held-out test period.

### MLflow Tracking and Registry
Each training run logs hyperparameters, feature view versions, dataset hashes, evaluation metrics, and model artifacts to MLflow. Models that pass the evaluation gate are registered in the MLflow Model Registry and tagged (`latest` or `production`) for automated deployment.

---

## 4. Model Serving and Alert Scoring (KServe)

Model inference is decoupled from Flink's real-time cash ingestion so model latency never blocks incoming Kafka cash events:

| Step | Component | Responsibility |
| --- | --- | --- |
| 1 | **Flink & Feast push** | Updates running cash balances and publishes windowed streaming features to Feast. |
| 2 | **Scoring worker** | Reads the account's latest eligible online features from Feast on a schedule or update trigger and calls KServe. |
| 3 | **KServe InferenceService** | Serves the registered `production` model using the `model-inside-image` pattern and returns `forecast_net_movement`. |
| 4 | **Policy & alert service** | Resolves the account owner's current `DimCustomer.Tier`, computes `projected_cash` and `shortfall`, and writes an idempotent alert record. |
| 5 | **Scoring & Alert Web API** | Exposes current cash, latest forecasts, active shortfall alerts, and model and policy versions to applications and Layer 3 MCP tools. |

---

## 5. Document Retrieval Pipeline (RAG)

An Airflow-orchestrated RAG pipeline ingests brokerage documents (account tier policies, fee schedules, trading rules, and company disclosures) for Layer 3 agents:
1. **Ingestion and chunking**: Reads raw Markdown and text documents from storage and splits them into semantic chunks with metadata (document ID, section, effective date).
2. **Embedding and Feast storage**: Generates dense embedding vectors for each chunk and writes the chunks and vectors into a Feast feature view backed by a vector-capable online store and Parquet offline store.
3. **Governance**: Validates chunk counts, embedding dimensions, and null rates, and publishes pipeline lineage and validation assertions to DataHub.

---

## 6. Layer 2 Design Decisions and Rationale

- **Defined business action**: The prediction must drive a concrete brokerage decision with an observable ground-truth label.
- **Stream utilization**: Real-time Flink cash features must add measurable signal over daily batch history alone.
- **Policy flexibility**: Changing a business threshold should not invalidate trained models or historical labels.
- **Ingestion isolation**: Slow or unavailable ML inference must not stall stateful stream processing.

| Decision | Selected Choice | Main Reason |
| --- | --- | --- |
| **Primary ML task** | Forecast future net account cash movement (regression) + tier-based minimum-cash policy | Connects the `CashTransaction` stream, `FactCashBalances`, and `DimCustomer.Tier`, and allows changing tier minimums without relabeling or retraining the model. |
| **Evaluation design** | Compare (1) historical heuristic baseline, (2) batch-only ML model, and (3) batch + streaming ML model on held-out time periods | Directly measures whether Flink's streaming features improve forecast accuracy and shortfall detection over daily batch features. |
| **Serving topology** | Asynchronous scoring worker calling KServe outside Flink | Keeps KServe network calls off the Flink checkpoint and watermark path. |
| **Explicit obligation schedules** | Deferred; forecast total net cash movement against tier buffers | TPC-DI has no account payment-schedule or margin-call ledger (`Financial.FI_LIABILITY` and `FI_MARGIN` belong to issuing companies, not brokerage accounts). |

<details>
<summary>Alternative ML Tasks Considered</summary>

| Candidate | Prediction Entity | Outcome | Why Not Selected as Primary |
| --- | --- | --- | --- |
| **Direct low-cash classifier** | Account | Binary low-cash flag | Bakes a fixed cash threshold into training labels; changing a tier minimum requires relabeling and retraining. |
| **Unusual cash activity detection** | Account or window | Anomaly flag | Requires inventing synthetic fraud labels unrelated to TPC-DI rules. |
| **Trading-activity prediction** | Account | Future completed trade count or binary trade flag | Viable secondary task, but depends primarily on trade history rather than the continuous cash stream. |
| **Limit-trade completion** | Trade | Whether a limit order fills within a horizon | Depends on exchange order-book dynamics that TPC-DI does not model. |
| **Market-volume forecast** | Security | Next-day trading volume | Uses daily market history only; account cash events carry no security symbol. |
| **Prospect conversion** | Prospect | Whether a prospect opens an account | Prospects without accounts have no cash transactions or streaming events. |

</details>
