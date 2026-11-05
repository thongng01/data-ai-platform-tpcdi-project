# Layer 2: Machine Learning Platform

This directory is the component root for the target Layer 2 account attrition, feature store, serving, and document retrieval system described in [ML Platform](../docs/ML-Platform.md). The model predicts account closure within 30 days; a separate tier-based policy supports broker outreach. The components are not implemented yet.

| Subdirectory | Purpose |
| --- | --- |
| `feature_repo/` | Feast feature store definitions (offline, online, and vector views). |
| `training/` | Dataset retrieval, rule baseline and attrition model training, evaluation, and MLflow data and model versioning. |
| `kfp/` | Kubeflow Pipeline definitions and compiled workflow manifests. |
| `serving/` | KServe predictor runtime and account attrition scoring with the outreach policy. |
| `rag/` | Brokerage document chunking and embedding generation jobs. |
