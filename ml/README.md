# Layer 2: Machine Learning Platform

This directory holds the Layer 2 machine learning, feature store, serving, and document retrieval components described in [ML Platform](../docs/ML-Platform.md).

| Subdirectory | Purpose |
| --- | --- |
| `feature_repo/` | Feast feature store definitions (offline, online, and vector views). |
| `training/` | Core ML package (dataset retrieval, baseline and gradient-boosted models, evaluation, MLflow logging). |
| `kfp/` | Kubeflow Pipeline definitions and compiled workflow manifests. |
| `serving/` | KServe predictor runtime and the Scoring and Alert FastAPI service. |
| `rag/` | Brokerage document chunking and embedding generation jobs. |
