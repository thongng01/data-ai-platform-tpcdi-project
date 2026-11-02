# Layer 3: Agent Platform

This directory holds the Layer 3 LLM serving configurations, gateway policies, MCP tool servers, and multi-agent definitions described in [Agent Platform](../docs/Agent-Platform.md).

| Subdirectory | Purpose |
| --- | --- |
| `mcp/` | FastAPI MCP tool servers (account and trade data, cash forecast and alerts, and policy RAG search). |
| `coordinator/` | Coordinator and specialist agent definitions, A2A routes, and KEDA autoscaling manifests. |
| `gateway/` | Unified gateway policies for LLM, MCP, and A2A routing, authentication, rate limits, and PII guardrails. |
| `prompts/` | Versioned system prompts. |
| `registry/` | Agent registry catalog and evaluation datasets. |
