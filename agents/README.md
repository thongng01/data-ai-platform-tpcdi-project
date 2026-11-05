# Layer 3: Agent Platform

This directory is the component root for the target Layer 3 large language model (LLM) serving configurations, gateway policies, Model Context Protocol (MCP) tool servers, and agent definitions described in [Agent Platform](../docs/Agent-Platform.md). Agents combine account data, 30-day account attrition predictions, and document retrieval to support broker outreach. The components are not implemented yet.

| Subdirectory | Purpose |
| --- | --- |
| `mcp/` | MCP tool servers for account and trade data, account attrition inference, and document retrieval. |
| `coordinator/` | Coordinator and specialist agent definitions, agent-to-agent (A2A) routes, and KEDA autoscaling manifests. |
| `gateway/` | Unified gateway policies for LLM, MCP, and A2A routing, authentication, rate limits, and personally identifiable information (PII) guardrails. |
| `prompts/` | Versioned system prompts. |
| `registry/` | Agent registry catalog and evaluation datasets. |
