# Layer 3: Agent Platform

The agent platform connects language models to brokerage data, 30-day account closure predictions, and retrieved documents through gateways, Model Context Protocol (MCP) tools, and agent coordination. This document defines the target platform and its course-required scope.

---

## 1. Agent Use Case and MCP Tool Plan

Brokers use the agent platform to investigate account attrition risk before they contact a customer. The agent combines structured account data, a 30-day closure probability, and retrieved brokerage documents. A tier-based outreach policy converts the probability into an outreach task.

| MCP Server | Backing Service | Input | Output |
| --- | --- | --- | --- |
| **Account and Trade Data Tool** | Layer 1 live tables and account history; Feast features | `account_id` or `customer_id` | Account profile, customer tier, current cash balance, recent trades, and cash-flow features |
| **Account Attrition Tool** | Layer 2 inference service and outreach policy | `account_id` | Probability of closure within 30 days and the tier-based outreach result |
| **Document Search Tool (RAG)** | Layer 2 Feast retrieval store | Natural-language query and filters | Relevant document chunks with source references |

For an account inquiry, the agent retrieves the account's history and live cash balance, requests an attrition prediction, and retrieves documents that support its answer. The broker uses the result to plan outreach. [ML Platform](ML-Platform.md) owns the prediction task, feature producers, and outreach policy.

---

## 2. LLM Inference Platform

- **Model serving**: Uses llm-d for the course-required LLM inference pool and router, with custom model configuration and an OpenAI-compatible inference API. Layer 2 supplies the document embedding pipeline.
- **Global model configuration**: Agents consume a shared declarative model configuration that routes all completions through the agent gateway rather than calling model endpoints directly.
- **Benchmarking and optimization**: Measures token throughput, time-to-first-token (TTFT), and end-to-end latency across concurrency levels and serving configurations.

---

## 3. Gateways and Access Policies

All traffic between users, agents, MCP servers, and LLM endpoints passes through a unified gateway layer:
- **LLM Gateway**: Enforces API key authentication, request rate limits, token budgets, and PII masking and safety guardrails on prompts and responses.
- **MCP Gateway**: Serves as the single entry point for all MCP tool calls so agents never connect directly to backend MCP servers; enforces per-agent tool authorization rules.
- **A2A Gateway**: Mediates agent-to-agent (A2A) requests between the coordinator agent and specialist agents with routing and access policies.

---

## 4. Agent Coordination, Registry, and Fleet Scaling

- **Coordinator and specialist agents**: A coordinator agent receives user questions, splits the task, delegates structured data lookup and account attrition evaluation to two specialist agents over A2A through the gateway, and combines their results into a response.
- **Agent registry**: Publishes the catalog of deployed agents, their A2A endpoints, and their authorized MCP servers for governance and discovery.
- **Autoscaling**: Scales coordinator and specialist agent replicas automatically from workload metrics.
- **Agent fleet warm-up**: Maintains warm sandboxed agent instances to minimize cold-start latency while sharing worker capacity across agents.
- **Prompt management**: Versions system prompts externally so prompt updates are tracked and deployed through CI/CD without hardcoded strings in agent code.

---

## 5. A/B Testing and Layer 3 Design Rationale

- **A/B testing**: Splits traffic at the gateway across candidate agent model configurations and LLM versions, comparing latency, token cost, tool-call error rates, and response quality on monitoring dashboards before full rollout.
- **Gateway-mediated architecture**: Routing all LLM, MCP, and A2A calls through a single gateway centralizes authentication, PII redaction, rate limiting, and telemetry (token counts, TTFT, per-agent and per-tool call and failure rates) without duplicating security logic inside each agent or tool server.
