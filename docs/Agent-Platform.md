# Layer 3: Agent Platform

The agent platform connects language models to brokerage data, live cash-shortfall predictions, and retrieved policy documents through governed gateways, MCP tools, and multi-agent coordination.

---

## 1. Agent Use Case and MCP Tool Plan

Brokerage operations and support staff query the agent platform to investigate account cash-shortfall alerts, review customer trading and balance history, and verify brokerage policies and fee rules. Answering these requests requires combining structured account data, live ML forecasts, and policy documents in one workflow.

| MCP Server | Backing Service | Input | Output |
| --- | --- | --- | --- |
| **Account & Trade Data Tool** | Layer 1 and Layer 2 Web API & Feast online store | `account_id` or `customer_id` | Account profile, customer tier, current cash balance, recent trades, and streaming cash velocity features |
| **Cash Forecast & Alert Tool** | Layer 2 Scoring & Alert API (KServe + Policy service) | `account_id` | `forecast_net_movement`, `projected_cash`, tier minimum, projected shortfall, and model and policy versions |
| **Policy & Document Search Tool (RAG)** | Layer 2 Feast vector store | Natural-language query and optional filters | Top-k relevant chunks from brokerage tier policies, fee schedules, and trading rules |

---

## 2. LLM Inference Platform

- **Model serving**: Hosts the custom chat LLM and embedding model behind an inference pool and router, exposing an OpenAI-compatible inference API.
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

- **Coordinator and specialist agents**: A Coordinator Agent receives user questions, decomposes the task, delegates structured data lookup and ML risk evaluation to two specialist agents over A2A through the gateway, and synthesizes the final response.
- **Agent registry**: Publishes the catalog of deployed agents, their A2A endpoints, and their authorized MCP servers for governance and discovery.
- **Autoscaling**: Scales coordinator and specialist agent replicas automatically from workload metrics.
- **Agent fleet warm-up**: Maintains warm sandboxed agent instances to minimize cold-start latency while sharing worker capacity across agents.
- **Prompt management**: Versions system prompts externally so prompt updates are tracked and deployed through CI/CD without hardcoded strings in agent code.

---

## 5. A/B Testing and Layer 3 Design Rationale

- **A/B testing**: Splits traffic at the gateway across candidate agent model configurations and LLM versions, comparing latency, token cost, tool-call error rates, and response quality on monitoring dashboards before full rollout.
- **Gateway-mediated architecture**: Routing all LLM, MCP, and A2A calls through a single gateway centralizes authentication, PII redaction, rate limiting, and telemetry (token counts, TTFT, per-agent and per-tool call and failure rates) without duplicating security logic inside each agent or tool server.
