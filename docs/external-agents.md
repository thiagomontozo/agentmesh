# External HTTP Agents

AgentMesh can execute multiple language-independent Agents through one runtime and one wire contract. An Agent implementation only needs to expose `POST /v1/runs` according to [Agent Protocol V1](agent-protocol-v1.md).

## Register two Agents

```bash
curl -X POST http://localhost:8080/api/v1/agents \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"legal-agent",
    "runtime":"remote",
    "protocol":"http",
    "endpoint":"http://legal-agent:9000",
    "capabilities":["legal-search","legal-analysis","summarization"]
  }'

curl -X POST http://localhost:8080/api/v1/agents \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"code-agent",
    "runtime":"remote",
    "protocol":"http",
    "endpoint":"http://code-agent:9001",
    "capabilities":["code-review","testing","debugging"]
  }'
```

Create a Run with the returned Agent ID:

```bash
curl -X POST http://localhost:8080/api/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"agt_REPLACE_ME","input":"Analyze this legal case"}'
```

The Run's explicit `agent_id` determines the endpoint. Capabilities remain metadata and are not used for automatic routing.

## Interoperability guarantee

`TestTwoExternalAgentsUseTheSameProtocolAndRuntime` starts two independent black-box HTTP endpoints, registers both through the public AgentMesh API, submits one Run to each, and verifies:

- only the selected endpoint receives each execution;
- both endpoints receive the same Agent Protocol V1 request shape;
- Run ID, Agent ID, attempt, idempotency key, and input are preserved;
- each endpoint returns an independent output;
- the Engine uses one registered HTTP runtime for both Agents.

The test servers are written in Go only as lightweight test infrastructure. No Go interface crosses the network, so a Python, Node.js, Java, Rust, or other implementation can replace either endpoint without changing AgentMesh.

## Adding another Agent

A third HTTP Agent using Protocol V1 can be added through `POST /api/v1/agents` while AgentMesh is running. It does not require a new Go executor, source-code change, recompilation, or restart. It does require the caller to select its `agent_id`; discovery and automatic routing are separate future capabilities.
