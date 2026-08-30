# LLM providers

AgentMesh supports an internal LLM provider contract without making the Engine
or queue depend on a vendor. An Agent selects it with `runtime: "llm"`; its
`protocol` chooses a provider registered in `internal/llm.Registry`.

## OpenAI-compatible provider

The first provider uses the Chat Completions JSON shape documented by the
[official OpenAI API reference](https://developers.openai.com/api/reference/resources/chat).
It intentionally uses `net/http` instead of an SDK so OpenAI-compatible private
or third-party endpoints can implement the same contract.

Register an Agent with an endpoint base URL and explicit model:

```json
{
  "name": "Reasoning Agent",
  "runtime": "llm",
  "protocol": "openai",
  "endpoint": "https://api.openai.com",
  "model": "your-enabled-model",
  "system_prompt": "Answer concisely and cite uncertainty.",
  "capabilities": ["reasoning", "summarization"]
}
```

If the endpoint already ends in `/v1`, AgentMesh appends
`/chat/completions`; otherwise it appends `/v1/chat/completions`. Query strings,
fragments, redirects, compressed responses, oversized bodies, invalid JSON, and
empty completions are rejected. The existing outbound HTTP policy performs the
same dial-time SSRF checks used by remote Agents.

The Run input becomes a `user` message. A non-empty `system_prompt` becomes the
preceding `system` message. The first assistant message is persisted as the Run
output; token usage is decoded by the provider contract but is not yet added to
the Run domain model.

## Credentials and retries

The provider reuses `AGENTMESH_AGENT_AUTH_CONFIG`. Configure the generated Agent
ID with a Bearer credential reference and restart the process once to load the
non-secret mapping; subsequent environment or mounted-file secret rotation is
visible on every request without another restart. Secrets are neither persisted
nor returned by the API.

HTTP `429` and `5xx` errors and transport failures are classified as retryable.
Authentication, invalid endpoint/schema, body-limit, and ordinary `4xx` errors
are permanent provider failures. The current Engine still applies its configured
attempt policy uniformly; every request receives the Run attempt identity in an
`Idempotency-Key` header. The provider call inherits
`AGENTMESH_ATTEMPT_TIMEOUT` and cancellation.

## Extension boundary

Adding another LLM protocol requires one `llm.Provider` implementation and one
registry entry in application wiring. It does not require changes to Engine,
Run lifecycle, queue, worker, Router, Workflow, or the public Run API.
