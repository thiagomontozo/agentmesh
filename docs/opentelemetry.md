# OpenTelemetry

AgentMesh supports opt-in OTLP/HTTP traces and metrics while preserving the
existing Prometheus endpoint. With no OTLP endpoint configured, the SDK uses
no-op providers and starts no exporter goroutines.

Configure the standard OpenTelemetry environment variables:

```bash
export OTEL_SERVICE_NAME=agentmesh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` can enable/configure one signal
independently. `OTEL_SDK_DISABLED=true` disables both. Exporter headers,
compression, timeouts, TLS, sampling, and resource attributes use the standard
SDK variables documented by OpenTelemetry.

Generated telemetry includes:

- inbound `net/http` server spans and outbound HTTP client spans;
- `agentmesh.run.execute` consumer spans with Run, Agent, request, and instance
  correlation attributes;
- `agentmesh.run.attempt` spans with attempt and duration attributes;
- `agentmesh.run.attempts` counter;
- `agentmesh.run.duration` histogram, labeled only by finite terminal status.

W3C Trace Context and Baggage are injected into outbound HTTP requests. Within
one process, request, Run, attempt, and remote call spans retain parentage.
Queue consumption after an asynchronous or cross-replica handoff starts a new
Run trace carrying the persisted `request.id`, `run.id`, and `agent.id`
correlation attributes; trace context itself is not yet persisted in the Run or
NATS message. Operators should use those fields to correlate the two traces.

Providers batch exports and are flushed with a five-second bounded shutdown.
Telemetry failures do not change Run state. Avoid placing secrets, prompts,
outputs, or errors into resource attributes or exporter headers visible to
unauthorized users.

The implementation follows the official [Go SDK guidance](https://opentelemetry.io/docs/languages/go/),
[OTLP exporter configuration](https://opentelemetry.io/docs/specs/otel/protocol/exporter/),
and [SDK environment variable specification](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
