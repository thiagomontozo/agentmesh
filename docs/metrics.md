# Operational metrics

`GET /metrics` exposes a small Prometheus text endpoint without adding a client
library dependency. When inbound API authentication is enabled, reader,
operator, or admin credentials are required; health probes remain the only
public endpoints.

Current series are deliberately bounded:

| Metric | Type | Meaning |
| --- | --- | --- |
| `agentmesh_runs{status}` | gauge | Persisted Runs in each finite lifecycle state. |
| `agentmesh_queue_depth` | gauge | Persisted queued Runs; a logical backlog, not JetStream internal depth. |
| `agentmesh_http_requests_total{method,status}` | counter | HTTP responses with method/status labels constrained by the server. |
| `agentmesh_http_request_duration_seconds_{sum,count}` | summary | Aggregate request duration without unbounded paths or quantiles. |
| `agentmesh_run_events_total{type}` | counter | Known Run lifecycle events, including attempts, timeout, failure, cancellation, lease loss, and recovery. |
| `agentmesh_routing_decisions_total{strategy}` | counter | Automatic Router decisions using its two declared strategies. |

Example:

```bash
curl http://localhost:8080/metrics
```

Counters are process-local and reset on restart. Prometheus or another scraper
must aggregate API/Worker replicas by its normal instance labels. Run-state
gauges are calculated from the authoritative repository at scrape time, so a
repository failure returns `503`. The implementation intentionally excludes
Agent IDs, Run IDs, paths, capabilities, errors, and user input from labels to
avoid cardinality and data-exposure problems.

API and combined roles expose `/metrics` on the normal API listener. A
worker-only process has no API listener, so set `AGENTMESH_METRICS_ADDR` (for
example `:9090`) to enable a metrics-only listener. It uses the same inbound
Bearer/RBAC middleware when `AGENTMESH_API_AUTH_CONFIG` is configured; otherwise
it is unauthenticated and should remain on a protected operations network.

OTLP/HTTP can additionally export the bounded `agentmesh.run.attempts` counter
and `agentmesh.run.duration` histogram. It also provides distributed tracing;
see [OpenTelemetry](opentelemetry.md). Alert rules and dashboard definitions
remain separate operational concerns.
