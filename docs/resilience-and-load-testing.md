# Resilience and load testing

AgentMesh has two complementary operational tests that run against the same
PostgreSQL, Redis, and NATS services used by distributed mode. They exercise the
production binary and public HTTP/metrics interfaces instead of replacing a
dependency with an in-process fake.

## Dependency outage and recovery

`scripts/dependency-resilience-test.sh` starts an API-only process and verifies
the following sequence:

1. PostgreSQL is paused, modelling a connection that stalls rather than fails
   immediately. `/readyz` returns `503` within its bounded deadline while the
   AgentMesh process remains alive.
2. PostgreSQL resumes and readiness returns to `200`.
3. Redis is stopped and readiness returns `503`; after Redis restarts, the
   existing client reconnects and readiness returns to `200`.
4. NATS is stopped and readiness returns `503`; after NATS restarts, the
   existing client reconnects and readiness returns to `200`.
5. A worker starts after recovery and completes a newly submitted Run, proving
   more than restoration of the health endpoint.

This is a liveness/readiness and reconnection test. It does not claim that API
mutations succeed while a required dependency is unavailable, nor does it model
an asymmetric partition in which different replicas observe different network
states.

Run it from a Bash environment with Docker, Compose, `curl`, and `jq`:

```bash
go build -o /tmp/agentmesh-current ./cmd/agentmesh
docker compose up -d --wait postgres nats redis
scripts/dependency-resilience-test.sh /tmp/agentmesh-current
docker compose down -v
```

The dedicated `resilience` GitHub Actions job performs this sequence on every
pull request and push to `main`.

## Concurrent multi-worker load

`TestSeparateProcessesSustainConcurrentLoadWithoutDuplicateAttempts` starts one
API process and two worker processes with four worker goroutines each. It
submits 120 Runs concurrently using distinct idempotency keys, waits for every
Run to succeed on its first attempt, and then reads the metrics endpoint of both
workers.

The sum of `agentmesh_run_events_total{type="run.started"}` must equal exactly
120. This assertion combines persisted terminal state with process-local event
counters, so a second normal execution attempt would fail the test even if the
final Run record still appeared successful.

Run the focused test after starting the distributed dependencies:

```bash
docker compose up -d --wait postgres nats redis
go test -tags=integration -count=1 \
  -run TestSeparateProcessesSustainConcurrentLoadWithoutDuplicateAttempts \
  ./internal/integration
docker compose down -v
```

## Guarantee boundary

These tests establish bounded readiness failure, client reconnection, successful
post-outage execution, concurrent admission, multi-worker consumption, and no
duplicate normal starts in the tested workload. They are deliberately small CI
acceptance tests, not capacity benchmarks or service-level objectives. Soak,
maximum-throughput, resource-exhaustion, DNS failure, packet loss, and
asymmetric-network-partition testing remain deployment responsibilities.
