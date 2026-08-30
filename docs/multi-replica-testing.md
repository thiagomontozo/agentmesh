# Multi-replica integration test

`TestRealMultiReplicaControlPlane` is the distributed acceptance test for the
AgentMesh control plane. It creates two independently configured replica stacks
inside one Go test process:

```text
API/Engine/Repository/Redis/Queue/Event Bus A
                         |
                PostgreSQL + Redis + NATS
                         |
API/Engine/Repository/Redis/Queue/Event Bus B
```

Each replica owns separate PostgreSQL pools, Redis clients, NATS JetStream queue
clients, persistent NATS event buses, HTTP servers, Engine lifecycles, instance
IDs, and executors. They share only the external distributed services and the Go
test process. This catches replica-bound state and coordination errors while
remaining deterministic enough for CI. It is not a process, container, network
partition, or production load test.

## Guarantees exercised

The test proves, against real PostgreSQL, Redis, and NATS services, that:

1. a Run submitted to replica A can be consumed and executed by replica B;
2. the resulting state is readable through either API replica;
3. an SSE client on A observes a terminal event published by B;
4. a normal Run is executed once and an idempotency-key replay returns it;
5. a Run left `running` by a stopped replica is recovered after its lease expires;
6. recovery on A does not requeue a Run while B still owns a renewed lease;
7. Redis lease ownership and PostgreSQL fencing participate in recovery;
8. an exhausted execution is published to the JetStream dead-letter subject;
9. API idempotency remains valid across the shared repository.

The recovery case deliberately simulates a crashed worker after its PostgreSQL
execution claim: the lease is not released, the persisted Run remains `running`,
and replica A may recover it only after the short lease expires.

## Running locally

Start only the distributed dependencies and execute the integration package:

```bash
docker compose up -d --wait postgres nats redis
go test -tags=integration -count=1 -run TestRealMultiReplicaControlPlane ./internal/integration
docker compose down -v
```

The complete integration suite uses the same services:

```bash
go test -tags=integration -count=1 ./internal/integration
```

GitHub Actions starts fresh Compose dependencies and runs this suite on every
pull request and push to `main`.

## Boundaries

Passing this test establishes a strong distributed-control-plane baseline, not
an unrestricted exactly-once guarantee. JetStream delivery is at least once;
Redis leases and PostgreSQL fencing prevent stale Run-state finalization, while
Agent Protocol idempotency is still required for irreversible side effects in a
remote Agent. Failure modes such as host loss, network partitions, DNS failure,
slow storage, rolling upgrades, and sustained load require separate container or
deployment-level tests.
