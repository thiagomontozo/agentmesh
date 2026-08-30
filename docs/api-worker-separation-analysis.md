# API and Worker process separation analysis

## Decision

Do not split the current command yet. Keep `agentmesh` as the default combined
API, Workflow scheduler, and Run worker process. A later, backward-compatible
increment should introduce an explicit role such as
`AGENTMESH_ROLE=all|api|worker`, with `all` as the default. Separation is useful
for independent scaling and a smaller API failure domain, but the current
component boundaries are not yet sufficient to make an API-only process honest.

This is an analysis only. No command, configuration, startup behavior, or
deployment topology changes in this item.

## Current startup ownership

`cmd/agentmesh/main.go` currently assembles one shared repository, queue,
coordinator, event bus, runtime resolver, `engine.Engine`, Workflow `Manager`,
Agent health service, and HTTP server. Startup and shutdown ordering is coupled:

```text
repository / queue / coordinator / events
                |
                +-> Engine.Recover -> Engine.Start
                +-> Workflow.Manager.Run -> Recover
                +-> Agent health Service.Start
                +-> HTTP API.ListenAndServe
```

The HTTP server stores a concrete `*engine.Engine`. Run creation, cancellation,
Agent-to-Agent child creation, and Workflow control call methods on that object.
The Workflow manager also stores a concrete Engine and uses it to enqueue and
cancel Runs. `Engine` owns both producer operations (`Enqueue`, `Cancel`) and
consumer lifecycle (`Start`, `Recover`, `Stop`). These are the main seams that
must be separated before process roles are safe.

Memory mode is intentionally process-local and cannot support split roles. Any
future `api` or `worker` role must require distributed mode with PostgreSQL,
Redis, and NATS.

## Benefits when justified

- scale HTTP/SSE capacity independently from remote Agent execution;
- deploy worker-only replicas without exposing a public API listener;
- isolate runtime panics, memory pressure, and slow external calls from API
  request handling;
- roll API and workers independently when wire/storage compatibility permits;
- assign narrower network policies and credentials to each role.

## Costs and operational risks

- another deployment, health policy, shutdown path, and capacity dimension;
- API readiness must not incorrectly depend on a local consumer loop;
- cancellation currently interrupts the context only in the worker process that
  owns it; split roles make distributed cancellation signaling more important;
- Workflow reconciliation needs single-owner or lease-safe scheduling before it
  can run on several API/scheduler replicas without duplicated reconciliation;
- Agent health checks need an explicit owner to avoid probe amplification;
- configuration errors could accidentally leave a cluster with no consumers;
- memory mode cannot provide cross-process state, events, or queueing.

## Smallest compatible design

When operational demand exists, preserve the current command and add a role
configuration with these responsibilities:

| Role | Starts | Does not start |
| --- | --- | --- |
| `all` | current behavior | nothing |
| `api` | repository/cache, event bus, queue producer, health owner, HTTP API | runtime resolver, Run consumers, Run recovery |
| `worker` | repository/cache, queue consumer, coordinator, runtime resolver, Run recovery | public HTTP API, SSE, Agent health probes |

Workflow scheduling should not be silently assigned to every API or worker.
Introduce a separate scheduler ownership decision after extracting interfaces;
until then, retain it only in `all`. An eventual `scheduler` role or a
lease-elected scheduler can be evaluated using real multi-replica tests.

The incremental code seams should be:

1. extract a narrow Run command interface used by HTTP and Workflow code
   (`Enqueue`, persisted `Cancel`, `MaxAttempts`);
2. separate queue producer ownership from consumer `Start/Stop` ownership;
3. make readiness role-aware;
4. add role validation that rejects split roles in memory mode;
5. add process-level Compose tests before documenting horizontal scaling.

The current `agentmesh` invocation and `AGENTMESH_MODE` behavior must remain
unchanged by default.

## Acceptance tests for a future implementation

- the default `all` role passes every current test unchanged;
- an API-only process creates a Run that a worker-only process executes;
- API-only shutdown does not close or disrupt the shared JetStream consumer;
- worker-only readiness detects PostgreSQL, Redis, NATS, and consumer failure;
- API-only readiness does not require a local Run consumer;
- cancellation requested through API reaches a worker process;
- no-worker deployment is observable and does not falsely report full readiness;
- split roles are rejected in memory mode;
- Workflow scheduling has an explicit, tested ownership model.

## Recommendation trigger

Implement the split only when independent API/worker scaling, security policy,
or failure isolation is an observed operational need. The distributed substrate
already supports separate producers and consumers, but the application lifecycle
and command interfaces should be decoupled first. Until those conditions hold,
the combined process is simpler and avoids a second partially distributed
control path.
