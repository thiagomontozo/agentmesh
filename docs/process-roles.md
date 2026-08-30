# Process roles and process-level replica test

AgentMesh supports three startup roles through `AGENTMESH_ROLE`:

| Role | Responsibilities |
| --- | --- |
| `all` | Backward-compatible default: HTTP API, health checks, Workflow reconciliation, Run recovery, and Run workers |
| `api` | HTTP API, SSE, health checks, Workflow reconciliation, queue producer, and persisted cancellation; no Run consumer or Run recovery |
| `worker` | Run recovery and queue consumption; no public HTTP listener, Agent health probes, or Workflow reconciliation |

Split roles require `AGENTMESH_MODE=distributed`. Memory mode rejects `api` and
`worker` because its repository, queue, events, and coordination are process
local. The existing command and default `all` behavior remain unchanged.

Example:

```bash
AGENTMESH_MODE=distributed AGENTMESH_ROLE=api agentmesh
AGENTMESH_MODE=distributed AGENTMESH_ROLE=worker agentmesh
```

Both processes must point to the same PostgreSQL, Redis, and NATS services. API
readiness continues at `/readyz`. A worker intentionally has no HTTP listener;
its process supervisor supplies liveness and its structured startup/error logs
report dependency or consumer failure.

## Process-level acceptance test

`TestSeparateProcessesExecutePreserveAndRecoverRuns` builds the real
`cmd/agentmesh` binary and starts independent OS processes:

```text
API process A -> PostgreSQL / Redis / NATS <- Worker process B
       |                                      |
       +-- restart while B owns Run ----------+
                                              X hard kill
                                               
                                Worker process C recovers after lease expiry
```

The test proves:

- API-created work is executed by a worker-only process;
- API SSE receives the worker process's terminal event;
- idempotency survives the process boundary;
- restarting the API does not recover or reset a Run owned by a healthy worker;
- hard-killing the worker leaves a running Run and lease behind;
- a replacement worker recovers only after the lease expires and completes the
  abandoned Run.

The integration CI starts real PostgreSQL, Redis, and NATS services before this
test. This closes the previous same-process evidence gap. Network partitions,
sustained load, and rolling upgrades remain separate deployment test concerns.
