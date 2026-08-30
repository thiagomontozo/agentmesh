# Audit log

AgentMesh records completed mutating HTTP requests in an append-only operational
audit history. Each event contains a generated event ID, UTC timestamp, request
and instance IDs, authenticated subject, roles, bound Agent ID when applicable,
HTTP method/path, and final response status. Request bodies, tokens, Agent
credentials, inputs, and outputs are deliberately excluded.

Memory mode retains a bounded in-process history. Distributed mode persists the
same records in PostgreSQL through migration `017_audit_events.sql`. Writes are
synchronous after the HTTP operation completes; a persistence failure is emitted
as a structured error log without rewriting a response that may already have
been delivered.

Retention is bounded by both:

- `AGENTMESH_AUDIT_RETENTION`, default `2160h` (90 days);
- `AGENTMESH_AUDIT_MAX_EVENTS`, default `100000` events.

An administrator can read the oldest-to-newest slice of the latest events:

```bash
curl 'http://localhost:8080/api/v1/audit-events?limit=100' \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

The maximum query limit is 1000. Authorization failures are currently visible
in request logs but are not persisted as audit events because authentication
rejects them before an identity is admitted. The audit table is operational
evidence, not an immutable external compliance archive; deployments needing
tamper-evident retention should export it to an external append-only sink.
