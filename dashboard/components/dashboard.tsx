"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../lib/api";
import type { Agent, Approval, Run, RunEvent, Workflow } from "../lib/types";

const terminal = new Set(["succeeded", "failed", "canceled"]);
const eventTypes = [
  "run.queued", "run.started", "run.retrying", "run.attempt_timed_out", "run.succeeded",
  "run.failed", "run.canceled", "run.lease_lost", "run.recovered",
];

function elapsed(milliseconds: number) {
  if (!milliseconds) return "—";
  return milliseconds < 1000 ? `${milliseconds} ms` : `${(milliseconds / 1000).toFixed(2)} s`;
}

function date(value: string) {
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}

function Status({ value }: { value: string }) {
  return <span className={`status status-${value}`}>{value}</span>;
}

function LiveRun({ run, onTerminal, onClose }: { run: Run; onTerminal: () => void; onClose: () => void }) {
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [connection, setConnection] = useState<"connecting" | "live" | "closed">("connecting");

  useEffect(() => {
    setEvents([]);
    setConnection("connecting");
    const source = new EventSource(`/api/agentmesh/api/v1/runs/${encodeURIComponent(run.id)}/events`);
    source.onopen = () => setConnection("live");
    source.onerror = () => setConnection(terminal.has(run.status) ? "closed" : "connecting");
    const receive = (raw: Event) => {
      const message = raw as MessageEvent<string>;
      try {
        const event = JSON.parse(message.data) as RunEvent;
        setEvents((current) => current.some((item) => item.id && item.id === event.id) ? current : [...current, event]);
        if (event.type === "run.succeeded" || event.type === "run.failed" || event.type === "run.canceled") {
          setConnection("closed");
          source.close();
          onTerminal();
        }
      } catch {
        // The control plane owns the event schema; malformed proxy data is ignored.
      }
    };
    eventTypes.forEach((type) => source.addEventListener(type, receive));
    return () => {
      eventTypes.forEach((type) => source.removeEventListener(type, receive));
      source.close();
    };
  }, [run.id, run.status, onTerminal]);

  return (
    <aside className="drawer" aria-label={`Live events for Run ${run.id}`}>
      <div className="drawer-head">
        <div>
          <p className="eyebrow">Live run</p>
          <h2>{run.id}</h2>
        </div>
        <div className="drawer-controls"><span className={`connection connection-${connection}`}><i />{connection}</span><button onClick={onClose} aria-label="Close live Run panel">×</button></div>
      </div>
      <dl className="run-facts">
        <div><dt>Agent</dt><dd>{run.agent_id}</dd></div>
        <div><dt>Status</dt><dd><Status value={run.status} /></dd></div>
        <div><dt>Attempt</dt><dd>{run.attempt} / {run.max_attempts}</dd></div>
        <div><dt>Duration</dt><dd>{elapsed(run.duration_ms)}</dd></div>
      </dl>
      <div className="payload"><strong>Input</strong><p>{run.input}</p></div>
      {(run.output || run.error) && <div className="payload"><strong>{run.error ? "Error" : "Output"}</strong><p>{run.error || run.output}</p></div>}
      <div className="timeline" aria-live="polite">
        {events.length === 0 && <p className="empty">Waiting for persisted or live events…</p>}
        {events.map((event, index) => (
          <article key={event.id || `${event.timestamp}-${index}`}>
            <i />
            <div><strong>{event.type}</strong><span>{date(event.timestamp)} · attempt {event.attempt}</span><p>{event.message}</p></div>
          </article>
        ))}
      </div>
    </aside>
  );
}

export function Dashboard() {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [selectedRunID, setSelectedRunID] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [agentID, setAgentID] = useState("");
  const [input, setInput] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const refresh = useCallback(async () => {
    const results = await Promise.allSettled([
      api<Agent[]>("/api/v1/agents"), api<Run[]>("/api/v1/runs"),
      api<Workflow[]>("/api/v1/workflows"), api<Approval[]>("/api/v1/approvals?limit=50"),
    ]);
    const failures = results.filter((result) => result.status === "rejected") as PromiseRejectedResult[];
    if (results[0].status === "fulfilled") setAgents(results[0].value);
    if (results[1].status === "fulfilled") setRuns([...results[1].value].reverse());
    if (results[2].status === "fulfilled") setWorkflows([...results[2].value].reverse());
    if (results[3].status === "fulfilled") setApprovals(results[3].value);
    setError(failures.length ? failures[0].reason instanceof Error ? failures[0].reason.message : "Could not load control-plane data" : "");
    setLoading(false);
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (!agentID && agents[0]) setAgentID(agents[0].id);
  }, [agentID, agents]);

  const selectedRun = useMemo(() => runs.find((run) => run.id === selectedRunID), [runs, selectedRunID]);
  const active = runs.filter((run) => run.status === "queued" || run.status === "running").length;
  const failed = runs.filter((run) => run.status === "failed").length;
  const pending = approvals.filter((approval) => approval.status === "pending").length;

  async function createRun(event: FormEvent) {
    event.preventDefault();
    if (!agentID || !input.trim()) return;
    setSubmitting(true);
    setError("");
    try {
      const run = await api<Run>("/api/v1/runs", { method: "POST", body: JSON.stringify({ agent_id: agentID, input: input.trim() }) });
      setInput("");
      setSelectedRunID(run.id);
      await refresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not create Run");
    } finally {
      setSubmitting(false);
    }
  }

  async function decide(approval: Approval, decision: "approve" | "reject") {
    setError("");
    try {
      await api(`/api/v1/approvals/${encodeURIComponent(approval.id)}/${decision}`, { method: "POST", body: "{}" });
      await refresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not update approval");
    }
  }

  return (
    <main>
      <header className="masthead">
        <div className="brand"><span className="brand-mark">AM</span><div><strong>AgentMesh</strong><small>Control Room</small></div></div>
        <div className="pulse"><i /> Control plane {error ? "degraded" : "online"}</div>
      </header>

      <section className="hero">
        <div><p className="eyebrow">Operations overview</p><h1>Every agent execution,<br /><em>in one mesh.</em></h1><p className="lede">Inspect live Runs, route work to registered Agents and govern tool approvals without bypassing the Go control plane.</p></div>
        <form className="run-form" onSubmit={createRun}>
          <span className="form-label">Launch a Run</span>
          <label>Agent<select value={agentID} onChange={(event) => setAgentID(event.target.value)} required><option value="">Select an Agent</option>{agents.map((agent) => <option key={agent.id} value={agent.id}>{agent.name} · {agent.runtime || "demo"}</option>)}</select></label>
          <label>Input<textarea value={input} onChange={(event) => setInput(event.target.value)} placeholder="Describe the work to execute…" rows={3} required /></label>
          <button disabled={submitting || agents.length === 0}>{submitting ? "Queuing…" : "Queue Run"}<span>→</span></button>
        </form>
      </section>

      {error && <div className="alert" role="alert"><strong>Control-plane request failed.</strong> {error}</div>}

      <section className="stats" aria-label="Control plane statistics">
        <article><span>Registered Agents</span><strong>{agents.length}</strong><small>{agents.filter((agent) => agent.runtime === "remote").length} remote runtimes</small></article>
        <article><span>Active Runs</span><strong>{active}</strong><small>{runs.length} retained total</small></article>
        <article><span>Pending approvals</span><strong>{pending}</strong><small>human-gated effects</small></article>
        <article><span>Failed Runs</span><strong>{failed}</strong><small>{workflows.length} workflows tracked</small></article>
      </section>

      <section className="grid">
        <div className="panel runs-panel">
          <div className="panel-head"><div><p className="eyebrow">Execution ledger</p><h2>Recent Runs</h2></div><button className="quiet" onClick={() => void refresh()}>Refresh</button></div>
          <div className="table-wrap"><table><thead><tr><th>Run</th><th>Agent</th><th>Status</th><th>Attempt</th><th>Duration</th><th>Created</th></tr></thead><tbody>
            {!loading && runs.length === 0 && <tr><td colSpan={6} className="empty">No Runs have been created.</td></tr>}
            {runs.slice(0, 20).map((run) => <tr key={run.id} className={selectedRunID === run.id ? "selected" : ""} onClick={() => setSelectedRunID(run.id)}><td><button className="run-link" onClick={() => setSelectedRunID(run.id)}>{run.id}</button></td><td>{run.agent_id}</td><td><Status value={run.status} /></td><td>{run.attempt}/{run.max_attempts}</td><td>{elapsed(run.duration_ms)}</td><td>{date(run.created_at)}</td></tr>)}
          </tbody></table></div>
        </div>

        <div className="panel agents-panel">
          <div className="panel-head"><div><p className="eyebrow">Registry</p><h2>Agents</h2></div><span>{agents.length}</span></div>
          <div className="agent-list">{agents.map((agent) => <article key={agent.id}><div className="agent-glyph">{agent.name.slice(0, 2).toUpperCase()}</div><div><strong>{agent.name}</strong><span>{agent.runtime || "demo"} / {agent.protocol || "internal"}</span><p>{agent.capabilities?.length ? agent.capabilities.join(" · ") : "No declared capabilities"}</p></div></article>)}{!loading && agents.length === 0 && <p className="empty">Registry is empty.</p>}</div>
        </div>

        <div className="panel approvals-panel">
          <div className="panel-head"><div><p className="eyebrow">Governance</p><h2>Tool approvals</h2></div><span>{pending} pending</span></div>
          <div className="approval-list">{approvals.slice(0, 8).map((approval) => <article key={approval.id}><div><strong>{approval.tool_name}</strong><span>{approval.server_id} · requested by {approval.requested_by}</span><p>{approval.reason || "No reason supplied"}</p></div><div className="approval-actions"><Status value={approval.status} />{approval.status === "pending" && <><button onClick={() => void decide(approval, "approve")}>Approve</button><button className="danger" onClick={() => void decide(approval, "reject")}>Reject</button></>}</div></article>)}{!loading && approvals.length === 0 && <p className="empty">No approval records.</p>}</div>
        </div>

        <div className="panel workflows-panel">
          <div className="panel-head"><div><p className="eyebrow">Orchestration</p><h2>Workflows</h2></div><span>{workflows.length}</span></div>
          <div className="workflow-list">{workflows.slice(0, 8).map((workflow) => <article key={workflow.id}><div><strong>{workflow.id}</strong><span>{workflow.steps?.length || 0} steps · {date(workflow.created_at)}</span></div><Status value={workflow.status} /></article>)}{!loading && workflows.length === 0 && <p className="empty">No workflows defined.</p>}</div>
        </div>
      </section>

      {selectedRun && <LiveRun run={selectedRun} onTerminal={refresh} onClose={() => setSelectedRunID("")} />}
      <footer><span>AgentMesh operational surface</span><span>Data refreshes every 5 seconds · events stream over SSE</span></footer>
    </main>
  );
}
