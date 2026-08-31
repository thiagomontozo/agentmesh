export type Agent = {
  id: string;
  name: string;
  runtime?: string;
  protocol?: string;
  endpoint?: string;
  model?: string;
  capabilities?: string[];
};

export type Run = {
  id: string;
  agent_id: string;
  status: "queued" | "running" | "succeeded" | "failed" | "canceled";
  input: string;
  output?: string;
  error?: string;
  attempt: number;
  max_attempts: number;
  duration_ms: number;
  created_at: string;
};

export type Workflow = {
  id: string;
  status: string;
  input: string;
  steps?: Array<{ id: string; agent_id: string; status: string }>;
  created_at: string;
};

export type Approval = {
  id: string;
  server_id: string;
  tool_name: string;
  reason?: string;
  status: "pending" | "approved" | "rejected" | "consumed";
  requested_by: string;
  created_at: string;
  expires_at: string;
};

export type RunEvent = {
  id?: string;
  run_id: string;
  type: string;
  message: string;
  attempt: number;
  timestamp: string;
};
