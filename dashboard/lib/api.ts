export async function api<T>(path: string, init?: RequestInit): Promise<T> {
	const headers = new Headers(init?.headers);
	if (init?.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api/agentmesh${path}`, {
    ...init,
    cache: "no-store",
		headers,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: { message?: string } } | null;
    throw new Error(body?.error?.message || `AgentMesh returned HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}
