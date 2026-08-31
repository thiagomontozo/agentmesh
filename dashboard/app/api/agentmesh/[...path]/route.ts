import type { NextRequest } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const requestLimit = 1 << 20;
const hopByHop = new Set(["connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade"]);

async function forward(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const base = process.env.AGENTMESH_API_URL || "http://127.0.0.1:8080";
  let upstream: URL;
  try {
    const parsed = new URL(base);
    if (!/^https?:$/.test(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) {
      throw new Error("invalid origin");
    }
    const { path } = await context.params;
    upstream = new URL(path.map(encodeURIComponent).join("/"), parsed.href.endsWith("/") ? parsed : `${parsed.href}/`);
    upstream.search = request.nextUrl.search;
  } catch {
    return Response.json({ error: { message: "Dashboard AgentMesh origin is invalid" } }, { status: 500 });
  }

  const headers = new Headers();
  for (const [name, value] of request.headers) {
    if (!hopByHop.has(name.toLowerCase()) && name.toLowerCase() !== "host" && name.toLowerCase() !== "authorization") {
      headers.set(name, value);
    }
  }
  const token = process.env.AGENTMESH_API_TOKEN?.trim();
  if (token) headers.set("Authorization", `Bearer ${token}`);

  let body: ArrayBuffer | undefined;
  if (request.method !== "GET" && request.method !== "HEAD") {
    const declared = Number(request.headers.get("content-length") || "0");
    if (declared > requestLimit) return Response.json({ error: { message: "Request is too large" } }, { status: 413 });
    body = await request.arrayBuffer();
    if (body.byteLength > requestLimit) return Response.json({ error: { message: "Request is too large" } }, { status: 413 });
  }

  try {
    const response = await fetch(upstream, { method: request.method, headers, body, cache: "no-store", redirect: "manual" });
    const responseHeaders = new Headers();
    for (const [name, value] of response.headers) {
      if (!hopByHop.has(name.toLowerCase())) responseHeaders.set(name, value);
    }
    responseHeaders.set("X-Accel-Buffering", "no");
    return new Response(response.body, { status: response.status, headers: responseHeaders });
  } catch {
    return Response.json({ error: { message: "AgentMesh API is unavailable" } }, { status: 502 });
  }
}

export const GET = forward;
export const POST = forward;
export const PUT = forward;
export const PATCH = forward;
export const DELETE = forward;
export const HEAD = forward;
