import http from "node:http";

const upstreamBaseUrl = (process.env.UPSTREAM_BASE_URL || "").replace(/\/$/, "");
const port = Number(process.env.PORT || 10000);
const maxBodyBytes = 1024 * 1024;
const siteOrigin = "https://phoenix-attention.phoenix-phas-6820.chatgpt.site";
const administrators = new Set([
  "phoenixphaseconverters@gmail.com",
  "appliedindustrialmotors@gmail.com",
]);

const routes = new Map([
  ["GET /", "/v1/health"],
  ["GET /health", "/v1/health"],
  ["GET /spine/v1/health", "/v1/health"],
  ["GET /spine/v1/attention", "/v1/attention"],
  ["POST /spine/v1/attention/actions", "/v1/attention/actions"],
]);

function corsHeaders(origin) {
  return origin === siteOrigin ? {
    "access-control-allow-origin": siteOrigin,
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-Phoenix-Relay-Session",
    "access-control-max-age": "600",
    "vary": "Origin",
  } : {};
}

function json(req, res, status, body) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...corsHeaders(req.headers.origin),
  });
  res.end(JSON.stringify(body));
}

function audit(method, pathname, status) {
  console.log(JSON.stringify({ event: "relay_request", method, pathname, status }));
}

async function readBody(req) {
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > maxBodyBytes) throw new Error("body_too_large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function openSession(encoded) {
  if (typeof encoded !== "string" || !encoded) throw new Error("missing_session");
  console.log(JSON.stringify({ event: "relay_session", encoded_length: encoded.length }));
  const rawKey = Buffer.from(process.env.RELAY_SESSION_KEY || "", "base64");
  if (rawKey.length !== 32) throw new Error("invalid_session_key");
  const sealed = Buffer.from(encoded.replace(/-/g, "+").replace(/_/g, "/"), "base64");
  if (sealed.length < 29) throw new Error("invalid_session");
  const key = await crypto.subtle.importKey("raw", rawKey, "AES-GCM", false, ["decrypt"]);
  let clear;
  try {
    clear = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv: sealed.subarray(0, 12) },
      key,
      sealed.subarray(12),
    );
  } catch {
    throw new Error("session_decrypt_failed");
  }
  const payload = JSON.parse(new TextDecoder().decode(clear));
  const email = String(payload.email || "").toLowerCase();
  const exp = Number(payload.exp);
  if (!administrators.has(email) || !payload.token || !Number.isFinite(exp) || exp < Date.now() || exp > Date.now() + 120_000) {
    throw new Error("invalid_session");
  }
  return { token: String(payload.token), email };
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", "http://relay").pathname;
  if (req.method === "OPTIONS") {
    const origin = req.headers.origin;
    const status = origin === siteOrigin ? 204 : 403;
    audit(req.method, pathname, status);
    res.writeHead(status, { "cache-control": "no-store", ...corsHeaders(origin) });
    return res.end();
  }

  const upstreamPath = routes.get(`${req.method} ${pathname}`);
  if (!upstreamBaseUrl) {
    audit(req.method, pathname, 503);
    return json(req, res, 503, { error: "relay_not_configured" });
  }
  if (!upstreamPath) {
    audit(req.method, pathname, 404);
    return json(req, res, 404, { error: "not_found" });
  }

  try {
    const isProtected = pathname === "/spine/v1/attention" || pathname === "/spine/v1/attention/actions";
    const session = isProtected ? await openSession(req.headers["x-phoenix-relay-session"]) : null;
    const headers = {
      accept: "application/json",
      "ngrok-skip-browser-warning": "phoenix-attention-relay",
    };
    if (session) {
      headers.authorization = `Bearer ${session.token}`;
      headers["x-phoenix-admin-email"] = session.email;
    }
    if (req.method === "POST") headers["content-type"] = "application/json";

    const body = req.method === "POST" ? await readBody(req) : undefined;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12_000);

    let response;
    try {
      response = await fetch(`${upstreamBaseUrl}${upstreamPath}`, {
        method: req.method,
        headers,
        body,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    const responseBody = Buffer.from(await response.arrayBuffer());
    audit(req.method, pathname, response.status);
    res.writeHead(response.status, {
      "content-type": response.headers.get("content-type") || "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...corsHeaders(req.headers.origin),
    });
    res.end(responseBody);
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    const status = message === "body_too_large" ? 413 : message.includes("session") ? 401 : 502;
    audit(req.method, pathname, status);
    const errorCode = status === 413 ? "request_too_large" : status === 401 ? message : "upstream_unreachable";
    console.log(JSON.stringify({ event: "relay_failure", stage: errorCode }));
    json(req, res, status, { error: errorCode });
  }
});

server.listen(port, "0.0.0.0");
