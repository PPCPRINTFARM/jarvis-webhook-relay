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
  ["GET /", { upstreamPath: "/v1/health", upstreamMethod: "GET", protected: false }],
  ["GET /health", { upstreamPath: "/v1/health", upstreamMethod: "GET", protected: false }],
  ["GET /spine/v1/health", { upstreamPath: "/v1/health", upstreamMethod: "GET", protected: false }],
  ["POST /spine/v1/attention/read", { upstreamPath: "/v1/attention", upstreamMethod: "GET", protected: true }],
  ["POST /spine/v1/attention/actions", { upstreamPath: "/v1/attention/actions", upstreamMethod: "POST", protected: true }],
]);

function corsHeaders(origin) {
  return origin === siteOrigin ? {
    "access-control-allow-origin": siteOrigin,
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type",
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

function audit(method, pathname, status, stage) {
  console.log(JSON.stringify({ event: "relay_request", method, pathname, status, ...(stage ? { stage } : {}) }));
}

async function readJson(req) {
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > maxBodyBytes) throw new Error("body_too_large");
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("invalid_json");
  }
}

async function openSession(encoded) {
  if (typeof encoded !== "string" || !encoded) throw new Error("missing_session");
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
    audit(req.method, pathname, status, "preflight");
    res.writeHead(status, { "cache-control": "no-store", ...corsHeaders(origin) });
    return res.end();
  }

  const route = routes.get(`${req.method} ${pathname}`);
  if (!upstreamBaseUrl) {
    audit(req.method, pathname, 503, "configuration");
    return json(req, res, 503, { error: "relay_not_configured" });
  }
  if (!route) {
    audit(req.method, pathname, 404, "routing");
    return json(req, res, 404, { error: "not_found" });
  }

  try {
    let session = null;
    let upstreamBody;
    if (route.protected) {
      const envelope = await readJson(req);
      session = await openSession(envelope.session);
      if (route.upstreamMethod === "POST") upstreamBody = JSON.stringify(envelope.payload ?? {});
    }

    const headers = {
      accept: "application/json",
      "ngrok-skip-browser-warning": "phoenix-attention-relay",
    };
    if (session) {
      headers.authorization = `Bearer ${session.token}`;
      headers["x-phoenix-admin-email"] = session.email;
    }
    if (upstreamBody !== undefined) headers["content-type"] = "application/json";

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12_000);
    let response;
    try {
      response = await fetch(`${upstreamBaseUrl}${route.upstreamPath}`, {
        method: route.upstreamMethod,
        headers,
        body: upstreamBody,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    const responseBody = Buffer.from(await response.arrayBuffer());
    audit(req.method, pathname, response.status, "upstream_response");
    res.writeHead(response.status, {
      "content-type": response.headers.get("content-type") || "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...corsHeaders(req.headers.origin),
    });
    res.end(responseBody);
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown";
    const status = message === "body_too_large" ? 413 : message === "invalid_json" ? 400 : message.includes("session") ? 401 : 502;
    audit(req.method, pathname, status, message);
    json(req, res, status, {
      error: status === 413 ? "request_too_large" : status === 400 ? "invalid_request" : status === 401 ? message : "upstream_unreachable",
    });
  }
});

server.listen(port, "0.0.0.0");
