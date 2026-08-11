import http from "node:http";

const upstreamBaseUrl = (process.env.UPSTREAM_BASE_URL || "").replace(/\/$/, "");
const port = Number(process.env.PORT || 10000);
const maxBodyBytes = 1024 * 1024;

const routes = new Map([
  ["GET /health", "/v1/health"],
  ["GET /spine/v1/health", "/v1/health"],
  ["GET /spine/v1/attention", "/v1/attention"],
  ["POST /spine/v1/attention/actions", "/v1/attention/actions"],
]);

function json(res, status, body) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(body));
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

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", "http://relay").pathname;
  const upstreamPath = routes.get(`${req.method} ${pathname}`);

  if (!upstreamBaseUrl) return json(res, 503, { error: "relay_not_configured" });
  if (!upstreamPath) return json(res, 404, { error: "not_found" });

  try {
    const headers = {
      accept: "application/json",
      "ngrok-skip-browser-warning": "phoenix-attention-relay",
    };

    for (const name of ["authorization", "x-phoenix-admin-email", "content-type"]) {
      const value = req.headers[name];
      if (typeof value === "string") headers[name] = value;
    }

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
    res.writeHead(response.status, {
      "content-type": response.headers.get("content-type") || "application/json; charset=utf-8",
      "cache-control": "no-store",
    });
    res.end(responseBody);
  } catch (error) {
    const status = error instanceof Error && error.message === "body_too_large" ? 413 : 502;
    json(res, status, { error: status === 413 ? "request_too_large" : "upstream_unreachable" });
  }
});

server.listen(port, "0.0.0.0");
