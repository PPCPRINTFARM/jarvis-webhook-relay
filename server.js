// Jarvis Webhook Relay
// ----------------------------------------------------------------------------
// Receives Shopify Flow webhooks for new drafts and new orders, then SMS-pings
// Glen via QUO. Runs as a single Node 20 process on Render (free tier OK).
//
// Endpoints:
//   GET  /                                   → liveness
//   GET  /health                             → JSON status
//   POST /webhooks/shopify/draft-created     → Shopify Flow "draft order created"
//   POST /webhooks/shopify/order-created     → Shopify Flow "order created"
//   GET  /log                                → last 50 events (auth required)
//
// All POSTs require header  X-Api-Key: <FLOW_TOKEN>  OR  ?token=<FLOW_TOKEN>
// ----------------------------------------------------------------------------

import http from "node:http";

const PORT = process.env.PORT || 8080;

const FLOW_TOKEN = process.env.FLOW_TOKEN
  || "727s155a3u51692n670b7s036h2j4n1j5j091h597r3c053u2q1n515o5a3q6r7f";

const QUO_KEY = process.env.QUO_KEY || "0opOboF8pDshpmpGndl31aoqwm5ZLm23";
const QUO_SENDER = process.env.QUO_SENDER || "+16029628859";
const GLEN = process.env.GLEN_PHONE || "+12513201372";

const SHOP_TOKEN = process.env.SHOP_TOKEN || "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";
const SHOP_STORE = process.env.SHOP_STORE || "electricmotorexperts.myshopify.com";

const DRAFT_SMS_THRESHOLD = Number(process.env.DRAFT_SMS_THRESHOLD || 1000);
const ORDER_SMS_THRESHOLD = Number(process.env.ORDER_SMS_THRESHOLD || 0);   // every order

const UA = "Mozilla/5.0 (Jarvis/1.0)";

// ---------- in-memory log -----------------------------------------------------

const RECENT = [];
function logEvent(ev) {
  RECENT.unshift({ at: new Date().toISOString(), ...ev });
  if (RECENT.length > 50) RECENT.length = 50;
  console.log(`[${ev.ok ? "OK" : "ERR"}] ${ev.kind}: ${ev.summary}`);
}

// ---------- helpers ----------------------------------------------------------

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function parseJsonOrFlow(raw) {
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return { _raw: raw };
  }
}

function lastTen(phone) {
  return String(phone || "").replace(/\D/g, "").slice(-10);
}

function safeNum(v) {
  const n = parseFloat(String(v || "0").replace(/[$,]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function checkToken(req, url) {
  const k = req.headers["x-api-key"] || req.headers["x-shopify-flow-token"];
  if (k && k === FLOW_TOKEN) return true;
  if (url.searchParams.get("token") === FLOW_TOKEN) return true;
  return false;
}

// ---------- QUO + Shopify side calls ----------------------------------------

async function quoSendSms(body) {
  try {
    const r = await fetch("https://api.openphone.com/v1/messages", {
      method: "POST",
      headers: {
        "Authorization": QUO_KEY,
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ from: QUO_SENDER, to: [GLEN], content: body.slice(0, 480) }),
    });
    if (!r.ok) return { ok: false, error: `HTTP ${r.status}: ${(await r.text()).slice(0, 200)}` };
    const j = await r.json().catch(() => ({}));
    return { ok: true, id: j?.data?.id };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function shopifyGet(path) {
  const r = await fetch(`https://${SHOP_STORE}/admin/api/2024-04/${path}`, {
    headers: {
      "X-Shopify-Access-Token": SHOP_TOKEN,
      "User-Agent": UA,
      "Accept": "application/json",
    },
  });
  if (!r.ok) throw new Error(`shop ${path} HTTP ${r.status}`);
  return r.json();
}

// Lead enrichment — does this caller already have a paid order or draft?
async function enrichLead(phone) {
  const ten = lastTen(phone);
  if (!ten) return "🆕NEW";
  try {
    const j = await shopifyGet(`customers/search.json?query=${encodeURIComponent("phone:*" + ten)}&limit=5`);
    const list = j.customers || [];
    const match = list.find(c => lastTen(c.phone) === ten) || list[0];
    if (!match) return "🆕NEW";
    const paid = await shopifyGet(`customers/${match.id}/orders.json?status=any&limit=5`)
      .then(r => (r.orders || []).filter(o => ["paid", "partially_paid"].includes(o.financial_status)))
      .catch(() => []);
    if (paid.length) return "✅BOUGHT";
    const drafts = await shopifyGet(`draft_orders.json?status=open&limit=100`)
      .then(r => (r.draft_orders || []).filter(d => d.customer && d.customer.id === match.id))
      .catch(() => []);
    if (drafts.length) {
      const newest = drafts.sort((a, b) => b.created_at.localeCompare(a.created_at))[0];
      const days = Math.floor((Date.now() - new Date(newest.created_at).getTime()) / 86400000);
      return `📋QUOTED ${days}d`;
    }
    return "🆕NEW";
  } catch {
    return "🆕NEW";
  }
}

// ---------- payload extractors ----------------------------------------------
//
// Shopify Flow's "Send HTTP Request" action lets users send any JSON body.
// The drafted Workflow 1 sends:
//   { customer_name, phone, email, created_at, invoice_url, total_price?, name? }
// Workflow 2 sends:
//   { customer_name, phone, email, created_at, order_url, total_price?, name? }
// We also accept the raw Shopify draft_order / order shapes as a fallback.

function extractCommon(p) {
  // Try the flat shape first (what Flow sends)
  let name = p.customer_name || p.customerName || p.displayName || "";
  let phone = p.phone || p.customer_phone || "";
  let email = p.email || p.customer_email || "";
  let total = safeNum(p.total_price ?? p.totalPrice);
  let order_name = p.name || p.order_name || "";
  let url = p.invoice_url || p.invoiceUrl || p.order_url || p.orderUrl || p.url || "";
  let created_at = p.created_at || p.createdAt || new Date().toISOString();

  // Fallback to nested customer shape
  const c = p.customer || p.draft_order?.customer || p.order?.customer;
  if (c) {
    if (!name) name = [c.first_name || c.firstName, c.last_name || c.lastName].filter(Boolean).join(" ").trim() || c.displayName || "";
    if (!phone) phone = c.phone || "";
    if (!email) email = c.email || "";
  }
  const inner = p.draft_order || p.order || null;
  if (inner) {
    if (!total) total = safeNum(inner.total_price ?? inner.totalPrice);
    if (!order_name) order_name = inner.name || "";
    if (!url) url = inner.invoice_url || inner.order_status_url || "";
    if (!created_at) created_at = inner.created_at || inner.createdAt;
  }

  return { name, phone, email, total, order_name, url, created_at };
}

// ---------- handlers ---------------------------------------------------------

async function handleDraft(req, res, url, body) {
  const data = extractCommon(body);
  const tag = data.phone ? await enrichLead(data.phone) : "🆕NEW";
  const who = data.name || data.phone || "Unknown";

  let smsResult;
  if (data.total >= DRAFT_SMS_THRESHOLD || (!data.total && data.url)) {
    const lines = [
      `Jarvis: New draft${data.order_name ? " " + data.order_name : ""}${data.total ? " · $" + data.total.toLocaleString() : ""}`,
      `${who}${data.phone ? " · " + data.phone : ""} ${tag}`.trim(),
    ];
    if (data.url) lines.push(data.url);
    smsResult = await quoSendSms(lines.join("\n"));
  }

  logEvent({
    kind: "draft.created",
    ok: true,
    summary: `${data.order_name || "draft"} · $${data.total} · ${who} · ${tag}${smsResult ? (smsResult.ok ? " · SMS✓" : " · SMS✗") : " · noSMS"}`,
    payload_size: JSON.stringify(body).length,
  });

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true, kind: "draft", received: data, sms: smsResult || null, tag }));
}

async function handleOrder(req, res, url, body) {
  const data = extractCommon(body);
  const tag = data.phone ? await enrichLead(data.phone) : "🆕NEW";
  const who = data.name || data.phone || "Unknown";

  let smsResult;
  if (data.total >= ORDER_SMS_THRESHOLD) {
    const lines = [
      `🎉 Jarvis: Order ${data.order_name || "placed"}${data.total ? " · $" + data.total.toLocaleString() : ""}`,
      `${who}${data.phone ? " · " + data.phone : ""} ${tag}`.trim(),
    ];
    if (data.url) lines.push(data.url);
    smsResult = await quoSendSms(lines.join("\n"));
  }

  logEvent({
    kind: "order.created",
    ok: true,
    summary: `${data.order_name || "order"} · $${data.total} · ${who} · ${tag}${smsResult ? (smsResult.ok ? " · SMS✓" : " · SMS✗") : " · noSMS"}`,
    payload_size: JSON.stringify(body).length,
  });

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true, kind: "order", received: data, sms: smsResult || null, tag }));
}

// ---------- server -----------------------------------------------------------

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;

  // CORS / preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Api-Key",
    });
    return res.end();
  }

  if (req.method === "GET" && path === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("Jarvis Webhook Relay — alive\n");
  }

  if (req.method === "GET" && path === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      ok: true,
      uptime_s: Math.floor(process.uptime()),
      received_count: RECENT.length,
      env: {
        has_quo: !!QUO_KEY,
        has_shop: !!SHOP_TOKEN,
        sender: QUO_SENDER,
        recipient_last4: GLEN.slice(-4),
        draft_threshold: DRAFT_SMS_THRESHOLD,
      },
    }));
  }

  if (req.method === "GET" && path === "/log") {
    if (!checkToken(req, url)) {
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "unauthorized" }));
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ events: RECENT }));
  }

  if (req.method === "POST" && (path === "/webhooks/shopify/draft-created" || path === "/draft-order")) {
    if (!checkToken(req, url)) {
      logEvent({ kind: "draft.created", ok: false, summary: "401 unauthorized" });
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "unauthorized" }));
    }
    const raw = await readBody(req);
    const body = parseJsonOrFlow(raw);
    return handleDraft(req, res, url, body);
  }

  if (req.method === "POST" && (path === "/webhooks/shopify/order-created" || path === "/order")) {
    if (!checkToken(req, url)) {
      logEvent({ kind: "order.created", ok: false, summary: "401 unauthorized" });
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "unauthorized" }));
    }
    const raw = await readBody(req);
    const body = parseJsonOrFlow(raw);
    return handleOrder(req, res, url, body);
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "not found", path }));
});

server.listen(PORT, () => {
  console.log(`Jarvis Webhook Relay listening on :${PORT}`);
  console.log(`Endpoints:`);
  console.log(`  POST /webhooks/shopify/draft-created  (or /draft-order)`);
  console.log(`  POST /webhooks/shopify/order-created  (or /order)`);
  console.log(`  Auth: X-Api-Key header  OR  ?token= query param`);
});
