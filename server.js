// Jarvis Webhook Relay + Hub API
// ----------------------------------------------------------------------------
// Receives webhook traffic from N8N / Shopify Flow / CallRail / QUO / Retell
// and persists events to Postgres. Replaces the dead Mac Studio hub-api.
//
// Endpoints:
//   GET  /                                   → liveness
//   GET  /health                             → JSON status (incl. DB)
//   POST /webhooks/shopify/draft-created     → Shopify Flow "draft created"
//   POST /webhooks/shopify/order-created     → Shopify Flow "order created"
//   POST /events                             → Generic write (N8N hub writer)
//   GET  /events?phone=+1...&limit=50        → Read events for a phone
//   GET  /events/recent?source=callrail      → Tail recent events
//   GET  /customers/:phone                   → Customer rollup
//   GET  /log                                → last 50 in-memory events (auth)
//
// All POSTs require header  X-Api-Key: <FLOW_TOKEN>  OR  ?token=<FLOW_TOKEN>
// ----------------------------------------------------------------------------

import http from "node:http";
import pg from "pg";
import crypto from "node:crypto";

const { Pool } = pg;
const PORT = process.env.PORT || 8080;

const FLOW_TOKEN = process.env.FLOW_TOKEN
  || "727s155a3u51692n670b7s036h2j4n1j5j091h597r3c053u2q1n515o5a3q6r7f";

const QUO_KEY = process.env.QUO_KEY || "0opOboF8pDshpmpGndl31aoqwm5ZLm23";
const QUO_SENDER = process.env.QUO_SENDER || "+16029628859";
const GLEN = process.env.GLEN_PHONE || "+12513201372";

const SHOP_TOKEN = process.env.SHOP_TOKEN || "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";
const SHOP_STORE = process.env.SHOP_STORE || "electricmotorexperts.myshopify.com";

const DRAFT_SMS_THRESHOLD = Number(process.env.DRAFT_SMS_THRESHOLD || 1000);
const ORDER_SMS_THRESHOLD = Number(process.env.ORDER_SMS_THRESHOLD || 0);

const UA = "Mozilla/5.0 (Jarvis/1.0)";
const DATABASE_URL = process.env.DATABASE_URL || "";

// ---------- in-memory log ----------------------------------------------------

const RECENT = [];
function logEvent(ev) {
  RECENT.unshift({ at: new Date().toISOString(), ...ev });
  if (RECENT.length > 50) RECENT.length = 50;
  console.log(`[${ev.ok ? "OK" : "ERR"}] ${ev.kind}: ${ev.summary}`);
}

// ---------- Postgres ---------------------------------------------------------

let pool = null;
let dbReady = false;

async function initDb() {
  if (!DATABASE_URL) {
    console.warn("⚠️  DATABASE_URL not set — running in memory-only mode (no persistence)");
    return;
  }
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes("render.com") || DATABASE_URL.includes("neon")
      ? { rejectUnauthorized: false }
      : false,
    max: 5,
  });
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id          TEXT PRIMARY KEY,
      source      TEXT NOT NULL,
      type        TEXT NOT NULL,
      timestamp   BIGINT NOT NULL,
      read        SMALLINT NOT NULL DEFAULT 0,
      from_addr   TEXT DEFAULT '',
      to_addr     TEXT DEFAULT '',
      caller_name TEXT DEFAULT '',
      subject     TEXT DEFAULT '',
      order_num   TEXT DEFAULT '',
      summary     TEXT DEFAULT '',
      content     TEXT DEFAULT '',
      raw         JSONB DEFAULT '{}'::jsonb,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
    CREATE INDEX IF NOT EXISTS idx_events_from ON events(from_addr);
    CREATE INDEX IF NOT EXISTS idx_events_phone10 ON events(RIGHT(REGEXP_REPLACE(from_addr, '[^0-9]', '', 'g'), 10));

    CREATE TABLE IF NOT EXISTS customers (
      phone           TEXT PRIMARY KEY,
      name            TEXT DEFAULT '',
      company         TEXT DEFAULT '',
      email           TEXT DEFAULT '',
      total_calls     INTEGER NOT NULL DEFAULT 0,
      total_texts     INTEGER NOT NULL DEFAULT 0,
      total_orders    INTEGER NOT NULL DEFAULT 0,
      total_revenue   NUMERIC(12,2) NOT NULL DEFAULT 0,
      last_contact    BIGINT,
      tags            TEXT DEFAULT '',
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  dbReady = true;
  console.log("✅ Postgres connected, schema ready");
}

function md5_16(s) { return crypto.createHash("md5").update(s).digest("hex").slice(0, 16); }

async function insertEvent({ id, source, type, timestamp, from_addr, to_addr, caller_name, subject, order_num, summary, content, raw }) {
  if (!dbReady) return { ok: false, error: "db_not_ready" };
  try {
    await pool.query(`
      INSERT INTO events (id, source, type, timestamp, from_addr, to_addr, caller_name, subject, order_num, summary, content, raw)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      ON CONFLICT (id) DO NOTHING
    `, [id, source, type, timestamp, from_addr || '', to_addr || '', caller_name || '',
        subject || '', order_num || '', (summary || '').slice(0, 500), content || '',
        JSON.stringify(raw || {})]);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function lastTen(p) { return String(p || "").replace(/\D/g, "").slice(-10); }

async function bumpCustomer(phone, deltaCall = 0, deltaText = 0, deltaOrder = 0, deltaRev = 0, ts = Date.now(), name = "") {
  if (!dbReady || !phone) return;
  const ten = lastTen(phone);
  if (!ten) return;
  try {
    await pool.query(`
      INSERT INTO customers (phone, name, total_calls, total_texts, total_orders, total_revenue, last_contact, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
      ON CONFLICT (phone) DO UPDATE SET
        total_calls   = customers.total_calls + EXCLUDED.total_calls,
        total_texts   = customers.total_texts + EXCLUDED.total_texts,
        total_orders  = customers.total_orders + EXCLUDED.total_orders,
        total_revenue = customers.total_revenue + EXCLUDED.total_revenue,
        last_contact  = GREATEST(COALESCE(customers.last_contact, 0), EXCLUDED.last_contact),
        name          = CASE WHEN customers.name = '' THEN EXCLUDED.name ELSE customers.name END,
        updated_at    = NOW()
    `, [phone, name, deltaCall, deltaText, deltaOrder, deltaRev, ts]);
  } catch (e) {
    console.warn("bumpCustomer err:", e.message);
  }
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
  try { return JSON.parse(raw); } catch { return { _raw: raw }; }
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

function json(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(obj));
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

// ---------- Webhook handlers -------------------------------------------------

async function handleDraftCreated(payload) {
  const id = payload.id || payload.draft_order_id || payload.admin_graphql_api_id || "";
  const total = safeNum(payload.total_price || payload.totalPrice || payload.total);
  const name = payload.name || payload.draft_order_name || `#${id}`;
  const customer = payload.customer || {};
  const cName = (customer.first_name || customer.firstName || "") + " " + (customer.last_name || customer.lastName || "");
  const cPhone = customer.phone || payload.phone || "";
  const cEmail = customer.email || payload.email || "";
  const ts = Date.now();

  const eid = md5_16(`shopify-draft-${id || ts}`);
  await insertEvent({
    id: eid, source: "shopify", type: "draft_order", timestamp: ts,
    from_addr: cPhone, to_addr: cEmail, caller_name: cName.trim(),
    subject: name, order_num: name, summary: `Draft ${name} · $${total.toFixed(2)}`,
    content: "", raw: payload,
  });
  await bumpCustomer(cPhone, 0, 0, 0, 0, ts, cName.trim());

  if (total >= DRAFT_SMS_THRESHOLD) {
    const sms = `📋 New draft ${name} · $${total.toFixed(2)}\n${cName.trim() || "Unknown"} · ${cPhone || ""}\nhttps://${SHOP_STORE}/admin/draft_orders/${id}`;
    const r = await quoSendSms(sms);
    return { ok: true, id: eid, sms: r };
  }
  return { ok: true, id: eid, sms: { skipped: `below threshold $${DRAFT_SMS_THRESHOLD}` } };
}

async function handleOrderCreated(payload) {
  const id = payload.id || payload.order_id || "";
  const total = safeNum(payload.total_price || payload.totalPrice || payload.total);
  const name = payload.name || `#${id}`;
  const customer = payload.customer || {};
  const cName = ((customer.first_name || "") + " " + (customer.last_name || "")).trim();
  const cPhone = customer.phone || payload.phone || "";
  const cEmail = customer.email || payload.email || "";
  const fin = payload.financial_status || "";
  const ts = Date.now();

  const eid = md5_16(`shopify-order-${id || ts}`);
  await insertEvent({
    id: eid, source: "shopify", type: "order", timestamp: ts,
    from_addr: cPhone, to_addr: cEmail, caller_name: cName,
    subject: name, order_num: name, summary: `Order ${name} · $${total.toFixed(2)} · ${fin}`,
    content: "", raw: payload,
  });
  const isPaid = fin === "paid" || fin === "partially_paid";
  await bumpCustomer(cPhone, 0, 0, 1, isPaid ? total : 0, ts, cName);

  if (total >= ORDER_SMS_THRESHOLD) {
    const sms = `🧾 Order ${name} · $${total.toFixed(2)}\n${cName || "Unknown"} · ${cPhone || ""}\nhttps://${SHOP_STORE}/admin/orders/${id}`;
    const r = await quoSendSms(sms);
    return { ok: true, id: eid, sms: r };
  }
  return { ok: true, id: eid, sms: { skipped: "threshold" } };
}

// Generic write — used by N8N for CallRail / QUO / Retell / etc.
async function handleGenericEvent(payload) {
  const source = (payload.source || "").toLowerCase();
  if (!source) return { ok: false, error: "missing 'source'" };

  const type = payload.type || "event";
  const ts = Number(payload.timestamp) || Date.now();
  const fromAddr = payload.from_addr || payload.from || payload.phone || payload.customer_phone_number || "";
  const toAddr = payload.to_addr || payload.to || "";
  const callerName = payload.caller_name || payload.customer_name || payload.name || "";
  const subject = payload.subject || "";
  const orderNum = payload.order_num || "";
  const summary = payload.summary || payload.call_summary || "";
  const content = payload.content || payload.transcription || payload.transcript || payload.body || "";

  // Generate id: use payload.id if provided, else md5 of (source + ts + fromAddr)
  const idKey = payload.id || payload.call_id || payload.event_id || `${source}-${ts}-${fromAddr}`;
  const eid = md5_16(`${source}-${idKey}`);

  const r = await insertEvent({
    id: eid, source, type, timestamp: ts,
    from_addr: fromAddr, to_addr: toAddr, caller_name: callerName,
    subject, order_num: orderNum, summary, content, raw: payload.raw || payload,
  });

  // Update customer rollup
  const isCall = type.includes("call") || type === "missed_call";
  const isText = type.includes("sms") || type.includes("text") || type.includes("message");
  if (fromAddr && (isCall || isText)) {
    await bumpCustomer(fromAddr, isCall ? 1 : 0, isText ? 1 : 0, 0, 0, ts, callerName);
  }

  return { ok: r.ok, id: eid, error: r.error };
}

// ---------- query endpoints --------------------------------------------------

async function getEventsByPhone(phone, limit = 50) {
  if (!dbReady) return [];
  const ten = lastTen(phone);
  if (!ten) return [];
  const r = await pool.query(`
    SELECT id, source, type, timestamp, from_addr, to_addr, caller_name, subject, summary, content
    FROM events
    WHERE RIGHT(REGEXP_REPLACE(from_addr, '[^0-9]', '', 'g'), 10) = $1
       OR RIGHT(REGEXP_REPLACE(to_addr,   '[^0-9]', '', 'g'), 10) = $1
    ORDER BY timestamp DESC
    LIMIT $2
  `, [ten, Math.min(limit, 200)]);
  return r.rows;
}

async function getRecentEvents(source, limit = 50) {
  if (!dbReady) return [];
  const r = source
    ? await pool.query(`SELECT * FROM events WHERE source=$1 ORDER BY timestamp DESC LIMIT $2`, [source, Math.min(limit, 200)])
    : await pool.query(`SELECT * FROM events ORDER BY timestamp DESC LIMIT $1`, [Math.min(limit, 200)]);
  return r.rows;
}

async function getCustomer(phone) {
  if (!dbReady) return null;
  const r = await pool.query(`SELECT * FROM customers WHERE phone=$1 OR
    RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 10) = $2`,
    [phone, lastTen(phone)]);
  return r.rows[0] || null;
}

// ---------- HTTP router ------------------------------------------------------

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;
  const method = req.method.toUpperCase();

  // CORS preflight
  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Api-Key, X-Shopify-Flow-Token",
    });
    return res.end();
  }

  try {
    // ---- liveness
    if (path === "/" && method === "GET") {
      res.writeHead(200, { "Content-Type": "text/plain" });
      return res.end("Jarvis Webhook Relay + Hub API\nOK\n");
    }
    if (path === "/health" && method === "GET") {
      let dbCount = null;
      if (dbReady) {
        try { const r = await pool.query("SELECT COUNT(*)::int AS n FROM events"); dbCount = r.rows[0].n; } catch {}
      }
      return json(res, 200, { ok: true, db: dbReady, events: dbCount, recent_count: RECENT.length });
    }

    // ---- log (auth)
    if (path === "/log" && method === "GET") {
      if (!checkToken(req, url)) return json(res, 401, { error: "unauthorized" });
      return json(res, 200, RECENT);
    }

    // ---- query: events for a phone
    if (path === "/events" && method === "GET") {
      const phone = url.searchParams.get("phone");
      const limit = Number(url.searchParams.get("limit") || 50);
      if (!phone) return json(res, 400, { error: "missing phone param" });
      const rows = await getEventsByPhone(phone, limit);
      return json(res, 200, { phone, count: rows.length, events: rows });
    }
    if (path === "/events/recent" && method === "GET") {
      const source = url.searchParams.get("source");
      const limit = Number(url.searchParams.get("limit") || 50);
      const rows = await getRecentEvents(source, limit);
      return json(res, 200, { count: rows.length, events: rows });
    }

    // ---- query: customer rollup
    if (path.startsWith("/customers/") && method === "GET") {
      const phone = decodeURIComponent(path.replace("/customers/", ""));
      const c = await getCustomer(phone);
      const events = await getEventsByPhone(phone, 25);
      return json(res, 200, { customer: c, recent_events: events });
    }

    // ---- write paths (auth required)
    if (method === "POST" && path.startsWith("/")) {
      if (!checkToken(req, url)) return json(res, 401, { error: "unauthorized" });

      const raw = await readBody(req);
      const payload = parseJsonOrFlow(raw);

      let result, kind;
      if (path === "/webhooks/shopify/draft-created") {
        kind = "shopify-draft";
        result = await handleDraftCreated(payload);
      } else if (path === "/webhooks/shopify/order-created") {
        kind = "shopify-order";
        result = await handleOrderCreated(payload);
      } else if (path === "/events") {
        kind = "generic-event";
        result = await handleGenericEvent(payload);
      } else {
        return json(res, 404, { error: "unknown path", path });
      }

      logEvent({
        ok: result.ok,
        kind,
        summary: `${kind} → ${result.id || result.error || "?"}`,
        result,
      });

      return json(res, result.ok ? 200 : 500, result);
    }

    return json(res, 404, { error: "not found", path });
  } catch (e) {
    console.error("router err:", e);
    return json(res, 500, { error: e.message });
  }
});

// ---------- boot -------------------------------------------------------------

initDb()
  .catch(e => console.error("DB init error:", e.message))
  .finally(() => {
    server.listen(PORT, () => {
      console.log(`Jarvis Webhook Relay + Hub API on :${PORT}`);
      console.log(`  DB ready: ${dbReady}`);
    });
  });
