// poller.js — pulls CallRail + QUO + Shopify every 5 min and inserts into events table.
// Reuses insertEvent from server.js. ESM module.
import crypto from "node:crypto";

const QUO_KEY = process.env.QUO_KEY || "0opOboF8pDshpmpGndl31aoqwm5ZLm23";
const CR_KEY = process.env.CR_KEY || "a67c8451aa1bb6409b013366259b0d28";
const CR_ACCT = process.env.CR_ACCT || "906309465";
const QUO_MAIN_ID = process.env.QUO_MAIN_ID || "PNpkeFjA2S";
const QUO_SENDER = process.env.QUO_SENDER || "+16029628859";
const SHOP_TOKEN = process.env.SHOP_TOKEN;
const SHOP_STORE = process.env.SHOP_STORE || "electricmotorexperts.myshopify.com";
const UA = "Mozilla/5.0 (Jarvis/1.0)";

// Pull-window in minutes, lookback overlap to avoid gaps
const WINDOW_MIN = Number(process.env.POLL_WINDOW_MIN || 15);
const INTERVAL_MIN = Number(process.env.POLL_INTERVAL_MIN || 5);

function shortId() { return crypto.randomBytes(8).toString("hex"); }

async function fetchJSON(url, headers) {
  const r = await fetch(url, { headers: { "User-Agent": UA, "Accept": "application/json", ...headers } });
  if (!r.ok) throw new Error(`${url} → ${r.status} ${(await r.text()).slice(0,120)}`);
  return r.json();
}

async function pollCallRail(insertEvent, sinceMs) {
  let posted = 0;
  try {
    const url = `https://api.callrail.com/v3/a/${CR_ACCT}/calls.json` +
      `?per_page=100&direction=inbound` +
      `&fields=id,customer_name,customer_phone_number,duration,call_summary,start_time,transcription,voicemail,recording`;
    const data = await fetchJSON(url, { Authorization: `Token token="${CR_KEY}"` });
    for (const c of (data.calls || [])) {
      const t = Date.parse(c.start_time);
      if (!t || t < sinceMs) continue;
      const phone = c.customer_phone_number || "";
      let name = c.customer_name || "";
      if (["wireless caller","unknown","unavailable","anonymous"].includes(name.toLowerCase())) name = "";
      const r = await insertEvent({
        id: `cr_${c.id}`,
        source: "callrail",
        type: c.voicemail ? "voicemail" : "call",
        timestamp: String(t),
        from_addr: phone,
        to_addr: "",
        caller_name: name,
        subject: "",
        order_num: "",
        summary: c.call_summary || "",
        content: c.transcription || "",
        raw: { call_id: c.id, duration: c.duration, started_at: c.start_time, recording: c.recording },
      });
      if (r && r.ok) posted++;
      else if (r && r.error) console.warn(`[poller cr] ${c.id}: ${r.error}`);
    }
  } catch (e) {
    console.error(`[poller cr] fetch err: ${e.message}`);
  }
  return posted;
}

async function pollQuo(insertEvent, sinceMs) {
  let posted = 0;
  try {
    const cu = `https://api.openphone.com/v1/conversations?phoneNumberId=${QUO_MAIN_ID}&maxResults=50`;
    const cdata = await fetchJSON(cu, { Authorization: QUO_KEY });
    for (const cv of (cdata.data || [])) {
      const lt = Date.parse(cv.lastActivityAt);
      if (!lt || lt < sinceMs) continue;
      const other = (cv.participants || []).find(p => p !== QUO_SENDER);
      if (!other) continue;
      const mu = `https://api.openphone.com/v1/messages?phoneNumberId=${QUO_MAIN_ID}&participants[]=${encodeURIComponent(other)}&maxResults=20`;
      let msgs;
      try { msgs = (await fetchJSON(mu, { Authorization: QUO_KEY })).data || []; }
      catch (e) { console.warn(`[poller quo msgs] ${other}: ${e.message}`); continue; }
      for (const m of msgs) {
        const ct = Date.parse(m.createdAt);
        if (!ct || ct < sinceMs) continue;
        if (m.direction !== "incoming") continue;
        const body = m.body || m.text || "";
        const r = await insertEvent({
          id: `quo_${m.id || shortId()}`,
          source: "quo",
          type: "text",
          timestamp: String(ct),
          from_addr: other,
          to_addr: QUO_SENDER,
          caller_name: "",
          subject: "",
          order_num: "",
          summary: body.slice(0, 120),
          content: body,
          raw: { msg_id: m.id, created_at: m.createdAt },
        });
        if (r && r.ok) posted++;
        else if (r && r.error) console.warn(`[poller quo] ${m.id}: ${r.error}`);
      }
    }
  } catch (e) {
    console.error(`[poller quo] fetch err: ${e.message}`);
  }
  return posted;
}

async function pollShopify(insertEvent, sinceMs) {
  if (!SHOP_TOKEN) return 0;
  let posted = 0;
  const sinceISO = new Date(sinceMs).toISOString();
  // Recently updated drafts (catches new drafts + status changes)
  try {
    const url = `https://${SHOP_STORE}/admin/api/2024-04/draft_orders.json?limit=50&updated_at_min=${encodeURIComponent(sinceISO)}`;
    const data = await fetchJSON(url, { "X-Shopify-Access-Token": SHOP_TOKEN });
    for (const d of (data.draft_orders || [])) {
      const phone = d.phone || (d.shipping_address && d.shipping_address.phone) || (d.customer && d.customer.phone) || "";
      const cust = d.customer || {};
      const name = `${cust.first_name||""} ${cust.last_name||""}`.trim();
      const r = await insertEvent({
        id: `shop_draft_${d.id}_${Date.parse(d.updated_at)}`,
        source: "shopify_draft",
        type: "draft",
        timestamp: String(Date.parse(d.updated_at)),
        from_addr: phone, to_addr: "", caller_name: name, subject: "",
        order_num: d.name || "",
        summary: `Draft ${d.name} · $${d.total_price} · ${d.status}`,
        content: "",
        raw: { id: d.id, total: d.total_price, status: d.status, invoice_url: d.invoice_url, email: d.email, created_at: d.created_at, updated_at: d.updated_at },
      });
      if (r && r.ok) posted++;
      else if (r && r.error) console.warn(`[poller draft] ${d.name}: ${r.error}`);
    }
  } catch (e) { console.error(`[poller drafts] ${e.message}`); }
  // Recently updated orders
  try {
    const url = `https://${SHOP_STORE}/admin/api/2024-04/orders.json?status=any&limit=50&updated_at_min=${encodeURIComponent(sinceISO)}`;
    const data = await fetchJSON(url, { "X-Shopify-Access-Token": SHOP_TOKEN });
    for (const o of (data.orders || [])) {
      const phone = o.phone || (o.shipping_address && o.shipping_address.phone) || (o.customer && o.customer.phone) || "";
      const cust = o.customer || {};
      const name = `${cust.first_name||""} ${cust.last_name||""}`.trim();
      const r = await insertEvent({
        id: `shop_order_${o.id}_${Date.parse(o.updated_at)}`,
        source: "shopify_order",
        type: "order",
        timestamp: String(Date.parse(o.updated_at)),
        from_addr: phone, to_addr: "", caller_name: name, subject: "",
        order_num: o.name || "",
        summary: `Order ${o.name} · $${o.total_price} · ${o.financial_status}`,
        content: "",
        raw: { id: o.id, total: o.total_price, financial_status: o.financial_status, fulfillment_status: o.fulfillment_status, email: o.email, created_at: o.created_at, updated_at: o.updated_at },
      });
      if (r && r.ok) posted++;
      else if (r && r.error) console.warn(`[poller order] ${o.name}: ${r.error}`);
    }
  } catch (e) { console.error(`[poller orders] ${e.message}`); }
  return posted;
}

let running = false;
async function tick(insertEvent) {
  if (running) return;
  running = true;
  const since = Date.now() - WINDOW_MIN * 60_000;
  try {
    const [cr, quo, shop] = await Promise.all([
      pollCallRail(insertEvent, since),
      pollQuo(insertEvent, since),
      pollShopify(insertEvent, since),
    ]);
    if (cr || quo || shop) {
      console.log(`[poller] +${cr} CR · +${quo} QUO · +${shop} Shopify`);
    }
  } catch (e) {
    console.error(`[poller tick] ${e.message}`);
  } finally {
    running = false;
  }
}

function startPoller(insertEvent) {
  if (process.env.DISABLE_POLLER === "1") {
    console.log("[poller] disabled via DISABLE_POLLER=1");
    return;
  }
  console.log(`[poller] starting · window=${WINDOW_MIN}m · interval=${INTERVAL_MIN}m`);
  // First tick after 30s so Render can finish booting
  setTimeout(() => tick(insertEvent), 30_000);
  setInterval(() => tick(insertEvent), INTERVAL_MIN * 60_000);
}

export { startPoller };
