# Jarvis Webhook Relay + Hub API

Replaces the Mac Studio `hub-api` (port 4821) for cloud-side workflows. Receives webhook traffic from N8N, Shopify Flow, CallRail, QUO, and Retell, then persists events to Postgres.

Render-hosted, single URL, no SSH, no Tailscale.

---

## Deploy

This repo is wired for one-click deploy via `render.yaml`:

1. Connect repo at https://dashboard.render.com/blueprints
2. Click **Apply** — Render creates the web service AND the free Postgres database, links them via `DATABASE_URL`
3. First boot creates the schema automatically

---

## Endpoints

| Method | Path                                  | Auth | Use |
|--------|---------------------------------------|------|-----|
| GET    | `/`                                   | none | liveness probe |
| GET    | `/health`                             | none | JSON status (db, event count) |
| POST   | `/events`                             | yes  | **N8N Hub Writer** — generic event insert |
| POST   | `/webhooks/shopify/draft-created`     | yes  | Shopify Flow draft webhook |
| POST   | `/webhooks/shopify/order-created`     | yes  | Shopify Flow order webhook |
| GET    | `/events?phone=+1...&limit=50`        | none | Read events for a phone number |
| GET    | `/events/recent?source=callrail`      | none | Tail recent events |
| GET    | `/customers/+1XXXXXXXXXX`             | none | Customer rollup + last 25 events |
| GET    | `/log`                                | yes  | last 50 in-memory events |

**Auth header:** `X-Api-Key: <FLOW_TOKEN>`  (or `?token=<FLOW_TOKEN>`)
Token: `727s155a3u51692n670b7s036h2j4n1j5j091h597r3c053u2q1n515o5a3q6r7f`

---

## N8N "Hub Writer" replacement

In each cloud N8N workflow that previously POSTed to the Mac Studio `hub-api`, change the URL:

```
OLD:  http://100.85.155.110:4821/events
NEW:  https://jarvis-webhook-relay.onrender.com/events
```

(Replace with the actual Render URL after deploy.)

Add header `X-Api-Key: 727s155a3u51692n670b7s036h2j4n1j5j091h597r3c053u2q1n515o5a3q6r7f`.

### Body shape (matches hub.db `events` table)

```json
{
  "source": "callrail",          // required: callrail|openphone|retell|gmail|shopify|...
  "type": "call",                // call|missed_call|sms_in|sms_out|order|draft_order|email|...
  "timestamp": 1745000000000,    // unix ms; defaults to now
  "id": "CAL019d24e560a37d99",   // optional; deduplicates if set
  "from_addr": "+15551234567",
  "to_addr": "+16029628859",
  "caller_name": "Jane Smith",
  "subject": "",                 // for emails
  "order_num": "",               // for shopify
  "summary": "60s call · phase converter inquiry",
  "content": "<full transcript>",
  "raw": { "...original webhook payload..." }
}
```

Returns `{"ok": true, "id": "<16-char-id>"}` on success.

---

## Customer lookup (used by Jarvis dashboard)

```bash
# Recent events for a phone
curl 'https://jarvis-webhook-relay.onrender.com/events?phone=%2B15551234567&limit=20'

# Customer rollup
curl 'https://jarvis-webhook-relay.onrender.com/customers/%2B15551234567'

# Tail recent calls across all numbers
curl 'https://jarvis-webhook-relay.onrender.com/events/recent?source=callrail&limit=10'
```

No auth required for read endpoints.

---

## Schema

Mirrors the Mac Studio hub.db:

```sql
events (
  id          TEXT PRIMARY KEY,    -- 16-char md5
  source      TEXT NOT NULL,       -- callrail|openphone|retell|gmail|shopify
  type        TEXT NOT NULL,       -- call|sms_in|order|...
  timestamp   BIGINT NOT NULL,     -- unix ms
  read        SMALLINT,
  from_addr   TEXT,
  to_addr     TEXT,
  caller_name TEXT,
  subject     TEXT,
  order_num   TEXT,
  summary     TEXT,
  content     TEXT,
  raw         JSONB,
  created_at  TIMESTAMPTZ
);

customers (
  phone           TEXT PRIMARY KEY,
  name, company, email,
  total_calls, total_texts, total_orders, total_revenue,
  last_contact, tags,
  updated_at
);
```

Customer rollups are bumped automatically on every event insert.

---

## Local dev

```bash
npm install
DATABASE_URL=postgres://user:pass@localhost:5432/jarvishub PORT=8080 npm start
```

Without `DATABASE_URL`, server runs in memory-only mode (writes succeed but don't persist) — use only for smoke testing.
