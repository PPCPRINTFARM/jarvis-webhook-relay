# Jarvis Webhook Relay

Tiny Node 20 service that receives Shopify Flow webhooks and SMS-pings Glen via QUO.

No npm dependencies. One file, ~300 lines.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Liveness check |
| GET | `/health` | JSON status |
| POST | `/webhooks/shopify/draft-created` | New draft order from Shopify Flow |
| POST | `/webhooks/shopify/order-created` | New order from Shopify Flow |
| GET | `/log?token=...` | Last 50 received events |

`/draft-order` and `/order` are accepted as aliases (matches the placeholder URLs in your Flow workflows).

## Auth

Every POST requires either:
- Header: `X-Api-Key: <FLOW_TOKEN>`
- Or query param: `?token=<FLOW_TOKEN>`

## Deploy to Render (3 minutes)

1. Push this folder to a new GitHub repo (or to a folder in your existing PPCPRINTFARM org).
2. On Render → New → Web Service → Connect repo.
3. Render auto-detects `render.yaml`. Click **Create Web Service**.
4. Wait ~60s for first deploy. Render will give you a URL like `https://jarvis-webhook-relay.onrender.com`.
5. Test: `curl https://jarvis-webhook-relay.onrender.com/health`

## Configure Shopify Flow

In each of the two drafted workflows, replace the placeholder URL:

| Workflow | New URL |
|---|---|
| Workflow 1 — Draft order | `https://jarvis-webhook-relay.onrender.com/webhooks/shopify/draft-created` |
| Workflow 2 — Order | `https://jarvis-webhook-relay.onrender.com/webhooks/shopify/order-created` |

Add header: `X-Api-Key: 727s155a3u51692n670b7s036h2j4n1j5j091h597r3c053u2q1n515o5a3q6r7f`

Body (JSON) for **draft-created**:
```json
{
  "customer_name": "{{ draftOrder.customer.displayName }}",
  "phone": "{{ draftOrder.customer.phone }}",
  "email": "{{ draftOrder.customer.email }}",
  "created_at": "{{ draftOrder.createdAt }}",
  "invoice_url": "{{ draftOrder.invoiceUrl }}",
  "total_price": "{{ draftOrder.totalPrice }}",
  "name": "{{ draftOrder.name }}"
}
```

Body for **order-created**:
```json
{
  "customer_name": "{{ order.customer.displayName }}",
  "phone": "{{ order.customer.phone }}",
  "email": "{{ order.customer.email }}",
  "created_at": "{{ order.createdAt }}",
  "order_url": "{{ order.statusPageUrl }}",
  "total_price": "{{ order.totalPrice }}",
  "name": "{{ order.name }}"
}
```

## What you'll get

When a draft of $1,000+ is created:

```
Jarvis: New draft #D-12345 · $9,800
Bryan Kimball · +18005551212 📋QUOTED 2d
https://electricmotorexperts.myshopify.com/...invoiceUrl
```

When any order is placed:

```
🎉 Jarvis: Order #26-19562 · $3,169
Jason Aldofer · +16122034172 ✅BOUGHT
https://...order-status-url
```

The status badge:
- ✅BOUGHT — existing customer with prior paid orders
- 📋QUOTED Nd — has an open draft from N days ago
- 🆕NEW — first contact

## Tweak

Change `DRAFT_SMS_THRESHOLD` (default 1000) or `ORDER_SMS_THRESHOLD` (default 0) in Render env vars to filter the noise.
