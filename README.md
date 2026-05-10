# ProxyMaze'26 — Torch Labs Engineering Challenge

Real-time proxy pool monitoring HTTP API.  All 13 endpoints + Slack/Discord bonus integrations.

## Quick Start

```bash
pip install flask requests
python app.py          # starts on port 8080
```

Or with gunicorn for production:
```bash
pip install gunicorn
gunicorn -w 1 -b 0.0.0.0:8080 app:app
```

> **Note:** Use `-w 1` (single worker) — the background monitoring loop and shared state must run in one process.

---

## API Reference

### Chapter 01 — `GET /health`
```json
{"status": "ok"}
```

### Chapter 02 — `POST /config`
```json
{"check_interval_seconds": 15, "request_timeout_ms": 3000}
```
Returns `200 OK` with the active config. Takes effect immediately for all subsequent probes.

### Chapter 03 — `GET /config`
Returns the currently active config (mirrors last `POST /config`).

### Chapter 04 — `POST /proxies`
```json
{
  "proxies": ["https://proxy-provider.example/proxy/px-101"],
  "replace": true
}
```
- `replace: true` clears the pool first; omitted/false appends.
- New proxies start as `pending` and transition on their own via background probes.
- Returns `201 Created` with `{ accepted, proxies[] }`.

### Chapter 05 — `GET /proxies`
Returns pool summary (`total`, `up`, `down`, `failure_rate`) plus per-proxy state.

### Chapter 06 — `GET /proxies/{id}`
Full dossier: adds `total_checks`, `uptime_percentage`, `history[]`. Returns `404` for unknown IDs.

### Chapter 07 — `GET /proxies/{id}/history`
JSON array of `{checked_at, status}` entries. Returns `404` for unknown IDs.

### Chapter 08 — `DELETE /proxies`
Clears the pool. Returns `204 No Content`. **Alert history is preserved.**

### Chapter 09 — `GET /alerts`
All alerts (active and resolved). Alert lifecycle:
- Fires when `failure_rate >= 0.20`
- Resolves when `failure_rate < 0.20`
- At most one `active` alert at a time; new breach after resolution mints a new `alert_id`

### Chapter 10 — `POST /webhooks`
```json
{"url": "https://receiver.example/hook"}
```
Registers a URL to receive `alert.fired` / `alert.resolved` events.
- Delivered within 60 seconds of state transition
- Retries on 5xx transient failures
- Exactly one successful delivery per transition

### Chapter 11 — `POST /integrations`
```json
{"type": "slack", "webhook_url": "...", "username": "ProxyWatch", "events": ["alert.fired", "alert.resolved"]}
{"type": "discord", "webhook_url": "...", "username": "ProxyWatch", "events": ["alert.fired", "alert.resolved"]}
```

### Chapter 12 — `GET /metrics`
```json
{
  "total_checks": 120,
  "current_pool_size": 10,
  "active_alerts": 1,
  "total_alerts": 3,
  "webhook_deliveries": 4
}
```

---

## Behavioral Notes

| Rule | Implementation |
|------|----------------|
| Monitoring cadence | Background daemon thread; respects `check_interval_seconds` live |
| Probe classification | `2xx within timeout_ms` → up; timeout/refused/5xx → down |
| Threshold | `≥ 0.20` fires; `< 0.20` resolves |
| Alert dedup | Single active alert; continuous breach never fires twice |
| Webhook retry | Retries on 500/502/503/504; stops on first success |
| Proxy ID | Last URL path segment (deterministic, stable) |
| Unknown JSON fields | Silently ignored on all endpoints |
| Pool clear | Alerts and history survive `DELETE /proxies` and `replace: true` |

---

## Run Tests

```bash
python test_all.py    # 80 checks covering all chapters
```
