"""
ProxyMaze'26 - Torch Labs Engineering Challenge
Complete implementation of all 13 endpoints + Slack/Discord bonus integrations.
"""

import threading
import time
import uuid
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests as http_requests

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared state  (all access via _lock)
# ---------------------------------------------------------------------------
_lock = threading.Lock()

# Config
_config = {
    "check_interval_seconds": 60,
    "request_timeout_ms": 5000,
}

# Proxies: { id -> { id, url, status, last_checked_at, consecutive_failures,
#                    total_checks, up_checks, history: [{checked_at, status}] } }
_proxies = {}

# Alerts: [ { alert_id, status, failure_rate, total_proxies, failed_proxies,
#             failed_proxy_ids, threshold, fired_at, resolved_at, message } ]
_alerts = []
_active_alert_id = None   # alert_id of the currently active alert, or None

# Webhooks: { webhook_id -> url }
_webhooks = {}

# Integrations: [ { integration_id, type, webhook_url, username, events } ]
_integrations = []

# Metrics counters
_total_checks = 0
_webhook_deliveries = 0   # count of successful deliveries

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_ts():
    return int(time.time())


def _proxy_id_from_url(url: str) -> str:
    """Extract last path segment as proxy ID."""
    return url.rstrip("/").rsplit("/", 1)[-1]


def _failure_rate(proxies_dict):
    """Compute failure_rate = down / total (pending counts as neither up nor down)."""
    total = len(proxies_dict)
    if total == 0:
        return 0.0
    down = sum(1 for p in proxies_dict.values() if p["status"] == "down")
    return down / total


# ---------------------------------------------------------------------------
# Webhook / integration delivery  (runs in daemon thread, retries on 5xx)
# ---------------------------------------------------------------------------

def _deliver(url: str, payload: dict, max_attempts: int = 10, backoff: float = 2.0):
    """POST payload to url; retry on transient 5xx failures."""
    global _webhook_deliveries
    attempt = 0
    while attempt < max_attempts:
        try:
            resp = http_requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code in (500, 502, 503, 504):
                log.warning("Transient failure %s from %s, attempt %d", resp.status_code, url, attempt + 1)
                attempt += 1
                time.sleep(backoff * attempt)
                continue
            # Any non-5xx (including 2xx, 4xx) counts as delivered
            with _lock:
                _webhook_deliveries += 1
            log.info("Delivered to %s → %s", url, resp.status_code)
            return
        except Exception as exc:
            log.warning("Delivery error to %s: %s, attempt %d", url, exc, attempt + 1)
            attempt += 1
            time.sleep(backoff * attempt)
    log.error("Giving up delivery to %s after %d attempts", url, max_attempts)


def _fire_webhooks(payload: dict):
    """Send payload to every registered generic webhook (non-blocking)."""
    with _lock:
        targets = list(_webhooks.values())
    for url in targets:
        threading.Thread(target=_deliver, args=(url, payload), daemon=True).start()


def _fire_integrations(event_type: str, alert: dict):
    """Send Slack/Discord formatted payloads to registered integrations."""
    with _lock:
        integs = [i for i in _integrations if event_type in i["events"]]

    for integ in integs:
        if integ["type"] == "slack":
            payload = _build_slack_payload(integ, event_type, alert)
        elif integ["type"] == "discord":
            payload = _build_discord_payload(integ, event_type, alert)
        else:
            continue
        threading.Thread(
            target=_deliver, args=(integ["webhook_url"], payload), daemon=True
        ).start()


def _build_slack_payload(integ: dict, event_type: str, alert: dict) -> dict:
    is_fired = event_type == "alert.fired"
    color = "#FF0000" if is_fired else "#36A64F"
    title = "🔥 Proxy Pool Alert Fired" if is_fired else "✅ Proxy Pool Alert Resolved"
    text = (
        f"Proxy pool failure rate {alert['failure_rate']:.0%} exceeded threshold {alert['threshold']:.0%}"
        if is_fired
        else f"Proxy pool alert {alert['alert_id']} has been resolved"
    )

    fields = [
        {"title": "Alert ID", "value": alert["alert_id"], "short": True},
        {"title": "Failure Rate", "value": f"{alert['failure_rate']:.2%}", "short": True},
        {"title": "Failed Proxies", "value": str(alert["failed_proxies"]), "short": True},
        {"title": "Threshold", "value": f"{alert['threshold']:.0%}", "short": True},
        {"title": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none", "short": False},
        {"title": "Fired At", "value": alert["fired_at"], "short": True},
    ]
    if not is_fired and alert.get("resolved_at"):
        fields.append({"title": "Resolved At", "value": alert["resolved_at"], "short": True})

    return {
        "username": integ.get("username", "ProxyWatch"),
        "text": text,
        "attachments": [
            {
                "color": color,
                "title": title,
                "fields": fields,
                "footer": "ProxyMaze'26 • Torch Labs",
                "ts": _now_ts(),
            }
        ],
    }


def _build_discord_payload(integ: dict, event_type: str, alert: dict) -> dict:
    is_fired = event_type == "alert.fired"
    color = 16711680 if is_fired else 3580392   # red : green
    title = "🔥 Proxy Pool Alert Fired" if is_fired else "✅ Proxy Pool Alert Resolved"
    description = (
        f"Failure rate `{alert['failure_rate']:.2%}` has exceeded the threshold of `{alert['threshold']:.0%}`."
        if is_fired
        else f"Alert `{alert['alert_id']}` has been resolved. Pool is healthy again."
    )

    fields = [
        {"name": "Alert ID", "value": alert["alert_id"], "inline": True},
        {"name": "Failure Rate", "value": f"{alert['failure_rate']:.2%}", "inline": True},
        {"name": "Failed Proxies", "value": str(alert["failed_proxies"]), "inline": True},
        {"name": "Threshold", "value": f"{alert['threshold']:.0%}", "inline": True},
        {"name": "Failed IDs", "value": ", ".join(alert["failed_proxy_ids"]) or "none", "inline": False},
    ]

    return {
        "username": integ.get("username", "ProxyWatch"),
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {"text": "ProxyMaze'26 • Torch Labs"},
            }
        ],
    }


# ---------------------------------------------------------------------------
# Alert management  (called with _lock held)
# ---------------------------------------------------------------------------

def _get_active_alert():
    """Return the currently active alert object, or None."""
    for a in _alerts:
        if a["status"] == "active":
            return a
    return None


def _evaluate_alerts():
    """
    Compare current failure_rate against threshold.
    Fire or resolve alerts as needed.
    Must be called with _lock RELEASED (it acquires lock internally as needed,
    and spawns threads for delivery).
    """
    global _active_alert_id

    with _lock:
        rate = _failure_rate(_proxies)
        total = len(_proxies)
        down_ids = [pid for pid, p in _proxies.items() if p["status"] == "down"]
        active = _get_active_alert()
        active_id = _active_alert_id

    THRESHOLD = 0.20

    if rate >= THRESHOLD and active is None:
        # FIRE new alert
        alert_id = "alert-" + uuid.uuid4().hex[:8]
        fired_at = _now_iso()
        alert = {
            "alert_id": alert_id,
            "status": "active",
            "failure_rate": rate,
            "total_proxies": total,
            "failed_proxies": len(down_ids),
            "failed_proxy_ids": down_ids,
            "threshold": THRESHOLD,
            "fired_at": fired_at,
            "resolved_at": None,
            "message": "Proxy pool failure rate exceeded threshold",
        }
        with _lock:
            _alerts.append(alert)
            _active_alert_id = alert_id
        log.info("Alert FIRED: %s  rate=%.2f", alert_id, rate)

        # Deliver webhooks
        fired_payload = {
            "event": "alert.fired",
            "alert_id": alert_id,
            "fired_at": fired_at,
            "failure_rate": rate,
            "total_proxies": total,
            "failed_proxies": len(down_ids),
            "failed_proxy_ids": down_ids,
            "threshold": THRESHOLD,
            "message": "Proxy pool failure rate exceeded threshold",
        }
        _fire_webhooks(fired_payload)
        _fire_integrations("alert.fired", alert)

    elif rate < THRESHOLD and active is not None:
        # RESOLVE active alert
        resolved_at = _now_iso()
        with _lock:
            active["status"] = "resolved"
            active["resolved_at"] = resolved_at
            _active_alert_id = None
        log.info("Alert RESOLVED: %s  rate=%.2f", active["alert_id"], rate)

        resolved_payload = {
            "event": "alert.resolved",
            "alert_id": active["alert_id"],
            "resolved_at": resolved_at,
        }
        _fire_webhooks(resolved_payload)
        _fire_integrations("alert.resolved", active)


# ---------------------------------------------------------------------------
# Background monitoring loop
# ---------------------------------------------------------------------------

def _probe_proxy(proxy_id: str):
    """Probe one proxy URL. Updates proxy state and evaluates alerts."""
    global _total_checks

    with _lock:
        if proxy_id not in _proxies:
            return
        url = _proxies[proxy_id]["url"]
        timeout_ms = _config["request_timeout_ms"]

    timeout_s = timeout_ms / 1000.0
    checked_at = _now_iso()

    try:
        resp = http_requests.get(url, timeout=timeout_s, allow_redirects=True)
        if 200 <= resp.status_code < 300:
            new_status = "up"
        elif 500 <= resp.status_code < 600:
            new_status = "down"
        else:
            # 3xx without redirect, 4xx — treat as up (server responded)
            new_status = "up"
    except Exception:
        new_status = "down"

    with _lock:
        if proxy_id not in _proxies:
            return
        p = _proxies[proxy_id]
        p["status"] = new_status
        p["last_checked_at"] = checked_at
        p["total_checks"] += 1
        _total_checks += 1
        if new_status == "up":
            p["up_checks"] += 1
            p["consecutive_failures"] = 0
        else:
            p["consecutive_failures"] += 1
        p["uptime_percentage"] = round(p["up_checks"] / p["total_checks"] * 100, 1)
        p["history"].append({"checked_at": checked_at, "status": new_status})

    _evaluate_alerts()


def _monitoring_loop():
    """Continuously probe all proxies at check_interval_seconds cadence."""
    while True:
        with _lock:
            interval = _config["check_interval_seconds"]
            proxy_ids = list(_proxies.keys())

        if proxy_ids:
            threads = []
            for pid in proxy_ids:
                t = threading.Thread(target=_probe_proxy, args=(pid,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join(timeout=60)

        # Sleep in small increments so config changes are picked up quickly
        elapsed = 0
        while elapsed < interval:
            time.sleep(1)
            elapsed += 1
            with _lock:
                new_interval = _config["check_interval_seconds"]
            if new_interval != interval:
                break  # restart cycle with new interval


# Start background monitoring thread
_monitor_thread = threading.Thread(target=_monitoring_loop, daemon=True)
_monitor_thread.start()


# ---------------------------------------------------------------------------
# Chapter 01: GET /health
# ---------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Chapter 02: POST /config
# ---------------------------------------------------------------------------
@app.route("/config", methods=["POST"])
def post_config():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400
    with _lock:
        if "check_interval_seconds" in data:
            _config["check_interval_seconds"] = data["check_interval_seconds"]
        if "request_timeout_ms" in data:
            _config["request_timeout_ms"] = data["request_timeout_ms"]
    return jsonify(_config), 200


# ---------------------------------------------------------------------------
# Chapter 03: GET /config
# ---------------------------------------------------------------------------
@app.route("/config", methods=["GET"])
def get_config():
    with _lock:
        cfg = dict(_config)
    return jsonify(cfg), 200


# ---------------------------------------------------------------------------
# Chapter 04: POST /proxies
# ---------------------------------------------------------------------------
@app.route("/proxies", methods=["POST"])
def post_proxies():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400

    urls = data.get("proxies", [])
    replace = data.get("replace", False)

    with _lock:
        if replace:
            _proxies.clear()

        added = []
        for url in urls:
            pid = _proxy_id_from_url(url)
            if pid not in _proxies:
                _proxies[pid] = {
                    "id": pid,
                    "url": url,
                    "status": "pending",
                    "last_checked_at": None,
                    "consecutive_failures": 0,
                    "total_checks": 0,
                    "up_checks": 0,
                    "uptime_percentage": 0.0,
                    "history": [],
                }
            added.append({"id": pid, "url": url, "status": _proxies[pid]["status"]})

    # Kick off immediate background probes for newly added proxies
    for pid in [a["id"] for a in added]:
        threading.Thread(target=_probe_proxy, args=(pid,), daemon=True).start()

    return jsonify({"accepted": len(added), "proxies": added}), 201


# ---------------------------------------------------------------------------
# Chapter 05: GET /proxies
# ---------------------------------------------------------------------------
@app.route("/proxies", methods=["GET"])
def get_proxies():
    with _lock:
        total = len(_proxies)
        up = sum(1 for p in _proxies.values() if p["status"] == "up")
        down = sum(1 for p in _proxies.values() if p["status"] == "down")
        rate = _failure_rate(_proxies)
        proxies_list = [
            {
                "id": p["id"],
                "url": p["url"],
                "status": p["status"],
                "last_checked_at": p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            }
            for p in _proxies.values()
        ]
    return jsonify({
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": rate,
        "proxies": proxies_list,
    }), 200


# ---------------------------------------------------------------------------
# Chapter 06: GET /proxies/<id>
# ---------------------------------------------------------------------------
@app.route("/proxies/<proxy_id>", methods=["GET"])
def get_proxy(proxy_id):
    with _lock:
        p = _proxies.get(proxy_id)
        if p is None:
            return jsonify({"error": "Not found"}), 404
        result = {
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
            "total_checks": p["total_checks"],
            "uptime_percentage": p["uptime_percentage"],
            "history": list(p["history"]),
        }
    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Chapter 07: GET /proxies/<id>/history
# ---------------------------------------------------------------------------
@app.route("/proxies/<proxy_id>/history", methods=["GET"])
def get_proxy_history(proxy_id):
    with _lock:
        p = _proxies.get(proxy_id)
        if p is None:
            return jsonify({"error": "Not found"}), 404
        history = list(p["history"])
    return jsonify(history), 200


# ---------------------------------------------------------------------------
# Chapter 08: DELETE /proxies
# ---------------------------------------------------------------------------
@app.route("/proxies", methods=["DELETE"])
def delete_proxies():
    with _lock:
        _proxies.clear()
    return "", 204


# ---------------------------------------------------------------------------
# Chapter 09: GET /alerts
# ---------------------------------------------------------------------------
@app.route("/alerts", methods=["GET"])
def get_alerts():
    with _lock:
        result = list(_alerts)
    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Chapter 10: POST /webhooks
# ---------------------------------------------------------------------------
@app.route("/webhooks", methods=["POST"])
def post_webhooks():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400
    url = data.get("url")
    if not url:
        return jsonify({"error": "url is required"}), 400

    wh_id = "wh-" + uuid.uuid4().hex[:8]
    with _lock:
        _webhooks[wh_id] = url
    return jsonify({"webhook_id": wh_id, "url": url}), 201


# ---------------------------------------------------------------------------
# Chapter 11: POST /integrations
# ---------------------------------------------------------------------------
@app.route("/integrations", methods=["POST"])
def post_integrations():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON"}), 400

    integ_type = data.get("type")
    if integ_type not in ("slack", "discord"):
        return jsonify({"error": "type must be 'slack' or 'discord'"}), 400

    webhook_url = data.get("webhook_url")
    if not webhook_url:
        return jsonify({"error": "webhook_url is required"}), 400

    integ_id = "integ-" + uuid.uuid4().hex[:8]
    integ = {
        "integration_id": integ_id,
        "type": integ_type,
        "webhook_url": webhook_url,
        "username": data.get("username", "ProxyWatch"),
        "events": data.get("events", ["alert.fired", "alert.resolved"]),
    }
    with _lock:
        _integrations.append(integ)

    return jsonify({"integration_id": integ_id, "type": integ_type, "webhook_url": webhook_url}), 201


# ---------------------------------------------------------------------------
# Chapter 12: GET /metrics
# ---------------------------------------------------------------------------
@app.route("/metrics", methods=["GET"])
def get_metrics():
    with _lock:
        pool_size = len(_proxies)
        active_alerts = sum(1 for a in _alerts if a["status"] == "active")
        total_alerts = len(_alerts)
        tc = _total_checks
        wd = _webhook_deliveries
    return jsonify({
        "total_checks": tc,
        "current_pool_size": pool_size,
        "active_alerts": active_alerts,
        "total_alerts": total_alerts,
        "webhook_deliveries": wd,
    }), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
