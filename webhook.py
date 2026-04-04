"""Fire outbound webhooks on approval events (reverse sync to HubSpot etc.)."""
import hashlib
import hmac
import json
import logging
import threading

logger = logging.getLogger(__name__)


def fire_webhook(payload: dict) -> None:
    """POST *payload* as JSON to the configured WEBHOOK_URL (non-blocking)."""
    from portal_config import get_setting

    url = get_setting("WEBHOOK_URL", "")
    if not url:
        return

    secret = get_setting("WEBHOOK_SECRET", "")
    body = json.dumps(payload, default=str)
    headers = {"Content-Type": "application/json"}
    if secret:
        sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        headers["X-Signature-SHA256"] = sig

    def _post():
        try:
            import urllib.request
            req = urllib.request.Request(url, data=body.encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("Webhook delivered to %s (status %s)", url, resp.status)
        except Exception as exc:
            logger.warning("Webhook to %s failed: %s", url, exc)

    threading.Thread(target=_post, daemon=True).start()
