"""
Unified portal configuration — DB-backed settings with env var fallback.

Usage:
    from portal_config import get_setting

    token = get_setting("META_ACCESS_TOKEN")  # checks DB first, then os.getenv()

Settings are cached for 60 seconds to avoid hitting the DB on every request.
"""
import logging
import os
import time

logger = logging.getLogger(__name__)

_cache: dict[str, str] = {}
_cache_ts: float = 0
_CACHE_TTL = 60  # seconds


def _refresh_cache():
    """Load all portal_settings rows into the in-memory cache."""
    global _cache, _cache_ts
    try:
        from database import SessionLocal
        from models import PortalSetting
        db = SessionLocal()
        try:
            rows = db.query(PortalSetting).all()
            _cache = {r.key: (r.value or "") for r in rows}
            _cache_ts = time.time()
        finally:
            db.close()
    except Exception as e:
        logger.debug("Could not refresh portal settings cache: %s", e)


def get_setting(key: str, default: str = "") -> str:
    """
    Get a portal setting. Priority:
      1. DB value (portal_settings table, 60s cache)
      2. Environment variable
      3. Provided default
    """
    global _cache_ts
    if time.time() - _cache_ts > _CACHE_TTL:
        _refresh_cache()

    val = _cache.get(key, "")
    if val:
        return val
    return os.getenv(key, default)


# ── Default settings to seed on first run ────────────────────────────────────
# (key, default_value, label, category, is_secret)
DEFAULTS = [
    # Meta / Facebook
    ("META_ACCESS_TOKEN",   "", "Meta System User Token",    "meta",   True),
    ("META_BUSINESS_IDS",   "", "Business Manager IDs (comma-separated)", "meta", False),
    ("FB_APP_ID",           "", "Facebook App ID",           "meta",   False),
    ("FB_APP_SECRET",       "", "Facebook App Secret",       "meta",   True),
    ("META_API_VERSION",    "v21.0", "Meta API Version",     "meta",   False),

    # Email
    ("GMAIL_SENDER_EMAIL",  "", "Gmail Sender Email",        "email",  False),
    ("GMAIL_ADDRESS",       "", "Gmail Poller Address (impersonate)", "email", False),
    ("GMAIL_APP_PASSWORD",  "", "Gmail App Password",        "email",  True),
    ("APP_URL",             "https://politika.run", "Portal URL (for approval links)", "email", False),

    # Email Approval
    ("EMAIL_QUEUE_ALIASES", "email@politikanyc.com", "Additional inbox aliases to pull into queue (comma-separated)", "email", False),
    ("EMAIL_DIRECT_ALIAS", "direct@politikanyc.com", "Alias that triggers auto-send for approval (no manual step)", "email", False),
    ("EMAIL_SUBJECT_FILTER", "test", "Subject Filter Word (only ingest emails containing this)", "email_approval", False),
    ("APPROVAL_DEADLINE_HOURS", "48", "Default Approval Deadline (hours)", "email_approval", False),
    ("APPROVAL_REMINDER_HOURS", "24", "Send Reminder After (hours)", "email_approval", False),

    # Webhook (reverse sync)
    ("WEBHOOK_URL", "", "Webhook URL for approval decisions", "email_approval", False),
    ("WEBHOOK_SECRET", "", "Webhook signing secret (HMAC-SHA256)", "email_approval", True),

    # Twilio SMS (optional — for texting approval links)
    ("TWILIO_ACCOUNT_SID",  "", "Twilio Account SID",              "twilio", True),
    ("TWILIO_AUTH_TOKEN",   "", "Twilio Auth Token",               "twilio", True),
    ("TWILIO_PHONE_NUMBER", "", "Twilio Phone Number (e.g. +1...)", "twilio", False),

    # FB Ad Approval
    ("BASE_URL", "https://politika.run/fb", "FB Ad Approval Base URL", "fb_approval", False),

    # Voter Pipeline tokens are managed dynamically via the settings UI
    # (HUBSPOT_TOKEN_*, CM_API_KEY_*, MAILCHIMP_KEY_* rows are user-created)

    # Facebook Custom Audience export (voter file → Meta)
    ("FB_ACCESS_TOKEN",  "", "FB Token for Voter Audiences (ads_management scope)", "meta",  True),
    ("FB_AD_ACCOUNT_ID", "", "FB Ad Account ID for Audiences (numeric, no act_ prefix)", "meta", False),

    # AI / Claude
    ("ANTHROPIC_API_KEY", "", "Anthropic API Key (for Voter Chat)", "ai", True),

    # Nightly CRM automation
    ("VOTER_NIGHTLY_SYNC", "false", "Enable Nightly CRM Sync (true/false)", "voter", False),
    ("VOTER_SYNC_HOUR",    "2",     "Nightly Sync Hour (0–23, server time)",  "voter", False),

]


def seed_defaults():
    """Insert default settings that don't exist yet. Safe to call on every startup."""
    try:
        from database import SessionLocal
        from models import PortalSetting
        db = SessionLocal()
        try:
            for key, default, label, category, is_secret in DEFAULTS:
                existing = db.query(PortalSetting).filter(PortalSetting.key == key).first()
                if not existing:
                    # Pre-populate from env var if available
                    env_val = os.getenv(key, "")
                    db.add(PortalSetting(
                        key=key,
                        value=env_val or default,
                        label=label,
                        category=category,
                        is_secret=is_secret,
                    ))
            # Remove stale settings — but preserve dynamically-managed voter tokens
            DYNAMIC_PREFIXES = ("HUBSPOT_TOKEN_", "CM_API_KEY_", "MAILCHIMP_KEY_", "FB_")
            valid_keys = {d[0] for d in DEFAULTS}
            stale = db.query(PortalSetting).filter(
                PortalSetting.key.notin_(valid_keys)
            ).all()
            for s in stale:
                if not any(s.key.startswith(p) for p in DYNAMIC_PREFIXES):
                    db.delete(s)

            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.warning("Could not seed portal settings: %s", e)
