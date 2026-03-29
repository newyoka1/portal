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
    ("APP_URL",             "https://connect.politikanyc.com", "Portal URL (for approval links)", "email", False),

    # SFTP
    ("SFTP_HOST",           "", "SFTP Host",                 "sftp",   False),
    ("SFTP_PORT",           "2222", "SFTP Port",             "sftp",   False),
    ("SFTP_USER",           "", "SFTP Username",             "sftp",   False),
    ("SFTP_PASS",           "", "SFTP Password",             "sftp",   True),
    ("SFTP_DIR",            "ad-images", "SFTP Upload Directory", "sftp", False),
    ("SFTP_BASE_URL",       "", "SFTP Public URL Base",      "sftp",   False),

    # Polling
    ("RECEIPT_POLL_SCHEDULE",   "hourly", "Receipt Poll Schedule", "polling", False),

    # FB Ad Approval
    ("BASE_URL", "https://connect.politikanyc.com/fb", "FB Ad Approval Base URL", "fb_approval", False),
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
            # Remove stale settings that no longer exist in DEFAULTS
            valid_keys = {d[0] for d in DEFAULTS}
            stale = db.query(PortalSetting).filter(
                PortalSetting.key.notin_(valid_keys)
            ).all()
            for s in stale:
                db.delete(s)

            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.warning("Could not seed portal settings: %s", e)
