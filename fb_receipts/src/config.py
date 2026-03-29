import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Portal settings helper ────────────────────────────────────────────────────
def _ps(key, default=""):
    """Read from portal_config DB settings, fall back to env var."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from portal_config import get_setting
        return get_setting(key, default)
    except Exception:
        return os.getenv(key, default)

# Meta
META_ACCESS_TOKEN = _ps("META_ACCESS_TOKEN", "")
META_BUSINESS_IDS = [
    bid.strip()
    for bid in _ps("META_BUSINESS_IDS", "").split(",")
    if bid.strip()
]
META_API_VERSION = _ps("META_API_VERSION", "v21.0")
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

# Google — resolve credentials from env var (Railway) or local file (dev)
import base64, json, tempfile

_sa_b64  = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
_sa_raw  = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
_sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service_account.json")

if _sa_b64 or _sa_raw:
    # Write the env-var credentials to a temp file so gspread/google-auth can read it
    _data = base64.b64decode(_sa_b64) if _sa_b64 else _sa_raw.encode()
    _tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="wb")
    _tmp.write(_data)
    _tmp.close()
    GOOGLE_SERVICE_ACCOUNT_FILE = _tmp.name
else:
    GOOGLE_SERVICE_ACCOUNT_FILE = _sa_file

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# Gmail
GMAIL_SENDER_EMAIL = _ps("GMAIL_SENDER_EMAIL", "")
GMAIL_APP_PASSWORD = _ps("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = _ps("NOTIFY_EMAIL", "George@politikanyc.com")

# Schedule
SCHEDULE_FREQUENCY = os.getenv("SCHEDULE_FREQUENCY", "weekly")
SCHEDULE_DAY = os.getenv("SCHEDULE_DAY", "friday")
SCHEDULE_TIME = os.getenv("SCHEDULE_TIME", "09:00")

# Storage — base folder is INVOICES; each run gets its own date-stamped subfolder
INVOICES_BASE_DIR = Path(os.getenv("RECEIPT_DOWNLOAD_DIR", "INVOICES"))
INVOICES_BASE_DIR.mkdir(parents=True, exist_ok=True)

# Kept for backwards-compatibility with any code that still imports RECEIPT_DOWNLOAD_DIR
RECEIPT_DOWNLOAD_DIR = INVOICES_BASE_DIR


def get_run_dir(start_date, end_date) -> Path:
    """
    Return  INVOICES/<start>_<end>/  e.g.  INVOICES/2026-03-10_2026-03-17/
    Creates the directory if it doesn't exist.
    """
    folder = INVOICES_BASE_DIR / (
        f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder
