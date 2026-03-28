import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Meta
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_BUSINESS_IDS = [
    bid.strip()
    for bid in os.getenv("META_BUSINESS_IDS", "").split(",")
    if bid.strip()
]
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0")
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
GMAIL_SENDER_EMAIL = os.getenv("GMAIL_SENDER_EMAIL", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "George@politikanyc.com")

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
