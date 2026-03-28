"""
Resolve Google service-account credentials for local dev and Railway/production.

Priority:
  1. GOOGLE_SERVICE_ACCOUNT_JSON  — raw JSON string (Railway env var, multiline ok)
  2. GOOGLE_SERVICE_ACCOUNT_JSON_B64 — base64-encoded JSON (alternative for Railway)
  3. GOOGLE_SERVICE_ACCOUNT_FILE  — path to a local JSON file (local dev)
"""
import base64
import json
import os

from google.oauth2 import service_account


def build_credentials(scopes: list, impersonate: str):
    """Return a service_account.Credentials object with DWD subject set."""
    raw_json  = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    b64_json  = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64")
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

    if raw_json:
        info = json.loads(raw_json)
    elif b64_json:
        info = json.loads(base64.b64decode(b64_json).decode())
    elif file_path:
        with open(file_path) as f:
            info = json.load(f)
    else:
        raise RuntimeError(
            "No Google service-account credentials found. Set GOOGLE_SERVICE_ACCOUNT_JSON, "
            "GOOGLE_SERVICE_ACCOUNT_JSON_B64, or GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    return service_account.Credentials.from_service_account_info(
        info, scopes=scopes
    ).with_subject(impersonate)
