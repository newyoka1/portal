"""
Google Sheets client for reading the client-to-ad-account mapping.

Expected sheet layout (first worksheet):
┌──────────────┬────────────────┬─────────────────────────────────────┬──────────────┐
│ client_name  │ ad_account_id  │ email                               │ active       │
├──────────────┼────────────────┼─────────────────────────────────────┼──────────────┤
│ Acme Corp    │ 123456789      │ billing@acme.com                    │ yes          │
│ Big Widget   │ 987654321      │ a@co.com, b@co.com, c@co.com        │ yes          │
└──────────────┴────────────────┴─────────────────────────────────────┴──────────────┘

- ad_account_id: numeric ID (without the "act_" prefix)
- email: one address, or multiple comma-separated addresses
- active: "yes" or "no" — skip inactive rows
"""

import logging
import gspread
from google.oauth2.service_account import Credentials

from src.config import GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/gmail.send",
]


class SheetsClient:
    def __init__(self):
        creds = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        self.gc = gspread.authorize(creds)
        self.sheet = self.gc.open_by_key(GOOGLE_SHEET_ID)

    def get_client_mappings(self) -> list[dict]:
        """
        Read the first worksheet and return a list of client mappings.
        Each dict has: client_name, ad_account_id, email, active

        Uses get_all_values() instead of get_all_records() so that extra blank
        columns in the sheet (which cause duplicate-header errors) are ignored.
        """
        worksheet = self.sheet.sheet1
        all_values = worksheet.get_all_values()

        if not all_values:
            logger.warning("Google Sheet is empty")
            return []

        # Build a column-index map from the header row, skipping blank headers
        header_row = [h.strip().lower() for h in all_values[0]]
        col = {name: idx for idx, name in enumerate(header_row) if name}

        required = {"client_name", "ad_account_id", "email", "active"}
        missing = required - col.keys()
        if missing:
            logger.error("Sheet is missing required column(s): %s", missing)
            return []

        mappings = []
        for row in all_values[1:]:
            # Pad short rows so index lookups don't raise IndexError
            padded = row + [""] * (len(header_row) - len(row))

            active = padded[col["active"]].strip().lower()
            if active != "yes":
                continue

            ad_id = padded[col["ad_account_id"]].strip()
            raw_email = padded[col["email"]].strip()
            name = padded[col["client_name"]].strip()

            if not ad_id or not raw_email:
                logger.warning("Skipping row with missing ad_account_id or email: %s", row)
                continue

            # Support comma-separated list of email addresses
            emails = [e.strip() for e in raw_email.split(",") if e.strip()]

            mappings.append({
                "client_name": name,
                "ad_account_id": ad_id,
                "emails": emails,
                "email": emails[0],  # kept for backward compatibility
            })

        logger.info("Loaded %d active client mappings from Google Sheet", len(mappings))
        return mappings
