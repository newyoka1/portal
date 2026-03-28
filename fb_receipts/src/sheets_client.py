"""
Google Sheets client for reading the client-to-ad-account mapping.

Expected sheet layout (first worksheet):
┌──────────────┬────────────────┬──────────────────────┬────────┬────────────────┐
│ client_name  │ ad_account_id  │ email                │ active │ schedule       │
├──────────────┼────────────────┼──────────────────────┼────────┼────────────────┤
│ Acme Corp    │ 123456789      │ billing@acme.com      │ yes    │ weekly_friday  │
│ Big Widget   │ 987654321      │ a@co.com, b@co.com   │ yes    │ monthly_1      │
└──────────────┴────────────────┴──────────────────────┴────────┴────────────────┘

- ad_account_id: numeric ID (without the "act_" prefix)
- email: one address, or multiple comma-separated addresses
- active: "yes" or "no" — skip inactive rows
- schedule: when to send (optional, defaults to weekly_friday)
    weekly_<day>  e.g. weekly_friday, weekly_monday
    monthly_<n>   e.g. monthly_1, monthly_15
"""

import logging
import gspread
from google.oauth2.service_account import Credentials

from src.config import GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]


class SheetsClient:
    def __init__(self):
        creds = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        self.gc = gspread.authorize(creds)
        self.sheet = self.gc.open_by_key(GOOGLE_SHEET_ID)

    def _get_all_rows(self):
        """Return all rows from sheet1, cached for the lifetime of this call."""
        return self.sheet.sheet1.get_all_values()

    def _find_client_table(self, all_rows: list) -> tuple[int, list]:
        """
        Scan rows top-to-bottom and return (header_row_index, all_rows).
        The client table starts at the first row whose first cell is 'client_name'.
        Rows above it are settings (key | value pairs).
        """
        for i, row in enumerate(all_rows):
            if row and row[0].strip().lower() == "client_name":
                return i, all_rows
        return 0, all_rows  # fallback: treat row 0 as header

    def get_settings(self) -> dict:
        """
        Read settings from the top of Sheet 1 (rows above the client table).
        Each row is expected to be: setting_name | value
        """
        all_rows = self._get_all_rows()
        header_idx, _ = self._find_client_table(all_rows)

        settings = {}
        for row in all_rows[:header_idx]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                key = row[0].strip().lower()
                settings[key] = row[1].strip()

        if settings:
            logger.info("Loaded %d setting(s) from Sheet 1: %s", len(settings), list(settings.keys()))
        return settings

    def get_client_mappings(self) -> list[dict]:
        """
        Read the client table from Sheet 1 (below the settings rows).
        Each dict has: client_name, ad_account_id, email, active, schedule
        """
        all_rows = self._get_all_rows()
        header_idx, _ = self._find_client_table(all_rows)

        if header_idx >= len(all_rows):
            logger.warning("Google Sheet is empty")
            return []

        # Build a column-index map from the header row, skipping blank headers
        header_row = [h.strip().lower() for h in all_rows[header_idx]]
        col = {name: idx for idx, name in enumerate(header_row) if name}

        required = {"client_name", "ad_account_id", "email", "active"}
        missing = required - col.keys()
        if missing:
            logger.error("Sheet is missing required column(s): %s", missing)
            return []

        mappings = []
        for row in all_rows[header_idx + 1:]:
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

            schedule = (
                padded[col["schedule"]].strip().lower()
                if "schedule" in col else ""
            ) or "weekly_friday"

            mappings.append({
                "client_name": name,
                "ad_account_id": ad_id,
                "emails": emails,
                "email": emails[0],  # kept for backward compatibility
                "schedule": schedule,
            })

        logger.info("Loaded %d active client mappings from Google Sheet", len(mappings))
        return mappings

    def get_all_clients_raw(self) -> list[dict]:
        """
        Return ALL client rows (active and inactive) as dicts — for the admin UI.
        Does not filter by active status.
        """
        all_rows = self._get_all_rows()
        header_idx, _ = self._find_client_table(all_rows)
        if header_idx >= len(all_rows):
            return []

        header_row = [h.strip().lower() for h in all_rows[header_idx]]
        col = {name: idx for idx, name in enumerate(header_row) if name}

        clients = []
        for row in all_rows[header_idx + 1:]:
            padded = row + [""] * (len(header_row) - len(row))
            client = {name: padded[idx] for name, idx in col.items()}
            if client.get("client_name") or client.get("ad_account_id"):
                clients.append(client)
        return clients

    def save_sheet_data(self, settings: dict, clients: list[dict]) -> None:
        """
        Write settings + client table back to Sheet 1, preserving layout.
        settings: dict of {admin_email, notify_email, schedule_time, default_schedule}
        clients:  list of dicts with client_name, ad_account_id, email, active, schedule
        """
        header = ["client_name", "ad_account_id", "email", "active", "schedule"]
        new_data = [
            ["admin_email",      settings.get("admin_email", "")],
            ["notify_email",     settings.get("notify_email", "")],
            ["schedule_time",    settings.get("schedule_time", "09:00")],
            ["default_schedule", settings.get("default_schedule", "weekly_friday")],
            [],
            header,
        ]
        for c in clients:
            new_data.append([
                c.get("client_name", ""),
                c.get("ad_account_id", ""),
                c.get("email", ""),
                c.get("active", "no"),
                c.get("schedule", "weekly_friday"),
            ])

        ws = self.sheet.sheet1
        ws.clear()
        ws.update(values=new_data, range_name="A1", value_input_option="RAW")
        logger.info("Saved %d clients + settings to Sheet 1", len(clients))

