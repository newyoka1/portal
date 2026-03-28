"""
One-time script to populate the Google Sheet with all ad accounts from Meta.
Sets headers and adds all accounts with active=no by default.
"""

import gspread
from google.oauth2.service_account import Credentials
from src.config import GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID
from src.meta_client import MetaClient

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(GOOGLE_SHEET_ID).sheet1

print("Fetching ad accounts from Meta...")
client = MetaClient()
accounts = client.get_all_ad_accounts()

# Deduplicate by account ID
seen = {}
for a in accounts:
    seen[a["id"]] = a
accounts = list(seen.values())

print(f"Found {len(accounts)} unique accounts. Writing to sheet...")

# Build rows
headers = ["client_name", "ad_account_id", "email", "active"]
rows = [headers]
for a in sorted(accounts, key=lambda x: x.get("name", "")):
    rows.append([
        a.get("name", ""),
        a.get("account_id", a["id"].replace("act_", "")),
        "",
        "no",
    ])

sheet.clear()
sheet.update(rows, "A1")

# Bold the header row
sheet.format("A1:D1", {"textFormat": {"bold": True}})

print(f"Done! {len(rows) - 1} accounts written to sheet.")
print(f"Open: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit")
print("Fill in the 'email' column and set 'active' to 'yes' for accounts you want to send receipts for.")
