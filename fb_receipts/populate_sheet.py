"""
Syncs ad accounts from Meta into the Google Sheet client table.

- Preserves settings rows at the top (rows 1-5)
- Keeps existing email / active / schedule values for known accounts
- Adds new accounts (active=no by default)
- Flags accounts in the sheet that no longer exist in Meta
"""

import gspread
from google.oauth2.service_account import Credentials
from src.config import GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID
from src.meta_client import MetaClient
from src.sheets_client import SheetsClient

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GOOGLE_SHEET_ID).sheet1

# ── Read current sheet state ───────────────────────────────────────────────────
all_rows = ws.get_all_values()

# Find where the client table starts
header_idx = next(
    (i for i, r in enumerate(all_rows) if r and r[0].strip().lower() == "client_name"),
    None,
)
if header_idx is None:
    print("ERROR: Could not find 'client_name' header row in sheet.")
    exit(1)

settings_rows = all_rows[:header_idx]   # preserve these exactly
client_header = all_rows[header_idx]
client_data   = all_rows[header_idx + 1:]

# Build index of existing clients keyed by ad_account_id
def col(header, name):
    h = [c.strip().lower() for c in header]
    return h.index(name) if name in h else None

col_id       = col(client_header, "ad_account_id") or 1
col_name     = col(client_header, "client_name")   or 0
col_email    = col(client_header, "email")          or 2
col_active   = col(client_header, "active")         or 3
col_schedule = col(client_header, "schedule")

existing = {}
for row in client_data:
    if not any(row):
        continue
    padded = row + [""] * (max(col_id, col_email or 0, col_active or 0) + 1 - len(row))
    acct_id = padded[col_id].strip()
    if acct_id:
        existing[acct_id] = padded

# ── Fetch accounts from Meta ───────────────────────────────────────────────────
print("Fetching ad accounts from Meta...")
client = MetaClient()
accounts = client.get_all_ad_accounts()

# account_status 1 = ACTIVE — filter out disabled/closed accounts
# See: https://developers.facebook.com/docs/marketing-api/reference/ad-account
ACTIVE_STATUS = 1
all_count = len(accounts)
seen = {}
for a in accounts:
    if int(a.get("account_status", 1)) == ACTIVE_STATUS:
        seen[a["id"]] = a
accounts = list(seen.values())
print(f"Found {all_count} total account(s), {len(accounts)} active after filtering.")

# ── Merge ──────────────────────────────────────────────────────────────────────
added = updated = 0
new_client_rows = []

for a in sorted(accounts, key=lambda x: x.get("name", "").lower()):
    acct_id = a.get("account_id") or a["id"].replace("act_", "")
    meta_name = a.get("name", "")

    if acct_id in existing:
        prev = existing[acct_id]
        # Keep existing values; update name from Meta
        name     = meta_name or (prev[col_name] if col_name < len(prev) else "")
        email    = prev[col_email]    if col_email    is not None and col_email    < len(prev) else ""
        active   = prev[col_active]   if col_active   is not None and col_active   < len(prev) else "no"
        schedule = prev[col_schedule] if col_schedule is not None and col_schedule < len(prev) else "weekly_friday"
        updated += 1
    else:
        name, email, active, schedule = meta_name, "", "no", "weekly_friday"
        added += 1

    new_client_rows.append([name, acct_id, email, active, schedule])

# Flag accounts in sheet not found in Meta
meta_ids = {a.get("account_id") or a["id"].replace("act_", "") for a in accounts}
removed = [r for r in client_data if any(r) and r[col_id].strip() and r[col_id].strip() not in meta_ids]
if removed:
    print(f"\n⚠ {len(removed)} account(s) in sheet not found in Meta (may be inactive/removed):")
    for r in removed:
        print(f"   - {r[col_name]} ({r[col_id]})")

# ── Write back ─────────────────────────────────────────────────────────────────
client_header_out = ["client_name", "ad_account_id", "email", "active", "schedule"]
full_data = settings_rows + [client_header_out] + new_client_rows

# Pad all rows to same width
max_cols = max(len(r) for r in full_data)
padded_data = [r + [""] * (max_cols - len(r)) for r in full_data]

ws.clear()
ws.update(values=padded_data, range_name="A1")

# Bold the client header row
client_header_row_num = header_idx + 1  # 1-based
ws.format(f"A{client_header_row_num}:E{client_header_row_num}", {"textFormat": {"bold": True}})

print(f"\nSheet updated:")
print(f"  {added} new account(s) added")
print(f"  {updated} existing account(s) refreshed (email/active/schedule preserved)")
print(f"\nRe-run setup_sheet.py to restore dropdowns after this update.")
print(f"Open: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit")
