"""
Syncs ad accounts from Meta into the fb_receipts MySQL database.

- Keeps existing email / active / schedule values for known accounts
- Adds new accounts (active=no by default)
- Removes accounts from DB that no longer exist in Meta

Run standalone:  python populate_db.py
Or called by the portal's "Import from Meta API" button.
"""

from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from src.meta_client import MetaClient
from src.db_client import DbClient

# ── Fetch from Meta ────────────────────────────────────────────────────────────
print("Fetching ad accounts from Meta...")
meta = MetaClient()
accounts = meta.get_all_ad_accounts()

ACTIVE_STATUS = 1
all_count = len(accounts)
accounts = [a for a in accounts if int(a.get("account_status", 1)) == ACTIVE_STATUS]
print(f"Found {all_count} total, {len(accounts)} active after filtering.")

# ── Load existing clients from DB ─────────────────────────────────────────────
db = DbClient()
existing = {c["ad_account_id"]: c for c in db.get_all_clients_raw()}

# ── Merge ──────────────────────────────────────────────────────────────────────
added = updated = 0
merged = []

for a in sorted(accounts, key=lambda x: x.get("name", "").lower()):
    acct_id   = a.get("account_id") or a["id"].replace("act_", "")
    meta_name = a.get("name", "")

    if acct_id in existing:
        prev = existing[acct_id]
        merged.append({
            "client_name":   meta_name or prev["client_name"],
            "ad_account_id": acct_id,
            "email":         prev["email"],
            "active":        prev["active"],
            "schedule":      prev["schedule"],
        })
        updated += 1
    else:
        merged.append({
            "client_name":   meta_name,
            "ad_account_id": acct_id,
            "email":         "",
            "active":        "no",
            "schedule":      "weekly_friday",
        })
        added += 1

# ── Flag removed accounts ──────────────────────────────────────────────────────
meta_ids = {a.get("account_id") or a["id"].replace("act_", "") for a in accounts}
removed  = [c for c in existing.values() if c["ad_account_id"] not in meta_ids]
if removed:
    print(f"\n⚠  {len(removed)} account(s) in DB not found in Meta (removing):")
    for c in removed:
        print(f"   - {c['client_name']} ({c['ad_account_id']})")

# ── Save ───────────────────────────────────────────────────────────────────────
db.save_clients(merged)

print(f"\nDatabase updated:")
print(f"  {added} new account(s) added")
print(f"  {updated} existing account(s) refreshed")
if removed:
    print(f"  {len(removed)} account(s) removed")
