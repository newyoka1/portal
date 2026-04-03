#!/usr/bin/env python3
"""
Load Campaign Monitor subscribers into the unified CRM contacts table.

Supports MULTIPLE Campaign Monitor accounts.  Each account's API key is
stored in .env using the naming convention ``CM_API_KEY_<NAME>=...``.
Lists are AUTO-DISCOVERED at runtime via the CM API — no need to maintain
list IDs manually.

Source: Campaign Monitor API v3.3
Target: crm_unified.contacts (shared table)

Usage:
    python load_cm_subscribers.py              # Incremental sync, all accounts
    python load_cm_subscribers.py --full       # Full re-sync, all accounts
    python load_cm_subscribers.py --account X  # Sync only account "X"
    python load_cm_subscribers.py --list ID    # Sync only one list (any account)

Setup:
    1. Log in to Campaign Monitor -> Account Settings -> API Keys
    2. Copy your API key
    3. Add to .env using the naming convention:
         CM_API_KEY_<ACCOUNT_NAME>=your_api_key_here
       Example:
         CM_API_KEY_POLITIKA=abc123...
         CM_API_KEY_KASSAR=def456...

Called by: python main.py cm-sync [--full] [--account NAME] [--list ID]
"""

import os, sys, time, argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymysql
import requests

# ---------------------------------------------------------------------------
# Progress helpers
# ---------------------------------------------------------------------------
def _fmt_elapsed(seconds):
    s = int(seconds)
    return f"{s//60}m {s%60:02d}s" if s >= 60 else f"{s}s"


class _Progress:
    """Time-gated progress reporter (no \r — works in browser log)."""
    def __init__(self, label, total=None, indent="      ", interval=8.0):
        self.label    = label
        self.total    = total
        self.indent   = indent
        self.interval = interval
        self.n        = 0
        self.t0       = time.time()
        self._t_last  = self.t0 - interval

    def update(self, n):
        self.n = n
        now = time.time()
        if now - self._t_last >= self.interval:
            self._emit(now)
            self._t_last = now

    def _emit(self, now=None):
        if now is None:
            now = time.time()
        elapsed = now - self.t0
        rate    = self.n / elapsed if elapsed > 0 else 0
        e_str   = _fmt_elapsed(elapsed)
        if self.total and self.total > 0 and self.n <= self.total:
            pct    = 100 * self.n / self.total
            bar_w  = 25
            filled = int(pct * bar_w / 100)
            bar    = "█" * filled + "░" * (bar_w - filled)
            eta    = (self.total - self.n) / rate if rate > 0 else 0
            eta_s  = f"  ETA {_fmt_elapsed(eta)}" if eta > 1 else ""
            print(f"{self.indent}[{bar}] {self.n:>8,} / {self.total:,}"
                  f"  {rate:,.0f}/s  {e_str}{eta_s}", flush=True)
        else:
            print(f"{self.indent}↻ {self.label}:  {self.n:,}  ({rate:,.0f}/s  {e_str})",
                  flush=True)

    def done(self, extra=""):
        elapsed = time.time() - self.t0
        rate    = self.n / elapsed if elapsed > 0 else 0
        e_str   = _fmt_elapsed(elapsed)
        suffix  = f"  — {extra}" if extra else ""
        print(f"{self.indent}✓ {self.label}: {self.n:,}  ({rate:,.0f}/s  {e_str}){suffix}",
              flush=True)


# Shared merge logic
from pipeline.crm_merge import (
    upsert_contacts as merge_upsert, CONTACTS_DDL, tag_cm_membership,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB = "crm_unified"
CM_API_BASE = "https://api.createsend.com/api/v3.3"


# ---------------------------------------------------------------------------
# Account discovery
# ---------------------------------------------------------------------------
def discover_cm_accounts():
    """Scan environment for Campaign Monitor API keys.

    Looks for:
      CM_API_KEY_<NAME>=...   ->  account name = lower(NAME)

    Returns list of {"name": str, "api_key": str} dicts.
    """
    accounts = []
    seen_keys = set()

    for key, val in sorted(os.environ.items()):
        if key.startswith("CM_API_KEY_") and val:
            name = key[len("CM_API_KEY_"):].lower()
            if val not in seen_keys:
                accounts.append({"name": name, "api_key": val})
                seen_keys.add(val)

    return accounts


def discover_lists(api_key):
    """Auto-discover all lists across all clients for this API key.

    Returns list of {"list_id": str, "list_name": str, "client_name": str}.
    """
    all_lists = []

    # Step 1: Get all clients
    resp = requests.get(f"{CM_API_BASE}/clients.json",
                        auth=(api_key, ""), timeout=30)
    if resp.status_code != 200:
        print(f"    WARNING: Could not fetch clients (HTTP {resp.status_code})")
        return all_lists

    clients = resp.json()

    # Step 2: Get lists for each client
    for client in clients:
        cid = client["ClientID"]
        cname = client["Name"]

        resp2 = requests.get(f"{CM_API_BASE}/clients/{cid}/lists.json",
                             auth=(api_key, ""), timeout=30)
        if resp2.status_code == 200:
            for lst in resp2.json():
                all_lists.append({
                    "list_id":     lst["ListID"],
                    "list_name":   lst["Name"],
                    "client_name": cname,
                })
        time.sleep(0.1)

    return all_lists


# ---------------------------------------------------------------------------
# MySQL helpers (same pattern as load_hubspot_contacts.py)
# ---------------------------------------------------------------------------
MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


def connect(db=None):
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, charset="utf8mb4",
        autocommit=True,
    )


def bootstrap(conn):
    """Create database and unified contacts table if they don't exist."""
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    conn.select_db(DB)

    # Unified contacts table
    cur.execute(CONTACTS_DDL)

    # Migration: add cm_lists / cm_segments if missing (existing DB)
    cur.execute("SHOW COLUMNS FROM contacts LIKE 'cm_lists'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE contacts "
                    "ADD COLUMN cm_lists VARCHAR(1000) DEFAULT NULL AFTER sources, "
                    "ADD COLUMN cm_segments VARCHAR(1000) DEFAULT NULL AFTER cm_lists")
        print("  Added cm_lists and cm_segments columns")

    # Watermark table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS load_metadata (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        load_type  VARCHAR(100) NOT NULL,
        file_hash  VARCHAR(64)  NOT NULL,
        row_count  INT,
        load_date  DATETIME     DEFAULT CURRENT_TIMESTAMP,
        INDEX(load_type, load_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    print(f"Database '{DB}' ready\n")


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
def get_watermark(cur, load_type):
    try:
        cur.execute(
            "SELECT file_hash FROM load_metadata "
            "WHERE load_type=%s ORDER BY load_date DESC LIMIT 1",
            (load_type,))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def clear_watermark(cur, load_type):
    try:
        cur.execute("DELETE FROM load_metadata WHERE load_type=%s", (load_type,))
    except Exception:
        pass


def store_watermark(cur, load_type, watermark, row_count):
    cur.execute(
        "INSERT INTO load_metadata (load_type, file_hash, row_count) "
        "VALUES (%s, %s, %s)",
        (load_type, watermark, row_count))


# ---------------------------------------------------------------------------
# Campaign Monitor API helpers
# ---------------------------------------------------------------------------
def cm_get(api_key, url, params=None):
    """GET with HTTP Basic auth (api key = username, blank password)."""
    for attempt in range(4):
        resp = requests.get(url, auth=(api_key, ""),
                            params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < 3:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
    return {}


def fetch_subscribers(api_key, list_id, since_date=None):
    """Paginate through active subscribers for a list.

    Parameters
    ----------
    api_key : str
    list_id : str
    since_date : str or None
        ISO date (``YYYY-MM-DD``) — only return subscribers added/modified
        after this date.  ``None`` fetches all.

    Yields pages (lists of subscriber dicts).
    """
    url = f"{CM_API_BASE}/lists/{list_id}/active.json"
    page_num = 1
    page_size = 1000

    while True:
        params = {"page": page_num, "pagesize": page_size}
        if since_date:
            params["date"] = since_date

        data = cm_get(api_key, url, params)

        results = data.get("Results", [])
        if results:
            yield results

        total_pages = data.get("NumberOfPages", 1)
        if page_num >= total_pages:
            break
        page_num += 1
        time.sleep(0.2)  # courtesy


# ---------------------------------------------------------------------------
# Segment discovery & fetching
# ---------------------------------------------------------------------------
def discover_segments(api_key, list_id):
    """Discover all segments for a given Campaign Monitor list.

    Returns list of {"segment_id": str, "title": str}.
    """
    url = f"{CM_API_BASE}/lists/{list_id}/segments.json"
    data = cm_get(api_key, url)
    if not data or not isinstance(data, list):
        return []
    return [{"segment_id": seg["SegmentID"], "title": seg["Title"]}
            for seg in data]


def fetch_segment_members(api_key, segment_id):
    """Paginate through active subscribers in a segment.

    Yields pages (lists of subscriber dicts with EmailAddress).
    """
    url = f"{CM_API_BASE}/segments/{segment_id}/active.json"
    page_num = 1
    page_size = 1000

    while True:
        params = {"page": page_num, "pagesize": page_size}
        data = cm_get(api_key, url, params)

        results = data.get("Results", [])
        if results:
            yield results

        total_pages = data.get("NumberOfPages", 1)
        if page_num >= total_pages:
            break
        page_num += 1
        time.sleep(0.2)


def sync_segments(cur, api_key, list_id, list_name):
    """Discover and tag segment membership for contacts already in the DB."""
    segments = discover_segments(api_key, list_id)
    if not segments:
        return

    print(f"      Segments for '{list_name}': {len(segments)} found")

    for seg in segments:
        seg_id = seg["segment_id"]
        seg_title = seg["title"]

        # Collect all emails in this segment
        emails = []
        for page in fetch_segment_members(api_key, seg_id):
            for sub in page:
                email = (sub.get("EmailAddress") or "").strip().lower()
                if email:
                    emails.append(email)

        if emails:
            tagged = tag_cm_membership(cur, emails, "cm_segments", seg_title)
            print(f"      ✓ Segment [{seg_title}]: {len(emails):,} members  {tagged:,} tagged")
        else:
            print(f"      · Segment [{seg_title}]: 0 members  (skipped)")


# ---------------------------------------------------------------------------
# Campaign Monitor -> Standard contact mapping
# ---------------------------------------------------------------------------
def map_cm_to_standard(subscribers):
    """Convert CM subscriber dicts to standardised contact dicts.

    Returns list of dicts compatible with ``crm_merge.upsert_contacts()``.
    """
    contacts = []
    for sub in subscribers:
        email = (sub.get("EmailAddress") or "").strip()
        if not email:
            continue

        # Split Name into first/last (handles middle names)
        # "John Patrick Doe" -> first="John", last="Doe"
        # "Doe, John" -> strip commas, same logic
        name = (sub.get("Name") or "").replace(",", "").strip()
        first_name = None
        last_name = None
        if name:
            parts = name.split()
            first_name = parts[0] if len(parts) >= 1 else None
            last_name  = parts[-1] if len(parts) >= 2 else None

        # Mobile (dedicated field from CM API)
        mobile_val = (sub.get("MobileNumber") or "").strip() or None

        # Phones (mobile also goes here — deduped during merge)
        phones = []
        if mobile_val:
            phones.append(mobile_val)

        # All emails (primary + any from custom fields)
        emails = [email]

        # Custom fields
        custom = sub.get("CustomFields", [])
        address = None
        city = None
        state = None
        zipval = None
        company = None

        # Map custom fields — covers all known naming conventions across accounts
        # Keys arrive as "[FieldName]"; we strip brackets and lowercase.
        for cf in custom:
            key = (cf.get("Key") or "").strip("[]").lower()
            val = (cf.get("Value") or "").strip()
            if not val:
                continue

            # Phones (many naming variants)
            if key in ("phone", "phonenumber", "phone_number", "telephone",
                        "phone-home", "phone-home2", "phone-mobile", "phone-work",
                        "phone-fax", "phone2", "officephone", "mobile",
                        "mobileold", "*phone(office)"):
                phones.append(val)
                # Also capture as mobile if it's a mobile-specific field
                if key in ("mobile", "phone-mobile") and not mobile_val:
                    mobile_val = val

            # Address (first non-null wins — address1 preferred over address2)
            elif key in ("address", "address1", "street", "streetaddress",
                         "street_address", "streetaddressline1-home",
                         "streetaddressline1-home2", "streetaddressline1-other"):
                if not address:
                    address = val
            elif key in ("address2", "addressline2", "street2"):
                # Append to address if we already have one
                if address:
                    address = address + " " + val
                else:
                    address = val

            # City
            elif key in ("city", "city-home", "city-home2", "city-other", "town"):
                if not city:
                    city = val

            # State
            elif key in ("state", "state/province-home", "state/province-home2",
                         "state/province-other", "province", "region"):
                if not state:
                    state = val

            # Zip
            elif key in ("zip", "zipcode", "zip_code", "zip/postalcode-home",
                         "zip/postalcode-home2", "zip/postalcode-other",
                         "postcode", "postal_code"):
                if not zipval:
                    zipval = str(val)

            # Company / organisation
            elif key in ("company", "organization", "organisation",
                         "companyname", "orginization", "employer"):
                if not company:
                    company = val

            # Additional emails
            elif key in ("secondemail", "e-mail2-value", "additionalemailaddresses"):
                emails.append(val)

        contacts.append({
            "email":      email,
            "emails":     emails,
            "first_name": first_name,
            "last_name":  last_name,
            "mobile":     mobile_val,
            "phones":     phones,
            "address":    address,
            "city":       city,
            "state":      state,
            "zip":        zipval,
            "company":    company,
        })

    return contacts


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def sync_list(cur, api_key, account_name, list_id, list_name,
              full=False, skip_segments=False):
    """Sync one Campaign Monitor list into the unified contacts table."""
    load_type = f"cm_subscribers:{account_name}:{list_id}"
    source_tag = f"cm_{account_name}"
    watermark = get_watermark(cur, load_type)

    display = f"{list_name} ({list_id[:8]}...)"

    since_date = None
    if full or not watermark:
        print(f"    {display}: FULL sync")
        clear_watermark(cur, load_type)
    else:
        since_date = watermark
        print(f"    {display}: incremental (since {since_date})")

    total_ins = 0
    total_upd = 0
    prog = _Progress(list_name[:40])

    for page in fetch_subscribers(api_key, list_id, since_date=since_date):
        std = map_cm_to_standard(page)

        # Tag each contact with the list name for cm_lists column
        for contact in std:
            contact["cm_lists"] = list_name

        ins, upd = merge_upsert(cur, std, source_tag)
        total_ins += ins
        total_upd += upd
        prog.update(total_ins + total_upd)

    total = total_ins + total_upd
    prog.done(f"{total_ins:,} new  {total_upd:,} updated")

    # Store watermark as today's date for next incremental run
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    store_watermark(cur, load_type, now_str, total)

    # Discover and tag segment membership
    if not skip_segments:
        sync_segments(cur, api_key, list_id, list_name)

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Sync Campaign Monitor subscribers to local MySQL")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-sync (re-fetch all subscribers)")
    parser.add_argument("--account", type=str, default=None,
                        help="Sync only this account (matches CM_API_KEY_<NAME> suffix)")
    parser.add_argument("--list", type=str, default=None,
                        help="Sync only this list ID (from any account)")
    parser.add_argument("--skip-segments", action="store_true",
                        help="Skip segment discovery and tagging (faster)")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    accounts = discover_cm_accounts()
    if not accounts:
        print("ERROR: No Campaign Monitor API keys found in .env\n")
        print("Setup:")
        print("  1. Log in to Campaign Monitor -> Account Settings -> API Keys")
        print("  2. Copy your API key")
        print("  3. Add to .env using the naming convention:")
        print("       CM_API_KEY_POLITIKA=your_api_key_here")
        print("       CM_API_KEY_KASSAR=another_key_here")
        sys.exit(1)

    # Filter to single account if requested
    if args.account:
        target = args.account.lower()
        accounts = [a for a in accounts if a["name"] == target]
        if not accounts:
            all_accounts = discover_cm_accounts()
            print(f"ERROR: No API key found for account '{args.account}'")
            print(f"  Available accounts: {', '.join(a['name'] for a in all_accounts)}")
            sys.exit(1)

    print("\n━━━ Campaign Monitor Sync " + "━" * 45)
    print(f"  Accounts: {', '.join(a['name'] for a in accounts)}")
    print(f"  Mode: {'full' if args.full else 'incremental'}\n")

    t0 = time.time()
    conn = connect()
    bootstrap(conn)
    cur = conn.cursor()

    grand_total = 0
    account_totals = {}

    for acct in accounts:
        name = acct["name"]
        api_key = acct["api_key"]
        print(f"\n  Account: {name.upper()}")
        print(f"  {'─' * 40}")

        # Auto-discover all lists for this account
        print(f"  Discovering lists...")
        lists = discover_lists(api_key)
        if not lists:
            print(f"  No lists found for account '{name}'")
            account_totals[name] = 0
            continue

        # Filter to single list if requested
        if args.list:
            lists = [l for l in lists if l["list_id"] == args.list]
            if not lists:
                continue

        # Group by client for display
        clients = {}
        for lst in lists:
            cn = lst["client_name"]
            if cn not in clients:
                clients[cn] = []
            clients[cn].append(lst)

        for client_name, client_lists in clients.items():
            print(f"  Client: {client_name}  ({len(client_lists)} list(s))")
            for lst in client_lists:
                count = sync_list(cur, api_key, name, lst["list_id"],
                                  lst["list_name"], full=args.full,
                                  skip_segments=args.skip_segments)
                grand_total += count

        account_totals[name] = grand_total
        print()

    elapsed = time.time() - t0
    print("\n━━━ Complete " + "━" * 57)
    print(f"  Subscribers merged:  {grand_total:,}")

    cur.execute("SELECT COUNT(*) FROM contacts")
    total_contacts = cur.fetchone()[0]
    print(f"  Total unified contacts: {total_contacts:,}")

    if elapsed >= 60:
        print(f"\n  Time: {elapsed/60:.1f} minutes")
    else:
        print(f"\n  Time: {elapsed:.0f} seconds")

    conn.close()


if __name__ == "__main__":
    main()
