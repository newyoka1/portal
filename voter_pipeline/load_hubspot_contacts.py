#!/usr/bin/env python3
"""
Load HubSpot CRM contacts + deals into local MySQL for voter/donor matching.

Supports MULTIPLE HubSpot accounts. Contacts are merged into the unified
``crm_unified.contacts`` table (email-based dedup, shared with Campaign
Monitor and other sources). Deals remain HubSpot-specific with an ``account``
column to distinguish portals.

Source: HubSpot CRM API v3 (private app access tokens)
Target: crm_unified.contacts (shared) + crm_unified.deals (HubSpot-only)

Usage:
    python load_hubspot_contacts.py              # Incremental sync, all accounts
    python load_hubspot_contacts.py --full       # Full re-sync, all accounts
    python load_hubspot_contacts.py --account X  # Sync only account "X"

Setup:
    1. Go to HubSpot > Settings > Integrations > Private Apps
    2. Create a new private app with scope: crm.objects.contacts.read, crm.objects.deals.read
    3. Copy the access token
    4. Add to .env using the naming convention:
         HUBSPOT_TOKEN_<ACCOUNT_NAME>=pat-na1-XXXXXXXX-...
       Example:
         HUBSPOT_TOKEN_MAIN=pat-na1-abc123-...
         HUBSPOT_TOKEN_CLIENT_X=pat-na1-def456-...
    5. Legacy: HUBSPOT_ACCESS_TOKEN is also supported (account name = "default")

Called by: python main.py hubspot-sync [--full] [--account NAME]
"""

import os, sys, re, time, argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymysql
import requests

# Shared merge logic
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline.crm_merge import (
    clean_name, upsert_contacts as merge_upsert, CONTACTS_DDL,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB = "crm_unified"
API_BASE = "https://api.hubapi.com/crm/v3/objects"

# Only fetch properties we map to the unified schema
CONTACT_PROPERTIES = [
    "firstname", "lastname",
    # Emails
    "email", "work_email", "hs_additional_emails",
    # Phones
    "phone", "mobilephone", "phone___work", "phone___other",
    "hs_whatsapp_phone_number",
    # Address
    "address", "city", "state", "zip",
    # Employment
    "company",
    # Timestamps (for watermark)
    "createdate", "lastmodifieddate",
]

DEAL_PROPERTIES = [
    "dealname", "amount", "closedate", "dealstage", "dealtype",
    "pipeline", "conduit_name", "campaign", "product_name",
    "recurring", "donation_status",
    "amount_before_fees", "amount_refunded", "donor_covered_fees",
    "lastmodifieddate",
]

# ---------------------------------------------------------------------------
# Account discovery
# ---------------------------------------------------------------------------
def discover_accounts():
    """Scan environment for HubSpot tokens.

    Looks for:
      HUBSPOT_TOKEN_<NAME>=pat-na1-...   ->  account name = lower(NAME)
      HUBSPOT_ACCESS_TOKEN=...           ->  account name = "default" (legacy)

    Returns list of {"name": str, "token": str} dicts.
    """
    accounts = []
    seen_tokens = set()

    # New convention: HUBSPOT_TOKEN_*
    for key, val in sorted(os.environ.items()):
        if key.startswith("HUBSPOT_TOKEN_") and val:
            name = key[len("HUBSPOT_TOKEN_"):].lower()
            if val not in seen_tokens:
                accounts.append({"name": name, "token": val})
                seen_tokens.add(val)

    # Legacy fallback: HUBSPOT_ACCESS_TOKEN -> account "default"
    legacy = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if legacy and legacy not in seen_tokens:
        accounts.append({"name": "default", "token": legacy})

    return accounts


# ---------------------------------------------------------------------------
# MySQL helpers
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
    """Create database and tables if they don't exist.

    Migration: if the old HubSpot-specific contacts table exists (has a
    ``hubspot_id`` column), drop it so the unified schema is created instead.
    Watermarks are cleared so that the next run does a full re-sync.
    """
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    conn.select_db(DB)

    # --- Migration: detect old HubSpot-specific contacts table ---------------
    cur.execute("SHOW TABLES LIKE 'contacts'")
    if cur.fetchone():
        cur.execute("SHOW COLUMNS FROM contacts LIKE 'hubspot_id'")
        if cur.fetchone():
            print("  Migrating contacts: old HubSpot schema -> unified schema")
            cur.execute("DROP TABLE contacts")
            # Clear watermarks so full re-sync kicks in
            try:
                cur.execute("DELETE FROM load_metadata WHERE load_type LIKE 'hubspot_contacts:%'")
            except Exception:
                pass

    # --- Unified contacts table (from crm_merge) -----------------------------
    cur.execute(CONTACTS_DDL)

    # Migration: add cm_lists / cm_segments if missing (existing DB)
    cur.execute("SHOW COLUMNS FROM contacts LIKE 'cm_lists'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE contacts "
                    "ADD COLUMN cm_lists VARCHAR(1000) DEFAULT NULL AFTER sources, "
                    "ADD COLUMN cm_segments VARCHAR(1000) DEFAULT NULL AFTER cm_lists")
        print("  Added cm_lists and cm_segments columns")

    # --- Deals table (HubSpot-specific, unchanged) ---------------------------
    # Also migrate old deals if missing account column
    cur.execute("SHOW TABLES LIKE 'deals'")
    if cur.fetchone():
        cur.execute("SHOW COLUMNS FROM deals LIKE 'account'")
        if not cur.fetchone():
            print("  Migrating deals: adding account column (requires table rebuild)")
            cur.execute("DROP TABLE deals")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deals (
        account            VARCHAR(50)   NOT NULL DEFAULT 'default',
        deal_id            BIGINT        NOT NULL,
        contact_id         BIGINT        DEFAULT NULL,
        dealname           VARCHAR(255)  DEFAULT NULL,
        amount             DECIMAL(12,2) DEFAULT NULL,
        closedate          DATETIME      DEFAULT NULL,
        dealstage          VARCHAR(50)   DEFAULT NULL,
        dealtype           VARCHAR(50)   DEFAULT NULL,
        pipeline           VARCHAR(50)   DEFAULT NULL,
        conduit_name       VARCHAR(255)  DEFAULT NULL,
        campaign           VARCHAR(255)  DEFAULT NULL,
        product_name       VARCHAR(255)  DEFAULT NULL,
        recurring          VARCHAR(50)   DEFAULT NULL,
        donation_status    VARCHAR(50)   DEFAULT NULL,
        amount_before_fees DECIMAL(12,2) DEFAULT NULL,
        amount_refunded    DECIMAL(12,2) DEFAULT NULL,
        donor_covered_fees VARCHAR(10)   DEFAULT NULL,
        lastmodifieddate   DATETIME      DEFAULT NULL,
        synced_at          DATETIME      DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (account, deal_id),
        INDEX idx_account   (account),
        INDEX idx_contact   (account, contact_id),
        INDEX idx_closedate (closedate),
        INDEX idx_conduit   (conduit_name(100)),
        INDEX idx_lastmod   (lastmodifieddate)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

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
# Watermark helpers (adapted from load_metadata pattern)
# ---------------------------------------------------------------------------
def get_watermark(cur, load_type):
    """Return the stored ISO timestamp watermark, or None."""
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
    """Remove watermark so a crash forces full reload."""
    try:
        cur.execute("DELETE FROM load_metadata WHERE load_type=%s", (load_type,))
    except Exception:
        pass


def store_watermark(cur, load_type, watermark, row_count):
    """Record successful sync."""
    cur.execute(
        "INSERT INTO load_metadata (load_type, file_hash, row_count) "
        "VALUES (%s, %s, %s)",
        (load_type, watermark, row_count))


# ---------------------------------------------------------------------------
# HubSpot API helpers
# ---------------------------------------------------------------------------
def api_get(token, url, params=None):
    """GET with rate-limit handling and retries."""
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(4):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
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


def api_post(token, url, body):
    """POST with rate-limit handling and retries."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    for attempt in range(4):
        resp = requests.post(url, headers=headers, json=body, timeout=30)
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


def fetch_all(token, object_type, properties, associations=None):
    """Paginate through all objects via the List endpoint. Yields pages."""
    url = f"{API_BASE}/{object_type}"
    params = {
        "limit": 100,
        "properties": ",".join(properties),
    }
    if associations:
        params["associations"] = associations

    after = None
    while True:
        if after:
            params["after"] = after
        data = api_get(token, url, params)
        results = data.get("results", [])
        if results:
            yield results
        # Next page
        paging = data.get("paging", {})
        nxt = paging.get("next", {})
        after = nxt.get("after")
        if not after:
            break
        time.sleep(0.1)  # rate-limit courtesy


def fetch_modified(token, object_type, properties, after_iso, associations=None):
    """Fetch objects modified after the given ISO timestamp via Search API.
    Returns (results_list, hit_limit_bool)."""
    url = f"{API_BASE}/{object_type}/search"
    # HubSpot Search filters use Unix milliseconds for datetime
    dt = datetime.fromisoformat(after_iso.replace("Z", "+00:00"))
    ms = int(dt.timestamp() * 1000)

    body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "lastmodifieddate",
                "operator": "GTE",
                "value": str(ms),
            }]
        }],
        "properties": properties,
        "limit": 100,
    }

    all_results = []
    after_cursor = None
    total = None

    while True:
        if after_cursor:
            body["after"] = after_cursor
        data = api_post(token, url, body)

        if total is None:
            total = data.get("total", 0)

        results = data.get("results", [])
        all_results.extend(results)

        paging = data.get("paging", {})
        nxt = paging.get("next", {})
        after_cursor = nxt.get("after")
        if not after_cursor:
            break
        time.sleep(0.1)

    hit_limit = total is not None and total > 10000
    return all_results, hit_limit


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
def parse_iso(s):
    """Parse ISO 8601 string to MySQL DATETIME string, or None."""
    if not s:
        return None
    try:
        # Handle both '2026-03-05T16:49:00Z' and '2026-03-05T16:49:00.123Z'
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def parse_decimal(s):
    """Parse string to float, or None."""
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# HubSpot -> Standard contact mapping
# ---------------------------------------------------------------------------
def map_hubspot_to_standard(page):
    """Convert a page of HubSpot contact API objects into standardised dicts.

    Returns a list of dicts compatible with ``crm_merge.upsert_contacts()``.
    """
    contacts = []
    for c in page:
        props = c.get("properties", {})

        # Primary email
        primary = (props.get("email") or "").strip()
        if not primary:
            continue  # email is required

        # All emails: primary + work_email + hs_additional_emails
        emails = [primary]
        work = (props.get("work_email") or "").strip()
        if work:
            emails.append(work)
        additional = (props.get("hs_additional_emails") or "")
        for e in additional.split(";"):
            e = e.strip()
            if e:
                emails.append(e)

        # Mobile (dedicated field)
        mobile_val = (props.get("mobilephone") or "").strip() or None

        # All phones (including mobile — deduped during merge)
        phones = []
        for field in ("phone", "mobilephone", "phone___work",
                      "phone___other", "hs_whatsapp_phone_number"):
            val = (props.get(field) or "").strip()
            if val:
                phones.append(val)

        contacts.append({
            "email":      primary,
            "emails":     emails,
            "first_name": (props.get("firstname") or "").strip() or None,
            "last_name":  (props.get("lastname") or "").strip() or None,
            "mobile":     mobile_val,
            "phones":     phones,
            "address":    (props.get("address") or "").strip() or None,
            "city":       (props.get("city") or "").strip() or None,
            "state":      (props.get("state") or "").strip() or None,
            "zip":        (props.get("zip") or "").strip() or None,
            "company":    (props.get("company") or "").strip() or None,
        })

    return contacts


# ---------------------------------------------------------------------------
# Contact sync
# ---------------------------------------------------------------------------
def sync_contacts(cur, token, account, full=False):
    """Sync contacts from one HubSpot account into the unified table."""
    load_type = f"hubspot_contacts:{account}"
    source_tag = f"hs_{account}"
    watermark = get_watermark(cur, load_type)

    if full or not watermark:
        mode = "full"
        print(f"  Contacts: FULL sync")
        clear_watermark(cur, load_type)
        # Full sync = re-merge everything (idempotent fill-blank).
        # We do NOT delete contacts because they may have data from other
        # sources.  Re-merging will add/update the hubspot source tag.
    else:
        # Try incremental
        print(f"  Contacts: incremental (since {watermark})")
        results, hit_limit = fetch_modified(
            token, "contacts", CONTACT_PROPERTIES, watermark)
        if hit_limit:
            print(f"    >10K changes -- falling back to full sync")
            clear_watermark(cur, load_type)
            mode = "full"
        else:
            # Incremental merge
            std = map_hubspot_to_standard(results)
            ins, upd = merge_upsert(cur, std, source_tag)
            total = ins + upd
            print(f"    Merged {total:,} contacts ({ins:,} new, {upd:,} updated)")
            if results:
                max_mod = max(
                    (r["properties"].get("lastmodifieddate", "") for r in results),
                    default=watermark)
                store_watermark(cur, load_type, max_mod, total)
            return total

    # Full sync path
    total_ins = 0
    total_upd = 0
    max_mod = ""
    page_num = 0
    t0 = time.time()

    for page in fetch_all(token, "contacts", CONTACT_PROPERTIES):
        page_num += 1
        std = map_hubspot_to_standard(page)
        ins, upd = merge_upsert(cur, std, source_tag)
        total_ins += ins
        total_upd += upd

        for r in page:
            mod = r["properties"].get("lastmodifieddate", "")
            if mod > max_mod:
                max_mod = mod

        if page_num % 100 == 0:
            total = total_ins + total_upd
            rate = total / (time.time() - t0) if time.time() - t0 > 0 else 0
            print(f"\r    {total:,} contacts ({rate:.0f}/sec)", end="", flush=True)

    total = total_ins + total_upd
    print(f"\r    {total:,} contacts synced ({total_ins:,} new, {total_upd:,} updated)")
    if max_mod:
        store_watermark(cur, load_type, max_mod, total)
    return total


# ---------------------------------------------------------------------------
# Deal sync (unchanged — HubSpot-specific with account column)
# ---------------------------------------------------------------------------
def extract_contact_id(deal):
    """Extract the first associated contact ID from a deal's associations."""
    assoc = deal.get("associations", {})
    contacts = assoc.get("contacts", {})
    results = contacts.get("results", [])
    if results:
        return int(results[0]["id"])
    return None


def upsert_deals(cur, deals, account):
    """Batch UPSERT deals into MySQL."""
    if not deals:
        return 0
    rows = []
    for d in deals:
        props = d.get("properties", {})
        did = int(d["id"])
        cid = extract_contact_id(d)
        rows.append((
            account, did, cid,
            (props.get("dealname") or "")[:255] or None,
            parse_decimal(props.get("amount")),
            parse_iso(props.get("closedate")),
            (props.get("dealstage") or "")[:50] or None,
            (props.get("dealtype") or "")[:50] or None,
            (props.get("pipeline") or "")[:50] or None,
            (props.get("conduit_name") or "")[:255] or None,
            (props.get("campaign") or "")[:255] or None,
            (props.get("product_name") or "")[:255] or None,
            (props.get("recurring") or "")[:50] or None,
            (props.get("donation_status") or "")[:50] or None,
            parse_decimal(props.get("amount_before_fees")),
            parse_decimal(props.get("amount_refunded")),
            (props.get("donor_covered_fees") or "")[:10] or None,
            parse_iso(props.get("lastmodifieddate")),
        ))

    cur.executemany("""
        INSERT INTO deals (
            account, deal_id, contact_id, dealname, amount, closedate,
            dealstage, dealtype, pipeline,
            conduit_name, campaign, product_name,
            recurring, donation_status,
            amount_before_fees, amount_refunded, donor_covered_fees,
            lastmodifieddate
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON DUPLICATE KEY UPDATE
            contact_id=VALUES(contact_id), dealname=VALUES(dealname),
            amount=VALUES(amount), closedate=VALUES(closedate),
            dealstage=VALUES(dealstage), dealtype=VALUES(dealtype),
            pipeline=VALUES(pipeline), conduit_name=VALUES(conduit_name),
            campaign=VALUES(campaign), product_name=VALUES(product_name),
            recurring=VALUES(recurring), donation_status=VALUES(donation_status),
            amount_before_fees=VALUES(amount_before_fees),
            amount_refunded=VALUES(amount_refunded),
            donor_covered_fees=VALUES(donor_covered_fees),
            lastmodifieddate=VALUES(lastmodifieddate),
            synced_at=CURRENT_TIMESTAMP
    """, rows)
    return len(rows)


def sync_deals(cur, token, account, full=False):
    """Sync deals from one HubSpot account."""
    load_type = f"hubspot_deals:{account}"
    watermark = get_watermark(cur, load_type)

    if full or not watermark:
        mode = "full"
        print(f"  Deals: FULL sync")
        clear_watermark(cur, load_type)
        cur.execute("DELETE FROM deals WHERE account = %s", (account,))
    else:
        # Try incremental (no associations in search -- backfill contact_id later)
        print(f"  Deals: incremental (since {watermark})")
        results, hit_limit = fetch_modified(
            token, "deals", DEAL_PROPERTIES, watermark)
        if hit_limit:
            print(f"    >10K changes -- falling back to full sync")
            clear_watermark(cur, load_type)
            cur.execute("DELETE FROM deals WHERE account = %s", (account,))
            mode = "full"
        else:
            # Incremental: search API doesn't return associations,
            # so fetch each changed deal individually with associations
            if results:
                enriched = []
                for r in results:
                    detail = api_get(token,
                        f"{API_BASE}/deals/{r['id']}",
                        {"properties": ",".join(DEAL_PROPERTIES),
                         "associations": "contacts"})
                    enriched.append(detail)
                    time.sleep(0.1)
                total = upsert_deals(cur, enriched, account)
            else:
                total = 0
            print(f"    Updated {total:,} deals")
            if results:
                max_mod = max(
                    (r["properties"].get("lastmodifieddate", "") for r in results),
                    default=watermark)
                store_watermark(cur, load_type, max_mod, total)
            return total

    # Full sync path -- List API supports associations param
    total = 0
    max_mod = ""
    page_num = 0
    t0 = time.time()

    for page in fetch_all(token, "deals", DEAL_PROPERTIES, associations="contacts"):
        page_num += 1
        count = upsert_deals(cur, page, account)
        total += count

        for r in page:
            mod = r["properties"].get("lastmodifieddate", "")
            if mod > max_mod:
                max_mod = mod

        if page_num % 50 == 0:
            rate = total / (time.time() - t0) if time.time() - t0 > 0 else 0
            print(f"\r    {total:,} deals ({rate:.0f}/sec)", end="", flush=True)

    print(f"\r    {total:,} deals synced")
    if max_mod:
        store_watermark(cur, load_type, max_mod, total)
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Sync HubSpot CRM to local MySQL")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-sync (delete + reload)")
    parser.add_argument("--account", type=str, default=None,
                        help="Sync only this account (matches HUBSPOT_TOKEN_<NAME> suffix)")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    accounts = discover_accounts()
    if not accounts:
        print("ERROR: No HubSpot tokens found in .env\n")
        print("Setup:")
        print("  1. Go to HubSpot > Settings > Integrations > Private Apps")
        print("  2. Create app with scopes: crm.objects.contacts.read, crm.objects.deals.read")
        print("  3. Copy access token")
        print("  4. Add to .env:")
        print("       HUBSPOT_TOKEN_MAIN=pat-na1-XXXXXXXX-...")
        print("       HUBSPOT_TOKEN_CLIENT_X=pat-na1-YYYYYYYY-...")
        print("     (Legacy: HUBSPOT_ACCESS_TOKEN is also supported)")
        sys.exit(1)

    # Filter to single account if requested
    if args.account:
        target = args.account.lower()
        accounts = [a for a in accounts if a["name"] == target]
        if not accounts:
            print(f"ERROR: No token found for account '{args.account}'")
            print(f"  Available accounts: {', '.join(a['name'] for a in discover_accounts())}")
            sys.exit(1)

    print("=" * 70)
    print("HUBSPOT CRM SYNC")
    print("=" * 70)
    print(f"  Accounts: {', '.join(a['name'] for a in accounts)}")
    print(f"  Mode: {'full' if args.full else 'incremental'}\n")

    t0 = time.time()
    conn = connect()
    bootstrap(conn)
    cur = conn.cursor()

    totals = {}  # {account: {"contacts": N, "deals": N}}

    for acct in accounts:
        name = acct["name"]
        token = acct["token"]
        print(f"--- Account: {name} ---")

        contact_count = sync_contacts(cur, token, name, full=args.full)
        deal_count = sync_deals(cur, token, name, full=args.full)

        totals[name] = {"contacts": contact_count, "deals": deal_count}
        print()

    # Summary
    elapsed = time.time() - t0
    print("=" * 70)
    print("SYNC COMPLETE")
    print("=" * 70)

    grand_contacts = 0
    grand_deals = 0
    for name, counts in totals.items():
        print(f"  [{name}]  contacts: {counts['contacts']:,}  deals: {counts['deals']:,}")
        grand_contacts += counts["contacts"]
        grand_deals += counts["deals"]

    if len(totals) > 1:
        print(f"  ----------")
        print(f"  Total:  contacts: {grand_contacts:,}  deals: {grand_deals:,}")

    # Quick stats
    cur.execute("SELECT COUNT(*) FROM contacts")
    total_contacts = cur.fetchone()[0]
    print(f"\n  Total unified contacts: {total_contacts:,}")

    cur.execute("SELECT COUNT(DISTINCT email_1) FROM contacts WHERE state IN ('NY','New York','new york')")
    ny = cur.fetchone()[0]
    print(f"  NY contacts: {ny:,}")

    cur.execute("SELECT COALESCE(SUM(amount),0) FROM deals WHERE dealstage='closedwon'")
    total_amt = cur.fetchone()[0]
    print(f"  Total closed-won (all accounts): ${total_amt:,.2f}")

    if elapsed >= 60:
        print(f"\n  Time: {elapsed/60:.1f} minutes")
    else:
        print(f"\n  Time: {elapsed:.0f} seconds")

    conn.close()


if __name__ == "__main__":
    main()
