#!/usr/bin/env python3
"""
enrich_crm_contacts.py — Append voter file data to crm_unified.contacts
========================================================================
Matches CRM contacts to NYS voter records using clean_last + clean_first + zip5,
then stamps voter file fields (party, districts, voter history, donor signals)
onto the contacts table.

Incremental: tracks updated_at watermark so only new/changed contacts since
last run get processed.

Usage:
    python enrich_crm_contacts.py              # Incremental (new contacts only)
    python enrich_crm_contacts.py --full       # Re-enrich all contacts
    python enrich_crm_contacts.py --stats      # Show match stats only

Called by: python main.py crm-enrich [--full] [--stats]
"""

import os, sys, time, argparse
from datetime import datetime
from dotenv import load_dotenv
import pymysql

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CRM_DB    = "crm_unified"
VOTER_DB  = "nys_voter_tagging"
VOTER_TBL = "voter_file"

MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

# Voter columns to copy onto CRM contacts.
# Format: (voter_file column, crm column name, SQL type)
VOTER_COLUMNS = [
    # Core voter ID
    ("StateVoterId",      "vf_state_voter_id",   "VARCHAR(50)"),
    # Party & registration
    ("OfficialParty",     "vf_party",            "VARCHAR(50)"),
    ("CalculatedParty",   "vf_calc_party",       "VARCHAR(50)"),
    ("RegistrationDate",  "vf_reg_date",         "DATE"),
    ("LastVoterActivity", "vf_last_activity",    "DATE"),
    # Districts
    ("CDName",            "vf_cd",               "VARCHAR(50)"),
    ("SDName",            "vf_sd",               "VARCHAR(50)"),
    ("LDName",            "vf_ld",               "VARCHAR(50)"),
    ("CountyName",        "vf_county",           "VARCHAR(50)"),
    # Address from voter file (canonical registered address)
    ("PrimaryAddress1",   "vf_address",          "VARCHAR(255)"),
    ("PrimaryCity",       "vf_city",             "VARCHAR(100)"),
    ("PrimaryZip",        "vf_zip",              "VARCHAR(10)"),
    # Contact info from voter file
    ("PrimaryPhone",      "vf_phone",            "VARCHAR(20)"),
    ("Mobile",            "vf_mobile",           "VARCHAR(20)"),
    ("Landline",          "vf_landline",         "VARCHAR(20)"),
    # Voter status
    ("RegistrationStatus","vf_status",           "VARCHAR(50)"),
    # Voter frequency/regularity
    ("GeneralFrequency",  "vf_gen_freq",         "VARCHAR(10)"),
    ("PrimaryFrequency",  "vf_pri_freq",         "VARCHAR(10)"),
    ("GeneralRegularity", "vf_gen_regularity",   "VARCHAR(10)"),
    ("PrimaryRegularity", "vf_pri_regularity",   "VARCHAR(10)"),
    ("OverAllFrequency",  "vf_overall_freq",     "VARCHAR(10)"),
    # Demographics
    ("ModeledEthnicity",  "vf_ethnicity",        "VARCHAR(50)"),
    ("Gender",            "vf_gender",           "VARCHAR(5)"),
    ("DOB",               "vf_dob",              "DATE"),
    ("Age",               "vf_age",              "VARCHAR(10)"),
    ("AgeRange",          "vf_age_range",        "VARCHAR(20)"),
    # Donor signals (BOE state)
    ("boe_total_amt",     "vf_boe_total_amt",    "DECIMAL(14,2)"),
    ("boe_total_count",   "vf_boe_total_count",  "INT"),
    ("boe_total_R_amt",   "vf_boe_R_amt",        "DECIMAL(14,2)"),
    ("boe_total_D_amt",   "vf_boe_D_amt",        "DECIMAL(14,2)"),
    ("boe_last_date",     "vf_boe_last_date",    "DATE"),
    ("boe_last_filer",    "vf_boe_last_filer",   "VARCHAR(255)"),
    # Donor signals (National/FEC)
    ("is_national_donor",         "vf_is_nat_donor",     "TINYINT"),
    ("national_total_amount",     "vf_nat_total_amt",    "DECIMAL(14,2)"),
    ("national_republican_amount","vf_nat_R_amt",        "DECIMAL(14,2)"),
    ("national_democratic_amount","vf_nat_D_amt",        "DECIMAL(14,2)"),
    # Donor signals (NYC CFB)
    ("cfb_total_amt",     "vf_cfb_total_amt",    "DECIMAL(14,2)"),
    ("cfb_total_count",   "vf_cfb_total_count",  "INT"),
    ("cfb_last_date",     "vf_cfb_last_date",    "DATE"),
    ("cfb_last_cand",     "vf_cfb_last_cand",    "VARCHAR(255)"),
]

# Timestamp column for tracking enrichment
ENRICHED_AT_COL = "vf_enriched_at"


def connect():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        charset="utf8mb4", autocommit=True,
    )


# ---------------------------------------------------------------------------
# Schema migration — add vf_* columns to contacts if missing
# ---------------------------------------------------------------------------
def ensure_columns(cur):
    """Add any missing vf_* columns to crm_unified.contacts, resize mismatched ones."""
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contacts'",
        (CRM_DB,))
    existing = {r[0]: r[1] for r in cur.fetchall()}

    # Map SQL type declarations to the information_schema COLUMN_TYPE strings
    # so we can compare them reliably.
    def _normalise_type(sql_type):
        """Convert our spec like VARCHAR(50) to what info_schema returns, e.g. varchar(50)."""
        return sql_type.lower().replace(' ', '')

    added = 0
    resized = 0
    for _, crm_col, sql_type in VOTER_COLUMNS:
        if crm_col not in existing:
            cur.execute(f"ALTER TABLE {CRM_DB}.contacts ADD COLUMN `{crm_col}` {sql_type} DEFAULT NULL")
            added += 1
            print(f"  + Added column: {crm_col} ({sql_type})")
        else:
            # Check if type needs widening
            current = existing[crm_col].lower().replace(' ', '')
            desired = _normalise_type(sql_type)
            if current != desired:
                cur.execute(f"ALTER TABLE {CRM_DB}.contacts MODIFY COLUMN `{crm_col}` {sql_type} DEFAULT NULL")
                resized += 1
                print(f"  ~ Resized column: {crm_col} ({current} -> {sql_type})")

    # Enrichment timestamp
    if ENRICHED_AT_COL not in existing:
        cur.execute(f"ALTER TABLE {CRM_DB}.contacts ADD COLUMN `{ENRICHED_AT_COL}` DATETIME DEFAULT NULL")
        added += 1
        print(f"  + Added column: {ENRICHED_AT_COL}")

    # Index on vf_state_voter_id for reverse lookups
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contacts' AND INDEX_NAME = 'idx_vf_svid'",
        (CRM_DB,))
    if cur.fetchone()[0] == 0:
        try:
            cur.execute(f"ALTER TABLE {CRM_DB}.contacts ADD INDEX idx_vf_svid (vf_state_voter_id)")
            print("  + Added index: idx_vf_svid")
        except Exception:
            pass  # Index might already exist

    if added or resized:
        parts = []
        if added:   parts.append(f"{added} columns added")
        if resized: parts.append(f"{resized} columns resized")
        print(f"  Schema updated: {', '.join(parts)}\n")
    else:
        print("  Schema OK (all vf_* columns present)\n")


# ---------------------------------------------------------------------------
# Discover which voter_file columns actually exist
# ---------------------------------------------------------------------------
def discover_voter_columns(cur):
    """Return the subset of VOTER_COLUMNS where the voter_file column exists."""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (VOTER_DB, VOTER_TBL))
    vf_cols = {r[0] for r in cur.fetchall()}

    available = []
    skipped = []
    for vf_col, crm_col, sql_type in VOTER_COLUMNS:
        if vf_col in vf_cols:
            available.append((vf_col, crm_col, sql_type))
        else:
            skipped.append(vf_col)

    if skipped:
        print(f"  Note: {len(skipped)} voter columns not found (skipped):")
        for s in skipped:
            print(f"    - {s}")
        print()

    return available


# ---------------------------------------------------------------------------
# Watermark helpers (reuse load_metadata pattern)
# ---------------------------------------------------------------------------
LOAD_TYPE = "crm_enrich_voters"


def get_watermark(cur):
    """Return last enrichment timestamp as string, or None."""
    try:
        cur.execute(
            f"SELECT file_hash FROM {CRM_DB}.load_metadata "
            "WHERE load_type = %s ORDER BY load_date DESC LIMIT 1",
            (LOAD_TYPE,))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def store_watermark(cur, timestamp_str, row_count):
    cur.execute(
        f"INSERT INTO {CRM_DB}.load_metadata (load_type, file_hash, row_count) "
        "VALUES (%s, %s, %s)",
        (LOAD_TYPE, timestamp_str, row_count))


def clear_watermark(cur):
    try:
        cur.execute(f"DELETE FROM {CRM_DB}.load_metadata WHERE load_type = %s",
                    (LOAD_TYPE,))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core enrichment
# ---------------------------------------------------------------------------
def enrich(cur, columns, full=False):
    """Match CRM contacts → voter_file and stamp vf_* columns.

    Matching strategy (in priority order):
      1. clean_last + clean_first + zip5  (name+zip match)

    Incremental: only processes contacts whose updated_at > last watermark
    OR whose vf_enriched_at IS NULL.
    """
    t0 = time.time()

    # Give each statement up to 10 min to acquire locks — the contacts table
    # can be held by lingering uvicorn connections or prior crashed runs.
    cur.execute("SET SESSION innodb_lock_wait_timeout = 600")

    # Build SELECT/SET clauses from available columns
    vf_select = ", ".join([f"v.`{vf_col}`" for vf_col, _, _ in columns])
    set_clauses = ", ".join([f"c.`{crm_col}` = v.`{vf_col}`" for vf_col, crm_col, _ in columns])
    set_clauses += f", c.`{ENRICHED_AT_COL}` = NOW()"

    # Scope: which contacts to process
    if full:
        where = "1=1"
        print("  Mode: FULL re-enrichment (all contacts)")
    else:
        watermark = get_watermark(cur)
        if watermark:
            # New rows since last run OR previously unmatched
            where = (f"(c.updated_at > '{watermark}' OR c.`{ENRICHED_AT_COL}` IS NULL)")
            print(f"  Mode: INCREMENTAL (since {watermark})")
        else:
            where = "1=1"
            print("  Mode: FULL (no prior watermark)")

    # Count candidates
    cur.execute(f"SELECT COUNT(*) FROM {CRM_DB}.contacts c WHERE {where}")
    total = cur.fetchone()[0]
    print(f"  Contacts to process: {total:,}")

    if total == 0:
        print("  Nothing to do.")
        return 0

    # Clear existing voter data for contacts being reprocessed.
    # Batched in chunks of 10K to avoid InnoDB lock-wait timeouts on large tables.
    null_clauses = ", ".join([f"`{crm_col}` = NULL" for _, crm_col, _ in columns])
    null_clauses += f", `{ENRICHED_AT_COL}` = NULL"
    cur.execute(f"SELECT MIN(id), MAX(id) FROM {CRM_DB}.contacts WHERE {where}")
    id_range = cur.fetchone()
    cleared = 0
    if id_range and id_range[0] is not None:
        min_id, max_id = id_range
        batch = 10_000
        for start in range(min_id, max_id + 1, batch):
            cur.execute(
                f"UPDATE {CRM_DB}.contacts SET {null_clauses} "
                f"WHERE id >= %s AND id < %s AND ({where})",
                (start, start + batch),
            )
            cleared += cur.rowcount
    print(f"  Cleared {cleared:,} contacts for re-matching")

    # ── Match 1: clean_last + clean_first + zip5 ────────────────────────────
    # Uses idx_name_zip on contacts and equivalent on voter_file.
    # If multiple voter records match, pick the one with the most recent
    # registration date (most likely the active record).
    print("  Matching: clean_last + clean_first + zip5 ...")

    # Step 1: For each unmatched contact, find best voter match via temp table.
    # voter_file has LastName/FirstName (not pre-cleaned), so apply the same
    # REGEXP_REPLACE(UPPER(...), '[^A-Z]', '') transform used by clean_name().
    cur.execute(f"""
        CREATE TEMPORARY TABLE _crm_vf_match AS
        SELECT c.id AS contact_id,
               (SELECT v.StateVoterId
                FROM {VOTER_DB}.{VOTER_TBL} v
                WHERE REGEXP_REPLACE(UPPER(v.LastName),  '[^A-Z]', '') = c.clean_last
                  AND REGEXP_REPLACE(UPPER(v.FirstName), '[^A-Z]', '') = c.clean_first
                  AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
                  AND v.RegistrationStatus = 'Active/Registered'
                ORDER BY v.RegistrationDate DESC
                LIMIT 1
               ) AS matched_svid
        FROM {CRM_DB}.contacts c
        WHERE {where}
          AND c.clean_last IS NOT NULL
          AND c.clean_first IS NOT NULL
          AND c.zip5 IS NOT NULL
    """)
    match_candidates = cur.rowcount
    print(f"    Candidates with name+zip: {match_candidates:,}")

    # Step 2: UPDATE contacts from voter_file via the match table
    cur.execute(f"""
        UPDATE {CRM_DB}.contacts c
        JOIN _crm_vf_match m ON m.contact_id = c.id
        JOIN {VOTER_DB}.{VOTER_TBL} v ON v.StateVoterId = m.matched_svid
        SET {set_clauses}
        WHERE m.matched_svid IS NOT NULL
    """)
    matched = cur.rowcount
    print(f"    Matched: {matched:,}")

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _crm_vf_match")

    # ── Mark unmatched contacts as processed (so incremental skips them) ────
    cur.execute(f"""
        UPDATE {CRM_DB}.contacts c
        SET c.`{ENRICHED_AT_COL}` = NOW()
        WHERE {where} AND c.`{ENRICHED_AT_COL}` IS NULL
    """)
    unmatched_stamped = cur.rowcount
    print(f"    Unmatched (stamped as processed): {unmatched_stamped:,}")

    # Store watermark
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if not full:
        clear_watermark(cur)
    store_watermark(cur, now_str, matched)

    elapsed = time.time() - t0
    rate_pct = (matched / total * 100) if total > 0 else 0
    print(f"\n  Done: {matched:,}/{total:,} matched ({rate_pct:.1f}%) in {elapsed:.1f}s")
    return matched


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def show_stats(cur):
    """Print enrichment coverage stats."""
    cur.execute(f"SELECT COUNT(*) FROM {CRM_DB}.contacts")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {CRM_DB}.contacts WHERE vf_state_voter_id IS NOT NULL")
    matched = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {CRM_DB}.contacts WHERE `{ENRICHED_AT_COL}` IS NOT NULL")
    processed = cur.fetchone()[0]

    pct = (matched / total * 100) if total > 0 else 0
    print(f"\n  CRM Contact Voter Enrichment Stats")
    print(f"  {'='*45}")
    print(f"  Total contacts:    {total:,}")
    print(f"  Processed:         {processed:,}")
    print(f"  Matched to voter:  {matched:,}  ({pct:.1f}%)")
    print(f"  Unmatched:         {total - matched:,}")

    if matched > 0:
        # Party breakdown
        cur.execute(f"""
            SELECT vf_party, COUNT(*) as cnt
            FROM {CRM_DB}.contacts
            WHERE vf_state_voter_id IS NOT NULL
            GROUP BY vf_party ORDER BY cnt DESC
        """)
        print(f"\n  Party breakdown:")
        for party, cnt in cur.fetchall():
            print(f"    {party or 'NULL':<10} {cnt:,}")

        # State breakdown (top 5)
        cur.execute(f"""
            SELECT vf_county, COUNT(*) as cnt
            FROM {CRM_DB}.contacts
            WHERE vf_state_voter_id IS NOT NULL
            GROUP BY vf_county ORDER BY cnt DESC LIMIT 10
        """)
        print(f"\n  Top counties:")
        for county, cnt in cur.fetchall():
            print(f"    {county or 'NULL':<25} {cnt:,}")

        # Donor coverage
        cur.execute(f"""
            SELECT
                SUM(CASE WHEN vf_boe_total_amt > 0 THEN 1 ELSE 0 END) as boe,
                SUM(CASE WHEN vf_is_nat_donor = 1 THEN 1 ELSE 0 END) as nat,
                SUM(CASE WHEN vf_cfb_total_amt > 0 THEN 1 ELSE 0 END) as cfb
            FROM {CRM_DB}.contacts
            WHERE vf_state_voter_id IS NOT NULL
        """)
        boe, nat, cfb = cur.fetchone()
        print(f"\n  Donor coverage among matched:")
        print(f"    BOE state donors:     {boe or 0:,}")
        print(f"    National/FEC donors:  {nat or 0:,}")
        print(f"    NYC CFB donors:       {cfb or 0:,}")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Enrich CRM contacts with voter file data")
    parser.add_argument("--full", action="store_true",
                        help="Re-enrich ALL contacts (ignore watermark)")
    parser.add_argument("--stats", action="store_true",
                        help="Show match stats only, no processing")
    parser.add_argument("--quiet",   action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug",   action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("CRM CONTACT VOTER ENRICHMENT")
    print("=" * 70)

    conn = connect()
    cur = conn.cursor()
    conn.select_db(CRM_DB)

    if args.stats:
        show_stats(cur)
        conn.close()
        return

    # 1. Ensure schema
    print("\n  Step 1: Schema check")
    ensure_columns(cur)

    # 2. Discover available voter columns
    print("  Step 2: Discovering voter_file columns")
    columns = discover_voter_columns(cur)
    print(f"  Available: {len(columns)}/{len(VOTER_COLUMNS)} voter columns\n")

    # 3. Enrich
    print("  Step 3: Enrichment")
    matched = enrich(cur, columns, full=args.full)

    # 4. Stats
    show_stats(cur)

    conn.close()
    print("=" * 70)
    print("ENRICHMENT COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
