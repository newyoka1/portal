#!/usr/bin/env python3
"""
pipeline/phone_match_crm.py — Phone-based second-pass CRM→voter matching
=========================================================================
For CRM contacts not matched by name+zip, attempts to match using mobile/phone
numbers against voter_file.Mobile and voter_file.Landline.

Strategy:
  1. Stream voter_file phones once into a Python dict (norm_phone → StateVoterId)
  2. Scan unmatched CRM contacts with a phone number
  3. Bulk UPDATE via a temporary JOIN table

Usage:
    python pipeline/phone_match_crm.py          # Match all unmatched contacts
    python pipeline/phone_match_crm.py --stats  # Show stats only

Called by: python main.py crm-phone [--stats]
"""
import os
import re
import sys
import time
import argparse
from dotenv import load_dotenv
import pymysql
import pymysql.cursors

load_dotenv()

CRM_DB    = "crm_unified"
VOTER_DB  = "nys_voter_tagging"
VOTER_TBL = "voter_file"

MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

# Mirror of enrich_crm_contacts.VOTER_COLUMNS — same vf_* target columns
VOTER_COLUMNS = [
    ("StateVoterId",              "vf_state_voter_id",   "VARCHAR(50)"),
    ("OfficialParty",             "vf_party",            "VARCHAR(50)"),
    ("CalculatedParty",           "vf_calc_party",       "VARCHAR(50)"),
    ("RegistrationDate",          "vf_reg_date",         "DATE"),
    ("LastVoterActivity",         "vf_last_activity",    "DATE"),
    ("CDName",                    "vf_cd",               "VARCHAR(50)"),
    ("SDName",                    "vf_sd",               "VARCHAR(50)"),
    ("LDName",                    "vf_ld",               "VARCHAR(50)"),
    ("CountyName",                "vf_county",           "VARCHAR(50)"),
    ("PrimaryAddress1",           "vf_address",          "VARCHAR(255)"),
    ("PrimaryCity",               "vf_city",             "VARCHAR(100)"),
    ("PrimaryZip",                "vf_zip",              "VARCHAR(10)"),
    ("PrimaryPhone",              "vf_phone",            "VARCHAR(20)"),
    ("Mobile",                    "vf_mobile",           "VARCHAR(20)"),
    ("Landline",                  "vf_landline",         "VARCHAR(20)"),
    ("RegistrationStatus",        "vf_status",           "VARCHAR(50)"),
    ("GeneralFrequency",          "vf_gen_freq",         "VARCHAR(10)"),
    ("PrimaryFrequency",          "vf_pri_freq",         "VARCHAR(10)"),
    ("GeneralRegularity",         "vf_gen_regularity",   "VARCHAR(10)"),
    ("PrimaryRegularity",         "vf_pri_regularity",   "VARCHAR(10)"),
    ("OverAllFrequency",          "vf_overall_freq",     "VARCHAR(10)"),
    ("ModeledEthnicity",          "vf_ethnicity",        "VARCHAR(50)"),
    ("Gender",                    "vf_gender",           "VARCHAR(5)"),
    ("DOB",                       "vf_dob",              "DATE"),
    ("Age",                       "vf_age",              "VARCHAR(10)"),
    ("AgeRange",                  "vf_age_range",        "VARCHAR(20)"),
    ("boe_total_amt",             "vf_boe_total_amt",    "DECIMAL(14,2)"),
    ("boe_total_count",           "vf_boe_total_count",  "INT"),
    ("boe_total_R_amt",           "vf_boe_R_amt",        "DECIMAL(14,2)"),
    ("boe_total_D_amt",           "vf_boe_D_amt",        "DECIMAL(14,2)"),
    ("boe_last_date",             "vf_boe_last_date",    "DATE"),
    ("boe_last_filer",            "vf_boe_last_filer",   "VARCHAR(255)"),
    ("is_national_donor",         "vf_is_nat_donor",     "TINYINT"),
    ("national_total_amount",     "vf_nat_total_amt",    "DECIMAL(14,2)"),
    ("national_republican_amount","vf_nat_R_amt",        "DECIMAL(14,2)"),
    ("national_democratic_amount","vf_nat_D_amt",        "DECIMAL(14,2)"),
    ("cfb_total_amt",             "vf_cfb_total_amt",    "DECIMAL(14,2)"),
    ("cfb_total_count",           "vf_cfb_total_count",  "INT"),
    ("cfb_last_date",             "vf_cfb_last_date",    "DATE"),
    ("cfb_last_cand",             "vf_cfb_last_cand",    "VARCHAR(255)"),
]
ENRICHED_AT_COL = "vf_enriched_at"


def connect():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        charset="utf8mb4", autocommit=True,
    )


def normalize_phone(phone) -> str | None:
    """Return 10-digit US phone string, or None if invalid."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    return digits if len(digits) == 10 else None


def discover_voter_columns(cur):
    """Return subset of VOTER_COLUMNS whose voter_file column actually exists."""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (VOTER_DB, VOTER_TBL),
    )
    vf_cols = {r[0] for r in cur.fetchall()}
    available = [(vc, cc, t) for vc, cc, t in VOTER_COLUMNS if vc in vf_cols]
    skipped   = [vc for vc, _, _ in VOTER_COLUMNS if vc not in vf_cols]
    if skipped:
        print(f"  Note: {len(skipped)} voter columns not in voter_file (skipped): "
              + ", ".join(skipped[:5]))
    return available


def show_stats(cur):
    cur.execute(
        "SELECT COUNT(*), SUM(vf_state_voter_id IS NOT NULL) "
        f"FROM {CRM_DB}.contacts"
    )
    total, matched = cur.fetchone()
    total   = int(total or 0)
    matched = int(matched or 0)
    pct     = matched / total * 100 if total else 0

    cur.execute(
        f"SELECT COUNT(*) FROM {CRM_DB}.contacts "
        "WHERE vf_state_voter_id IS NULL "
        "  AND (mobile IS NOT NULL AND mobile != '' "
        "   OR phone_1 IS NOT NULL AND phone_1 != '')"
    )
    has_phone = int(cur.fetchone()[0] or 0)

    print(f"\n{'='*60}")
    print(f"  Total contacts        : {total:,}")
    print(f"  Voter-matched         : {matched:,} ({pct:.1f}%)")
    print(f"  Unmatched             : {total - matched:,}")
    print(f"  Unmatched + has phone : {has_phone:,}")
    print(f"{'='*60}\n")


def build_phone_map(conn) -> dict:
    """
    Stream voter_file once via SSCursor into a Python dict:
      normalized_10digit_phone → StateVoterId

    Mobile takes precedence over Landline (INSERT IGNORE style — first wins).
    Memory: typically ~2-4M entries × ~80 bytes = ~200–400 MB.
    """
    print("  Building phone lookup from voter_file "
          "(streaming ~13M rows, ~30-60s)...", flush=True)
    t0 = time.time()
    phone_map: dict[str, str] = {}

    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute(
        f"SELECT StateVoterId, Mobile, Landline "
        f"FROM {VOTER_DB}.{VOTER_TBL} "
        f"WHERE Mobile IS NOT NULL OR Landline IS NOT NULL"
    )

    scanned = 0
    for svid, mobile, landline in cur:
        scanned += 1
        for ph in (mobile, landline):          # Mobile first → takes priority
            norm = normalize_phone(ph)
            if norm and norm not in phone_map:
                phone_map[norm] = svid
        if scanned % 1_000_000 == 0:
            print(f"    {scanned // 1_000_000}M voter rows scanned...", flush=True)
    cur.close()

    elapsed = time.time() - t0
    print(
        f"  Phone map ready: {len(phone_map):,} unique numbers "
        f"from {scanned:,} voter rows ({elapsed:.0f}s)",
        flush=True,
    )
    return phone_map


def phone_match(conn, columns):
    """Match unmatched CRM contacts by phone and UPDATE vf_* columns."""
    t0 = time.time()
    cur = conn.cursor()
    cur.execute("SET SESSION innodb_lock_wait_timeout = 600")

    # ── 1. Build phone lookup ─────────────────────────────────────────────────
    phone_map = build_phone_map(conn)
    if not phone_map:
        print("  No phone data in voter_file — aborting.")
        return

    # ── 2. Fetch unmatched contacts that have at least one phone ──────────────
    cur.execute(f"""
        SELECT id, mobile, phone_1, phone_2, phone_3
        FROM {CRM_DB}.contacts
        WHERE vf_state_voter_id IS NULL
          AND (
               (mobile  IS NOT NULL AND mobile  != '')
            OR (phone_1 IS NOT NULL AND phone_1 != '')
          )
    """)
    contacts = cur.fetchall()
    print(f"  Unmatched contacts with phone: {len(contacts):,}", flush=True)

    if not contacts:
        print("  Nothing to match.")
        return

    # ── 3. Python-side phone match ────────────────────────────────────────────
    matches: list[tuple[int, str]] = []   # [(contact_id, StateVoterId)]
    for cid, mobile, ph1, ph2, ph3 in contacts:
        svid = None
        for ph in (mobile, ph1, ph2, ph3):
            norm = normalize_phone(ph)
            if norm:
                svid = phone_map.get(norm)
                if svid:
                    break
        if svid:
            matches.append((cid, svid))

    print(f"  Phone matches resolved: {len(matches):,}", flush=True)
    if not matches:
        print("  No phone matches found.")
        return

    # ── 4. Load matches into a temp table ────────────────────────────────────
    cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS _phone_match_tmp (
            contact_id INT NOT NULL,
            svid       VARCHAR(50) NOT NULL,
            PRIMARY KEY (contact_id)
        ) ENGINE=InnoDB
    """)
    cur.execute("TRUNCATE TABLE _phone_match_tmp")

    BATCH = 1000
    for i in range(0, len(matches), BATCH):
        batch = matches[i : i + BATCH]
        vals  = ",".join(["(%s,%s)"] * len(batch))
        flat  = [v for pair in batch for v in pair]
        cur.execute(f"INSERT IGNORE INTO _phone_match_tmp VALUES {vals}", flat)

    # ── 5. Bulk UPDATE via JOIN ───────────────────────────────────────────────
    # Build SET clause from available voter columns
    set_clauses = ", ".join(
        [f"c.`{cc}` = v.`{vc}`" for vc, cc, _ in columns]
        + [f"c.`{ENRICHED_AT_COL}` = NOW()", "c.`vf_match_method` = 'phone'"]
    )

    print("  Running bulk UPDATE (contacts ← voter_file via temp table)...", flush=True)
    cur.execute(f"""
        UPDATE {CRM_DB}.contacts c
        JOIN _phone_match_tmp pm ON c.id = pm.contact_id
        JOIN {VOTER_DB}.{VOTER_TBL} v ON v.StateVoterId = pm.svid
        SET {set_clauses}
        WHERE c.vf_state_voter_id IS NULL
    """)
    updated = cur.rowcount
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _phone_match_tmp")

    elapsed = time.time() - t0
    print(
        f"\n  ✓ Phone match complete: {updated:,} contacts updated "
        f"({len(matches):,} phone matches found, {elapsed:.0f}s total)",
        flush=True,
    )


def main():
    p = argparse.ArgumentParser(
        description="Second-pass CRM→voter matching via phone numbers"
    )
    p.add_argument("--stats", action="store_true", help="Show match stats only")
    args = p.parse_args()

    print(f"\n{'='*60}")
    print("  CRM Phone Match — second-pass voter file matching")
    print(f"{'='*60}\n")

    conn = connect()
    cur  = conn.cursor()
    columns = discover_voter_columns(cur)

    if args.stats:
        show_stats(cur)
        conn.close()
        return

    print("Pre-match stats:")
    show_stats(cur)

    phone_match(conn, columns)

    print("\nPost-match stats:")
    show_stats(cur)
    conn.close()


if __name__ == "__main__":
    main()
