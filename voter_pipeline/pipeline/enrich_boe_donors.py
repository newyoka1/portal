#!/usr/bin/env python3
"""
BOE Donor Enrichment
=====================
Syncs boe_donors.boe_donor_summary -> nys_voter_tagging.voter_file.

Columns written to voter_file (aggregates only - per-year detail stays in
boe_donor_summary for direct join in export queries):

    boe_total_D_amt    DECIMAL(14,2)   -- total Democratic donations (all years)
    boe_total_D_count  INT             -- total Democratic contribution count
    boe_total_R_amt    DECIMAL(14,2)   -- total Republican donations
    boe_total_R_count  INT
    boe_total_U_amt    DECIMAL(14,2)   -- total Unaffiliated donations
    boe_total_U_count  INT
    boe_total_amt      DECIMAL(14,2)   -- grand total all parties
    boe_total_count    INT
    boe_last_date      DATE            -- date of most recent contribution
    boe_last_filer     VARCHAR(255)    -- committee donated to most recently

Called by: python main.py boe-enrich
"""

import os, sys, time, datetime

# Allow imports from the project root regardless of CWD
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

YEAR_MAX = datetime.date.today().year
YEAR_MIN = YEAR_MAX - 9

# Columns written to voter_file
BOE_COLUMNS = [
    ("boe_total_D_amt",   "DECIMAL(14,2) DEFAULT NULL"),
    ("boe_total_D_count", "INT           DEFAULT NULL"),
    ("boe_total_R_amt",   "DECIMAL(14,2) DEFAULT NULL"),
    ("boe_total_R_count", "INT           DEFAULT NULL"),
    ("boe_total_U_amt",   "DECIMAL(14,2) DEFAULT NULL"),
    ("boe_total_U_count", "INT           DEFAULT NULL"),
    ("boe_total_amt",     "DECIMAL(14,2) DEFAULT NULL"),
    ("boe_total_count",   "INT           DEFAULT NULL"),
    ("boe_last_date",     "DATE          DEFAULT NULL"),
    ("boe_last_filer",    "VARCHAR(255)  DEFAULT NULL"),
]


def connect(db=None):
    return get_conn(database=db, autocommit=True)


def main():
    print("=" * 80)
    print("BOE DONOR ENRICHMENT")
    print(f"  Source: boe_donors.boe_donor_summary ({YEAR_MIN}-{YEAR_MAX})")
    print(f"  Target: nys_voter_tagging.voter_file")
    print("=" * 80)
    print()

    # ------------------------------------------------------------------
    # Step 1: Verify source table
    # ------------------------------------------------------------------
    print("Step 1: Verifying source data...")
    conn_boe = connect("boe_donors")
    cur_boe = conn_boe.cursor()

    cur_boe.execute("SHOW TABLES LIKE 'boe_donor_summary'")
    if not cur_boe.fetchone():
        print("  ERROR: boe_donors.boe_donor_summary not found.")
        print("  Run: python load_raw_boe.py")
        conn_boe.close()
        sys.exit(1)

    cur_boe.execute("SELECT COUNT(*), SUM(total_amt > 0), SUM(last_date IS NOT NULL) FROM boe_donor_summary")
    total, with_amt, with_date = cur_boe.fetchone()
    conn_boe.close()

    if not total:
        print("  ERROR: boe_donor_summary is empty.")
        print("  Run: python load_raw_boe.py")
        sys.exit(1)

    print(f"  OK: {int(total):,} donors  |  {int(with_amt):,} with amounts  |  {int(with_date):,} with last_date")
    print()

    # ------------------------------------------------------------------
    # Step 2: Add/verify columns on voter_file
    # ------------------------------------------------------------------
    print("Step 2: Syncing columns on voter_file...")
    conn = connect("nys_voter_tagging")
    cur = conn.cursor()

    # Drop any OLD-style per-year columns (boe_D_2018, boe_U_2021, etc.)
    cur.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'nys_voter_tagging'
          AND TABLE_NAME   = 'voter_file'
          AND COLUMN_NAME LIKE 'boe_%'
    """)
    old_cols = [r[0] for r in cur.fetchall()]
    new_col_names = {c for c, _ in BOE_COLUMNS}
    stale = [c for c in old_cols if c not in new_col_names]

    if stale:
        print(f"  Dropping {len(stale)} stale BOE columns...")
        for col in stale:
            cur.execute(f"ALTER TABLE voter_file DROP COLUMN {col}")
        print(f"  Dropped: {', '.join(stale)}")

    # Add missing new columns
    added = 0
    for col_name, col_def in BOE_COLUMNS:
        cur.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='nys_voter_tagging'
              AND TABLE_NAME='voter_file'
              AND COLUMN_NAME='{col_name}'
        """)
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE voter_file ADD COLUMN {col_name} {col_def}")
            added += 1

    if added:
        print(f"  Added {added} new column(s)")
    else:
        print(f"  All {len(BOE_COLUMNS)} columns already exist")

    # Add index if missing
    cur.execute("SHOW INDEX FROM voter_file WHERE Key_name = 'idx_boe_totals'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE voter_file ADD INDEX idx_boe_totals (boe_total_amt, boe_total_R_amt, boe_total_D_amt)")
        print("  Added index: idx_boe_totals")
    print()

    # ------------------------------------------------------------------
    # Step 3: Clear existing BOE values
    # ------------------------------------------------------------------
    print("Step 3: Clearing existing BOE values...")
    t0 = time.time()
    set_null = ", ".join([f"{col} = NULL" for col, _ in BOE_COLUMNS])
    cur.execute(f"""
        UPDATE voter_file SET {set_null}
        WHERE boe_total_amt IS NOT NULL OR boe_last_date IS NOT NULL
    """)
    print(f"  Cleared {cur.rowcount:,} rows  ({time.time()-t0:.1f}s)")
    print()

    # ------------------------------------------------------------------
    # Step 4: JOIN boe_donor_summary -> voter_file
    # ------------------------------------------------------------------
    print("Step 4: Enriching voter_file from boe_donor_summary...")
    t0 = time.time()
    cur.execute("""
        UPDATE nys_voter_tagging.voter_file v
        JOIN boe_donors.boe_donor_summary b ON v.StateVoterId = b.StateVoterId
        SET
            v.boe_total_D_amt   = NULLIF(b.total_D_amt,   0),
            v.boe_total_D_count = NULLIF(b.total_D_count, 0),
            v.boe_total_R_amt   = NULLIF(b.total_R_amt,   0),
            v.boe_total_R_count = NULLIF(b.total_R_count, 0),
            v.boe_total_U_amt   = NULLIF(b.total_U_amt,   0),
            v.boe_total_U_count = NULLIF(b.total_U_count, 0),
            v.boe_total_amt     = NULLIF(b.total_amt,     0),
            v.boe_total_count   = NULLIF(b.total_count,   0),
            v.boe_last_date     = b.last_date,
            v.boe_last_filer    = b.last_filer
    """)
    matched = cur.rowcount
    print(f"  Enriched {matched:,} voters  ({time.time()-t0:.1f}s)")
    print()

    # ------------------------------------------------------------------
    # Step 5: Summary stats
    # ------------------------------------------------------------------
    print("Step 5: Summary statistics...")
    cur.execute("""
        SELECT
            COUNT(*)                                         AS total_voters,
            SUM(boe_total_amt  IS NOT NULL)                  AS total_donors,
            SUM(boe_total_D_amt IS NOT NULL)                 AS dem_donors,
            SUM(boe_total_R_amt IS NOT NULL)                 AS rep_donors,
            SUM(boe_total_U_amt IS NOT NULL)                 AS una_donors,
            SUM(COALESCE(boe_total_D_amt, 0))                AS dem_amt,
            SUM(COALESCE(boe_total_R_amt, 0))                AS rep_amt,
            SUM(COALESCE(boe_total_U_amt, 0))                AS una_amt,
            SUM(COALESCE(boe_total_amt,   0))                AS grand_amt
        FROM voter_file
    """)
    row = cur.fetchone()
    tv, td, dd, rd, ud, da, ra, ua, ga = row

    print(f"  Total voters:          {int(tv):>12,}")
    print(f"  Total BOE donors:      {int(td or 0):>12,}  ({int(td or 0)/int(tv)*100:.2f}%)")
    print(f"    Democratic:          {int(dd or 0):>12,}  ${float(da or 0):>14,.2f}")
    print(f"    Republican:          {int(rd or 0):>12,}  ${float(ra or 0):>14,.2f}")
    print(f"    Unaffiliated:        {int(ud or 0):>12,}  ${float(ua or 0):>14,.2f}")
    print(f"    Grand total:                       ${float(ga or 0):>14,.2f}")
    print()

    cur.execute("""
        SELECT OfficialParty, COUNT(*) AS donors,
               SUM(COALESCE(boe_total_amt,0)) AS amt
        FROM voter_file
        WHERE boe_total_amt IS NOT NULL
        GROUP BY OfficialParty
        ORDER BY donors DESC
        LIMIT 8
    """)
    print(f"  {'Reg Party':<24} {'Donors':>8}  {'Total $':>14}")
    print(f"  {'-'*50}")
    for party, cnt, amt in cur.fetchall():
        print(f"  {(party or 'Unknown'):<24} {int(cnt):>8,}  ${float(amt or 0):>13,.2f}")
    print()

    conn.close()

    print("=" * 80)
    print("COMPLETE")
    print("=" * 80)
    print(f"  voter_file BOE columns: boe_total_D/R/U_amt, boe_total_D/R/U_count,")
    print(f"                          boe_total_amt, boe_total_count,")
    print(f"                          boe_last_date, boe_last_filer")
    print(f"  Per-year detail: boe_donors.boe_donor_summary (y{YEAR_MIN}_D_amt ... y{YEAR_MAX}_U_count)")
    print(f"  Ready for: python main.py export --ld XX")
    print()


if __name__ == "__main__":
    main()
