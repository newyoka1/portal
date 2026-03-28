#!/usr/bin/env python3
"""
FEC Donor Enrichment
=====================
Matches FEC individual contributions to voter_file by cleaned
last_name + first_name + zip5, then writes national_* columns.

This replaces the old step5_build_unified_table.py + enrich workflow.
No intermediate table is created — contributions are matched directly
against voter_file, consistent with the BOE and CFB pipelines.

Columns written to voter_file:
    national_total_amount       DECIMAL(14,2)
    national_total_count        INT
    national_democratic_amount   DECIMAL(14,2)
    national_democratic_count    INT
    national_republican_amount   DECIMAL(14,2)
    national_republican_count    INT
    national_independent_amount  DECIMAL(14,2)
    national_independent_count   INT
    national_unknown_amount      DECIMAL(14,2)
    national_unknown_count       INT
    is_national_donor            BOOLEAN

Called by: python main.py national-enrich
"""

import os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

NATIONAL_COLUMNS = [
    ("national_total_amount",       "DECIMAL(14,2) DEFAULT NULL"),
    ("national_total_count",        "INT           DEFAULT NULL"),
    ("national_democratic_amount",  "DECIMAL(14,2) DEFAULT NULL"),
    ("national_democratic_count",   "INT           DEFAULT NULL"),
    ("national_republican_amount",  "DECIMAL(14,2) DEFAULT NULL"),
    ("national_republican_count",   "INT           DEFAULT NULL"),
    ("national_independent_amount", "DECIMAL(14,2) DEFAULT NULL"),
    ("national_independent_count",  "INT           DEFAULT NULL"),
    ("national_unknown_amount",     "DECIMAL(14,2) DEFAULT NULL"),
    ("national_unknown_count",      "INT           DEFAULT NULL"),
    ("is_national_donor",           "TINYINT(1)    DEFAULT 0"),
]


def connect(db=None):
    return get_conn(database=db, autocommit=True)


def ensure_fec_clean_columns(conn):
    """Add cleaned name columns + index to fec_contributions if missing."""
    cur = conn.cursor()

    added = 0
    for col, typedef in [
        ("contributor_last_clean",  "VARCHAR(100) DEFAULT NULL"),
        ("contributor_first_clean", "VARCHAR(100) DEFAULT NULL"),
    ]:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'National_Donors'
              AND TABLE_NAME   = 'fec_contributions'
              AND COLUMN_NAME  = %s
        """, (col,))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE fec_contributions ADD COLUMN {col} {typedef}")
            added += 1

    # Index for the match JOIN
    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = 'National_Donors'
          AND TABLE_NAME   = 'fec_contributions'
          AND INDEX_NAME   = 'idx_clean_match'
    """)
    if cur.fetchone()[0] == 0:
        print("  Adding clean-name index (one-time)...")
        cur.execute("""
            ALTER TABLE fec_contributions
            ADD INDEX idx_clean_match (contributor_last_clean, contributor_first_clean, contributor_zip5)
        """)
        added += 1

    cur.close()
    return added


def clean_fec_names(conn):
    """Populate clean name columns via SQL REGEXP_REPLACE (only NULLs)."""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fec_contributions WHERE contributor_last_clean IS NULL AND contributor_last_name IS NOT NULL")
    todo = cur.fetchone()[0]
    if todo == 0:
        print("  FEC names already cleaned")
        cur.close()
        return

    print(f"  Cleaning {todo:,} FEC contributor names via SQL...")
    t0 = time.time()
    cur.execute("""
        UPDATE fec_contributions SET
            contributor_last_clean  = REGEXP_REPLACE(UPPER(COALESCE(contributor_last_name, '')),  '[^A-Z]', ''),
            contributor_first_clean = REGEXP_REPLACE(UPPER(COALESCE(contributor_first_name, '')), '[^A-Z]', '')
        WHERE contributor_last_clean IS NULL
          AND contributor_last_name IS NOT NULL
    """)
    print(f"  Cleaned {cur.rowcount:,} rows ({time.time() - t0:.1f}s)")
    cur.close()


def ensure_voter_columns(conn):
    """Add national_* columns to voter_file if missing."""
    cur = conn.cursor()
    added = 0
    for col_name, col_def in NATIONAL_COLUMNS:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'nys_voter_tagging'
              AND TABLE_NAME   = 'voter_file'
              AND COLUMN_NAME  = %s
        """, (col_name,))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE nys_voter_tagging.voter_file ADD COLUMN {col_name} {col_def}")
            added += 1

    # Indexes
    for idx_name, idx_col in [
        ("idx_fec_donor", "is_national_donor"),
        ("idx_fec_total", "national_total_amount"),
    ]:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'nys_voter_tagging'
              AND TABLE_NAME   = 'voter_file'
              AND INDEX_NAME   = %s
        """, (idx_name,))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE nys_voter_tagging.voter_file ADD INDEX {idx_name} ({idx_col})")
            added += 1

    cur.close()
    if added:
        print(f"  Added {added} new column(s)/index(es) to voter_file")
    return added


def match_and_enrich(conn):
    """
    Join voter_file to fec_contributions on cleaned names + zip5,
    aggregate donation amounts by party, and write to voter_file.
    """
    cur = conn.cursor()

    # Clear previous enrichment
    t0 = time.time()
    cur.execute("""
        UPDATE nys_voter_tagging.voter_file
        SET national_total_amount = NULL, national_total_count = NULL,
            national_democratic_amount = NULL, national_democratic_count = NULL,
            national_republican_amount = NULL, national_republican_count = NULL,
            national_independent_amount = NULL, national_independent_count = NULL,
            national_unknown_amount = NULL, national_unknown_count = NULL,
            is_national_donor = 0
        WHERE is_national_donor = 1
    """)
    cleared = cur.rowcount
    if cleared:
        print(f"  Cleared {cleared:,} previous donor rows ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Pass 1: Exact name match (uses composite index, fast)
    # ------------------------------------------------------------------
    print("  Pass 1: Exact name match...")
    t0 = time.time()
    t0_total = t0

    cur.execute("""
        UPDATE nys_voter_tagging.voter_file v
        JOIN (
            SELECT
                v2.StateVoterId,
                SUM(f.contribution_amount) AS total_amt,
                COUNT(*)                   AS total_cnt,
                SUM(CASE WHEN f.party_signal = 'Democratic'  THEN f.contribution_amount ELSE 0 END) AS dem_amt,
                SUM(CASE WHEN f.party_signal = 'Democratic'  THEN 1 ELSE 0 END)                     AS dem_cnt,
                SUM(CASE WHEN f.party_signal = 'Republican'  THEN f.contribution_amount ELSE 0 END) AS rep_amt,
                SUM(CASE WHEN f.party_signal = 'Republican'  THEN 1 ELSE 0 END)                     AS rep_cnt,
                SUM(CASE WHEN f.party_signal = 'Independent' THEN f.contribution_amount ELSE 0 END) AS ind_amt,
                SUM(CASE WHEN f.party_signal = 'Independent' THEN 1 ELSE 0 END)                     AS ind_cnt,
                SUM(CASE WHEN f.party_signal IS NULL OR f.party_signal = 'Unknown'
                         THEN f.contribution_amount ELSE 0 END) AS unk_amt,
                SUM(CASE WHEN f.party_signal IS NULL OR f.party_signal = 'Unknown'
                         THEN 1 ELSE 0 END)                     AS unk_cnt
            FROM nys_voter_tagging.voter_file v2
            JOIN National_Donors.fec_contributions f
              ON f.contributor_last_clean  = v2.clean_last
             AND f.contributor_first_clean = v2.clean_first
             AND f.contributor_zip5        = LEFT(v2.PrimaryZip, 5)
            WHERE v2.clean_last IS NOT NULL
              AND f.contributor_last_clean IS NOT NULL
              AND f.contributor_first_clean IS NOT NULL
              AND f.contributor_zip5 IS NOT NULL
            GROUP BY v2.StateVoterId
        ) AS agg ON v.StateVoterId = agg.StateVoterId
        SET
            v.national_total_amount       = agg.total_amt,
            v.national_total_count        = agg.total_cnt,
            v.national_democratic_amount   = agg.dem_amt,
            v.national_democratic_count    = agg.dem_cnt,
            v.national_republican_amount   = agg.rep_amt,
            v.national_republican_count    = agg.rep_cnt,
            v.national_independent_amount  = agg.ind_amt,
            v.national_independent_count   = agg.ind_cnt,
            v.national_unknown_amount      = agg.unk_amt,
            v.national_unknown_count       = agg.unk_cnt,
            v.is_national_donor            = 1
    """)

    exact = cur.rowcount
    print(f"    Exact: {exact:,} voters ({time.time() - t0:.1f}s)")

    # ------------------------------------------------------------------
    # Pass 2: Hyphenated last-name fallback
    # Two separate UPDATEs (h1 then h2) so each can use idx_clean_match.
    # Only voters with clean_last_h1 set who didn't match in pass 1.
    # ------------------------------------------------------------------
    print("  Pass 2: Hyphenated name fallback...")
    t0 = time.time()
    hyph = 0

    for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
        cur.execute(f"""
            UPDATE nys_voter_tagging.voter_file v
            JOIN (
                SELECT
                    v2.StateVoterId,
                    SUM(f.contribution_amount) AS total_amt,
                    COUNT(*)                   AS total_cnt,
                    SUM(CASE WHEN f.party_signal = 'Democratic'  THEN f.contribution_amount ELSE 0 END) AS dem_amt,
                    SUM(CASE WHEN f.party_signal = 'Democratic'  THEN 1 ELSE 0 END)                     AS dem_cnt,
                    SUM(CASE WHEN f.party_signal = 'Republican'  THEN f.contribution_amount ELSE 0 END) AS rep_amt,
                    SUM(CASE WHEN f.party_signal = 'Republican'  THEN 1 ELSE 0 END)                     AS rep_cnt,
                    SUM(CASE WHEN f.party_signal = 'Independent' THEN f.contribution_amount ELSE 0 END) AS ind_amt,
                    SUM(CASE WHEN f.party_signal = 'Independent' THEN 1 ELSE 0 END)                     AS ind_cnt,
                    SUM(CASE WHEN f.party_signal IS NULL OR f.party_signal = 'Unknown'
                             THEN f.contribution_amount ELSE 0 END) AS unk_amt,
                    SUM(CASE WHEN f.party_signal IS NULL OR f.party_signal = 'Unknown'
                             THEN 1 ELSE 0 END)                     AS unk_cnt
                FROM nys_voter_tagging.voter_file v2
                JOIN National_Donors.fec_contributions f
                  ON f.contributor_last_clean  = v2.{part_col}
                 AND f.contributor_first_clean = v2.clean_first
                 AND f.contributor_zip5        = LEFT(v2.PrimaryZip, 5)
                WHERE v2.{part_col} IS NOT NULL
                  AND (v2.is_national_donor IS NULL OR v2.is_national_donor != 1)
                  AND f.contributor_last_clean IS NOT NULL
                  AND f.contributor_first_clean IS NOT NULL
                  AND f.contributor_zip5 IS NOT NULL
                GROUP BY v2.StateVoterId
            ) AS agg ON v.StateVoterId = agg.StateVoterId
            SET
                v.national_total_amount       = agg.total_amt,
                v.national_total_count        = agg.total_cnt,
                v.national_democratic_amount   = agg.dem_amt,
                v.national_democratic_count    = agg.dem_cnt,
                v.national_republican_amount   = agg.rep_amt,
                v.national_republican_count    = agg.rep_cnt,
                v.national_independent_amount  = agg.ind_amt,
                v.national_independent_count   = agg.ind_cnt,
                v.national_unknown_amount      = agg.unk_amt,
                v.national_unknown_count       = agg.unk_cnt,
                v.is_national_donor            = 1
        """)
        matched = cur.rowcount
        hyph += matched
        print(f"    Hyphenated ({part_label}): {matched:,} voters")

    print(f"    Hyphenated total: {hyph:,} additional voters ({time.time() - t0:.1f}s)")

    total = exact + hyph
    print(f"  Matched {total:,} voters total ({time.time() - t0_total:.1f}s)")
    cur.close()
    return total


def print_stats(conn):
    """Print summary statistics."""
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*)                            AS total_voters,
            SUM(is_national_donor)              AS fec_donors,
            SUM(COALESCE(national_total_amount, 0))       AS overall,
            SUM(COALESCE(national_democratic_amount, 0))   AS dem,
            SUM(COALESCE(national_republican_amount, 0))   AS rep,
            SUM(COALESCE(national_independent_amount, 0))  AS ind
        FROM nys_voter_tagging.voter_file
    """)
    total, donors, overall, dem, rep, ind = cur.fetchone()
    donors = int(donors or 0)

    print(f"  Total voters:       {int(total):>12,}")
    print(f"  FEC donors:         {donors:>12,}  ({donors / int(total) * 100:.2f}%)")
    print()
    print(f"  Total contributed:          ${float(overall or 0):>14,.2f}")
    print(f"    Democratic:               ${float(dem or 0):>14,.2f}")
    print(f"    Republican:               ${float(rep or 0):>14,.2f}")
    print(f"    Independent:              ${float(ind or 0):>14,.2f}")

    # Party breakdown
    print()
    print("  FEC donors by registered party:")
    cur.execute("""
        SELECT OfficialParty, COUNT(*) AS n, SUM(national_total_amount) AS amt
        FROM nys_voter_tagging.voter_file
        WHERE is_national_donor = 1
        GROUP BY OfficialParty
        ORDER BY n DESC
        LIMIT 10
    """)
    print(f"  {'Party':<20} {'Donors':>8}  {'Total $':>14}")
    print(f"  {'-' * 46}")
    for party, n, amt in cur.fetchall():
        print(f"  {(party or 'Unknown'):<20} {int(n):>8,}  ${float(amt or 0):>13,.2f}")

    cur.close()


def main():
    print("=" * 80)
    print("FEC DONOR ENRICHMENT")
    print("  Source : National_Donors.fec_contributions")
    print("  Target : nys_voter_tagging.voter_file")
    print("=" * 80)
    print()

    # Step 1: Check National_Donors exists
    print("Step 1: Checking National_Donors database...")
    conn_check = connect("information_schema")
    cur = conn_check.cursor()
    cur.execute("SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME = 'National_Donors'")
    if not cur.fetchone():
        print("  ERROR: National_Donors database not found!")
        print("  Run: python main.py national-enrich --refresh")
        conn_check.close()
        sys.exit(1)
    cur.execute("""
        SELECT TABLE_NAME FROM TABLES
        WHERE TABLE_SCHEMA = 'National_Donors' AND TABLE_NAME = 'fec_contributions'
    """)
    if not cur.fetchone():
        print("  ERROR: fec_contributions table not found!")
        print("  Run steps 1-4 first (download, extract, load, classify)")
        conn_check.close()
        sys.exit(1)
    conn_check.close()
    print("  OK")
    print()

    # Step 2: Add clean name columns to fec_contributions
    print("Step 2: Preparing FEC clean name columns...")
    conn_fec = connect("National_Donors")
    added = ensure_fec_clean_columns(conn_fec)
    if added:
        print(f"  Added {added} column(s)/index(es)")

    # Step 3: Clean FEC names via SQL
    print()
    print("Step 3: Cleaning FEC contributor names...")
    clean_fec_names(conn_fec)
    conn_fec.close()
    print()

    # Step 4: Add national_* columns to voter_file
    print("Step 4: Ensuring voter_file columns...")
    conn_voter = connect("nys_voter_tagging")
    ensure_voter_columns(conn_voter)
    print()

    # Step 5: Match and enrich
    print("Step 5: Matching voters to FEC contributions...")
    match_and_enrich(conn_voter)
    print()

    # Step 6: Stats
    print("Step 6: Summary statistics...")
    print_stats(conn_voter)

    conn_voter.close()
    print()
    print("=" * 80)
    print("COMPLETE")
    print("  voter_file now includes FEC contribution data")
    print("  Ready for export: python main.py export --ld XX")
    print("=" * 80)


if __name__ == "__main__":
    main()
