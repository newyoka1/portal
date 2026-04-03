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

import os, sys, time, threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn


def _start_heartbeat(label: str, interval: int = 20):
    """Spawn a daemon thread that prints elapsed time every `interval` seconds.

    Returns a stop callable — invoke it once the blocking query finishes.
    Because the main thread blocks inside cursor.execute(), this is the only
    way to produce live output while MySQL is running a long UPDATE.
    """
    t0 = time.time()
    stop = threading.Event()

    def _beat():
        while not stop.wait(interval):
            elapsed = time.time() - t0
            m, s = divmod(int(elapsed), 60)
            print(f"    ... {label} [{m}m {s:02d}s elapsed]", flush=True)

    t = threading.Thread(target=_beat, daemon=True)
    t.start()
    return lambda: (stop.set(), t.join(timeout=2))

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

    print(f"  Cleaning {todo:,} FEC contributor names via SQL...", flush=True)
    t0 = time.time()
    stop = _start_heartbeat("name cleaning", interval=20)
    cur.execute("""
        UPDATE fec_contributions SET
            contributor_last_clean  = REGEXP_REPLACE(UPPER(COALESCE(contributor_last_name, '')),  '[^A-Z]', ''),
            contributor_first_clean = REGEXP_REPLACE(UPPER(COALESCE(contributor_first_name, '')), '[^A-Z]', '')
        WHERE contributor_last_clean IS NULL
          AND contributor_last_name IS NOT NULL
    """)
    stop()
    print(f"  Cleaned {cur.rowcount:,} rows ({time.time() - t0:.1f}s)", flush=True)
    cur.close()


def build_fec_summary(conn_fec):
    """Pre-aggregate fec_contributions -> fec_donor_summary (791K rows vs 16M).
    Keyed on (last_clean, first_clean, zip5). Rebuilds from scratch each run.
    Turns the enrichment JOIN from 16M-row to ~791K-row (~20x smaller).
    """
    cur = conn_fec.cursor()
    print("  Building fec_donor_summary (pre-aggregation)...", flush=True)
    t0 = time.time()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS fec_donor_summary ("
        "    contributor_last_clean  VARCHAR(100) NOT NULL,"
        "    contributor_first_clean VARCHAR(100) NOT NULL,"
        "    contributor_zip5        VARCHAR(10)  NOT NULL,"
        "    total_amount  DECIMAL(14,2) NOT NULL DEFAULT 0,"
        "    total_count   INT           NOT NULL DEFAULT 0,"
        "    dem_amount    DECIMAL(14,2) DEFAULT NULL,"
        "    dem_count     INT           DEFAULT NULL,"
        "    rep_amount    DECIMAL(14,2) DEFAULT NULL,"
        "    rep_count     INT           DEFAULT NULL,"
        "    ind_amount    DECIMAL(14,2) DEFAULT NULL,"
        "    ind_count     INT           DEFAULT NULL,"
        "    unk_amount    DECIMAL(14,2) DEFAULT NULL,"
        "    unk_count     INT           DEFAULT NULL,"
        "    PRIMARY KEY (contributor_last_clean, contributor_first_clean, contributor_zip5)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    )
    cur.execute("TRUNCATE TABLE fec_donor_summary")
    stop = _start_heartbeat("building fec_donor_summary", interval=20)
    cur.execute(
        "INSERT INTO fec_donor_summary"
        "    (contributor_last_clean, contributor_first_clean, contributor_zip5,"
        "     total_amount, total_count,"
        "     dem_amount, dem_count, rep_amount, rep_count,"
        "     ind_amount, ind_count, unk_amount, unk_count)"
        " SELECT"
        "    contributor_last_clean, contributor_first_clean, contributor_zip5,"
        "    SUM(contribution_amount), COUNT(*),"
        "    SUM(CASE WHEN party_signal = 'Democratic'  THEN contribution_amount ELSE 0 END),"
        "    SUM(CASE WHEN party_signal = 'Democratic'  THEN 1 ELSE 0 END),"
        "    SUM(CASE WHEN party_signal = 'Republican'  THEN contribution_amount ELSE 0 END),"
        "    SUM(CASE WHEN party_signal = 'Republican'  THEN 1 ELSE 0 END),"
        "    SUM(CASE WHEN party_signal = 'Independent' THEN contribution_amount ELSE 0 END),"
        "    SUM(CASE WHEN party_signal = 'Independent' THEN 1 ELSE 0 END),"
        "    SUM(CASE WHEN party_signal IS NULL OR party_signal = 'Unknown' THEN contribution_amount ELSE 0 END),"
        "    SUM(CASE WHEN party_signal IS NULL OR party_signal = 'Unknown' THEN 1 ELSE 0 END)"
        " FROM fec_contributions"
        " WHERE contributor_last_clean IS NOT NULL"
        "   AND contributor_first_clean IS NOT NULL"
        "   AND contributor_zip5 IS NOT NULL"
        " GROUP BY contributor_last_clean, contributor_first_clean, contributor_zip5"
    )
    stop()
    rows = cur.rowcount
    print(f"  fec_donor_summary: {rows:,} unique donor keys ({time.time()-t0:.1f}s)", flush=True)
    cur.close()
    return rows


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

    # Hyphenated last-name split columns (stored generated, derived from clean_last)
    for col_name, expr in [
        ("clean_last_h1",
         "VARCHAR(100) GENERATED ALWAYS AS ("
         "IF(LOCATE(\'-\', clean_last) > 0,"
         "LEFT(clean_last, LOCATE(\'-\', clean_last) - 1),"
         "NULL)) STORED"),
        ("clean_last_h2",
         "VARCHAR(100) GENERATED ALWAYS AS ("
         "IF(LOCATE(\'-\', clean_last) > 0,"
         "SUBSTRING(clean_last, LOCATE(\'-\', clean_last) + 1),"
         "NULL)) STORED"),
    ]:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'nys_voter_tagging'
              AND TABLE_NAME   = 'voter_file'
              AND COLUMN_NAME  = %s
        """, (col_name,))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE nys_voter_tagging.voter_file ADD COLUMN {col_name} {expr}")
            added += 1

    # Indexes
    for idx_name, idx_col in [
        ("idx_fec_donor", "is_national_donor"),
        ("idx_fec_total", "national_total_amount"),
        ("idx_fec_h1",    "clean_last_h1"),
        ("idx_fec_h2",    "clean_last_h2"),
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

    # Pre-query scale estimate
    cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.voter_file WHERE clean_last IS NOT NULL")
    vf_matchable = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM National_Donors.fec_contributions WHERE contributor_last_clean IS NOT NULL")
    fec_rows = cur.fetchone()[0]
    print(f"  voter_file rows with clean name: {vf_matchable:,}", flush=True)
    print(f"  fec_contributions rows to match: {fec_rows:,}", flush=True)
    print(flush=True)

    # Clear previous enrichment
    t0 = time.time()
    cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.voter_file WHERE is_national_donor = 1")
    prev_donors = cur.fetchone()[0]
    if prev_donors:
        print(f"  Clearing {prev_donors:,} previous donor rows...", flush=True)
        stop = _start_heartbeat("clearing previous data", interval=20)
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
        stop()
        print(f"  Cleared {cur.rowcount:,} rows ({time.time() - t0:.1f}s)", flush=True)

    # ------------------------------------------------------------------
    # Pass 1: Exact name match (uses composite index, fast)
    # ------------------------------------------------------------------
    print("  Pass 1: Exact name match (last + first + zip5)...", flush=True)
    t0 = time.time()
    t0_total = t0

    # Materialize aggregation into a temp table to avoid lock-table overflow
    print("  Pass 1: Building aggregate temp table...", flush=True)
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _fec_agg")
    cur.execute("""
        CREATE TEMPORARY TABLE _fec_agg (
            id          INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            StateVoterId VARCHAR(50) NOT NULL,
            total_amt   DECIMAL(14,2), total_cnt INT,
            dem_amt     DECIMAL(14,2), dem_cnt   INT,
            rep_amt     DECIMAL(14,2), rep_cnt   INT,
            ind_amt     DECIMAL(14,2), ind_cnt   INT,
            unk_amt     DECIMAL(14,2), unk_cnt   INT,
            INDEX (StateVoterId)
        ) ENGINE=InnoDB
    """)
    stop = _start_heartbeat("Pass 1 — exact match (aggregate)", interval=20)
    cur.execute("""
        INSERT INTO _fec_agg (StateVoterId,
            total_amt, total_cnt, dem_amt, dem_cnt,
            rep_amt, rep_cnt, ind_amt, ind_cnt, unk_amt, unk_cnt)
        SELECT
            v.StateVoterId,
            s.total_amount, s.total_count,
            s.dem_amount,   s.dem_count,
            s.rep_amount,   s.rep_count,
            s.ind_amount,   s.ind_count,
            s.unk_amount,   s.unk_count
        FROM nys_voter_tagging.voter_file v
        JOIN National_Donors.fec_donor_summary s
          ON s.contributor_last_clean  = v.clean_last
         AND s.contributor_first_clean = v.clean_first
         AND s.contributor_zip5        = LEFT(v.PrimaryZip, 5)
        WHERE v.clean_last IS NOT NULL
    """)
    stop()
    cur.execute("SELECT MAX(id) FROM _fec_agg")
    max_id = cur.fetchone()[0] or 0
    print(f"    Aggregate rows: {max_id:,} — batching UPDATE...", flush=True)

    BATCH = 50_000
    exact = 0
    stop = _start_heartbeat("Pass 1 — exact match (update)", interval=20)
    for lo in range(1, max_id + 1, BATCH):
        cur.execute("""
            UPDATE nys_voter_tagging.voter_file v
            JOIN _fec_agg agg ON v.StateVoterId = agg.StateVoterId
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
            WHERE agg.id BETWEEN %s AND %s
        """, (lo, lo + BATCH - 1))
        exact += cur.rowcount
        conn.commit()
    stop()
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _fec_agg")
    print(f"    Exact: {exact:,} voters ({time.time() - t0:.1f}s)", flush=True)

    # ------------------------------------------------------------------
    # Pass 2: Hyphenated last-name fallback
    # Two separate UPDATEs (h1 then h2) so each can use idx_clean_match.
    # Only voters with clean_last_h1 set who didn't match in pass 1.
    # ------------------------------------------------------------------
    print("  Pass 2: Hyphenated name fallback (voters not matched in Pass 1)...", flush=True)
    t0 = time.time()
    hyph = 0

    for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _fec_agg_h")
        cur.execute("""
            CREATE TEMPORARY TABLE _fec_agg_h (
                id          INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                StateVoterId VARCHAR(50) NOT NULL,
                total_amt   DECIMAL(14,2), total_cnt INT,
                dem_amt     DECIMAL(14,2), dem_cnt   INT,
                rep_amt     DECIMAL(14,2), rep_cnt   INT,
                ind_amt     DECIMAL(14,2), ind_cnt   INT,
                unk_amt     DECIMAL(14,2), unk_cnt   INT,
                INDEX (StateVoterId)
            ) ENGINE=InnoDB
        """)
        stop = _start_heartbeat(f"Pass 2 hyphen-{part_label} (aggregate)", interval=20)
        cur.execute(f"""
            INSERT INTO _fec_agg_h (StateVoterId,
                total_amt, total_cnt, dem_amt, dem_cnt,
                rep_amt, rep_cnt, ind_amt, ind_cnt, unk_amt, unk_cnt)
            SELECT
                v.StateVoterId,
                s.total_amount, s.total_count,
                s.dem_amount,   s.dem_count,
                s.rep_amount,   s.rep_count,
                s.ind_amount,   s.ind_count,
                s.unk_amount,   s.unk_count
            FROM nys_voter_tagging.voter_file v
            JOIN National_Donors.fec_donor_summary s
              ON s.contributor_last_clean  = v.{part_col}
             AND s.contributor_first_clean = v.clean_first
             AND s.contributor_zip5        = LEFT(v.PrimaryZip, 5)
            WHERE v.{part_col} IS NOT NULL
              AND (v.is_national_donor IS NULL OR v.is_national_donor != 1)
        """)
        stop()
        cur.execute("SELECT MAX(id) FROM _fec_agg_h")
        max_id_h = cur.fetchone()[0] or 0
        matched = 0
        stop = _start_heartbeat(f"Pass 2 hyphen-{part_label} (update)", interval=20)
        for lo in range(1, max_id_h + 1, BATCH):
            cur.execute("""
                UPDATE nys_voter_tagging.voter_file v
                JOIN _fec_agg_h agg ON v.StateVoterId = agg.StateVoterId
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
                WHERE agg.id BETWEEN %s AND %s
            """, (lo, lo + BATCH - 1))
            matched += cur.rowcount
            conn.commit()
        stop()
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _fec_agg_h")
        hyph += matched
        print(f"    Hyphenated ({part_label}): {matched:,} voters", flush=True)

    print(f"    Hyphenated total: {hyph:,} additional voters ({time.time() - t0:.1f}s)", flush=True)

    total = exact + hyph
    print(f"  Matched {total:,} voters total ({time.time() - t0_total:.1f}s)", flush=True)
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
    print()
    print("Step 3b: Pre-aggregating FEC contributions -> fec_donor_summary...")
    build_fec_summary(conn_fec)
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
