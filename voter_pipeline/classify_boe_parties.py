#!/usr/bin/env python3
"""
classify_boe_parties.py — Reclassify BOE contribution party labels
===================================================================

Runs AFTER load_raw_boe.py to reduce the "U" (Unknown) party bucket.

Strategy (multi-tier):
  1. Load COMMCAND.CSV filer lookup → boe_filers table
  2. Keyword matching on filer names (expanded: unions, PACs, known patterns)
  3. Candidate committee cross-reference: match filer's candidate name against
     voter_file OfficialParty registration
  4. Update contributions.party based on filer→party mapping

Called by: python main.py boe-enrich  (after load_raw_boe.py)
Standalone: python classify_boe_parties.py
"""

import os, sys, csv, time
from pathlib import Path
from dotenv import load_dotenv

import pymysql
load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.db import get_conn


def _column_exists(cur, database, table, column):
    """Check if a column exists in the given table."""
    cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (database, table, column),
    )
    return cur.fetchone() is not None


def _drop_and_create(cur, table_name, create_sql):
    """DROP + CREATE with retry if InnoDB DDL purge lags behind."""
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    try:
        cur.execute(create_sql)
    except pymysql.err.OperationalError as e:
        if e.args[0] == 1050:
            time.sleep(1)
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(create_sql)
        else:
            raise

# ---------------------------------------------------------------------------
# COMMCAND.CSV path — extracted by download_boe.py alongside the other CSVs
# ---------------------------------------------------------------------------
COMMCAND_PATH = Path(__file__).parent / "data" / "boe_donors" / "extracted" / "COMMCAND.CSV"

# ---------------------------------------------------------------------------
# Keyword lists (modeled after step4_classify_parties.py FEC classifier)
# ---------------------------------------------------------------------------
DEM_KEYWORDS = [
    'democratic', 'democrat', 'dem party',
    'dccc', 'dscc', 'dnc ',
    'progressive', 'working families', 'dsa ',
    'actblue',
    # Unions (overwhelmingly D in NYS)
    'seiu', 'afscme', 'afl-cio', 'afl cio',
    'teachers', 'nurses', 'aft ',
    'cwa ', 'uaw ', 'ibew',
    'laborers', 'plumbers', 'pipefitters', 'steamfitters',
    'ironworkers', 'carpenters', 'painters', 'electricians',
    'teamsters', 'mason tenders', 'bricklayers',
    'transport workers', 'transit workers',
    'hotel trades', 'hotel workers',
    'retail wholesale', 'rwdsu',
    'unite here', 'ufcw', 'liuna',
    'building trades', 'sheet metal',
    'doctors council', 'workers united',
    'moveon', 'emily\'s list', 'priorities usa',
    'planned parenthood',
    'for the many', 'courage to change',
    'dga ',  # Democratic Governors Association
    'dlcc',  # Democratic Legislative Campaign Committee
]

REP_KEYWORDS = [
    'republican', 'gop',
    'rnc ', 'nrcc', 'nrsc',
    'conservative party', 'conservative ',
    'trump', 'maga', 'america first',
    'winred',
    'club for growth', 'heritage',
    'liberty', 'freedom caucus',
    'right to life', 'pro-life',
    'national rifle', 'nra ',
    'job creators',
    'rga ',  # Republican Governors Association
    'rlcc',  # Republican Legislative Campaign Committee
]


def _classify_name(name):
    """Classify a filer/committee name by keyword matching.

    Returns 'D', 'R', or None (unknown).
    """
    if not name:
        return None
    nl = name.lower()
    d_hits = any(k in nl for k in DEM_KEYWORDS)
    r_hits = any(k in nl for k in REP_KEYWORDS)
    if d_hits and not r_hits:
        return 'D'
    if r_hits and not d_hits:
        return 'R'
    return None


def load_boe_filers(conn):
    """Load COMMCAND.CSV into boe_filers table.

    Schema:
      filer_id INT PK, name VARCHAR, type ENUM(COMMITTEE,CANDIDATE),
      level VARCHAR, status VARCHAR, committee_type VARCHAR, office VARCHAR

    Always creates the table (even when COMMCAND.CSV is missing) so
    downstream classify steps find 0 rows instead of a missing table.
    """
    cur = conn.cursor()
    _drop_and_create(cur, "boe_filers", """
        CREATE TABLE boe_filers (
            filer_id        INT PRIMARY KEY,
            name            VARCHAR(255),
            type            VARCHAR(20),
            level           VARCHAR(20),
            status          VARCHAR(20),
            committee_type  VARCHAR(100),
            office          VARCHAR(100),
            district        VARCHAR(50),
            county          VARCHAR(100),
            party           CHAR(1) DEFAULT NULL,
            INDEX idx_name  (name),
            INDEX idx_type  (type),
            INDEX idx_party (party)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    if not COMMCAND_PATH.exists():
        print(f"  WARNING: {COMMCAND_PATH} not found — table created empty")
        return 0

    rows = []
    with open(COMMCAND_PATH, "r", encoding="latin-1") as f:
        reader = csv.reader(f)
        for r in reader:
            if len(r) < 10:
                continue
            try:
                filer_id = int(r[0].strip('"'))
            except (ValueError, IndexError):
                continue
            name = r[1].strip('"').strip()
            ftype = r[2].strip('"').strip()        # COMMITTEE or CANDIDATE
            level = r[3].strip('"').strip()        # State, County
            status = r[4].strip('"').strip()       # ACTIVE, TERMINATED
            ctype = r[5].strip('"').strip()        # "Authorized Single Candidate Committee" etc
            office = r[6].strip('"').strip() if len(r) > 6 else ''
            district = r[7].strip('"').strip() if len(r) > 7 else ''
            county = r[8].strip('"').strip() if len(r) > 8 else ''
            rows.append((filer_id, name, ftype, level, status, ctype, office, district, county))

    cur.executemany(
        "INSERT IGNORE INTO boe_filers "
        "(filer_id, name, type, level, status, committee_type, office, district, county) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        rows
    )
    conn.commit()
    print(f"  Loaded {len(rows):,} filers from COMMCAND.CSV")
    return len(rows)


def classify_by_keywords(conn):
    """Tier 1: Classify filers by expanded keyword matching on name."""
    cur = conn.cursor()
    cur.execute("SELECT filer_id, name FROM boe_filers WHERE party IS NULL")
    filers = cur.fetchall()

    updates = []
    for fid, name in filers:
        p = _classify_name(name)
        if p:
            updates.append((p, fid))

    if updates:
        cur.executemany("UPDATE boe_filers SET party = %s WHERE filer_id = %s", updates)
        conn.commit()

    print(f"  Tier 1 (keywords): {len(updates):,} filers classified")
    return len(updates)


def classify_by_voter_registration(conn):
    """Tier 2: Batch-classify filers by matching candidate names to voter_file.

    Uses a temp table + single JOIN instead of per-filer queries for performance.

    1. CANDIDATE type filers: parse first/last from filer name
    2. COMMITTEE type filers: extract candidate name via regex patterns
       (e.g. 'Friends of Kathy Hochul' → first=KATHY, last=HOCHUL)
    3. Batch-JOIN against voter_file to get dominant OfficialParty
    """
    import re
    cur = conn.cursor()

    # Regex patterns for extracting candidate names from committee names
    name_patterns = [
        re.compile(r'(?:friends of|friends for|people for|citizens for|committee to (?:re-?)?elect|vote for)\s+(.+?)(?:\s+(?:for|inc|pac|committee|cmte|\d{4}).*)?$', re.I),
        re.compile(r'^(.+?)\s+(?:for|4)\s+(?:ny|new york|governor|senate|congress|assembly|mayor|council|da|district attorney|comptroller|ag|attorney general|state senate|state assembly)\b', re.I),
        re.compile(r'^(.+?)\s+\d{4}\s*$', re.I),  # "Name 2024"
    ]

    FALSE_POSITIVES = frozenset(('FOR', 'THE', 'OF', 'AND', 'INC', 'PAC', 'NYC', 'NYS'))

    def _extract_name(filer_name, filer_type):
        """Extract (first, last) from a filer name. Returns None if unparseable."""
        if filer_type == 'CANDIDATE':
            parts = filer_name.strip().split()
        else:
            # COMMITTEE: try regex extraction
            candidate_name = None
            for pat in name_patterns:
                m = pat.search(filer_name)
                if m:
                    candidate_name = m.group(1).strip()
                    break
            if not candidate_name:
                return None
            parts = candidate_name.split()

        if len(parts) < 2:
            return None
        first = parts[0].upper()
        last = parts[-1].upper()
        if len(first) < 2 or len(last) < 2:
            return None
        if last in FALSE_POSITIVES:
            return None
        return (first, last)

    # Collect all unclassified filers (both CANDIDATE and COMMITTEE types)
    cur.execute("SELECT filer_id, name, type FROM boe_filers WHERE party IS NULL")
    all_filers = cur.fetchall()

    # Build temp table of (filer_id, first, last) for batch lookup
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_filer_names")
    cur.execute("""
        CREATE TEMPORARY TABLE _tmp_filer_names (
            filer_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            INDEX idx_name (first_name, last_name)
        )
    """)

    batch = []
    for fid, name, ftype in all_filers:
        parsed = _extract_name(name, ftype)
        if parsed:
            batch.append((fid, parsed[0], parsed[1]))

    if not batch:
        print("  Tier 2 (voter registration): 0 filers to check")
        return 0

    cur.executemany(
        "INSERT INTO _tmp_filer_names (filer_id, first_name, last_name) VALUES (%s, %s, %s)",
        batch
    )
    print(f"  Tier 2: {len(batch):,} filer names extracted, batch-matching to voter_file...")

    # Single batch query: for each (first, last), find dominant party in voter_file
    cur.execute("""
        UPDATE boe_filers f
        JOIN _tmp_filer_names t ON f.filer_id = t.filer_id
        JOIN (
            SELECT first_u, last_u, OfficialParty,
                   ROW_NUMBER() OVER (
                       PARTITION BY first_u, last_u
                       ORDER BY cnt DESC
                   ) AS rn
            FROM (
                SELECT UPPER(FirstName) AS first_u,
                       UPPER(LastName) AS last_u,
                       OfficialParty,
                       COUNT(*) AS cnt
                FROM nys_voter_tagging.voter_file
                WHERE OfficialParty IN ('Democrat', 'Republican')
                GROUP BY UPPER(FirstName), UPPER(LastName), OfficialParty
            ) sub
        ) v ON t.first_name = v.first_u
           AND t.last_name  = v.last_u
           AND v.rn = 1
        SET f.party = CASE v.OfficialParty
            WHEN 'Democrat' THEN 'D'
            WHEN 'Republican' THEN 'R'
        END
        WHERE f.party IS NULL
    """)
    classified = cur.rowcount
    conn.commit()

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_filer_names")
    print(f"  Tier 2 (voter registration): {classified:,} filers classified")
    return classified


def apply_to_contributions(conn):
    """Update contributions.party from the classified boe_filers lookup."""
    cur = conn.cursor()

    # Count current U
    cur.execute("SELECT COUNT(*) FROM contributions WHERE party = 'U'")
    before_u = cur.fetchone()[0]

    cur.execute("""
        UPDATE contributions c
        JOIN boe_filers f ON c.filer = f.name
        SET c.party = f.party
        WHERE c.party = 'U'
          AND f.party IS NOT NULL
    """)
    reclassified = cur.rowcount
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM contributions WHERE party = 'U'")
    after_u = cur.fetchone()[0]

    print(f"\n  Applied to contributions:")
    print(f"    Before: {before_u:,} unknown")
    print(f"    Reclassified: {reclassified:,}")
    print(f"    After:  {after_u:,} unknown")

    return reclassified


def rebuild_donor_summary_parties(conn):
    """Re-aggregate boe_donor_summary D/R/U columns after reclassification.

    This replays the pivot from voter_contribs (which itself pulls from
    contributions.party).  We rebuild voter_contribs from scratch to pick up
    the new party labels.
    """
    import datetime
    YEAR_MAX = datetime.date.today().year
    YEAR_MIN = YEAR_MAX - 9
    YEARS = list(range(YEAR_MIN, YEAR_MAX + 1))

    cur = conn.cursor()

    print("\n  Rebuilding voter_contribs with reclassified parties...")
    cur.execute("TRUNCATE TABLE voter_contribs")

    # Exact match
    cur.execute(
        "INSERT INTO voter_contribs (StateVoterId, year, party, total, count)"
        " SELECT v.StateVoterId, c.year, c.party, SUM(c.amount), COUNT(*)"
        " FROM nys_voter_tagging.voter_file v"
        " JOIN boe_donors.contributions c"
        "   ON v.clean_last  = c.last"
        "  AND v.clean_first = c.first"
        "  AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5"
        " WHERE v.clean_last IS NOT NULL"
        "  AND c.zip5 != ''"
        " GROUP BY v.StateVoterId, c.year, c.party"
    )
    exact = cur.rowcount
    print(f"    Exact: {exact:,}")

    # Hyphenated fallback (requires clean_last_h1/h2 from 'pipeline' command)
    if _column_exists(cur, "nys_voter_tagging", "voter_file", "clean_last_h1"):
        for part_col in ["clean_last_h1", "clean_last_h2"]:
            cur.execute(
                "INSERT IGNORE INTO voter_contribs (StateVoterId, year, party, total, count)"
                " SELECT v.StateVoterId, c.year, c.party, SUM(c.amount), COUNT(*)"
                " FROM nys_voter_tagging.voter_file v"
                " JOIN boe_donors.contributions c"
                f"   ON v.{part_col}  = c.last"
                "  AND v.clean_first = c.first"
                "  AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5"
                f" WHERE v.{part_col} IS NOT NULL"
                "  AND c.zip5 != ''"
                " GROUP BY v.StateVoterId, c.year, c.party"
            )

    # Re-pivot boe_donor_summary
    print("  Re-pivoting boe_donor_summary...")

    # Reset all D/R/U columns to 0
    zero_cols = []
    for yr in YEARS:
        for p in ['D', 'R', 'U']:
            zero_cols += [f"y{yr}_{p}_amt = 0", f"y{yr}_{p}_count = 0"]
    zero_cols += ["total_D_amt = 0", "total_D_count = 0",
                  "total_R_amt = 0", "total_R_count = 0",
                  "total_U_amt = 0", "total_U_count = 0",
                  "total_amt = 0", "total_count = 0"]
    cur.execute(f"UPDATE boe_donor_summary SET {', '.join(zero_cols)}")

    for yr in YEARS:
        for party in ['D', 'R', 'U']:
            cur.execute(
                f"UPDATE boe_donor_summary s"
                f" JOIN ("
                f"   SELECT StateVoterId, SUM(total) AS amt, SUM(count) AS cnt"
                f"   FROM voter_contribs"
                f"   WHERE year = {yr} AND party = '{party}'"
                f"   GROUP BY StateVoterId"
                f" ) v ON s.StateVoterId = v.StateVoterId"
                f" SET s.y{yr}_{party}_amt = v.amt, s.y{yr}_{party}_count = v.cnt"
            )

    # Grand totals
    d_sum = " + ".join([f"y{yr}_D_amt" for yr in YEARS])
    d_cnt = " + ".join([f"y{yr}_D_count" for yr in YEARS])
    r_sum = " + ".join([f"y{yr}_R_amt" for yr in YEARS])
    r_cnt = " + ".join([f"y{yr}_R_count" for yr in YEARS])
    u_sum = " + ".join([f"y{yr}_U_amt" for yr in YEARS])
    u_cnt = " + ".join([f"y{yr}_U_count" for yr in YEARS])

    cur.execute(
        f"UPDATE boe_donor_summary SET"
        f"  total_D_amt   = {d_sum},"
        f"  total_D_count = {d_cnt},"
        f"  total_R_amt   = {r_sum},"
        f"  total_R_count = {r_cnt},"
        f"  total_U_amt   = {u_sum},"
        f"  total_U_count = {u_cnt},"
        f"  total_amt     = ({d_sum}) + ({r_sum}) + ({u_sum}),"
        f"  total_count   = ({d_cnt}) + ({r_cnt}) + ({u_cnt})"
    )
    conn.commit()
    print("  OK boe_donor_summary re-aggregated")


def print_stats(conn):
    """Print final party distribution."""
    cur = conn.cursor()
    cur.execute("SELECT party, COUNT(*) FROM contributions GROUP BY party ORDER BY COUNT(*) DESC")
    total = 0
    rows = cur.fetchall()
    for _, c in rows:
        total += c

    print(f"\n{'='*60}")
    print("BOE PARTY CLASSIFICATION RESULTS")
    print(f"{'='*60}")
    print(f"\n  {'Party':<15} {'Contributions':>15} {'%':>8}")
    print(f"  {'-'*40}")
    for p, c in rows:
        label = {'D': 'Democrat', 'R': 'Republican', 'U': 'Unknown'}[p]
        print(f"  {label:<15} {c:>15,} {c/total*100:>7.1f}%")
    print(f"  {'Total':<15} {total:>15,}")

    # Filer stats
    cur.execute("""
        SELECT
            SUM(party IS NOT NULL) as classified,
            SUM(party IS NULL) as unclassified,
            COUNT(*) as total
        FROM boe_filers
    """)
    row = cur.fetchone()
    cf = int(row[0] or 0)
    uf = int(row[1] or 0)
    tf = int(row[2] or 0)
    print(f"\n  Filer lookup: {cf:,} classified / {uf:,} unknown / {tf:,} total")


def main():
    t0 = time.time()
    print("=" * 60)
    print("BOE PARTY RECLASSIFICATION")
    print("=" * 60)

    conn = get_conn('boe_donors')

    # Check contributions table exists and has filer column
    cur = conn.cursor()
    try:
        cur.execute("SELECT filer FROM contributions LIMIT 0")
    except Exception:
        print("\nERROR: contributions table not found or missing 'filer' column.")
        print("Re-run: python main.py boe-enrich  (will rebuild contributions)")
        conn.close()
        sys.exit(1)

    print("\nStep 1: Loading COMMCAND.CSV filer lookup...")
    load_boe_filers(conn)

    print("\nStep 2: Classifying filers...")
    classify_by_keywords(conn)
    classify_by_voter_registration(conn)

    print("\nStep 3: Applying to contributions...")
    reclassified = apply_to_contributions(conn)

    if reclassified > 0:
        print("\nStep 4: Rebuilding donor summary with new party labels...")
        rebuild_donor_summary_parties(conn)
    else:
        print("\nStep 4: No reclassifications — skipping summary rebuild")

    print_stats(conn)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    conn.close()


if __name__ == "__main__":
    main()
