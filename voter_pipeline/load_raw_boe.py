#!/usr/bin/env python3
"""
Load BOE Bulk Reports - Full audit trail.

Sources (data/boe_donors/):
  ALL_REPORTS_StateCandidate.zip   -> raw_state_candidate   (source='SC')
  ALL_REPORTS_CountyCandidate.zip  -> raw_county_candidate  (source='CC')
  ALL_REPORTS_StateCommittee.zip   -> raw_state_committee   (source='SM')
  ALL_REPORTS_CountyCommittee.zip  -> raw_county_committee  (source='CM')

Flow:
  1. Hash check  - skip if all 4 zips unchanged
  2. Stream-extract each CSV -> temp file -> LOAD DATA LOCAL INFILE -> raw_* table
     (raw tables = complete audit trail, all years, all schedule types)
  3. Build contributions (Sched A + Individual, last 10 years) from raw tables via SQL
  4. Add indexes, match to voter_file
  5. Build boe_donor_summary:
       - D/R/U amount + count per year (rolling 10 years)
       - Grand total D/R/U amount + count
       - Overall total amount + count
       - last_date (most recent contribution date)
       - last_filer (committee donated to most recently)

Column layout (58 cols, verified):
  c02 = filer_name      c03 = year          c05 = county
  c10 = schedule        c15 = date          c17 = contributor_type
  c25 = first_name      c26 = middle_name   c27 = last_name
  c28 = address         c29 = city          c30 = state
  c31 = zip             c36 = amount
"""

import os, io, zipfile, hashlib, tempfile, time, datetime, subprocess, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils.db import get_conn


def _auto_download_boe():
    """Try to refresh BOE zip files via download_boe.py before loading.
    Skips silently if playwright is not installed or download fails."""
    script = Path(__file__).parent / "download_boe.py"
    if not script.exists():
        return
    try:
        from playwright.sync_api import sync_playwright  # noqa: just a check
    except ImportError:
        print("  [boe-download] playwright not installed - using existing zip files")
        print("  [boe-download] To enable auto-download: pip install playwright && python -m playwright install firefox")
        return
    print("  [boe-download] Checking BOE site for updated files...")
    result = subprocess.run([sys.executable, str(script)], timeout=1800)
    if result.returncode != 0:
        print("  [boe-download] WARNING: download step failed - proceeding with existing files")
    else:
        print("  [boe-download] Done.")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOE_DIR     = Path(__file__).parent / "data" / "boe_donors"
EXTRACT_DIR = BOE_DIR / "extracted"
TEMP_DIR    = Path(tempfile.gettempdir())

SOURCES = [
    ("ALL_REPORTS_StateCandidate.zip",  "STATE_CANDIDATE.zip",  "STATE_CANDIDATE.csv",  "raw_state_candidate",  "SC"),
    ("ALL_REPORTS_CountyCandidate.zip", "COUNTY_CANDIDATE.zip", "COUNTY_CANDIDATE.csv", "raw_county_candidate", "CC"),
    ("ALL_REPORTS_StateCommittee.zip",  "STATE_COMMITTEE.zip",  "STATE_COMMITTEE.csv",  "raw_state_committee",  "SM"),
    ("ALL_REPORTS_CountyCommittee.zip", "COUNTY_COMMITTEE.zip", "COUNTY_COMMITTEE.csv", "raw_county_committee", "CM"),
]

# Rolling 10-year window
YEAR_MAX = datetime.date.today().year
YEAR_MIN = YEAR_MAX - 9
YEARS    = list(range(YEAR_MIN, YEAR_MAX + 1))

# 58 generic column names for raw staging tables
COL_NAMES = [f"c{i:02d}" for i in range(58)]
COL_LIST  = ", ".join(COL_NAMES)
COL_DEFS  = ",\n    ".join([f"c{i:02d} VARCHAR(255)" for i in range(58)])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def connect(db=None, local_infile=False):
    return get_conn(database=db, autocommit=True, local_infile=local_infile)


def calculate_hash(zip_files):
    hasher = hashlib.md5()
    for f in sorted(zip_files):
        stat = f.stat()
        hasher.update(f"{f.name}:{stat.st_size}:{stat.st_mtime}".encode())
    return hasher.hexdigest()


def get_stored_hash(cur):
    cur.execute("SHOW TABLES LIKE 'load_metadata'")
    if not cur.fetchone():
        return None
    cur.execute("SELECT file_hash FROM load_metadata WHERE load_type='boe_raw' ORDER BY load_date DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def store_hash(cur, file_hash, total_raw, total_contribs, total_donors):
    cur.execute("DROP TABLE IF EXISTS load_metadata")
    cur.execute("""CREATE TABLE load_metadata (
        id INT AUTO_INCREMENT PRIMARY KEY,
        load_type VARCHAR(50), file_hash VARCHAR(32),
        total_raw_rows INT, total_contributions INT, total_donors INT,
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX(load_type, load_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    cur.execute(
        "INSERT INTO load_metadata (load_type, file_hash, total_raw_rows, total_contributions, total_donors)"
        " VALUES ('boe_raw', %s, %s, %s, %s)",
        (file_hash, total_raw, total_contribs, total_donors)
    )


def stream_extract_csv(outer_zip_path, inner_zip_name, csv_name, tmp_path):
    with zipfile.ZipFile(outer_zip_path) as outer:
        with outer.open(inner_zip_name) as inner_f:
            inner_bytes = io.BytesIO(inner_f.read())
        with zipfile.ZipFile(inner_bytes) as inner:
            info = inner.getinfo(csv_name)
            total_bytes = info.file_size
            written = 0
            chunk = 4 * 1024 * 1024
            with inner.open(csv_name) as src, open(tmp_path, 'wb') as dst:
                while True:
                    buf = src.read(chunk)
                    if not buf:
                        break
                    dst.write(buf)
                    written += len(buf)
                    if total_bytes > 100_000_000:
                        pct = written / total_bytes * 100
                        print(f"\r    Extracting... {written/1e9:.2f} GB / {total_bytes/1e9:.2f} GB ({pct:.0f}%)", end='', flush=True)
            if total_bytes > 100_000_000:
                print()
    return written


def load_raw_table(cur, table_name, tmp_path):
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    cur.execute(f"""CREATE TABLE {table_name} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        {COL_DEFS}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    infile = Path(tmp_path).as_posix()
    # Ensure the connection won't time out on large files
    cur.execute("SET SESSION net_read_timeout = 600")
    cur.execute("SET SESSION net_write_timeout = 600")
    try:
        cur.execute(
            f"LOAD DATA LOCAL INFILE '{infile}'"
            f" INTO TABLE {table_name}"
            f" CHARACTER SET latin1"
            f" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
            f" LINES TERMINATED BY '\\r\\n'"
            f" ({COL_LIST})"
        )
    except Exception as e:
        if ("local_infile" in str(e).lower() or "1148" in str(e) or "3948" in str(e)
                or "loading local data is disabled" in str(e).lower()
                or "input stream" in str(e).lower()):
            print(f"    LOAD DATA not available — using batch INSERT fallback...")
            _batch_insert_csv(cur, table_name, tmp_path)
        else:
            raise
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def _batch_insert_csv(cur, table_name, csv_path, batch_size=5000):
    """Fallback loader using batch INSERT when LOAD DATA LOCAL INFILE is disabled."""
    import csv
    placeholders = ", ".join(["%s"] * 58)
    sql = f"INSERT INTO {table_name} ({COL_LIST}) VALUES ({placeholders})"
    batch = []
    total = 0
    with open(csv_path, "r", encoding="latin1") as f:
        reader = csv.reader(f)
        for row in reader:
            # Pad/truncate to exactly 58 columns
            row = (row + [""] * 58)[:58]
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                total += len(batch)
                print(f"\r    Inserted {total:,} rows...", end="", flush=True)
                batch = []
        if batch:
            cur.executemany(sql, batch)
            total += len(batch)
    print(f"\r    Inserted {total:,} rows.      ")


def build_contributions_from(cur, raw_table, source_code):
    cur.execute(
        f"INSERT INTO contributions"
        f"    (filer_id, year, filer, date, first, middle, last,"
        f"     address, city, state, zip, zip5, amount, party, source)"
        f" SELECT"
        f"    IF(TRIM(c01) REGEXP '^[0-9]+$', CAST(TRIM(c01) AS UNSIGNED), NULL),"
        f"    CAST(NULLIF(TRIM(c03), '') AS UNSIGNED),"
        f"    LEFT(c02, 255),"
        f"    NULLIF(LEFT(c15, 10), ''),"
        f"    LEFT(REGEXP_REPLACE(UPPER(TRIM(c25)), '[^A-Z]', ''), 100),"
        f"    LEFT(REGEXP_REPLACE(UPPER(TRIM(c26)), '[^A-Z]', ''), 100),"
        f"    LEFT(REGEXP_REPLACE(UPPER(TRIM(c27)), '[^A-Z]', ''), 100),"
        f"    LEFT(c28, 255),"
        f"    LEFT(c29, 100),"
        f"    LEFT(c30, 50),"
        f"    LEFT(c31, 20),"
        f"    IF(c31 REGEXP '[0-9]{{5}}', REGEXP_SUBSTR(c31, '[0-9]{{5}}'), ''),"
        f"    CAST(REPLACE(REPLACE(TRIM(c36), ',', ''), '$', '') AS DECIMAL(12,2)),"
        f"    CASE"
        f"        WHEN UPPER(c02) REGEXP 'DEMOCRAT|DEMOCRATIC|DEM PARTY|DCCC|DSCC|DNC |PROGRESSIVE|WORKING FAMILIES|DSA |ACTBLUE' THEN 'D'"
        f"        WHEN UPPER(c02) REGEXP 'REPUBLICAN|GOP|RNC |NRCC|NRSC|CONSERVATIVE|TRUMP|WINRED|MAGA' THEN 'R'"
        f"        ELSE 'U'"
        f"    END,"
        f"    '{source_code}'"
        f" FROM {raw_table}"
        f" WHERE c10 = 'A'"
        f"   AND c17 = 'Individual'"
        f"   AND TRIM(c36) != ''"
        f"   AND TRIM(c36) REGEXP '^[0-9.,]+$'"
        f"   AND CAST(REPLACE(REPLACE(TRIM(c36), ',', ''), '$', '') AS DECIMAL(12,2)) > 0"
        f"   AND CAST(NULLIF(TRIM(c03), '') AS UNSIGNED) >= YEAR(CURDATE()) - 9"
    )
    return cur.rowcount


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
print("=" * 80)
print("LOADING BOE BULK REPORTS - Full Audit Trail")
print("=" * 80)
print()

# Step 0: Auto-download fresh zips from BOE (no-op if playwright not installed)
_auto_download_boe()
print()

conn = connect()
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS boe_donors CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
cur.execute("ALTER DATABASE boe_donors CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
conn.close()

conn = connect("boe_donors", local_infile=True)
cur = conn.cursor()
try:
    cur.execute("SET GLOBAL local_infile = 1")
except Exception:
    pass

print("OK: boe_donors database ready\n")

# --------------------------------------------------
# Step 1: Hash check
# --------------------------------------------------
print("Step 1: Checking for changes...")
# Prefer extracted CSVs (ZIPs deleted after extraction); fall back to legacy ZIPs
csv_files = [EXTRACT_DIR / s[2] for s in SOURCES]
zip_files  = [BOE_DIR    / s[0] for s in SOURCES]

if all(f.exists() for f in csv_files):
    source_files = csv_files
elif all(f.exists() for f in zip_files):
    source_files = zip_files
else:
    missing = [f for f in csv_files if not f.exists()]
    for f in missing:
        print(f"  ERROR: Missing source file: {f}")
    print("  Run: python main.py boe-download")
    conn.close()
    exit(1)

current_hash = calculate_hash(source_files)
print(f"  Hash: {current_hash}")

stored_hash = get_stored_hash(cur)
if stored_hash and stored_hash == current_hash:
    cur.execute("SHOW TABLES LIKE 'boe_donor_summary'")
    if cur.fetchone():
        cur.execute("SELECT COUNT(*) FROM boe_donor_summary")
        n = cur.fetchone()[0]
        if n > 0:
            print(f"  No changes detected - {n:,} donors already loaded")
            print("  To force reload: DELETE FROM boe_donors.load_metadata;")
            conn.close()
            exit(0)

print("  Changes detected or first load - proceeding\n")

# --------------------------------------------------
# Step 2: Raw load
# --------------------------------------------------
print("Step 2: Loading raw staging tables (all years, all schedules)...")
print("  NOTE: Committee files are 2-5 GB uncompressed - allow several minutes\n")

total_raw_rows = 0
for outer_name, inner_zip, csv_name, raw_table, src_code in SOURCES:
    print(f"  [{src_code}] {csv_name}")
    t0 = time.time()

    extracted_csv = EXTRACT_DIR / csv_name
    outer_path    = BOE_DIR / outer_name
    tmp_path      = None

    if extracted_csv.exists():
        # Fast path: CSV already extracted by download_boe.py
        load_path = extracted_csv
        print(f"    Using extracted CSV: {extracted_csv.stat().st_size/1e6:.0f} MB")
    elif outer_path.exists():
        # Legacy fallback: extract from nested ZIP on the fly
        tmp_path = TEMP_DIR / f"boe_{raw_table}.csv"
        print(f"    Extracting from ZIP...", end='', flush=True)
        bytes_written = stream_extract_csv(outer_path, inner_zip, csv_name, tmp_path)
        print(f" {bytes_written/1e6:.1f} MB  ({time.time()-t0:.1f}s)")
        load_path = tmp_path
    else:
        print(f"    ERROR: neither {extracted_csv} nor {outer_path} found")
        exit(1)

    print(f"    Loading into {raw_table}...", end='', flush=True)
    t1 = time.time()
    row_count = load_raw_table(cur, raw_table, load_path)
    if tmp_path:
        tmp_path.unlink(missing_ok=True)
    total_raw_rows += row_count
    print(f" {row_count:,} rows  ({time.time()-t1:.1f}s)\n")

print(f"OK: {total_raw_rows:,} total raw rows loaded\n")

# --------------------------------------------------
# Step 3: Contributions
# --------------------------------------------------
print(f"Step 3: Building contributions table (Sched A, Individual, {YEAR_MIN}-{YEAR_MAX})...")
cur.execute("DROP TABLE IF EXISTS contributions")
cur.execute("""CREATE TABLE contributions (
    id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    filer_id  INT,
    year      INT,
    filer     VARCHAR(255),
    date      DATE,
    first     VARCHAR(100),
    middle    VARCHAR(100),
    last      VARCHAR(100),
    address   VARCHAR(255),
    city      VARCHAR(100),
    state     VARCHAR(50),
    zip       VARCHAR(20),
    zip5      VARCHAR(5),
    amount    DECIMAL(12,2),
    party     CHAR(1),
    source    CHAR(2),
    INDEX idx_filer_id (filer_id),
    INDEX idx_source   (source),
    INDEX idx_party    (party),
    INDEX idx_year     (year),
    INDEX idx_date     (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")

total_contributions = 0
for _, _, _, raw_table, src_code in SOURCES:
    t0 = time.time()
    n = build_contributions_from(cur, raw_table, src_code)
    total_contributions += n
    print(f"  [{src_code}] {n:,} contributions  ({time.time() - t0:.1f}s)")

print(f"\n  Adding match index...")
cur.execute("ALTER TABLE contributions ADD INDEX idx_match (last, first, zip5)")
print(f"OK: {total_contributions:,} total individual contributions\n")

# --------------------------------------------------
# Step 4: Match to voter_file
# --------------------------------------------------
print("Step 4: Matching to voter_file (name + zip5)...")
cur.execute("DROP TABLE IF EXISTS voter_contribs")
cur.execute("""CREATE TABLE voter_contribs (
    StateVoterId VARCHAR(50),
    year         INT,
    party        CHAR(1),
    total        DECIMAL(12,2),
    count        INT,
    PRIMARY KEY  (StateVoterId, year, party),
    INDEX idx_svid (StateVoterId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")

# Pass 1: exact name match (uses pre-computed clean columns + index)
t0 = time.time()
t0_total = t0
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
print(f"  Exact: {exact:,} matched  ({time.time() - t0:.1f}s)")

# Pass 2: hyphenated last-name fallback — two separate queries (avoids OR)
t0 = time.time()
hyph = 0
for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
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
    n = cur.rowcount
    hyph += n
    print(f"  Hyphenated ({part_label}): {n:,} additional  ({time.time() - t0:.1f}s)")
    t0 = time.time()
matched = exact + hyph
print(f"OK: {matched:,} total matched records  ({time.time() - t0_total:.1f}s)\n")

# --------------------------------------------------
# Step 5: boe_donor_summary
# --------------------------------------------------
print("Step 5: Building boe_donor_summary...")

# Build column definitions dynamically for each year
year_col_defs = []
for yr in YEARS:
    year_col_defs += [
        f"y{yr}_D_amt DECIMAL(12,2) DEFAULT 0",
        f"y{yr}_D_count INT DEFAULT 0",
        f"y{yr}_R_amt DECIMAL(12,2) DEFAULT 0",
        f"y{yr}_R_count INT DEFAULT 0",
        f"y{yr}_U_amt DECIMAL(12,2) DEFAULT 0",
        f"y{yr}_U_count INT DEFAULT 0",
    ]

cur.execute("DROP TABLE IF EXISTS boe_donor_summary")
cur.execute(
    "CREATE TABLE boe_donor_summary (\n"
    "    StateVoterId  VARCHAR(50) PRIMARY KEY,\n"
    "    " + ",\n    ".join(year_col_defs) + ",\n"
    "    total_D_amt   DECIMAL(12,2) DEFAULT 0,\n"
    "    total_D_count INT           DEFAULT 0,\n"
    "    total_R_amt   DECIMAL(12,2) DEFAULT 0,\n"
    "    total_R_count INT           DEFAULT 0,\n"
    "    total_U_amt   DECIMAL(12,2) DEFAULT 0,\n"
    "    total_U_count INT           DEFAULT 0,\n"
    "    total_amt     DECIMAL(12,2) DEFAULT 0,\n"
    "    total_count   INT           DEFAULT 0,\n"
    "    last_date     DATE,\n"
    "    last_filer    VARCHAR(255),\n"
    "    INDEX idx_total (total_amt),\n"
    "    INDEX idx_D     (total_D_amt),\n"
    "    INDEX idx_R     (total_R_amt)\n"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
)

# Seed one row per matched donor
cur.execute("INSERT INTO boe_donor_summary (StateVoterId) SELECT DISTINCT StateVoterId FROM voter_contribs")
print(f"  Seeded rows...")

# Pivot per year x party
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
    print(f"  Pivoted {yr}...")

# Grand totals by party
d_sum  = " + ".join([f"y{yr}_D_amt"   for yr in YEARS])
d_cnt  = " + ".join([f"y{yr}_D_count" for yr in YEARS])
r_sum  = " + ".join([f"y{yr}_R_amt"   for yr in YEARS])
r_cnt  = " + ".join([f"y{yr}_R_count" for yr in YEARS])
u_sum  = " + ".join([f"y{yr}_U_amt"   for yr in YEARS])
u_cnt  = " + ".join([f"y{yr}_U_count" for yr in YEARS])

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
print(f"  Rolled up totals...")

# Last donation date + filer
# voter_contribs is already keyed by StateVoterId - use it to find the most
# recent year, then look up the exact date + filer in contributions once.
print(f"  Setting last_date and last_filer...")

# Use a regular (non-TEMPORARY) work table so MySQL can reference it
# multiple times in the same statement — TEMPORARY tables cannot be
# opened twice in a single query (MySQL bug #14521).
cur.execute("DROP TABLE IF EXISTS _boe_last_donation")
cur.execute("""
    CREATE TABLE _boe_last_donation (
        StateVoterId VARCHAR(50) PRIMARY KEY,
        last_date    DATE,
        last_filer   VARCHAR(255)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")

# Step A: max contribution date per donor (exact match)
cur.execute("""
    INSERT INTO _boe_last_donation (StateVoterId, last_date)
    SELECT v.StateVoterId, MAX(c.date)
    FROM nys_voter_tagging.voter_file v
    JOIN boe_donors.contributions c
      ON v.clean_last  = c.last
     AND v.clean_first = c.first
     AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
    WHERE v.clean_last IS NOT NULL
      AND c.zip5 != '' AND c.date IS NOT NULL
    GROUP BY v.StateVoterId
    ON DUPLICATE KEY UPDATE last_date = VALUES(last_date)
""")
# Step A2: hyphenated fallback — two separate queries (avoids OR)
for part_col in ["clean_last_h1", "clean_last_h2"]:
    cur.execute(f"""
        INSERT INTO _boe_last_donation (StateVoterId, last_date)
        SELECT v.StateVoterId, MAX(c.date)
        FROM nys_voter_tagging.voter_file v
        JOIN boe_donors.contributions c
          ON v.{part_col}  = c.last
         AND v.clean_first = c.first
         AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
        WHERE v.{part_col} IS NOT NULL
          AND c.zip5 != '' AND c.date IS NOT NULL
        GROUP BY v.StateVoterId
        ON DUPLICATE KEY UPDATE last_date = GREATEST(last_date, VALUES(last_date))
    """)

# Step B: filer at that max date (MIN for determinism on ties)
# Regular table can be joined twice without the "Can't reopen" error.
# Run exact match first, then hyphenated parts to avoid OR.
cur.execute("""
    UPDATE _boe_last_donation t
    JOIN (
        SELECT v.StateVoterId, MIN(c.filer) AS filer
        FROM nys_voter_tagging.voter_file v
        JOIN boe_donors.contributions c
          ON v.clean_last  = c.last
         AND v.clean_first = c.first
         AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
        JOIN _boe_last_donation t2 ON v.StateVoterId = t2.StateVoterId
        WHERE v.clean_last IS NOT NULL
          AND c.zip5 != ''
          AND c.date = t2.last_date
        GROUP BY v.StateVoterId
    ) x ON t.StateVoterId = x.StateVoterId
    SET t.last_filer = x.filer
""")
# Hyphenated filer fallback
for part_col in ["clean_last_h1", "clean_last_h2"]:
    cur.execute(f"""
        UPDATE _boe_last_donation t
        JOIN (
            SELECT v.StateVoterId, MIN(c.filer) AS filer
            FROM nys_voter_tagging.voter_file v
            JOIN boe_donors.contributions c
              ON v.{part_col}  = c.last
             AND v.clean_first = c.first
             AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
            JOIN _boe_last_donation t2 ON v.StateVoterId = t2.StateVoterId
            WHERE v.{part_col} IS NOT NULL
              AND c.zip5 != ''
              AND c.date = t2.last_date
            GROUP BY v.StateVoterId
        ) x ON t.StateVoterId = x.StateVoterId
        SET t.last_filer = COALESCE(t.last_filer, x.filer)
    """)

# Apply to summary
cur.execute("""
    UPDATE boe_donor_summary s
    JOIN _boe_last_donation t ON s.StateVoterId = t.StateVoterId
    SET s.last_date  = t.last_date,
        s.last_filer = t.last_filer
""")
cur.execute("DROP TABLE IF EXISTS _boe_last_donation")

cur.execute("SELECT COUNT(*) FROM boe_donor_summary")
total_donors = cur.fetchone()[0]
print(f"OK: {total_donors:,} unique matched donors\n")

# --------------------------------------------------
# Step 6: Stats
# --------------------------------------------------
print(f"Step 6: Summary  (contributions {YEAR_MIN}-{YEAR_MAX})")
print()

hdr = f"  {'Year':<8}  {'Dem $':>14}  {'(n)':>8}  {'Rep $':>14}  {'(n)':>8}  {'Unaf $':>14}  {'(n)':>8}  {'Total $':>14}  {'(n)':>8}"
print(hdr)
print("  " + "-" * (len(hdr) - 2))

for yr in YEARS:
    cur.execute(
        f"SELECT"
        f"  SUM(y{yr}_D_amt), SUM(y{yr}_D_count),"
        f"  SUM(y{yr}_R_amt), SUM(y{yr}_R_count),"
        f"  SUM(y{yr}_U_amt), SUM(y{yr}_U_count),"
        f"  SUM(y{yr}_D_amt + y{yr}_R_amt + y{yr}_U_amt),"
        f"  SUM(y{yr}_D_count + y{yr}_R_count + y{yr}_U_count)"
        f" FROM boe_donor_summary"
    )
    da, dc, ra, rc, ua, uc, ta, tc = cur.fetchone()
    print(f"  {yr:<8}  ${da or 0:>13,.2f}  {int(dc or 0):>8,}  ${ra or 0:>13,.2f}  {int(rc or 0):>8,}  ${ua or 0:>13,.2f}  {int(uc or 0):>8,}  ${ta or 0:>13,.2f}  {int(tc or 0):>8,}")

print("  " + "-" * (len(hdr) - 2))
cur.execute(
    "SELECT SUM(total_D_amt), SUM(total_D_count),"
    "       SUM(total_R_amt), SUM(total_R_count),"
    "       SUM(total_U_amt), SUM(total_U_count),"
    "       SUM(total_amt),   SUM(total_count)"
    " FROM boe_donor_summary"
)
da, dc, ra, rc, ua, uc, ta, tc = cur.fetchone()
print(f"  {'TOTAL':<8}  ${da or 0:>13,.2f}  {int(dc or 0):>8,}  ${ra or 0:>13,.2f}  {int(rc or 0):>8,}  ${ua or 0:>13,.2f}  {int(uc or 0):>8,}  ${ta or 0:>13,.2f}  {int(tc or 0):>8,}")

print()
print(f"  Raw rows loaded (all years):    {total_raw_rows:,}")
print(f"  Contributions ({YEAR_MIN}-{YEAR_MAX}):       {total_contributions:,}")
print(f"  Matched to voters:              {matched:,}")
print(f"  Unique donors in summary:       {total_donors:,}")

# --------------------------------------------------
# Store hash + cleanup
# --------------------------------------------------
print()
print("Storing hash for change detection...")
store_hash(cur, current_hash, total_raw_rows, total_contributions, total_donors)
print("OK: Hash stored\n")

for f in ["inspect.py", "inspect2.py", "inspect3.py", "diag_out.txt", "diag_out2.txt"]:
    (BOE_DIR / f).unlink(missing_ok=True)

print("=" * 80)
print("COMPLETE")
print("=" * 80)
print()
print(f"  Raw tables (full audit trail):")
print(f"    raw_state_candidate, raw_county_candidate")
print(f"    raw_state_committee, raw_county_committee")
print(f"  Contributions ({YEAR_MIN}-{YEAR_MAX}): {total_contributions:,} rows")
print(f"  Summary:  boe_donor_summary  ({total_donors:,} donors)")
print()
print("  Next: python main.py boe-enrich")
print()

conn.close()
