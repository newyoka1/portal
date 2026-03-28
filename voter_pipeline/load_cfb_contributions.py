#!/usr/bin/env python3
"""
Load NYC CFB contribution CSV files into cfb_donors database.

Sources (data/cfb/):
  2021_Contributions.csv
  2023_Contributions.csv
  2025_Contributions.csv

Flow:
  1. Hash check  - skip if all CSVs unchanged
  2. LOAD DATA LOCAL INFILE -> cfb_raw_contributions (staging)
  3. Build cfb_contributions (IND only, clean columns, last 3 cycles)
  4. Build cfb_donor_summary (one row per voter: D/R/U amounts by cycle)
  5. Match to voter_file via last_name + zip
  6. Enrich voter_file with cfb_ columns

Columns written to voter_file:
  cfb_total_amt     DECIMAL(14,2)  - total NYC CFB donations
  cfb_total_count   INT            - number of NYC CFB contributions
  cfb_last_date     DATE           - most recent contribution date
  cfb_last_cand     VARCHAR(255)   - candidate donated to most recently
  cfb_last_office   VARCHAR(100)   - office of that candidate
  cfb_2021_amt      DECIMAL(14,2)  - 2021 cycle donations
  cfb_2023_amt      DECIMAL(14,2)  - 2023 cycle donations
  cfb_2025_amt      DECIMAL(14,2)  - 2025 cycle donations

Called by: python main.py cfb-enrich
"""

import os, sys, csv, hashlib, time, tempfile
from pathlib import Path
from dotenv import load_dotenv
import pymysql

LOG_FILE = Path(r"D:\git\nys-voter-pipeline\data\cfb\load_out.txt")

class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()

_log_handle = open(LOG_FILE, "w", encoding="utf-8", errors="replace")
sys.stdout = Tee(sys.__stdout__, _log_handle)
sys.stderr = Tee(sys.__stderr__, _log_handle)

load_dotenv()

BASE    = Path(__file__).parent
CFB_DIR = BASE / "data" / "cfb"

MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

DB      = "cfb_donors"
CYCLES  = ["2017", "2021", "2023", "2025"]

# Columns added to voter_file
CFB_VOTER_COLUMNS = [
    ("cfb_total_amt",   "DECIMAL(14,2) DEFAULT NULL"),
    ("cfb_total_count", "INT           DEFAULT NULL"),
    ("cfb_last_date",   "DATE          DEFAULT NULL"),
    ("cfb_last_cand",   "VARCHAR(255)  DEFAULT NULL"),
    ("cfb_last_office", "VARCHAR(100)  DEFAULT NULL"),
    ("cfb_2017_amt",    "DECIMAL(14,2) DEFAULT NULL"),
    ("cfb_2021_amt",    "DECIMAL(14,2) DEFAULT NULL"),
    ("cfb_2023_amt",    "DECIMAL(14,2) DEFAULT NULL"),
    ("cfb_2025_amt",    "DECIMAL(14,2) DEFAULT NULL"),
]

# NYC CFB office code -> readable name
OFFICE_NAMES = {
    "1": "Mayor", "11": "Mayor",
    "2": "Public Advocate", "22": "Public Advocate",
    "3": "Comptroller", "33": "Comptroller",
    "4": "Borough President", "44": "Borough President",
    "5": "City Council", "55": "City Council",
    "6": "Undeclared", "66": "Undeclared",
    "IS": "Independent Spender",
}

# Borough code -> borough name (for logging)
BOROUGH_NAMES = {
    "K": "Brooklyn", "M": "Manhattan", "Q": "Queens",
    "S": "Staten Island", "X": "Bronx", "Z": "Outside NYC",
}


def connect(db=None, local_infile=False):
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, charset="utf8mb4",
        autocommit=True, local_infile=local_infile
    )


def files_hash(paths: list) -> str:
    h = hashlib.md5()
    for p in sorted(paths):
        if p.exists():
            stat = p.stat()
            h.update(f"{p.name}:{stat.st_size}:{stat.st_mtime}".encode())
    return h.hexdigest()


def get_stored_hash(cur, load_type: str):
    cur.execute("SHOW TABLES LIKE 'load_metadata'")
    if not cur.fetchone():
        return None
    cur.execute(
        "SELECT file_hash FROM load_metadata "
        "WHERE load_type=%s ORDER BY load_date DESC LIMIT 1",
        (load_type,)
    )
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------
def bootstrap(conn):
    cur = conn.cursor()

    # Database
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {DB} "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
    )
    conn.select_db(DB)

    # Raw staging table (all 52 CFB columns by name)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB}.cfb_raw_contributions (
            ELECTION    VARCHAR(10),
            OFFICECD    VARCHAR(10),
            RECIPID     VARCHAR(20),
            CANCLASS    VARCHAR(30),
            RECIPNAME   VARCHAR(255),
            COMMITTEE   VARCHAR(20),
            FILING      VARCHAR(10),
            SCHEDULE    VARCHAR(10),
            PAGENO      VARCHAR(10),
            SEQUENCENO  VARCHAR(10),
            REFNO       VARCHAR(30),
            CONT_DATE   VARCHAR(20),
            REFUNDDATE  VARCHAR(20),
            NAME        VARCHAR(255),
            C_CODE      VARCHAR(10),
            STRNO       VARCHAR(20),
            STRNAME     VARCHAR(255),
            APARTMENT   VARCHAR(30),
            BOROUGHCD   VARCHAR(5),
            CITY        VARCHAR(100),
            STATE       VARCHAR(10),
            ZIP         VARCHAR(20),
            OCCUPATION  VARCHAR(100),
            EMPNAME     VARCHAR(255),
            EMPSTRNO    VARCHAR(20),
            EMPSTRNAME  VARCHAR(255),
            EMPCITY     VARCHAR(100),
            EMPSTATE    VARCHAR(10),
            AMNT        VARCHAR(20),
            MATCHAMNT   VARCHAR(20),
            PREVAMNT    VARCHAR(20),
            PAY_METHOD  VARCHAR(10),
            INTERMNO    VARCHAR(20),
            INTERMNAME  VARCHAR(255),
            INTSTRNO    VARCHAR(20),
            INTSTRNM    VARCHAR(255),
            INTAPTNO    VARCHAR(20),
            INTCITY     VARCHAR(100),
            INTST       VARCHAR(10),
            INTZIP      VARCHAR(20),
            INTEMPNAME  VARCHAR(255),
            INTEMPSTNO  VARCHAR(20),
            INTEMPSTNM  VARCHAR(255),
            INTEMPCITY  VARCHAR(100),
            INTEMPST    VARCHAR(10),
            INTOCCUPA   VARCHAR(100),
            PURPOSECD   VARCHAR(20),
            EXEMPTCD    VARCHAR(10),
            ADJTYPECD   VARCHAR(10),
            RR_IND      VARCHAR(5),
            SEG_IND     VARCHAR(5),
            INT_C_CODE  VARCHAR(10)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    # Clean contributions table (IND only, parsed types)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB}.cfb_contributions (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            election_year SMALLINT        NOT NULL,
            office_cd     VARCHAR(10),
            office_name   VARCHAR(100),
            recip_id      VARCHAR(20),
            recip_name    VARCHAR(255),
            contrib_date  DATE,
            last_name     VARCHAR(100),
            first_name    VARCHAR(100),
            full_name     VARCHAR(255),
            borough_cd    VARCHAR(5),
            borough_name  VARCHAR(30),
            city          VARCHAR(100),
            state_cd      VARCHAR(10),
            zip5          VARCHAR(10),
            amount        DECIMAL(12,2),
            match_amount  DECIMAL(12,2),
            KEY idx_name_zip      (last_name(50), zip5),
            KEY idx_election_year (election_year),
            KEY idx_recip         (recip_name(50)),
            KEY idx_date          (contrib_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    # Donor summary (one row per matched StateVoterId)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB}.cfb_donor_summary (
            StateVoterId    VARCHAR(20)     NOT NULL PRIMARY KEY,
            total_amt       DECIMAL(14,2)   DEFAULT 0,
            total_count     INT             DEFAULT 0,
            amt_2017        DECIMAL(14,2)   DEFAULT 0,
            amt_2021        DECIMAL(14,2)   DEFAULT 0,
            amt_2023        DECIMAL(14,2)   DEFAULT 0,
            amt_2025        DECIMAL(14,2)   DEFAULT 0,
            last_date       DATE,
            last_cand       VARCHAR(255),
            last_office     VARCHAR(100),
            KEY idx_total   (total_amt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    # Migrate: add amt_2017 if missing (added in 10-year expansion)
    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME='cfb_donor_summary'
        AND COLUMN_NAME='amt_2017'
    """, (DB,))
    if cur.fetchone()[0] == 0:
        cur.execute(f"ALTER TABLE {DB}.cfb_donor_summary ADD COLUMN amt_2017 DECIMAL(14,2) DEFAULT 0 AFTER total_count")
        print("  Migrated cfb_donor_summary: added amt_2017 column")

    # Metadata
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB}.load_metadata (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            load_type   VARCHAR(50)  NOT NULL,
            file_hash   VARCHAR(32)  NOT NULL,
            row_count   INT          NOT NULL,
            load_date   DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    cur.close()


# ---------------------------------------------------------------------------
# Load raw CSVs
# ---------------------------------------------------------------------------
def load_raw(conn, force: bool = False) -> bool:
    """Load all cycle CSVs into cfb_raw_contributions. Returns True if reloaded."""
    csv_files = [CFB_DIR / f"{c}_Contributions.csv" for c in CYCLES]
    present   = [f for f in csv_files if f.exists()]

    if not present:
        print("  ERROR: No CFB contribution CSV files found in data/cfb/")
        print("  Run: python main.py cfb-download")
        sys.exit(1)

    cur  = conn.cursor()
    fhash = files_hash(present)
    stored = get_stored_hash(cur, "cfb_raw")

    if not force and stored == fhash:
        cur.execute(f"SELECT COUNT(*) FROM {DB}.cfb_raw_contributions")
        n = cur.fetchone()[0]
        print(f"  No change detected ({n:,} raw rows). Skipping raw load.")
        cur.close()
        return False

    print(f"  Truncating raw staging table...")
    cur.execute(f"TRUNCATE TABLE {DB}.cfb_raw_contributions")

    total_rows = 0
    for csv_path in present:
        cycle = csv_path.stem.split("_")[0]
        size_mb = csv_path.stat().st_size / 1e6
        print(f"  Loading {csv_path.name} ({size_mb:.1f} MB)...")
        t0 = time.time()

        # Use LOAD DATA LOCAL INFILE for speed
        # Need local_infile connection
        conn_li = connect(DB, local_infile=True)
        cur_li  = conn_li.cursor()

        csv_path_escaped = str(csv_path).replace("\\", "/")
        cur_li.execute(f"""
            LOAD DATA LOCAL INFILE '{csv_path_escaped}'
            INTO TABLE {DB}.cfb_raw_contributions
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
            (ELECTION, OFFICECD, RECIPID, CANCLASS, RECIPNAME, COMMITTEE,
             FILING, SCHEDULE, PAGENO, SEQUENCENO, REFNO, @dt, REFUNDDATE,
             NAME, C_CODE, STRNO, STRNAME, APARTMENT, BOROUGHCD, CITY, STATE,
             ZIP, OCCUPATION, EMPNAME, EMPSTRNO, EMPSTRNAME, EMPCITY, EMPSTATE,
             AMNT, MATCHAMNT, PREVAMNT, PAY_METHOD, INTERMNO, INTERMNAME,
             INTSTRNO, INTSTRNM, INTAPTNO, INTCITY, INTST, INTZIP, INTEMPNAME,
             INTEMPSTNO, INTEMPSTNM, INTEMPCITY, INTEMPST, INTOCCUPA, PURPOSECD,
             EXEMPTCD, ADJTYPECD, RR_IND, SEG_IND, INT_C_CODE)
            SET CONT_DATE = @dt
        """)
        rows = cur_li.rowcount
        total_rows += rows
        print(f"    {rows:,} rows in {time.time()-t0:.1f}s")
        cur_li.close()
        conn_li.close()

    # Store hash
    cur.execute(
        f"INSERT INTO {DB}.load_metadata (load_type, file_hash, row_count) "
        "VALUES ('cfb_raw', %s, %s)",
        (fhash, total_rows)
    )
    print(f"  Total raw rows: {total_rows:,}")
    cur.close()
    return True


# ---------------------------------------------------------------------------
# Build clean contributions
# ---------------------------------------------------------------------------
def build_clean(conn):
    print("  Building clean cfb_contributions (IND only, parsed types)...")
    cur = conn.cursor()
    t0  = time.time()

    cur.execute(f"TRUNCATE TABLE {DB}.cfb_contributions")

    # Parse from raw: individuals only, monetary schedules (ABC)
    # NAME field is "LASTNAME, FIRSTNAME" format for individuals
    cur.execute(f"""
        INSERT INTO {DB}.cfb_contributions
            (election_year, office_cd, office_name, recip_id, recip_name,
             contrib_date, last_name, first_name, full_name,
             borough_cd, borough_name, city, state_cd, zip5,
             amount, match_amount)
        SELECT
            CAST(NULLIF(TRIM(ELECTION), '') AS UNSIGNED),
            TRIM(OFFICECD),
            CASE TRIM(OFFICECD)
                WHEN '1'  THEN 'Mayor'
                WHEN '11' THEN 'Mayor'
                WHEN '2'  THEN 'Public Advocate'
                WHEN '22' THEN 'Public Advocate'
                WHEN '3'  THEN 'Comptroller'
                WHEN '33' THEN 'Comptroller'
                WHEN '4'  THEN 'Borough President'
                WHEN '44' THEN 'Borough President'
                WHEN '5'  THEN 'City Council'
                WHEN '55' THEN 'City Council'
                WHEN '6'  THEN 'Undeclared'
                ELSE 'Other'
            END,
            TRIM(RECIPID),
            TRIM(RECIPNAME),
            STR_TO_DATE(NULLIF(TRIM(CONT_DATE),''), '%m/%d/%Y'),
            LEFT(REGEXP_REPLACE(UPPER(TRIM(SUBSTRING_INDEX(NAME, ',', 1))), '[^A-Z]', ''), 100),
            LEFT(REGEXP_REPLACE(UPPER(TRIM(SUBSTRING_INDEX(NAME, ',', -1))), '[^A-Z]', ''), 100),
            UPPER(TRIM(NAME)),
            TRIM(BOROUGHCD),
            CASE TRIM(BOROUGHCD)
                WHEN 'K' THEN 'Brooklyn'
                WHEN 'M' THEN 'Manhattan'
                WHEN 'Q' THEN 'Queens'
                WHEN 'S' THEN 'Staten Island'
                WHEN 'X' THEN 'Bronx'
                WHEN 'Z' THEN 'Outside NYC'
                ELSE 'Unknown'
            END,
            TRIM(CITY),
            TRIM(STATE),
            LEFT(TRIM(ZIP), 5),
            CAST(NULLIF(TRIM(AMNT), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(MATCHAMNT), '') AS DECIMAL(12,2))
        FROM {DB}.cfb_raw_contributions
        WHERE TRIM(C_CODE) = 'IND'
          AND TRIM(SCHEDULE) IN ('ABC','ICONT')
          AND TRIM(AMNT) REGEXP '^[0-9]+(\\.[0-9]+)?$'
          AND CAST(TRIM(AMNT) AS DECIMAL(12,2)) > 0
          AND TRIM(NAME) != ''
          AND TRIM(ZIP)  != ''
    """)
    n = cur.rowcount
    print(f"  {n:,} individual contributions built  ({time.time()-t0:.1f}s)")
    cur.close()
    return n


# ---------------------------------------------------------------------------
# Build donor summary
# ---------------------------------------------------------------------------
def build_summary(conn):
    print("  Building cfb_donor_summary (match on last_name + zip5)...")
    cur = conn.cursor()
    t0  = time.time()

    cur.execute(f"TRUNCATE TABLE {DB}.cfb_donor_summary")

    # Raise GROUP_CONCAT limit so long recip_name lists don't get cut
    cur.execute("SET SESSION group_concat_max_len = 65536")

    # Pass 1: exact name match (uses pre-computed clean_last/clean_first)
    cur.execute(f"""
        INSERT INTO {DB}.cfb_donor_summary
            (StateVoterId, total_amt, total_count,
             amt_2017, amt_2021, amt_2023, amt_2025,
             last_date, last_cand, last_office)
        SELECT
            v.StateVoterId,
            SUM(c.amount),
            COUNT(*),
            SUM(CASE WHEN c.election_year = 2017 THEN c.amount ELSE 0 END),
            SUM(CASE WHEN c.election_year = 2021 THEN c.amount ELSE 0 END),
            SUM(CASE WHEN c.election_year = 2023 THEN c.amount ELSE 0 END),
            SUM(CASE WHEN c.election_year = 2025 THEN c.amount ELSE 0 END),
            MAX(c.contrib_date),
            SUBSTRING_INDEX(
                GROUP_CONCAT(c.recip_name ORDER BY c.contrib_date DESC SEPARATOR '|'),
                '|', 1
            ),
            SUBSTRING_INDEX(
                GROUP_CONCAT(c.office_name ORDER BY c.contrib_date DESC SEPARATOR '|'),
                '|', 1
            )
        FROM {DB}.cfb_contributions c
        JOIN nys_voter_tagging.voter_file v
          ON v.clean_last  = c.last_name
         AND v.clean_first = c.first_name
         AND v.PrimaryZip  = c.zip5
        WHERE v.clean_last IS NOT NULL
        GROUP BY v.StateVoterId
        ON DUPLICATE KEY UPDATE
            total_amt   = VALUES(total_amt),
            total_count = VALUES(total_count),
            amt_2017    = VALUES(amt_2017),
            amt_2021    = VALUES(amt_2021),
            amt_2023    = VALUES(amt_2023),
            amt_2025    = VALUES(amt_2025),
            last_date   = VALUES(last_date),
            last_cand   = VALUES(last_cand),
            last_office = VALUES(last_office)
    """)
    exact = cur.rowcount
    print(f"    Exact: {exact:,} voters  ({time.time()-t0:.1f}s)")

    # Pass 2: hyphenated last-name fallback (two indexed UPDATEs, no OR)
    t1 = time.time()
    hyph = 0
    for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
        cur.execute(f"""
            INSERT INTO {DB}.cfb_donor_summary
                (StateVoterId, total_amt, total_count,
                 amt_2017, amt_2021, amt_2023, amt_2025,
                 last_date, last_cand, last_office)
            SELECT
                v.StateVoterId,
                SUM(c.amount),
                COUNT(*),
                SUM(CASE WHEN c.election_year = 2017 THEN c.amount ELSE 0 END),
                SUM(CASE WHEN c.election_year = 2021 THEN c.amount ELSE 0 END),
                SUM(CASE WHEN c.election_year = 2023 THEN c.amount ELSE 0 END),
                SUM(CASE WHEN c.election_year = 2025 THEN c.amount ELSE 0 END),
                MAX(c.contrib_date),
                SUBSTRING_INDEX(
                    GROUP_CONCAT(c.recip_name ORDER BY c.contrib_date DESC SEPARATOR '|'),
                    '|', 1
                ),
                SUBSTRING_INDEX(
                    GROUP_CONCAT(c.office_name ORDER BY c.contrib_date DESC SEPARATOR '|'),
                    '|', 1
                )
            FROM {DB}.cfb_contributions c
            JOIN nys_voter_tagging.voter_file v
              ON v.{part_col}  = c.last_name
             AND v.clean_first = c.first_name
             AND v.PrimaryZip  = c.zip5
            WHERE v.{part_col} IS NOT NULL
            GROUP BY v.StateVoterId
            ON DUPLICATE KEY UPDATE
                total_amt   = VALUES(total_amt),
                total_count = VALUES(total_count),
                amt_2017    = VALUES(amt_2017),
                amt_2021    = VALUES(amt_2021),
                amt_2023    = VALUES(amt_2023),
                amt_2025    = VALUES(amt_2025),
                last_date   = VALUES(last_date),
                last_cand   = VALUES(last_cand),
                last_office = VALUES(last_office)
        """)
        matched = cur.rowcount
        hyph += matched
        print(f"    Hyphenated ({part_label}): {matched:,} voters")

    print(f"    Hyphenated total: {hyph:,} additional  ({time.time()-t1:.1f}s)")
    matched = exact + hyph
    print(f"  {matched:,} voters matched total  ({time.time()-t0:.1f}s)")
    cur.close()
    return matched


# ---------------------------------------------------------------------------
# Enrich voter_file
# ---------------------------------------------------------------------------
def enrich_voter_file(conn):
    print("  Enriching nys_voter_tagging.voter_file with CFB columns...")
    cur = conn.cursor()

    # Add missing columns
    added = 0
    for col_name, col_def in CFB_VOTER_COLUMNS:
        cur.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='nys_voter_tagging'
              AND TABLE_NAME='voter_file'
              AND COLUMN_NAME='{col_name}'
        """)
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE nys_voter_tagging.voter_file ADD COLUMN {col_name} {col_def}")
            added += 1
    if added:
        print(f"  Added {added} new column(s) to voter_file")

    # Clear old values (only if cfb_total_amt column exists and has data)
    t0 = time.time()
    cur.execute("""SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='nys_voter_tagging' AND TABLE_NAME='voter_file'
        AND COLUMN_NAME='cfb_total_amt'""")
    if cur.fetchone()[0] > 0:
        set_null = ", ".join([f"{c} = NULL" for c, _ in CFB_VOTER_COLUMNS])
        cur.execute(f"UPDATE nys_voter_tagging.voter_file SET {set_null} WHERE cfb_total_amt IS NOT NULL")
        print(f"  Cleared {cur.rowcount:,} rows  ({time.time()-t0:.1f}s)")
    else:
        print(f"  No existing CFB data to clear (first run)")

    # Update from summary
    t0 = time.time()
    cur.execute(f"""
        UPDATE nys_voter_tagging.voter_file v
        JOIN {DB}.cfb_donor_summary s ON v.StateVoterId = s.StateVoterId
        SET
            v.cfb_total_amt   = NULLIF(s.total_amt,   0),
            v.cfb_total_count = NULLIF(s.total_count, 0),
            v.cfb_last_date   = s.last_date,
            v.cfb_last_cand   = s.last_cand,
            v.cfb_last_office = s.last_office,
            v.cfb_2017_amt    = NULLIF(s.amt_2017, 0),
            v.cfb_2021_amt    = NULLIF(s.amt_2021, 0),
            v.cfb_2023_amt    = NULLIF(s.amt_2023, 0),
            v.cfb_2025_amt    = NULLIF(s.amt_2025, 0)
    """)
    enriched = cur.rowcount
    print(f"  Enriched {enriched:,} voter rows  ({time.time()-t0:.1f}s)")

    # Summary stats
    cur.execute("""
        SELECT
            COUNT(*)                          AS total_voters,
            SUM(cfb_total_amt IS NOT NULL)    AS cfb_donors,
            SUM(COALESCE(cfb_total_amt, 0))   AS total_donated,
            SUM(COALESCE(cfb_2017_amt, 0))    AS amt_2017,
            SUM(COALESCE(cfb_2021_amt, 0))    AS amt_2021,
            SUM(COALESCE(cfb_2023_amt, 0))    AS amt_2023,
            SUM(COALESCE(cfb_2025_amt, 0))    AS amt_2025
        FROM nys_voter_tagging.voter_file
    """)
    row = cur.fetchone()
    tv, donors, total, a17, a21, a23, a25 = row
    print()
    print(f"  Total voters:       {int(tv):>12,}")
    print(f"  NYC CFB donors:     {int(donors or 0):>12,}  ({int(donors or 0)/int(tv)*100:.3f}%)")
    print(f"  Total donated:              ${float(total or 0):>12,.2f}")
    print(f"    2017 cycle:               ${float(a17 or 0):>12,.2f}")
    print(f"    2021 cycle:               ${float(a21 or 0):>12,.2f}")
    print(f"    2023 cycle:               ${float(a23 or 0):>12,.2f}")
    print(f"    2025 cycle:               ${float(a25 or 0):>12,.2f}")

    # Top offices donated to
    cur.execute(f"""
        SELECT last_office, COUNT(*) n, SUM(total_amt) amt
        FROM {DB}.cfb_donor_summary
        GROUP BY last_office ORDER BY n DESC LIMIT 8
    """)
    print()
    print(f"  {'Last Office Donated To':<25} {'Donors':>8}  {'Total $':>14}")
    print(f"  {'-'*50}")
    for office, n, amt in cur.fetchall():
        print(f"  {(office or 'Unknown'):<25} {int(n):>8,}  ${float(amt or 0):>13,.2f}")

    cur.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(force: bool = False, skip_raw: bool = False):
    print("=" * 80)
    print("CFB CONTRIBUTION LOADER & VOTER ENRICHMENT")
    print("  Source : data/cfb/*_Contributions.csv")
    print("  Target : cfb_donors DB + nys_voter_tagging.voter_file")
    print("=" * 80)
    print()

    conn = connect()
    bootstrap(conn)

    if skip_raw:
        # Skip raw load entirely — use whatever is already staged
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {DB}.cfb_raw_contributions")
        raw_count = cur.fetchone()[0]
        cur.close()
        print(f"Step 1: Skipping raw load (--skip-raw). {raw_count:,} rows already staged.")
        print()
    else:
        # Step 1: Load raw CSVs
        print("Step 1: Load raw CFB CSVs")
        reloaded = load_raw(conn, force=force)
        print()

        if not reloaded and not force:
            # Raw unchanged AND not forcing — check if clean tables already built
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {DB}.cfb_contributions")
            existing = cur.fetchone()[0]
            cur.close()
            if existing > 0:
                print(f"Step 2-3: Skipping rebuild ({existing:,} contributions already built)")
                print("  Raw unchanged — re-enriching voter_file from existing data")
                print()
                print("Step 3: Build cfb_donor_summary + match voters")
                build_summary(conn)
                print()
                print("Step 4: Enrich voter_file")
                enrich_voter_file(conn)
                conn.close()
                print()
                print("=" * 80)
                print("COMPLETE")
                print("=" * 80)
                return

    # Step 2: Build clean contributions
    print("Step 2: Build clean cfb_contributions")
    try:
        build_clean(conn)
    except Exception as e:
        import traceback
        print(f"ERROR in build_clean: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)
    sys.stdout.flush()
    print()

    # Step 3: Build donor summary
    print("Step 3: Build cfb_donor_summary + match voters")
    try:
        build_summary(conn)
    except Exception as e:
        import traceback
        print(f"ERROR in build_summary: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)
    sys.stdout.flush()
    print()

    # Step 4: Enrich voter_file
    print("Step 4: Enrich voter_file")
    try:
        enrich_voter_file(conn)
    except Exception as e:
        import traceback
        print(f"ERROR in enrich_voter_file: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)
    print()

    conn.close()
    print()
    print("=" * 80)
    print("COMPLETE")
    print("  Voter columns: cfb_total_amt, cfb_total_count, cfb_last_date,")
    print("                 cfb_last_cand, cfb_last_office,")
    print("                 cfb_2021_amt, cfb_2023_amt, cfb_2025_amt")
    print("  Reference tables: cfb_donors.cfb_contributions, cfb_donor_summary")
    print("=" * 80)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",    action="store_true", help="Force reload even if unchanged")
    ap.add_argument("--skip-raw", action="store_true", help="Skip raw load, re-enrich only")
    args = ap.parse_args()
    main(force=args.force, skip_raw=args.skip_raw)
