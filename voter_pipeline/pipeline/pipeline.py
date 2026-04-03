#!/usr/bin/env python3
"""
NYS Voter Tagging Pipeline - StateVoterId Direct Matching
============================================================
UPDATED VERSION - Uses StateVoterId for direct, accurate matching

KEY FEATURES:
- Direct matching on StateVoterId (official NYS voter ID)
- Keeps ALL columns from audience CSV files (87 columns)
- Much faster than match key approach
- More accurate - no ambiguous matches
- Simpler code - no hash computation needed
- Auto-loads credentials from .env file

WORKFLOW:
1. Load full voter file into voter_file
2. For each audience CSV:
   - Load into stg_audience (with ALL columns)
   - JOIN on StateVoterId → voter_audience_bridge
3. Update origin field with comma-separated audiences
4. Build summary tables by district
"""
import os
import sys
import time
import hashlib
import re
import csv
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pymysql
from pymysql.constants import CLIENT

# Load credentials from .env via shared utils.db module
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db import get_conn, DB_PASSWORD

if not DB_PASSWORD:
    raise ValueError("MYSQL_PASSWORD not set - check your .env file")

# =========================
# CONFIG
# =========================
DB_NAME = "nys_voter_tagging"


BASE_DIR       = Path(__file__).parent.parent  # D:\git\nys-voter-pipeline
DATA_DIR       = BASE_DIR / "data"
ZIPPED_DIR     = DATA_DIR / "zipped"
FULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
RUN_ID = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"run_pipeline_{RUN_ID}.log"

GROUP_CONCAT_MAX_LEN = int(os.getenv("GROUP_CONCAT_MAX_LEN", "500000"))

_BRIDGE_DDL = """
CREATE TABLE voter_audience_bridge (
  StateVoterId VARCHAR(50) NOT NULL,
  SDName VARCHAR(50) NOT NULL,
  LDName VARCHAR(50) NOT NULL,
  CDName VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  PRIMARY KEY (StateVoterId, audience),
  KEY idx_sd_aud (SDName, audience),
  KEY idx_ld_aud (LDName, audience),
  KEY idx_cd_aud (CDName, audience),
  KEY idx_aud (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=3, encoding='utf-8'),
    ],
)
logger = logging.getLogger("pipeline")

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def die(msg: str):
    logger.critical(msg)
    sys.exit(1)

# =========================
# DB HELPERS
# =========================
def connect_root():
    """Root connection (no default database) with LOCAL_FILES support."""
    return get_conn(database=None, autocommit=True, local_infile=True)

def connect_db():
    """Connection to the main nys_voter_tagging database with LOCAL_FILES support."""
    return get_conn(database=DB_NAME, autocommit=True, local_infile=True)

def exec_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount

def fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

_ident_ok = re.compile(r"^[A-Za-z0-9_]+$")

def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def sanitize_identifier(name: str) -> str:
    if _ident_ok.match(name):
        return name
    s = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    if not s:
        die(f"Bad column name: {name!r}")
    return s

def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)

def normalize_path_for_load(p: Path) -> str:
    return str(p.resolve()).replace("\\", "/")

# =========================
# CHANGE DETECTION
# =========================
def file_hash(path: Path) -> str:
    """Cheap metadata hash of a single file (size + mtime)."""
    stat = path.stat()
    return hashlib.md5(f"{path.name}:{stat.st_size}:{stat.st_mtime}".encode()).hexdigest()


def files_hash(paths: list[Path]) -> str:
    """Cheap metadata hash of multiple files combined — same pattern as CFB."""
    h = hashlib.md5()
    for p in sorted(paths):
        if p.exists():
            stat = p.stat()
            h.update(f"{p.name}:{stat.st_size}:{stat.st_mtime}".encode())
    return h.hexdigest()


def get_stored_hash(conn, load_type: str) -> str | None:
    """Check load_metadata for last successful load of the given type."""
    try:
        row = fetch_one(conn,
            "SELECT file_hash FROM load_metadata "
            "WHERE load_type=%s ORDER BY load_date DESC LIMIT 1",
            (load_type,))
        return row[0] if row else None
    except Exception:
        return None  # Table doesn't exist yet


def clear_hash(conn, load_type: str):
    """Remove hash so a failed load forces full rebuild on next run."""
    try:
        exec_sql(conn, "DELETE FROM load_metadata WHERE load_type=%s", (load_type,))
    except Exception:
        pass  # Table may not exist yet


def store_hash(conn, load_type: str, fhash: str, row_count: int | None = None):
    """Record successful load."""
    exec_sql(conn, """
        CREATE TABLE IF NOT EXISTS load_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            load_type   VARCHAR(50),
            file_hash   VARCHAR(32) NOT NULL,
            row_count   INT,
            load_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX(load_type, load_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    exec_sql(conn,
        "INSERT INTO load_metadata (load_type, file_hash, row_count) "
        "VALUES (%s, %s, %s)",
        (load_type, fhash, row_count))


# =========================
# ZIP FILE EXTRACTION
# =========================
def extract_zip_to_csv(zip_path: Path, output_folder: Path) -> tuple[str, bool]:
    """Extract CSV from zip and rename to match zip filename"""
    import zipfile
    
    zip_name = zip_path.name
    base_name = zip_path.stem  # Remove .zip extension
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of CSV files in zip
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            
            if len(csv_files) == 0:
                return f"No CSV in {zip_name}", False
            
            # Extract the first CSV
            csv_file = csv_files[0]
            zip_ref.extract(csv_file, output_folder)
            
            # Get extracted path and target path
            extracted_path = output_folder / csv_file
            target_path = output_folder / f"{base_name}.csv"
            
            # Remove existing target if exists
            if target_path.exists():
                target_path.unlink()
            
            # Rename to match zip name
            extracted_path.rename(target_path)
            
            # Clean up subfolder if CSV was in one
            if '/' in csv_file or '\\' in csv_file:
                folder_path = extracted_path.parent
                if folder_path != output_folder and folder_path.exists():
                    try:
                        folder_path.rmdir()
                    except OSError:
                        pass
            
            # Get file size
            file_size_mb = target_path.stat().st_size / (1024 * 1024)
            return f"{base_name}.csv ({file_size_mb:.1f} MB)", True
    
    except Exception as e:
        return f"Error extracting {zip_name}: {e}", False

def check_and_extract_zips():
    """Check if zip files are newer than CSVs and extract if needed"""
    if not ZIPPED_DIR.exists():
        logger.warning(f"Zipped folder not found: {ZIPPED_DIR}")
        return 0
    
    zip_files = sorted(ZIPPED_DIR.glob("*.zip"))
    if not zip_files:
        logger.info("No zip files found in ziped folder")
        return 0
    
    logger.info(f"Checking {len(zip_files)} zip files for updates...")
    
    extracted_count = 0
    skipped_count = 0
    
    for zip_path in zip_files:
        base_name = zip_path.stem
        csv_path = DATA_DIR / f"{base_name}.csv"
        
        # Check if extraction is needed
        needs_extract = False
        if not csv_path.exists():
            needs_extract = True
            reason = "CSV not found"
        elif zip_path.stat().st_mtime > csv_path.stat().st_mtime:
            needs_extract = True
            reason = "ZIP newer than CSV"
        
        if needs_extract:
            logger.info(f"  Extracting {zip_path.name} ({reason})...")
            result, success = extract_zip_to_csv(zip_path, DATA_DIR)
            if success:
                logger.info(f"    OK: {result}")
                extracted_count += 1
            else:
                logger.error(f"    FAIL: {result}")
        else:
            skipped_count += 1
    
    if extracted_count > 0:
        logger.info(f"Extracted {extracted_count} files, skipped {skipped_count} (already up-to-date)")
    else:
        logger.info(f"All {skipped_count} CSV files are up-to-date")
    
    return extracted_count

# =========================
# FILE HELPERS
# =========================
def list_audience_files() -> list[Path]:
    """Get all audience CSV files from data folder"""
    files = []
    for f in DATA_DIR.iterdir():
        if f.is_file() and f.suffix.lower() == ".csv" and f.name != "fullnyvoter.csv":
            # Skip DONORS TO CONS if it doesn't have StateVoterId
            files.append(f)
    return sorted(files)

# =========================
# DATABASE SETUP
# =========================
def ensure_database():
    root = connect_root()
    try:
        exec_sql(root, f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;")
    finally:
        root.close()

# VARCHAR sizes for voter file columns — sized to ~2x observed max values.
# Prevents TEXT columns which cause off-page InnoDB storage and block indexing.
_VARCHAR_SIZES = {
    # Address
    "PrimaryAddress1": 80, "PrimaryCity": 50, "PrimaryOddEvenCode": 5,
    "PrimaryHouseNumber": 20, "PrimaryHouseHalf": 5, "PrimaryStreetPre": 5,
    "PrimaryStreetName": 60, "PrimaryStreetType": 10, "PrimaryStreetPost": 5,
    "PrimaryUnit": 20, "PrimaryUnitNumber": 15,
    "SecondaryAddress1": 80, "SecondaryCity": 50, "SecondaryUnit": 20,
    "SecondaryUnitNumber": 20,
    # Phone
    "PrimaryPhone": 20, "PrimaryPhoneTRC": 5, "Landline": 20, "UserLandline": 20,
    "LandlineTRC": 5, "LandlineDNC": 10, "HasPrimaryPhone": 10,
    "Mobile": 20, "MobileTRC": 5, "UserMobile": 20, "MobileDNC": 10,
    # Demographics
    "AgeRange": 15, "Age": 5, "Gender": 5,
    "ObservedParty": 30, "OfficialParty": 30, "CalculatedParty": 30,
    "HouseholdParty": 30, "RegistrationStatus": 30,
    # Voting behavior
    "GeneralFrequency": 5, "PrimaryFrequency": 5, "MailSortCodeRoute": 10,
    "MailDeliveryPt": 5, "MailDeliveryPtChkDigit": 5, "MailLineOfTravel": 10,
    "MailLineOfTravelOrder": 5, "MailDPVStatus": 5, "NeighborhoodId": 5,
    "NeighborhoodSegmentId": 5, "OverAllFrequency": 5,
    "GeneralAbsenteeStatus": 20, "PrimaryAbsenteeStatus": 20, "AbsenteeStatus": 20,
    "GeneralRegularity": 10, "PrimaryRegularity": 10, "Moved": 10,
    # Geography
    "CountyName": 30, "CountyNumber": 5, "PrecinctNumber": 15,
    "PrecinctName": 60, "DMA": 50, "Turf": 10, "CensusBlock": 30,
    # IDs
    "VoterKey": 15, "HHRecId": 15, "HHMemberId": 5, "HHCode": 5,
    "JurisdictionalVoterId": 20, "ClientId": 20, "RNCRegId": 50, "MapCode": 5,
    # Ethnicity / origin
    "StateEthnicity": 30, "ModeledEthnicity": 50, "ObservedEthnicity": 30,
    "origin": 160,
}

def _coltype_for_fullvoter(col: str) -> str:
    """Map column names to MySQL types — never returns TEXT."""
    if col == "StateVoterId":
        return "VARCHAR(50) NOT NULL"
    if col in ("PrimaryZip", "SecondaryZip"):
        return "CHAR(5) NULL"
    if col in ("PrimaryZip4", "SecondaryZip4"):
        return "CHAR(4) NULL"
    if col in ("PrimaryState", "SecondaryState"):
        return "CHAR(2) NULL"
    if col == "DOB":
        return "DATE NULL"
    if col in ("RegistrationDate", "LastVoterActivity"):
        return "DATE NULL"
    if col in ("Latitude", "Longitude"):
        return "DECIMAL(9,6) NULL"
    if col in ("CDName", "LDName", "SDName"):
        return "VARCHAR(50) NOT NULL DEFAULT \'\'"  # Must be NOT NULL for partitioning
    if col in ("FirstName", "LastName", "MiddleName"):
        return "VARCHAR(100) NULL"
    if col == "SuffixName":
        return "VARCHAR(50) NULL"
    # Look up known column sizes — fall back to VARCHAR(255), never TEXT
    size = _VARCHAR_SIZES.get(col)
    if size:
        return f"VARCHAR({size}) NULL"
    return "VARCHAR(255) NULL"

def _load_expr(col: str, var: str) -> str:
    """Generate a LOAD DATA SET expression that coerces @var into the correct type for col."""
    if col == "StateVoterId":
        return f"TRIM({var})"
    if col in ("CDName", "LDName", "SDName"):
        return f"COALESCE(NULLIF(TRIM({var}), ''), '')"
    if col in ("PrimaryZip", "SecondaryZip"):
        return f"LEFT(REGEXP_REPLACE(COALESCE({var},''), '[^0-9]', ''), 5)"
    if col in ("PrimaryZip4", "SecondaryZip4"):
        return f"LEFT(REGEXP_REPLACE(COALESCE({var},''), '[^0-9]', ''), 4)"
    if col in ("PrimaryState", "SecondaryState"):
        return f"LEFT(UPPER(NULLIF(TRIM({var}), '')), 2)"
    if col == "DOB":
        return (f"COALESCE("
                f"STR_TO_DATE(TRIM({var}), '%%c/%%e/%%Y'), "
                f"STR_TO_DATE(TRIM({var}), '%%Y-%%m-%%d'), "
                f"STR_TO_DATE(TRIM({var}), '%%m-%%d-%%Y'), "
                f"STR_TO_DATE(TRIM({var}), '%%c/%%e/%%y'))")
    if col in ("RegistrationDate", "LastVoterActivity"):
        return (f"COALESCE("
                f"STR_TO_DATE(TRIM({var}), '%%c/%%e/%%Y'), "
                f"STR_TO_DATE(TRIM({var}), '%%Y-%%m-%%d'), "
                f"STR_TO_DATE(TRIM({var}), '%%m-%%d-%%Y'), "
                f"STR_TO_DATE(TRIM({var}), '%%c/%%e/%%y'))")
    return f"NULLIF(TRIM({var}), '')"


def rebuild_tables(conn, fullvoter_header: list[str]):
    """Create main tables"""
    logger.info("Dropping old tables...")
    for t in [
        "stg_voter_raw",          # legacy staging table (no longer created)
        "voter_file",
        "voter_audience_bridge",
        "counts_sd_audience",
        "counts_ld_audience",
        "counts_cd_audience",
        "counts_state_audience",
    ]:
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(t)};")

    # Create main voter_file table (secondary indexes deferred until after bulk load)
    logger.info("Creating voter_file...")
    col_defs = []
    sanitized = [sanitize_identifier(c) for c in fullvoter_header]
    for c in sanitized:
        col_defs.append(f"{qident(c)} {_coltype_for_fullvoter(c)}")
    col_defs.append("`origin` VARCHAR(160) NULL")

    create_sql = f"""
    CREATE TABLE voter_file (
      {', '.join(col_defs)},
      PRIMARY KEY (StateVoterId, CDName)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY KEY(CDName) PARTITIONS 32;
    """
    exec_sql(conn, create_sql)
    
    # Bridge table (voter-audience many-to-many)
    logger.info("Creating voter_audience_bridge...")
    exec_sql(conn, _BRIDGE_DDL)

def refresh_audiences_only(conn):
    """Keep voter_file intact, but drop + rebuild audience-related tables.

    Called when fullnyvoter.csv is unchanged — voter data stays,
    but audience CSVs may have changed so we re-match them.
    """
    logger.info("Refreshing audience tables (voter_file unchanged)...")

    for t in [
        "stg_voter_raw",
        "voter_audience_bridge",
        "counts_sd_audience",
        "counts_ld_audience",
        "counts_cd_audience",
        "counts_state_audience",
    ]:
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(t)};")

    # Recreate bridge table
    logger.info("  Recreating voter_audience_bridge...")
    exec_sql(conn, _BRIDGE_DDL)

    # Clear origin on voter_file (will be re-populated from bridge)
    logger.info("  Clearing origin column...")
    exec_sql(conn, "UPDATE voter_file SET origin = NULL WHERE origin IS NOT NULL;")


# =========================
# LOAD FUNCTIONS
# =========================
def load_voter_file(conn, src_path: Path, header: list[str]):
    """Load full voter CSV directly into voter_file (skip staging table).

    Uses LOAD DATA with typed SET expressions so every row is parsed,
    coerced, and inserted in a single pass — no intermediate staging table.
    """
    logger.info(f"Loading full voter from {src_path.name} directly into voter_file...")
    psql = normalize_path_for_load(src_path)

    sanitized = [sanitize_identifier(c) for c in header]
    if "StateVoterId" not in sanitized:
        die("fullnyvoter.csv missing StateVoterId column")

    vars_list = ", ".join([f"@v{i+1}" for i in range(len(sanitized))])

    sets = []
    for i, col in enumerate(sanitized):
        var = f"@v{i+1}"
        sets.append(f"{qident(col)} = {_load_expr(col, var)}")
    set_clause = ",\n  ".join(sets)

    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE voter_file
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES
({vars_list})
SET
  {set_clause};
"""

    with conn.cursor() as cur:
        cur.execute(sql)
        loaded = cur.rowcount if cur.rowcount is not None else 0
    logger.info(f"  Loaded {loaded:,} voters into voter_file")

    # Remove rows with blank StateVoterId (header glitches, blank lines, etc.)
    deleted = exec_sql(conn, "DELETE FROM voter_file WHERE StateVoterId = ''")
    if deleted > 0:
        logger.info(f"  Removed {deleted} rows with empty StateVoterId")
        loaded -= deleted

    if loaded == 0:
        die("Zero rows loaded from full voter file")

    exec_sql(conn, "ANALYZE TABLE voter_file;")
    return loaded

def add_voter_indexes(conn):
    """Add secondary indexes after bulk load (much faster than maintaining during INSERT)."""
    logger.info("Building secondary indexes on voter_file...")
    t0 = time.time()
    exec_sql(conn, """
        ALTER TABLE voter_file
            ADD KEY idx_cd (CDName),
            ADD KEY idx_ld (LDName),
            ADD KEY idx_sd (SDName)
    """)
    logger.info(f"  Indexes built in {time.time()-t0:.1f}s")


def load_audience_file(conn, src_path: Path, audience_name: str):
    """Load audience CSV, extract only StateVoterId, match against voter_file.

    Only the StateVoterId column is staged — the other 80+ columns from the
    audience CSV are parsed by LOAD DATA but never written to disk, cutting
    staging I/O by ~95%.
    """
    logger.info(f"  Loading audience: {audience_name}")
    psql = normalize_path_for_load(src_path)

    # Read CSV header and find StateVoterId position
    header = read_csv_header(src_path)
    if "StateVoterId" not in header:
        logger.error(f"    SKIP: No StateVoterId column in {audience_name}")
        return 0

    sv_idx = header.index("StateVoterId")

    # Minimal staging table — only StateVoterId
    exec_sql(conn, "DROP TABLE IF EXISTS stg_audience;")
    exec_sql(conn, """
        CREATE TABLE stg_audience (
            StateVoterId VARCHAR(50) NOT NULL,
            KEY idx_sv (StateVoterId)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    # LOAD DATA: parse all CSV columns into @vars, only store StateVoterId
    vars_list = ", ".join([f"@v{i+1}" for i in range(len(header))])
    sv_var = f"@v{sv_idx + 1}"

    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE stg_audience
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES
({vars_list})
SET StateVoterId = NULLIF(TRIM({sv_var}), '');
"""

    with conn.cursor() as cur:
        cur.execute(sql)
        loaded = cur.rowcount if cur.rowcount is not None else 0

    logger.info(f"    Loaded {loaded:,} records from {audience_name}")

    if loaded == 0:
        logger.warning(f"    WARNING: Zero rows loaded")
        return 0

    # Insert into bridge table (direct StateVoterId match)
    logger.info(f"    Matching on StateVoterId...")
    audience_escaped = conn.escape(audience_name).strip("'")

    exec_sql(conn, f"""
INSERT IGNORE INTO voter_audience_bridge (StateVoterId, SDName, LDName, CDName, audience)
SELECT
  f.StateVoterId,
  COALESCE(NULLIF(TRIM(f.SDName), ''), 'UNKNOWN'),
  COALESCE(NULLIF(TRIM(f.LDName), ''), 'UNKNOWN'),
  COALESCE(NULLIF(TRIM(f.CDName), ''), 'UNKNOWN'),
  '{audience_escaped}'
FROM voter_file f
INNER JOIN stg_audience a ON a.StateVoterId = f.StateVoterId;
""")

    matched = fetch_one(conn, "SELECT ROW_COUNT();")[0]
    logger.info(f"    Matched {matched:,} voters")

    return loaded

def compute_clean_names(conn):
    """Pre-compute cleaned name columns on voter_file for donor matching.

    Columns added (all idempotent — only fills NULLs):
      clean_last   – UPPER, alpha-only LastName
      clean_first  – UPPER, alpha-only FirstName
      clean_last_h1 – first half of hyphenated LastName (NULL if no hyphen)
      clean_last_h2 – second half of hyphenated LastName (NULL if no hyphen)

    Index: idx_clean_name (clean_last, clean_first, PrimaryZip)
    """
    logger.info("Pre-computing cleaned name columns on voter_file...")
    t0 = time.time()

    # Ensure columns exist
    for col, typedef in [
        ("clean_last",     "VARCHAR(100) DEFAULT NULL"),
        ("clean_first",    "VARCHAR(100) DEFAULT NULL"),
        ("clean_last_h1",  "VARCHAR(100) DEFAULT NULL"),
        ("clean_last_h2",  "VARCHAR(100) DEFAULT NULL"),
    ]:
        row = fetch_one(conn,
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='voter_file' AND COLUMN_NAME=%s",
            (DB_NAME, col))
        if row[0] == 0:
            exec_sql(conn, f"ALTER TABLE voter_file ADD COLUMN {col} {typedef}")

    # Ensure index
    row = fetch_one(conn,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='voter_file' AND INDEX_NAME='idx_clean_name'",
        (DB_NAME,))
    if row[0] == 0:
        logger.info("  Creating index idx_clean_name...")
        exec_sql(conn,
            "ALTER TABLE voter_file "
            "ADD INDEX idx_clean_name (clean_last(50), clean_first(50), PrimaryZip)")

    # Populate clean_last / clean_first in batches (avoid giant transaction)
    batch_size = 500000
    remaining = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE clean_last IS NULL")[0]
    logger.info(f"  {remaining:,} rows need clean_last/clean_first...")
    total_done = 0
    while remaining > 0:
        exec_sql(conn, """
            UPDATE voter_file SET
                clean_last  = REGEXP_REPLACE(UPPER(COALESCE(LastName,  '')), '[^A-Z]', ''),
                clean_first = REGEXP_REPLACE(UPPER(COALESCE(FirstName, '')), '[^A-Z]', '')
            WHERE clean_last IS NULL
            LIMIT %s
        """, (batch_size,))
        total_done += batch_size
        remaining = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE clean_last IS NULL")[0]
        logger.info(f"    batch done — {remaining:,} remaining ({time.time()-t0:.0f}s)")
    cleaned = fetch_one(conn,
        "SELECT COUNT(*) FROM voter_file WHERE clean_last IS NOT NULL")[0]
    logger.info(f"  clean_last/clean_first: {cleaned:,} voters ({time.time()-t0:.1f}s)")

    # Populate hyphen parts (only where LastName contains a hyphen)
    t1 = time.time()
    exec_sql(conn, """
        UPDATE voter_file SET
            clean_last_h1 = REGEXP_REPLACE(UPPER(SUBSTRING_INDEX(LastName, '-', 1)),  '[^A-Z]', ''),
            clean_last_h2 = REGEXP_REPLACE(UPPER(SUBSTRING_INDEX(LastName, '-', -1)), '[^A-Z]', '')
        WHERE LastName LIKE '%-%'
          AND clean_last_h1 IS NULL
    """)
    hyphen_count = fetch_one(conn,
        "SELECT COUNT(*) FROM voter_file WHERE clean_last_h1 IS NOT NULL")[0]
    logger.info(f"  clean_last_h1/h2: {hyphen_count:,} hyphenated voters ({time.time()-t1:.1f}s)")


def update_origin_field(conn):
    """Update origin field with comma-separated audience list"""
    logger.info("Updating origin field with matched audiences...")
    
    exec_sql(conn, f"SET SESSION group_concat_max_len = {GROUP_CONCAT_MAX_LEN};")
    
    exec_sql(conn, """
UPDATE voter_file f
INNER JOIN (
  SELECT
    StateVoterId,
    GROUP_CONCAT(DISTINCT audience ORDER BY audience SEPARATOR ',') AS origin
  FROM voter_audience_bridge
  GROUP BY StateVoterId
) x ON x.StateVoterId = f.StateVoterId
SET f.origin = x.origin;
""")
    
    updated = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL;")[0]
    total = fetch_one(conn, "SELECT COUNT(*) FROM voter_file;")[0]
    logger.info(f"  Updated {updated:,} of {total:,} voters with audience matches")
    
    # Show sample of multi-audience voters
    multi = fetch_all(conn, """
        SELECT StateVoterId, FirstName, LastName, origin
        FROM voter_file
        WHERE origin LIKE '%%,%%'
        LIMIT 5;
    """)
    if multi:
        logger.info("  Sample voters with multiple audiences:")
        for vid, fname, lname, orig in multi:
            aud_count = orig.count(',') + 1
            logger.info(f"    {vid} ({fname} {lname}): {aud_count} audiences")

def build_summary_tables(conn):
    """Build district-level summary tables"""
    logger.info("Building summary tables...")

    # (table_name, grouping_column or None for statewide)
    _summaries = [
        ("counts_sd_audience",    "SDName"),
        ("counts_ld_audience",    "LDName"),
        ("counts_cd_audience",    "CDName"),
        ("counts_state_audience", None),
    ]
    for table, col in _summaries:
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(table)};")
        if col:
            group_cols = f"{qident(col)}, audience"
            pk_cols = f"{qident(col)}, audience"
        else:
            group_cols = "audience"
            pk_cols = "audience"
        exec_sql(conn, f"""
            CREATE TABLE {qident(table)} AS
            SELECT {group_cols}, COUNT(*) AS voters
            FROM voter_audience_bridge
            GROUP BY {group_cols};
        """)
        exec_sql(conn, f"ALTER TABLE {qident(table)} ADD PRIMARY KEY ({pk_cols});")

    logger.info("  Summary tables created")

# =========================
# MAIN WORKFLOW
# =========================
def main():
    logger.info("=" * 80)
    logger.info("NYS VOTER TAGGING PIPELINE - StateVoterId Direct Matching")
    logger.info("=" * 80)
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Full voter file: {FULLVOTER_PATH}")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Zipped directory: {ZIPPED_DIR}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("")
    
    # Check and extract zip files if needed
    logger.info("Step 0: Checking for updated zip files...")
    extracted = check_and_extract_zips()
    if extracted > 0:
        logger.info(f"  Refreshed {extracted} CSV files from zip archives")
    logger.info("")
    
    # Verify files exist
    if not FULLVOTER_PATH.exists():
        die(f"Full voter file not found: {FULLVOTER_PATH}")
    if not DATA_DIR.exists():
        die(f"Data directory not found: {DATA_DIR}")
    
    # Get audience files
    audience_files = list_audience_files()
    if not audience_files:
        die(f"No audience CSV files found in {DATA_DIR}")
    
    logger.info(f"Found {len(audience_files)} audience files:")
    for f in audience_files:
        logger.info(f"  - {f.name}")
    logger.info("")
    
    # Setup database
    logger.info("Step 1: Database setup...")
    ensure_database()
    conn = connect_db()
    
    try:
        # Read full voter header
        fv_header = read_csv_header(FULLVOTER_PATH)
        logger.info(f"  Full voter has {len(fv_header)} columns")

        # ----- Change detection -----
        voter_hash     = file_hash(FULLVOTER_PATH)
        voter_hash_old = get_stored_hash(conn, 'voter_file')
        aud_hash       = files_hash(audience_files)
        aud_hash_old   = get_stored_hash(conn, 'audiences')

        voter_table_ok = False
        try:
            row = fetch_one(conn,
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='voter_file'",
                (DB_NAME,))
            if row and row[0] > 0:
                voter_count = fetch_one(conn, "SELECT COUNT(*) FROM voter_file")[0]
                voter_table_ok = voter_count > 0
        except Exception:
            pass

        voter_current = voter_table_ok and voter_hash_old == voter_hash
        aud_current   = aud_hash_old == aud_hash

        # ── Path A: everything up-to-date ─────────────────────────────────
        if voter_current and aud_current:
            logger.info(f"\nAll files up-to-date -- nothing to do")
            logger.info(f"  Voter hash:    {voter_hash}")
            logger.info(f"  Audience hash: {aud_hash}")
            logger.info(f"  Voters in table: {voter_count:,}")

        # ── Path B: voter unchanged, audiences changed ────────────────────
        elif voter_current and not aud_current:
            logger.info(f"\nvoter_file is up-to-date -- refreshing audiences only")
            logger.info(f"  Voters in table: {voter_count:,}")
            clear_hash(conn, 'audiences')
            refresh_audiences_only(conn)

            logger.info(f"\nStep 5: Loading and matching {len(audience_files)} audience files...")
            total_loaded = 0
            for i, aud_file in enumerate(audience_files, 1):
                logger.info(f"[{i}/{len(audience_files)}] {aud_file.name}")
                loaded = load_audience_file(conn, aud_file, aud_file.name)
                total_loaded += loaded
            logger.info(f"\n  Total audience records loaded: {total_loaded:,}")

            logger.info("\nStep 6: Updating origin field...")
            update_origin_field(conn)

            logger.info("\nStep 7: Building district summaries...")
            build_summary_tables(conn)

            store_hash(conn, 'audiences', aud_hash)

        # ── Path C: voter changed (full rebuild) ──────────────────────────
        else:
            if voter_hash_old and voter_hash_old != voter_hash:
                logger.info("\nfullnyvoter.csv has changed -- rebuilding voter_file")
            else:
                logger.info("\nFirst load -- building voter_file")

            # Clear old hashes — if load crashes, next run will retry
            clear_hash(conn, 'voter_file')
            clear_hash(conn, 'audiences')

            # Rebuild tables (no secondary indexes yet — deferred until after bulk load)
            logger.info("\nStep 2: Rebuilding tables...")
            rebuild_tables(conn, fv_header)

            # Disable checks for bulk load performance
            exec_sql(conn, "SET SESSION unique_checks=0, foreign_key_checks=0")

            # Load full voter directly (no staging table)
            logger.info("\nStep 3: Loading full voter file...")
            row_count = load_voter_file(conn, FULLVOTER_PATH, fv_header)

            # Re-enable checks and build secondary indexes
            exec_sql(conn, "SET SESSION unique_checks=1, foreign_key_checks=1")
            add_voter_indexes(conn)

            store_hash(conn, 'voter_file', voter_hash, row_count)
            logger.info(f"  Stored voter_file hash: {voter_hash}")

            # Load audiences
            logger.info(f"\nStep 5: Loading and matching {len(audience_files)} audience files...")
            total_loaded = 0
            for i, aud_file in enumerate(audience_files, 1):
                logger.info(f"[{i}/{len(audience_files)}] {aud_file.name}")
                loaded = load_audience_file(conn, aud_file, aud_file.name)
                total_loaded += loaded
            logger.info(f"\n  Total audience records loaded: {total_loaded:,}")

            logger.info("\nStep 6: Updating origin field...")
            update_origin_field(conn)

            logger.info("\nStep 7: Building district summaries...")
            build_summary_tables(conn)

            store_hash(conn, 'audiences', aud_hash)

        # Pre-compute cleaned name columns for donor matching (idempotent)
        compute_clean_names(conn)

        # ----- Final stats (always) -----
        bridge_rows = fetch_one(conn, "SELECT COUNT(*) FROM voter_audience_bridge;")[0]
        bridge_voters = fetch_one(conn, "SELECT COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge;")[0]
        logger.info(f"\n  Bridge table rows (voter-audience pairs): {bridge_rows:,}")
        logger.info(f"  Unique voters with matches: {bridge_voters:,}")

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)

        total_voters = fetch_one(conn, "SELECT COUNT(*) FROM voter_file;")[0]
        matched_voters = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL;")[0]
        pct = (matched_voters / total_voters * 100) if total_voters > 0 else 0

        logger.info(f"Total voters in database: {total_voters:,}")
        logger.info(f"Voters with audience matches: {matched_voters:,} ({pct:.1f}%)")
        logger.info(f"Voters without matches: {(total_voters - matched_voters):,}")
        logger.info(f"Total audience files processed: {len(audience_files)}")
        logger.info(f"Log file: {LOG_FILE}")
        logger.info("")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
