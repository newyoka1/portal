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
import shutil
import tempfile
import atexit
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import re
import csv
import contextlib

import pymysql
import pymysql.connections as pmc
from pymysql.constants import CLIENT

# Load environment variables from .env file
try:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded configuration from {env_path}")
except ImportError:
    print("python-dotenv not installed, using system environment variables only")
    print("Install with: pip install python-dotenv")

# =========================
# CONFIG
# =========================
DB_NAME = "NYS_VOTER_TAGGING"

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

BASE_DIR = Path(r"C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE")
DATA_DIR = BASE_DIR / "data"
ZIPPED_DIR = BASE_DIR / "ziped"
FULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
RUN_ID = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"run_pipeline_{RUN_ID}.log"

GROUP_CONCAT_MAX_LEN = int(os.getenv("GROUP_CONCAT_MAX_LEN", "500000"))
BULK_INSERT_BUFFER_SIZE = 512 * 1024 * 1024  # 512MB

# Progress logging
COPY_LOG_EVERY_MB = 256
LOCAL_INFILE_LOG_EVERY_MB = 256

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
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        charset="utf8mb4", autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.LOCAL_FILES, local_infile=1,
    )

def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=DB_NAME, charset="utf8mb4", autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.LOCAL_FILES, local_infile=1,
    )

def exec_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount

def fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()

def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
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
                    except:
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

def _coltype_for_fullvoter(col: str) -> str:
    """Map column names to MySQL types"""
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
        return "VARCHAR(50) NOT NULL DEFAULT ''"  # Must be NOT NULL for partitioning
    if col in ("FirstName", "LastName", "MiddleName"):
        return "VARCHAR(100) NULL"
    if col == "SuffixName":
        return "VARCHAR(50) NULL"
    return "TEXT NULL"

def rebuild_tables(conn, fullvoter_header: list[str]):
    """Create main tables"""
    logger.info("Dropping old tables...")
    for t in [
        "stg_voter_raw",
        "voter_file",
        "voter_audience_bridge",
        "counts_sd_audience",
        "counts_ld_audience",
        "counts_cd_audience",
        "counts_state_audience",
    ]:
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(t)};")
    
    # Create staging table for full voter
    logger.info("Creating stg_voter_raw...")
    cols = []
    for c in fullvoter_header:
        name = sanitize_identifier(c)
        cols.append(f"{qident(name)} TEXT NULL")
    
    create_sql = f"CREATE TABLE stg_voter_raw ({', '.join(cols)}, KEY idx_sv (StateVoterId(50))) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    exec_sql(conn, create_sql)
    
    # Create main voter_file table
    logger.info("Creating voter_file...")
    col_defs = []
    sanitized = [sanitize_identifier(c) for c in fullvoter_header]
    for c in sanitized:
        col_defs.append(f"{qident(c)} {_coltype_for_fullvoter(c)}")
    col_defs.append("`origin` TEXT NULL")
    
    # Partitioned by CDName (32 partitions)
    # NOTE: PRIMARY KEY must include partitioning column (CDName)
    create_sql = f"""
    CREATE TABLE voter_file (
      {', '.join(col_defs)},
      PRIMARY KEY (StateVoterId, CDName),
      KEY idx_cd (CDName),
      KEY idx_ld (LDName),
      KEY idx_sd (SDName)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    PARTITION BY KEY(CDName) PARTITIONS 32;
    """
    exec_sql(conn, create_sql)
    
    # Bridge table (voter-audience many-to-many)
    logger.info("Creating voter_audience_bridge...")
    exec_sql(conn, """
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# =========================
# LOAD FUNCTIONS
# =========================
def load_fullvoter(conn, src_path: Path, header: list[str]):
    """Load full voter file into staging"""
    logger.info(f"Loading full voter from {src_path.name}...")
    psql = normalize_path_for_load(src_path)
    
    vars_list = ", ".join([f"@v{i+1}" for i in range(len(header))])
    
    sets = []
    for i, col in enumerate(header):
        col_s = sanitize_identifier(col)
        sets.append(f"{qident(col_s)} = NULLIF(TRIM(@v{i+1}), '')")
    
    set_clause = ",\n  ".join(sets)
    
    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE stg_voter_raw
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
    logger.info(f"  Loaded {loaded:,} voters into staging")
    
    if loaded == 0:
        die("Zero rows loaded from full voter file")
    
    return loaded

def load_audience_file(conn, src_path: Path, audience_name: str):
    """Load audience CSV with ALL columns, match on StateVoterId"""
    logger.info(f"  Loading audience: {audience_name}")
    psql = normalize_path_for_load(src_path)
    
    # Read CSV header
    header = read_csv_header(src_path)
    
    # Verify StateVoterId exists
    if "StateVoterId" not in header:
        logger.error(f"    SKIP: No StateVoterId column in {audience_name}")
        return 0
    
    # Drop and recreate staging table with dynamic schema
    exec_sql(conn, "DROP TABLE IF EXISTS stg_audience;")
    
    col_defs = []
    for col in header:
        col_safe = sanitize_identifier(col)
        if col == "StateVoterId":
            col_defs.append(f"{qident(col_safe)} VARCHAR(50) NOT NULL")
        else:
            col_defs.append(f"{qident(col_safe)} TEXT NULL")
    
    col_defs.append("KEY idx_sv (StateVoterId)")
    create_sql = f"CREATE TABLE stg_audience ({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    exec_sql(conn, create_sql)
    
    # Load data
    vars_list = ", ".join([f"@v{i+1}" for i in range(len(header))])
    
    sets = []
    for i, col in enumerate(header):
        col_safe = sanitize_identifier(col)
        sets.append(f"{qident(col_safe)} = NULLIF(TRIM(@v{i+1}), '')")
    
    set_clause = ",\n      ".join(sets)
    
    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE stg_audience
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

# =========================
# TRANSFORM FUNCTIONS
# =========================
def copy_fullvoter_to_main(conn, header: list[str]):
    """Copy all voters from staging to main table"""
    logger.info("Copying voters from staging to voter_file...")
    
    sanitized = [sanitize_identifier(c) for c in header]
    if "StateVoterId" not in sanitized:
        die("fullnyvoter.csv missing StateVoterId column")
    
    dest_cols = [qident(c) for c in sanitized] + ["`origin`"]
    
    def _sel_expr(col: str) -> str:
        if col == "StateVoterId":
            return "TRIM(StateVoterId)"
        if col in ("CDName", "LDName", "SDName"):
            return f"COALESCE(NULLIF(TRIM({qident(col)}), ''), '')"  # Must not be NULL
        if col in ("PrimaryZip", "SecondaryZip"):
            return f"LEFT(REGEXP_REPLACE(COALESCE({qident(col)},''), '[^0-9]', ''), 5)"
        if col in ("PrimaryZip4", "SecondaryZip4"):
            return f"LEFT(REGEXP_REPLACE(COALESCE({qident(col)},''), '[^0-9]', ''), 4)"
        if col in ("PrimaryState", "SecondaryState"):
            return f"LEFT(UPPER(NULLIF(TRIM({qident(col)}), '')), 2)"
        if col == "DOB":
            return ("COALESCE("
                    "STR_TO_DATE(TRIM(DOB), '%%c/%%e/%%Y'), "
                    "STR_TO_DATE(TRIM(DOB), '%%Y-%%m-%%d'), "
                    "STR_TO_DATE(TRIM(DOB), '%%m-%%d-%%Y'), "
                    "STR_TO_DATE(TRIM(DOB), '%%c/%%e/%%y'))")
        if col in ("RegistrationDate", "LastVoterActivity"):
            return (f"COALESCE("
                    f"STR_TO_DATE(TRIM({qident(col)}), '%%c/%%e/%%Y'), "
                    f"STR_TO_DATE(TRIM({qident(col)}), '%%Y-%%m-%%d'), "
                    f"STR_TO_DATE(TRIM({qident(col)}), '%%m-%%d-%%Y'), "
                    f"STR_TO_DATE(TRIM({qident(col)}), '%%c/%%e/%%y'))")
        return f"NULLIF(TRIM({qident(col)}), '')"
    
    sel_cols = [_sel_expr(c) for c in sanitized]
    
    insert_sql = f"""
INSERT INTO voter_file ({", ".join(dest_cols)})
SELECT {", ".join(sel_cols)}, NULL AS origin
FROM stg_voter_raw
WHERE TRIM(StateVoterId) <> '';
"""
    exec_sql(conn, insert_sql)
    
    fv = fetch_one(conn, "SELECT COUNT(*) FROM voter_file;")[0]
    logger.info(f"  Copied {fv:,} voters to voter_file")
    
    exec_sql(conn, "ANALYZE TABLE voter_file;")

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
    
    exec_sql(conn, "DROP TABLE IF EXISTS counts_sd_audience;")
    exec_sql(conn, """
CREATE TABLE counts_sd_audience AS
SELECT SDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY SDName, audience;
""")
    exec_sql(conn, "ALTER TABLE counts_sd_audience ADD PRIMARY KEY (SDName, audience);")
    
    exec_sql(conn, "DROP TABLE IF EXISTS counts_ld_audience;")
    exec_sql(conn, """
CREATE TABLE counts_ld_audience AS
SELECT LDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY LDName, audience;
""")
    exec_sql(conn, "ALTER TABLE counts_ld_audience ADD PRIMARY KEY (LDName, audience);")
    
    exec_sql(conn, "DROP TABLE IF EXISTS counts_cd_audience;")
    exec_sql(conn, """
CREATE TABLE counts_cd_audience AS
SELECT CDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY CDName, audience;
""")
    exec_sql(conn, "ALTER TABLE counts_cd_audience ADD PRIMARY KEY (CDName, audience);")
    
    exec_sql(conn, "DROP TABLE IF EXISTS counts_state_audience;")
    exec_sql(conn, """
CREATE TABLE counts_state_audience AS
SELECT audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY audience;
""")
    exec_sql(conn, "ALTER TABLE counts_state_audience ADD PRIMARY KEY (audience);")
    
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
        
        # Rebuild tables
        logger.info("\nStep 2: Rebuilding tables...")
        rebuild_tables(conn, fv_header)
        
        # Load full voter
        logger.info("\nStep 3: Loading full voter file...")
        load_fullvoter(conn, FULLVOTER_PATH, fv_header)
        
        # Copy to main table
        logger.info("\nStep 4: Copying voters to main table...")
        copy_fullvoter_to_main(conn, fv_header)
        
        # Load each audience file and match
        logger.info(f"\nStep 5: Loading and matching {len(audience_files)} audience files...")
        total_loaded = 0
        for i, aud_file in enumerate(audience_files, 1):
            logger.info(f"[{i}/{len(audience_files)}] {aud_file.name}")
            loaded = load_audience_file(conn, aud_file, aud_file.name)
            total_loaded += loaded
        
        logger.info(f"\n  Total audience records loaded: {total_loaded:,}")
        
        # Check bridge table
        bridge_rows = fetch_one(conn, "SELECT COUNT(*) FROM voter_audience_bridge;")[0]
        bridge_voters = fetch_one(conn, "SELECT COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge;")[0]
        logger.info(f"  Bridge table rows (voter-audience pairs): {bridge_rows:,}")
        logger.info(f"  Unique voters with matches: {bridge_voters:,}")
        
        # Update origin field
        logger.info("\nStep 6: Updating origin field...")
        update_origin_field(conn)
        
        # Build summaries
        logger.info("\nStep 7: Building district summaries...")
        build_summary_tables(conn)
        
        # Final stats
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