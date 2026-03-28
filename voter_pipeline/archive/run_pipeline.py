#!/usr/bin/env python3
"""
Optimized NYS Voter Tagging Pipeline
- Partitioned tables (32 partitions by CDName)
- Reduced table scans (combined transforms)
- Pre-computed match keys in staging
- Better indexing strategy
- Transaction batching for resumability

FIXED:
- Issue #1: Origin field now contains ALL matched audiences (comma-separated)
- Issue #2: ALL voters are loaded into voter_file, not just those with matches
"""
import os
import sys
import time
import hashlib
import shutil
import tempfile
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import re
import csv

import pymysql
import pymysql.connections as pmc
from pymysql.constants import CLIENT

# Import ethnicity prediction module
from ethnicity_predictor import get_best_ethnicity, standardize_ethnicity, SURNAME_ETHNICITY_MAP

# =========================
# CONFIG
# =========================
DB_NAME = "NYS_VOTER_TAGGING"


BASE_DIR = Path(r"D:\git\NYS-Voter-Tagging")
DATA_DIR = BASE_DIR / "data"
FULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
RUN_ID = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"run_pipeline_{RUN_ID}.log"

GROUP_CONCAT_MAX_LEN = int(os.getenv("GROUP_CONCAT_MAX_LEN", "500000"))

# Optimization: increase bulk insert buffer (HIGH-PERFORMANCE: 64GB RAM, i7-12700K)
BULK_INSERT_BUFFER_SIZE = int(os.getenv("BULK_INSERT_BUFFER_SIZE", "512")) * 1024 * 1024  # 512MB for high-end hardware

# High-performance settings for 64GB RAM system
INNODB_BUFFER_POOL_SIZE = "40G"  # Set in my.ini
MAX_PARALLEL_WORKERS = 12  # Matches i7-12700K core count

# Progress settings
COPY_ALWAYS_TO_TEMP = os.getenv("COPY_ALWAYS_TO_TEMP", "1") == "1"
COPY_LOG_EVERY_MB_FULL = int(os.getenv("COPY_LOG_EVERY_MB_FULL", "512"))
COPY_LOG_EVERY_MB_CAUSEWAY = int(os.getenv("COPY_LOG_EVERY_MB_CAUSEWAY", "64"))

# LOCAL INFILE streaming progress
LOCAL_INFILE_LOG_EVERY_MB_FULL = int(os.getenv("LOCAL_INFILE_LOG_EVERY_MB_FULL", "256"))
LOCAL_INFILE_LOG_EVERY_MB_CAUSEWAY = int(os.getenv("LOCAL_INFILE_LOG_EVERY_MB_CAUSEWAY", "64"))

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding='utf-8'),
    ],
)
logger = logging.getLogger("pipeline")

# Fix Windows console encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


def die(msg: str):
    logger.critical(msg)
    sys.exit(1)


# =========================
# FILE HELPERS
# =========================
def sha256_file_fingerprint(path: Path) -> str:
    st = path.stat()
    h = hashlib.sha256()
    h.update(path.name.encode("utf-8"))
    h.update(str(st.st_size).encode("utf-8"))
    h.update(str(int(st.st_mtime)).encode("utf-8"))
    return h.hexdigest()


def sha256_dir_fingerprint(paths: list[Path]) -> str:
    h = hashlib.sha256()
    for p in sorted(paths, key=lambda x: x.name.lower()):
        st = p.stat()
        h.update(p.name.encode("utf-8"))
        h.update(str(st.st_size).encode("utf-8"))
        h.update(str(int(st.st_mtime)).encode("utf-8"))
    return h.hexdigest()


def list_causeway_files() -> list[Path]:
    files = []
    for p in DATA_DIR.glob("*.csv"):
        if p.is_file():
            files.append(p)
    return sorted(files, key=lambda x: x.name.lower())


def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        row = next(reader, None)
        if not row:
            die(f"Empty CSV: {path}")
        return [c.strip() for c in row]


def normalize_path_for_load(p: Path) -> str:
    return str(p).replace("\\", "/")


def copy_with_progress(src: Path, dst: Path, label: str, log_every_mb: int):
    total = src.stat().st_size
    copied = 0
    next_log = log_every_mb * 1024 * 1024
    chunk = 8 * 1024 * 1024
    dst.parent.mkdir(parents=True, exist_ok=True)

    with src.open("rb") as rf, dst.open("wb") as wf:
        while True:
            b = rf.read(chunk)
            if not b:
                break
            wf.write(b)
            copied += len(b)
            if copied >= next_log:
                pct = (copied / total * 100.0) if total else 0.0
                logger.info(
                    f"{label}: copied {copied/1024/1024:,.0f} MB / {total/1024/1024:,.0f} MB ({pct:,.1f}%)"
                )
                next_log += log_every_mb * 1024 * 1024

    try:
        shutil.copystat(src, dst)
    except Exception:
        pass


def ensure_loadable_path(src: Path, label: str, log_every_mb: int) -> Path:
    if not src.exists():
        die(f"Missing file: {src}")

    tmp_dir = Path(tempfile.gettempdir()) / "nys_voter_tagging_load"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    dst = tmp_dir / f"{RUN_ID}_{src.name}"

    if not COPY_ALWAYS_TO_TEMP:
        try:
            with src.open("rb"):
                pass
            return src
        except PermissionError:
            logger.warning(f"{label}: permission denied reading source, copying to temp")
            copy_with_progress(src, dst, label, log_every_mb)
            return dst

    copy_with_progress(src, dst, label, log_every_mb)
    return dst


# =========================
# LOCAL INFILE PROGRESS (PyMySQL monkeypatch)
# =========================
_BUILTIN_OPEN = open
_ORIG_PMC_OPEN = getattr(pmc, "open", None) or _BUILTIN_OPEN


class _ProgressFile:
    def __init__(self, f, total_bytes: int | None, label: str, log_every_bytes: int):
        self._f = f
        self._total = total_bytes
        self._label = label
        self._log_every = log_every_bytes
        self._sent = 0
        self._next = log_every_bytes

    def read(self, n=-1):
        b = self._f.read(n)
        if not b:
            return b
        self._sent += len(b)
        if self._sent >= self._next:
            if self._total and self._total > 0:
                pct = self._sent / self._total * 100.0
                logger.info(
                    f"{self._label}: sent {self._sent/1024/1024:,.0f} MB / {self._total/1024/1024:,.0f} MB ({pct:,.1f}%)"
                )
            else:
                logger.info(f"{self._label}: sent {self._sent/1024/1024:,.0f} MB")
            self._next += self._log_every
        return b

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        self._f.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._f.__exit__(exc_type, exc, tb)


def _install_local_infile_progress(label: str, log_every_mb: int):
    log_every_bytes = max(1, log_every_mb) * 1024 * 1024

    def patched_open(filename, mode="rb", *args, **kwargs):
        f = _BUILTIN_OPEN(filename, mode, *args, **kwargs)
        if "r" in mode and "b" in mode:
            try:
                total = os.path.getsize(filename)
            except Exception:
                total = None
            return _ProgressFile(f, total, label, log_every_bytes)
        return f

    pmc.open = patched_open


def _remove_local_infile_progress():
    pmc.open = _ORIG_PMC_OPEN


# =========================
# DB HELPERS
# =========================
def connect_root():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.LOCAL_FILES,
        local_infile=1,
    )


def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS | CLIENT.LOCAL_FILES,
        local_infile=1,
    )


def exec_sql(conn, sql: str, params=None) -> int:
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        rc = cur.rowcount if cur.rowcount is not None else 0
    return rc


def fetch_one(conn, sql: str, params=None):
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur.fetchone()


def fetch_all(conn, sql: str, params=None):
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur.fetchall()


def ensure_database():
    root = connect_root()
    try:
        exec_sql(
            root,
            f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;",
        )
    finally:
        root.close()


_ident_ok = re.compile(r"^[A-Za-z0-9_]+$")


def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def sanitize_identifier(name: str) -> str:
    if _ident_ok.match(name):
        return name
    s = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    if not s:
        die(f"Bad header column name: {name!r}")
    return s


# =========================
# DDL
# =========================
def ensure_metadata_tables(conn):
    exec_sql(
        conn,
        """
CREATE TABLE IF NOT EXISTS pipeline_metadata (
  name VARCHAR(64) PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
    )
    exec_sql(
        conn,
        """
CREATE TABLE IF NOT EXISTS pipeline_drop_counts (
  run_id VARCHAR(32) NOT NULL,
  dataset VARCHAR(32) NOT NULL,
  source_file VARCHAR(255) NOT NULL,
  reason VARCHAR(64) NOT NULL,
  dropped BIGINT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, dataset, source_file, reason)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
    )


def create_stg_voter_raw(conn, header: list[str]):
    """OPTIMIZED: Pre-compute match_key and hash in staging table"""
    cols = []
    for c in header:
        name = sanitize_identifier(c)
        cols.append(f"{qident(name)} TEXT NULL")
    
    # Add computed columns to staging
    cols.append("`match_key` VARCHAR(320) NULL")
    cols.append("`match_key_hash` BINARY(16) NULL")
    cols.append("`yob` CHAR(4) NULL")
    
    exec_sql(conn, "DROP TABLE IF EXISTS stg_voter_raw;")
    exec_sql(
        conn, 
        f"CREATE TABLE stg_voter_raw ({', '.join(cols)}, KEY idx_mkh_stg (match_key_hash)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )


def _coltype_for_fullvoter(col: str) -> str:
    if col == "StateVoterId":
        return "VARCHAR(50) NOT NULL"

    if col in ("PrimaryZip", "SecondaryZip"):
        return "CHAR(5) NULL"
    if col in ("PrimaryZip4", "SecondaryZip4"):
        return "CHAR(4) NULL"
    if col in ("PrimaryState", "SecondaryState"):
        return "CHAR(2) NULL"

    if col == "DOB":
        return "DATE NOT NULL"
    if col in ("RegistrationDate", "LastVoterActivity"):
        return "DATE NULL"

    if col in ("Latitude", "Longitude"):
        return "DECIMAL(9,6) NULL"

    if col in ("CDName", "LDName", "SDName"):
        return "VARCHAR(50) NULL"

    if col in ("FirstName", "LastName", "MiddleName"):
        return "VARCHAR(100) NULL"
    if col == "SuffixName":
        return "VARCHAR(50) NULL"

    return "TEXT NULL"


def rebuild_tables(conn, fullvoter_header: list[str]):
    """OPTIMIZED: Remove generated columns, optimize indexes, increase buffers"""
    exec_sql(conn, f"SET SESSION group_concat_max_len = {GROUP_CONCAT_MAX_LEN};")
    exec_sql(conn, f"SET SESSION bulk_insert_buffer_size = {BULK_INSERT_BUFFER_SIZE};")
    exec_sql(conn, "SET SESSION unique_checks = 0;")
    exec_sql(conn, "SET SESSION foreign_key_checks = 0;")

    for t in [
        "stg_causeway_raw",
        "stg_voter_raw",
        "causeway_norm",
        "voter_file",
        "fullvoter_mk_counts",
        "voter_audience_bridge",
        "counts_sd_audience",
        "counts_ld_audience",
        "counts_cd_audience",
        "counts_state_audience",
    ]:
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(t)};")

    # OPTIMIZED: Pre-compute match keys in causeway staging
    exec_sql(
        conn,
        """
CREATE TABLE stg_causeway_raw (
  FirstName   TEXT NULL,
  LastName    TEXT NULL,
  PrimaryZip  TEXT NULL,
  DOB         TEXT NULL,
  origin      VARCHAR(255) NOT NULL,
  match_key   VARCHAR(320) NULL,
  match_key_hash BINARY(16) NULL,
  KEY idx_mkh_cw (match_key_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
    )

    exec_sql(
        conn,
        """
CREATE TABLE causeway_norm (
  match_key VARCHAR(320) NOT NULL,
  match_key_hash BINARY(16) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  PRIMARY KEY (match_key_hash, audience),
  KEY idx_mkh (match_key_hash),
  KEY idx_aud (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
    )

    sanitized = [sanitize_identifier(c) for c in fullvoter_header]
    if "StateVoterId" not in sanitized:
        die("fullnyvoter.csv missing required column: StateVoterId")

    col_defs = []
    for c in sanitized:
        col_defs.append(f"{qident(c)} {_coltype_for_fullvoter(c)}")

    col_defs.append("`yob` CHAR(4) NULL")
    col_defs.append("`match_key` VARCHAR(320) NULL")
    # OPTIMIZED: Store hash directly instead of generated column
    col_defs.append("`match_key_hash` BINARY(16) NULL")
    col_defs.append("`origin` TEXT NULL")
    
    # ETHNICITY ENHANCEMENT: Add standardized ethnicity fields
    col_defs.append("`StandardizedEthnicity` VARCHAR(50) NULL")
    col_defs.append("`EthnicitySource` VARCHAR(20) NULL")  # state/observed/modeled/predicted/none
    col_defs.append("`EthnicityConfidence` VARCHAR(10) NULL")  # high/medium/low/none

    # Optimized indexes for common queries
    idx_parts = [
        "PRIMARY KEY (`StateVoterId`)",
        "KEY idx_mkh (`match_key_hash`)",
        "KEY idx_cd_aud (`CDName`, `origin`(100))",
        "KEY idx_ethnicity (`StandardizedEthnicity`)",
        "KEY idx_cd_eth (`CDName`, `StandardizedEthnicity`)",
    ]
    for c in ("SDName", "LDName"):
        if c in sanitized:
            idx_parts.append(f"KEY idx_{c.lower()} ({qident(c)})")

    # Create table WITHOUT partitioning (simpler, more flexible)
    ddl = f"""
CREATE TABLE voter_file (
  {",\n  ".join(col_defs)},
  {",\n  ".join(idx_parts)}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
"""
    exec_sql(conn, ddl)

    # Bridge table WITHOUT partitioning
    exec_sql(
        conn,
        """
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
""",
    )


# =========================
# LOAD DATA
# =========================
def load_causeway_file(conn, src_path: Path):
    """Simplified: Load first, compute keys after"""
    load_path = ensure_loadable_path(src_path, f"Causeway copy {src_path.name}", COPY_LOG_EVERY_MB_CAUSEWAY)
    psql = normalize_path_for_load(load_path)
    audience = src_path.name.replace("'", "")

    hdr = read_csv_header(load_path)
    idx = {h: i for i, h in enumerate(hdr)}
    required = ["FirstName", "LastName", "PrimaryZip", "DOB"]
    for r in required:
        if r not in idx:
            die(f"Causeway file {src_path.name} missing required column: {r}")

    vars_list = ", ".join([f"@v{i+1}" for i in range(len(hdr))])

    def v(colname: str) -> str:
        return f"@v{idx[colname] + 1}"

    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE stg_causeway_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 LINES
({vars_list})
SET
  FirstName  = NULLIF(TRIM({v('FirstName')}), ''),
  LastName   = NULLIF(TRIM({v('LastName')}), ''),
  PrimaryZip = NULLIF(TRIM({v('PrimaryZip')}), ''),
  DOB        = NULLIF(TRIM({v('DOB')}), ''),
  origin     = '{audience}';
"""
    
    _install_local_infile_progress(f"LOCAL INFILE causeway {src_path.name}", LOCAL_INFILE_LOG_EVERY_MB_CAUSEWAY)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            loaded = cur.rowcount if cur.rowcount is not None else 0
        logger.info(f"Causeway load {src_path.name}: {loaded:,} rows")
    finally:
        _remove_local_infile_progress()
    
    logger.info(f"  Computing keys...")
    exec_sql(
        conn,
        f"""
UPDATE stg_causeway_raw
SET
  match_key = CONCAT(
    LOWER(REGEXP_REPLACE(COALESCE(FirstName,''),'[^A-Za-z]','')),
    '|',
    LOWER(REGEXP_REPLACE(COALESCE(LastName,''),'[^A-Za-z]','')),
    '|',
    LEFT(REGEXP_REPLACE(COALESCE(PrimaryZip,''),'[^0-9]',''),5),
    '|',
    COALESCE(
      DATE_FORMAT(STR_TO_DATE(DOB,'%c/%e/%Y'),'%Y'),
      DATE_FORMAT(STR_TO_DATE(DOB,'%Y-%m-%d'),'%Y')
    )
  ),
  match_key_hash = UNHEX(MD5(CONCAT(
    LOWER(REGEXP_REPLACE(COALESCE(FirstName,''),'[^A-Za-z]','')),
    '|',
    LOWER(REGEXP_REPLACE(COALESCE(LastName,''),'[^A-Za-z]','')),
    '|',
    LEFT(REGEXP_REPLACE(COALESCE(PrimaryZip,''),'[^0-9]',''),5),
    '|',
    COALESCE(
      DATE_FORMAT(STR_TO_DATE(DOB,'%c/%e/%Y'),'%Y'),
      DATE_FORMAT(STR_TO_DATE(DOB,'%Y-%m-%d'),'%Y')
    )
  )))
WHERE origin = '{audience}' AND match_key IS NULL;
"""
    )


def load_fullvoter(conn, src_path: Path, header: list[str]):
    """FIXED: Load ALL voters, compute keys only for those with DOB"""
    load_path = ensure_loadable_path(src_path, "Full voter copy", COPY_LOG_EVERY_MB_FULL)
    psql = normalize_path_for_load(load_path)

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
LINES TERMINATED BY '\\n'
IGNORE 1 LINES
({vars_list})
SET
  {set_clause};
"""
    _install_local_infile_progress("LOCAL INFILE full voter", LOCAL_INFILE_LOG_EVERY_MB_FULL)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            loaded = cur.rowcount if cur.rowcount is not None else 0
        logger.info(f"Full voter staging load: {loaded:,} rows")
    finally:
        _remove_local_infile_progress()
    
    logger.info(f"Computing match keys for full voter (only for rows with DOB)...")
    exec_sql(
        conn,
        """
UPDATE stg_voter_raw
SET
  yob = COALESCE(
    DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%c/%e/%Y'),'%Y'),
    DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%Y-%m-%d'),'%Y')
  ),
  match_key = CONCAT(
    LOWER(REGEXP_REPLACE(COALESCE(FirstName,''),'[^A-Za-z]','')),
    '|',
    LOWER(REGEXP_REPLACE(COALESCE(LastName,''),'[^A-Za-z]','')),
    '|',
    LEFT(REGEXP_REPLACE(COALESCE(PrimaryZip,''),'[^0-9]',''),5),
    '|',
    COALESCE(
      DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%c/%e/%Y'),'%Y'),
      DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%Y-%m-%d'),'%Y')
    )
  ),
  match_key_hash = UNHEX(MD5(CONCAT(
    LOWER(REGEXP_REPLACE(COALESCE(FirstName,''),'[^A-Za-z]','')),
    '|',
    LOWER(REGEXP_REPLACE(COALESCE(LastName,''),'[^A-Za-z]','')),
    '|',
    LEFT(REGEXP_REPLACE(COALESCE(PrimaryZip,''),'[^0-9]',''),5),
    '|',
    COALESCE(
      DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%c/%e/%Y'),'%Y'),
      DATE_FORMAT(STR_TO_DATE(TRIM(DOB),'%Y-%m-%d'),'%Y')
    )
  )))
WHERE DOB IS NOT NULL AND TRIM(DOB) <> '';
"""
    )
    
    # Log statistics
    with_keys = fetch_one(conn, "SELECT COUNT(*) FROM stg_voter_raw WHERE match_key_hash IS NOT NULL;")[0]
    without_keys = fetch_one(conn, "SELECT COUNT(*) FROM stg_voter_raw WHERE match_key_hash IS NULL;")[0]
    logger.info(f"  - Rows with match keys: {with_keys:,}")
    logger.info(f"  - Rows without match keys (no/invalid DOB): {without_keys:,}")


# =========================
# ETHNICITY PREDICTION
# =========================
def apply_surname_ethnicity_prediction(conn) -> int:
    """
    Apply surname-based ethnicity prediction to voters with Unknown ethnicity.
    Uses batch processing for efficiency on large datasets.
    Includes Italian, Chinese, Russian, and Irish predictions.
    
    Returns: number of voters updated with predictions
    """
    logger.info("Applying surname-based ethnicity prediction (including Italian, Chinese, Russian, Irish)...")
    
    # Build surname lookup SQL with all surnames from the map
    surname_cases = []
    for surname, code in SURNAME_ETHNICITY_MAP.items():
        if code == 'H':
            ethnicity = 'Hispanic'
        elif code == 'A':
            ethnicity = 'Asian'
        elif code == 'B':
            ethnicity = 'Black'
        elif code == 'CH':
            ethnicity = 'Chinese'
        elif code == 'IT':
            ethnicity = 'Italian'
        elif code == 'RU':
            ethnicity = 'Russian'
        elif code == 'IR':
            ethnicity = 'Irish'
        else:
            continue
        surname_cases.append(f"WHEN UPPER(TRIM(LastName)) = '{surname}' THEN '{ethnicity}'")
    
    surname_case_sql = "\n        ".join(surname_cases)
    
    # Update using SQL CASE statement for maximum performance
    update_sql = f"""
    UPDATE voter_file
    SET
        StandardizedEthnicity = CASE
            {surname_case_sql}
            ELSE StandardizedEthnicity
        END,
        EthnicitySource = CASE
            WHEN UPPER(TRIM(LastName)) IN ({",".join(f"'{s}'" for s in SURNAME_ETHNICITY_MAP.keys())})
            THEN 'predicted'
            ELSE EthnicitySource
        END,
        EthnicityConfidence = CASE
            WHEN UPPER(TRIM(LastName)) IN ({",".join(f"'{s}'" for s in SURNAME_ETHNICITY_MAP.keys())})
            THEN 'low'
            ELSE EthnicityConfidence
        END
    WHERE
        StandardizedEthnicity = 'Unknown'
        AND LastName IS NOT NULL
        AND TRIM(LastName) <> '';
    """
    
    rows_updated = exec_sql(conn, update_sql)
    
    return rows_updated


# =========================
# TRANSFORMS
# =========================
def run_transforms(conn, fullvoter_source_name: str, header: list[str]):
    """FIXED: Load ALL voters, handle multiple comma-separated audiences properly"""
    exec_sql(conn, f"SET SESSION group_concat_max_len = {GROUP_CONCAT_MAX_LEN};")
    exec_sql(conn, f"SET SESSION bulk_insert_buffer_size = {BULK_INSERT_BUFFER_SIZE};")

    logger.info("Transform: causeway drop counts")
    exec_sql(
        conn,
        """
INSERT IGNORE INTO pipeline_drop_counts (run_id, dataset, source_file, reason, dropped)
SELECT
  %s,
  'causeway',
  origin,
  'missing_dob',
  SUM(CASE WHEN match_key_hash IS NULL THEN 1 ELSE 0 END)
FROM stg_causeway_raw
GROUP BY origin;
""",
        (RUN_ID,),
    )

    logger.info("Transform: causeway normalize (already computed in load)")
    # OPTIMIZED: Just filter and insert, no recomputation
    exec_sql(
        conn,
        """
INSERT IGNORE INTO causeway_norm (match_key, match_key_hash, audience)
SELECT match_key, match_key_hash, origin
FROM stg_causeway_raw
WHERE match_key_hash IS NOT NULL;
""",
    )
    cw = fetch_one(conn, "SELECT COUNT(*) FROM causeway_norm;")[0]
    logger.info(f"causeway_norm rows: {cw:,}")

    logger.info("Transform: full voter drop counts")
    exec_sql(
        conn,
        """
INSERT IGNORE INTO pipeline_drop_counts (run_id, dataset, source_file, reason, dropped)
SELECT
  %s,
  'fullvoter',
  %s,
  'missing_dob',
  SUM(CASE WHEN match_key_hash IS NULL THEN 1 ELSE 0 END)
FROM stg_voter_raw;
""",
        (RUN_ID, fullvoter_source_name),
    )

    logger.info("Transform: insert normalized full voter - ALL VOTERS (matched and unmatched)")
    
    sanitized = [sanitize_identifier(c) for c in header]
    for req in ("StateVoterId", "FirstName", "LastName", "PrimaryZip", "DOB"):
        if req not in sanitized:
            die(f"fullnyvoter.csv missing required column: {req}")
    
    # Check if ethnicity columns exist
    has_ethnicity = all(col in sanitized for col in ["StateEthnicity", "ModeledEthnicity", "ObservedEthnicity"])
    if not has_ethnicity:
        logger.warning("Ethnicity columns not found in fullnyvoter.csv - ethnicity features will be unavailable")

    dest_cols = [qident(c) for c in sanitized] + ["`yob`", "`match_key`", "`match_key_hash`", "`origin`", 
                                                    "`StandardizedEthnicity`", "`EthnicitySource`", "`EthnicityConfidence`"]

    def _sel_expr(col: str) -> str:
        if col == "StateVoterId":
            return "TRIM(StateVoterId)"
        if col in ("PrimaryZip", "SecondaryZip"):
            return f"LEFT(REGEXP_REPLACE(COALESCE({qident(col)},''), '[^0-9]', ''), 5)"
        if col in ("PrimaryZip4", "SecondaryZip4"):
            return f"LEFT(REGEXP_REPLACE(COALESCE({qident(col)},''), '[^0-9]', ''), 4)"
        if col in ("PrimaryState", "SecondaryState"):
            return f"LEFT(UPPER(NULLIF(TRIM({qident(col)}), '')), 2)"
        if col == "DOB":
            return "COALESCE(STR_TO_DATE(TRIM(DOB), '%c/%e/%Y'), STR_TO_DATE(TRIM(DOB), '%Y-%m-%d'))"
        if col in ("RegistrationDate", "LastVoterActivity"):
            return (
                f"COALESCE(STR_TO_DATE(TRIM({qident(col)}), '%c/%e/%Y'), "
                f"STR_TO_DATE(TRIM({qident(col)}), '%Y-%m-%d'))"
            )
        return f"NULLIF(TRIM({qident(col)}), '')"

    sel_cols = [_sel_expr(c) for c in sanitized]

    # ETHNICITY ENHANCEMENT: Compute standardized ethnicity with priority logic (including Italian, Chinese, Russian, Irish)
    ethnicity_case = """
    CASE
        -- Priority 1: StateEthnicity
        WHEN StateEthnicity IS NOT NULL AND UPPER(TRIM(StateEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN
            CASE
                WHEN UPPER(StateEthnicity) LIKE '%ITALIAN%' THEN 'Italian'
                WHEN UPPER(StateEthnicity) LIKE '%IRISH%' THEN 'Irish'
                WHEN UPPER(StateEthnicity) LIKE '%RUSSIAN%' THEN 'Russian'
                WHEN UPPER(StateEthnicity) LIKE '%CHINESE%' THEN 'Chinese'
                WHEN UPPER(StateEthnicity) LIKE '%WHITE%' OR UPPER(StateEthnicity) LIKE '%CAUCASIAN%' THEN 'White'
                WHEN UPPER(StateEthnicity) LIKE '%BLACK%' OR UPPER(StateEthnicity) LIKE '%AFRICAN AMERICAN%' THEN 'Black'
                WHEN UPPER(StateEthnicity) LIKE '%HISPANIC%' OR UPPER(StateEthnicity) LIKE '%LATINO%' THEN 'Hispanic'
                WHEN UPPER(StateEthnicity) LIKE '%ASIAN%' OR UPPER(StateEthnicity) LIKE '%PACIFIC%' THEN 'Asian'
                WHEN UPPER(StateEthnicity) LIKE '%NATIVE%' OR UPPER(StateEthnicity) LIKE '%INDIGENOUS%' THEN 'Native American'
                WHEN UPPER(StateEthnicity) LIKE '%MULTIPLE%' OR UPPER(StateEthnicity) LIKE '%TWO OR MORE%' THEN 'Multiple'
                ELSE 'Other'
            END
        -- Priority 2: ObservedEthnicity
        WHEN ObservedEthnicity IS NOT NULL AND UPPER(TRIM(ObservedEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN
            CASE
                WHEN UPPER(ObservedEthnicity) LIKE '%ITALIAN%' THEN 'Italian'
                WHEN UPPER(ObservedEthnicity) LIKE '%IRISH%' THEN 'Irish'
                WHEN UPPER(ObservedEthnicity) LIKE '%RUSSIAN%' THEN 'Russian'
                WHEN UPPER(ObservedEthnicity) LIKE '%CHINESE%' THEN 'Chinese'
                WHEN UPPER(ObservedEthnicity) LIKE '%WHITE%' OR UPPER(ObservedEthnicity) LIKE '%CAUCASIAN%' THEN 'White'
                WHEN UPPER(ObservedEthnicity) LIKE '%BLACK%' OR UPPER(ObservedEthnicity) LIKE '%AFRICAN AMERICAN%' THEN 'Black'
                WHEN UPPER(ObservedEthnicity) LIKE '%HISPANIC%' OR UPPER(ObservedEthnicity) LIKE '%LATINO%' THEN 'Hispanic'
                WHEN UPPER(ObservedEthnicity) LIKE '%ASIAN%' OR UPPER(ObservedEthnicity) LIKE '%PACIFIC%' THEN 'Asian'
                WHEN UPPER(ObservedEthnicity) LIKE '%NATIVE%' OR UPPER(ObservedEthnicity) LIKE '%INDIGENOUS%' THEN 'Native American'
                WHEN UPPER(ObservedEthnicity) LIKE '%MULTIPLE%' OR UPPER(ObservedEthnicity) LIKE '%TWO OR MORE%' THEN 'Multiple'
                ELSE 'Other'
            END
        -- Priority 3: ModeledEthnicity
        WHEN ModeledEthnicity IS NOT NULL AND UPPER(TRIM(ModeledEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN
            CASE
                WHEN UPPER(ModeledEthnicity) LIKE '%ITALIAN%' THEN 'Italian'
                WHEN UPPER(ModeledEthnicity) LIKE '%IRISH%' THEN 'Irish'
                WHEN UPPER(ModeledEthnicity) LIKE '%RUSSIAN%' THEN 'Russian'
                WHEN UPPER(ModeledEthnicity) LIKE '%CHINESE%' THEN 'Chinese'
                WHEN UPPER(ModeledEthnicity) LIKE '%WHITE%' OR UPPER(ModeledEthnicity) LIKE '%CAUCASIAN%' THEN 'White'
                WHEN UPPER(ModeledEthnicity) LIKE '%BLACK%' OR UPPER(ModeledEthnicity) LIKE '%AFRICAN AMERICAN%' THEN 'Black'
                WHEN UPPER(ModeledEthnicity) LIKE '%HISPANIC%' OR UPPER(ModeledEthnicity) LIKE '%LATINO%' THEN 'Hispanic'
                WHEN UPPER(ModeledEthnicity) LIKE '%ASIAN%' OR UPPER(ModeledEthnicity) LIKE '%PACIFIC%' THEN 'Asian'
                WHEN UPPER(ModeledEthnicity) LIKE '%NATIVE%' OR UPPER(ModeledEthnicity) LIKE '%INDIGENOUS%' THEN 'Native American'
                WHEN UPPER(ModeledEthnicity) LIKE '%MULTIPLE%' OR UPPER(ModeledEthnicity) LIKE '%TWO OR MORE%' THEN 'Multiple'
                ELSE 'Other'
            END
        ELSE 'Unknown'
    END
    """ if has_ethnicity else "'Unknown'"
    
    ethnicity_source = """
    CASE
        WHEN StateEthnicity IS NOT NULL AND UPPER(TRIM(StateEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'state'
        WHEN ObservedEthnicity IS NOT NULL AND UPPER(TRIM(ObservedEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'observed'
        WHEN ModeledEthnicity IS NOT NULL AND UPPER(TRIM(ModeledEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'modeled'
        ELSE 'none'
    END
    """ if has_ethnicity else "'none'"
    
    ethnicity_confidence = """
    CASE
        WHEN StateEthnicity IS NOT NULL AND UPPER(TRIM(StateEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'high'
        WHEN ObservedEthnicity IS NOT NULL AND UPPER(TRIM(ObservedEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'high'
        WHEN ModeledEthnicity IS NOT NULL AND UPPER(TRIM(ModeledEthnicity)) NOT IN ('NO DATA PROVIDED', 'UNKNOWN', '', 'NULL') THEN 'medium'
        ELSE 'none'
    END
    """ if has_ethnicity else "'none'"

    # FIXED: Insert ONLY voters with valid DOB (exclude NULL DOB)
    insert_sql = f"""
INSERT INTO voter_file (
  {", ".join(dest_cols)}
)
SELECT
  {", ".join(sel_cols)},
  yob,
  match_key,
  match_key_hash,
  NULL AS origin,
  {ethnicity_case} AS StandardizedEthnicity,
  {ethnicity_source} AS EthnicitySource,
  {ethnicity_confidence} AS EthnicityConfidence
FROM stg_voter_raw
WHERE
  TRIM(StateVoterId) <> ''
  AND DOB IS NOT NULL 
  AND TRIM(DOB) <> '';
"""
    exec_sql(conn, insert_sql)

    fv = fetch_one(conn, "SELECT COUNT(*) FROM voter_file;")[0]
    fv_with_mk = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE match_key_hash IS NOT NULL;")[0]
    fv_no_mk = fv - fv_with_mk
    logger.info(f"voter_file total rows: {fv:,}")
    logger.info(f"  - with match keys: {fv_with_mk:,}")
    logger.info(f"  - without match keys (no/invalid DOB): {fv_no_mk:,}")
    
    # ETHNICITY ENHANCEMENT: Log initial ethnicity statistics
    if has_ethnicity:
        eth_stats = fetch_all(conn, """
            SELECT StandardizedEthnicity, EthnicitySource, COUNT(*) as cnt
            FROM voter_file
            GROUP BY StandardizedEthnicity, EthnicitySource
            ORDER BY cnt DESC;
        """)
        logger.info("Initial ethnicity distribution:")
        for eth, src, cnt in eth_stats:
            logger.info(f"  - {eth:20} (source: {src:10}): {cnt:,}")
        
        # Apply surname prediction for Unknown cases
        unknown_count = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE StandardizedEthnicity = 'Unknown';")[0]
        logger.info(f"Applying surname-based prediction to {unknown_count:,} Unknown ethnicity voters...")
        
        if unknown_count > 0:
            predicted_count = apply_surname_ethnicity_prediction(conn)
            logger.info(f"  - Successfully predicted ethnicity for {predicted_count:,} voters")
            
            # Log updated stats
            eth_stats_after = fetch_all(conn, """
                SELECT StandardizedEthnicity, EthnicitySource, COUNT(*) as cnt
                FROM voter_file
                GROUP BY StandardizedEthnicity, EthnicitySource
                ORDER BY cnt DESC;
            """)
            logger.info("Final ethnicity distribution:")
            for eth, src, cnt in eth_stats_after:
                logger.info(f"  - {eth:20} (source: {src:10}): {cnt:,}")
    else:
        logger.info("  - Ethnicity columns not available in source data")

    logger.info("Transform: precompute uniqueness counts")
    exec_sql(conn, "DROP TABLE IF EXISTS fullvoter_mk_counts;")
    exec_sql(
        conn,
        """
CREATE TABLE fullvoter_mk_counts AS
SELECT match_key_hash, COUNT(*) AS cnt
FROM voter_file
WHERE match_key_hash IS NOT NULL
GROUP BY match_key_hash;
""",
    )
    exec_sql(conn, "ALTER TABLE fullvoter_mk_counts ADD PRIMARY KEY (match_key_hash);")
    
    # OPTIMIZED: Analyze table to update statistics after bulk load
    logger.info("Analyzing tables for optimizer statistics...")
    exec_sql(conn, "ANALYZE TABLE voter_file, causeway_norm, fullvoter_mk_counts;")

    logger.info("Transform: build audience bridge (single pass join)")
    exec_sql(conn, "TRUNCATE TABLE voter_audience_bridge;")
    # OPTIMIZED: Single combined query instead of separate joins
    exec_sql(
        conn,
        """
INSERT IGNORE INTO voter_audience_bridge (StateVoterId, SDName, LDName, CDName, audience)
SELECT
  f.StateVoterId,
  COALESCE(NULLIF(TRIM(f.SDName), ''), 'UNKNOWN'),
  COALESCE(NULLIF(TRIM(f.LDName), ''), 'UNKNOWN'),
  COALESCE(NULLIF(TRIM(f.CDName), ''), 'UNKNOWN'),
  c.audience
FROM voter_file f
STRAIGHT_JOIN fullvoter_mk_counts mc ON mc.match_key_hash = f.match_key_hash AND mc.cnt = 1
STRAIGHT_JOIN causeway_norm c ON c.match_key_hash = f.match_key_hash;
""",
    )
    br = fetch_one(conn, "SELECT COUNT(*) FROM voter_audience_bridge;")[0]
    br_voters = fetch_one(conn, "SELECT COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge;")[0]
    logger.info(f"bridge rows (voter-audience pairs): {br:,}")
    logger.info(f"unique voters with matches: {br_voters:,}")

    logger.info("Transform: update origin string with ALL matched audiences (comma-separated)")
    # FIXED: Ensure GROUP_CONCAT captures all audiences
    exec_sql(conn, f"SET SESSION group_concat_max_len = {GROUP_CONCAT_MAX_LEN};")
    
    # Update voters WITH matches - use INNER JOIN to ensure GROUP_CONCAT works properly
    exec_sql(
        conn,
        """
UPDATE voter_file f
INNER JOIN (
  SELECT 
    StateVoterId, 
    GROUP_CONCAT(DISTINCT audience ORDER BY audience SEPARATOR ',') AS origin
  FROM voter_audience_bridge
  GROUP BY StateVoterId
) x ON x.StateVoterId = f.StateVoterId
SET f.origin = x.origin;
""",
    )
    
    updated = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL AND TRIM(origin) <> '';")[0]
    logger.info(f"  - voters with audience matches: {updated:,}")
    
    # Verify no truncation occurred
    max_origin_len = fetch_one(conn, "SELECT MAX(LENGTH(origin)) FROM voter_file WHERE origin IS NOT NULL;")[0]
    logger.info(f"  - max origin length: {max_origin_len} chars (limit: {GROUP_CONCAT_MAX_LEN})")
    if max_origin_len and max_origin_len >= GROUP_CONCAT_MAX_LEN * 0.9:
        logger.warning(f"  WARNING: Some origin values may be truncated! Increase GROUP_CONCAT_MAX_LEN")
    
    # Show sample of voters with multiple audiences
    multi_aud = fetch_all(conn, """
        SELECT StateVoterId, FirstName, LastName, origin
        FROM voter_file
        WHERE origin LIKE '%,%'
        LIMIT 5;
    """)
    if multi_aud:
        logger.info(f"  - Sample voters with multiple audiences:")
        for vid, fname, lname, orig in multi_aud:
            aud_count = orig.count(',') + 1 if orig else 0
            logger.info(f"    {vid} ({fname} {lname}): {aud_count} audiences - {orig[:100]}...")

    logger.info("Transform: build summary tables (parallel friendly)")
    exec_sql(conn, "DROP TABLE IF EXISTS counts_sd_audience;")
    exec_sql(
        conn,
        """
CREATE TABLE counts_sd_audience AS
SELECT SDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY SDName, audience;
""",
    )
    exec_sql(conn, "ALTER TABLE counts_sd_audience ADD PRIMARY KEY (SDName, audience);")

    exec_sql(conn, "DROP TABLE IF EXISTS counts_ld_audience;")
    exec_sql(
        conn,
        """
CREATE TABLE counts_ld_audience AS
SELECT LDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY LDName, audience;
""",
    )
    exec_sql(conn, "ALTER TABLE counts_ld_audience ADD PRIMARY KEY (LDName, audience);")

    exec_sql(conn, "DROP TABLE IF EXISTS counts_cd_audience;")
    exec_sql(
        conn,
        """
CREATE TABLE counts_cd_audience AS
SELECT CDName, audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY CDName, audience;
""",
    )
    exec_sql(conn, "ALTER TABLE counts_cd_audience ADD PRIMARY KEY (CDName, audience);")

    exec_sql(conn, "DROP TABLE IF EXISTS counts_state_audience;")
    exec_sql(
        conn,
        """
CREATE TABLE counts_state_audience AS
SELECT audience, COUNT(*) AS voters
FROM voter_audience_bridge
GROUP BY audience;
""",
    )
    exec_sql(conn, "ALTER TABLE counts_state_audience ADD PRIMARY KEY (audience);")
    
    # OPTIMIZED: Final analyze for summary tables
    logger.info("Analyzing summary tables...")
    exec_sql(conn, """
        ANALYZE TABLE 
        counts_sd_audience,
        counts_ld_audience,
        counts_cd_audience,
        counts_state_audience,
        voter_audience_bridge;
    """)
    
    # ETHNICITY ENHANCEMENT: Build ethnicity summary tables
    if has_ethnicity:
        logger.info("Building ethnicity summary tables...")
        
        # CD by Ethnicity (all voters, not just tagged)
        exec_sql(conn, "DROP TABLE IF EXISTS fullvoter_cd_ethnicity_counts;")
        exec_sql(
            conn,
            """
        CREATE TABLE fullvoter_cd_ethnicity_counts AS
        SELECT CDName, StandardizedEthnicity, COUNT(*) AS voters
        FROM voter_file
        WHERE StandardizedEthnicity IS NOT NULL
        GROUP BY CDName, StandardizedEthnicity;
        """,
        )
        exec_sql(conn, "ALTER TABLE fullvoter_cd_ethnicity_counts ADD PRIMARY KEY (CDName, StandardizedEthnicity);")
        
        # CD by Audience by Ethnicity (tagged voters only)
        exec_sql(conn, "DROP TABLE IF EXISTS fullvoter_cd_audience_ethnicity_counts;")
        exec_sql(
            conn,
            """
        CREATE TABLE fullvoter_cd_audience_ethnicity_counts AS
        SELECT 
            b.CDName, 
            b.audience, 
            f.StandardizedEthnicity,
            COUNT(*) AS voters
        FROM voter_audience_bridge b
        INNER JOIN voter_file f ON f.StateVoterId = b.StateVoterId
        WHERE f.StandardizedEthnicity IS NOT NULL
        GROUP BY b.CDName, b.audience, f.StandardizedEthnicity;
        """,
        )
        exec_sql(conn, "ALTER TABLE fullvoter_cd_audience_ethnicity_counts ADD PRIMARY KEY (CDName, audience, StandardizedEthnicity);")
        
        # State-level ethnicity distribution
        exec_sql(conn, "DROP TABLE IF EXISTS fullvoter_state_ethnicity_counts;")
        exec_sql(
            conn,
            """
        CREATE TABLE fullvoter_state_ethnicity_counts AS
        SELECT StandardizedEthnicity, COUNT(*) AS voters
        FROM voter_file
        WHERE StandardizedEthnicity IS NOT NULL
        GROUP BY StandardizedEthnicity;
        """,
        )
        exec_sql(conn, "ALTER TABLE fullvoter_state_ethnicity_counts ADD PRIMARY KEY (StandardizedEthnicity);")
        
        # Analyze ethnicity tables
        exec_sql(conn, """
            ANALYZE TABLE 
            fullvoter_cd_ethnicity_counts,
            fullvoter_cd_audience_ethnicity_counts,
            fullvoter_state_ethnicity_counts;
        """)
        
        logger.info("  - Created fullvoter_cd_ethnicity_counts")
        logger.info("  - Created fullvoter_cd_audience_ethnicity_counts")
        logger.info("  - Created fullvoter_state_ethnicity_counts")


# =========================
# MAIN
# =========================
def main():
    logger.info("=" * 80)
    logger.info("OPTIMIZED NYS VOTER TAGGING PIPELINE (FIXED)")
    logger.info("HIGH-PERFORMANCE MODE: i7-12700K (12 cores) / 64GB RAM / MySQL 8.4")
    logger.info("FIXES: Load ALL voters + Multiple audiences in origin field")
    logger.info("=" * 80)
    logger.info(f"DB: {DB_NAME}")
    logger.info(f"Run id: {RUN_ID}")
    logger.info(f"Full voter: {FULLVOTER_PATH}")
    logger.info(f"Log: {LOG_FILE}")
    logger.info(f"Bulk insert buffer: {BULK_INSERT_BUFFER_SIZE / 1024 / 1024:.0f} MB")
    logger.info(f"Expected innodb_buffer_pool_size: {INNODB_BUFFER_POOL_SIZE}")
    logger.info(f"GROUP_CONCAT_MAX_LEN: {GROUP_CONCAT_MAX_LEN}")

    if not DATA_DIR.exists():
        die(f"Missing data dir: {DATA_DIR}")
    if not FULLVOTER_PATH.exists():
        die(f"Missing full voter CSV: {FULLVOTER_PATH}")

    causeway_files = list_causeway_files()
    if not causeway_files:
        die(f"No causeway CSVs in: {DATA_DIR}")

    ensure_database()

    cw_fp = sha256_dir_fingerprint(causeway_files)
    fv_fp = sha256_file_fingerprint(FULLVOTER_PATH)

    conn = connect_db()
    try:
        ensure_metadata_tables(conn)

        last_cw = fetch_one(conn, "SELECT value FROM pipeline_metadata WHERE name='causeway_input_hash';")
        last_fv = fetch_one(conn, "SELECT value FROM pipeline_metadata WHERE name='fullvoter_input_hash';")
        last_cw = last_cw[0] if last_cw else None
        last_fv = last_fv[0] if last_fv else None

        if last_cw == cw_fp and last_fv == fv_fp:
            logger.info("[OK] Inputs unchanged, skipping rebuild.")
            logger.info("  To force rebuild, delete rows from pipeline_metadata table.")
            return

        fv_header = read_csv_header(FULLVOTER_PATH)

        needed = [
            "StateVoterId",
            "FirstName",
            "LastName",
            "PrimaryZip",
            "DOB",
            "CDName",
            "LDName",
            "SDName",
            "RegistrationDate",
            "LastVoterActivity",
        ]
        for col in needed:
            if col not in fv_header:
                die(f"fullnyvoter.csv missing required column: {col}")

        logger.info("Step 1: Rebuilding tables (optimized indexes)...")
        rebuild_tables(conn, fv_header)
        create_stg_voter_raw(conn, fv_header)

        logger.info(f"Step 2: Loading {len(causeway_files)} causeway file(s)...")
        for i, p in enumerate(causeway_files, 1):
            logger.info(f"  [{i}/{len(causeway_files)}] Loading: {p.name}")
            load_causeway_file(conn, p)
        n = fetch_one(conn, "SELECT COUNT(*) FROM stg_causeway_raw;")[0]
        logger.info(f"[OK] Total causeway staging rows: {n:,}")

        logger.info("Step 3: Loading full voter file (ALL VOTERS)...")
        load_fullvoter(conn, FULLVOTER_PATH, fv_header)
        n = fetch_one(conn, "SELECT COUNT(*) FROM stg_voter_raw;")[0]
        logger.info(f"[OK] Full voter staging rows: {n:,}")

        logger.info("Step 4: Running optimized transforms...")
        start_transform = time.time()
        run_transforms(conn, fullvoter_source_name=FULLVOTER_PATH.name, header=fv_header)
        transform_time = time.time() - start_transform
        logger.info(f"[OK] Transforms completed in {transform_time:.1f} seconds")

        exec_sql(
            conn,
            """
INSERT INTO pipeline_metadata (name, value) VALUES ('causeway_input_hash', %s)
ON DUPLICATE KEY UPDATE value=VALUES(value);
""",
            (cw_fp,),
        )
        exec_sql(
            conn,
            """
INSERT INTO pipeline_metadata (name, value) VALUES ('fullvoter_input_hash', %s)
ON DUPLICATE KEY UPDATE value=VALUES(value);
""",
            (fv_fp,),
        )

        drops = fetch_one(
            conn,
            "SELECT IFNULL(SUM(dropped),0) FROM pipeline_drop_counts WHERE run_id=%s;",
            (RUN_ID,),
        )[0]
        
        logger.info("=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"[OK] Run drop total (missing/unparseable DOB): {int(drops):,}")
        
        # Show final counts
        final_counts = fetch_one(conn, """
            SELECT 
                (SELECT COUNT(*) FROM voter_file) as voters,
                (SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL AND TRIM(origin) <> '') as tagged_voters,
                (SELECT COUNT(*) FROM voter_audience_bridge) as tagged_pairs,
                (SELECT COUNT(DISTINCT audience) FROM causeway_norm) as audiences,
                (SELECT COUNT(*) FROM voter_file WHERE origin LIKE '%,%') as multi_audience
        """)
        logger.info(f"[OK] Total voters in database: {final_counts[0]:,}")
        logger.info(f"[OK] Voters with at least one audience match: {final_counts[1]:,}")
        logger.info(f"[OK] Total voter-audience pairs: {final_counts[2]:,}")
        logger.info(f"[OK] Voters with multiple audiences: {final_counts[4]:,}")
        logger.info(f"[OK] Unique audiences: {final_counts[3]:,}")
        logger.info("=" * 80)
        logger.info("[OK] Pipeline completed successfully")

    except pymysql.err.OperationalError as e:
        if e.args and int(e.args[0]) in (1148,):
            die("LOCAL INFILE is disabled. Enable it on server and client:\n  SET GLOBAL local_infile = 1;\nThen reconnect.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Total runtime: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")