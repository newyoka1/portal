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
import atexit
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import re
import csv
import contextlib

import argparse

import pymysql
import pymysql.connections as pmc
from pymysql.constants import CLIENT

# =========================
# CONFIG
# =========================
DB_NAME = "NYS_VOTER_TAGGING"

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

BASE_DIR = Path(r"C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE")
DATA_DIR = BASE_DIR / "data"
FULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
RUN_ID = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"run_pipeline_{RUN_ID}.log"

GROUP_CONCAT_MAX_LEN = int(os.getenv("GROUP_CONCAT_MAX_LEN", "500000"))

CENSUS_SURNAMES_CSV = Path(os.path.dirname(__file__)) / "census_surnames" / "Names_2010Census.csv"

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
    """Hash actual file contents (not just metadata) for reliable change detection."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_dir_fingerprint(paths: list[Path]) -> str:
    """Hash contents of all files for reliable change detection."""
    h = hashlib.sha256()
    for p in sorted(paths, key=lambda x: x.name.lower()):
        h.update(p.name.encode("utf-8"))
        with p.open("rb") as f:
            while True:
                chunk = f.read(8 * 1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
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


# Track temp files for cleanup
_temp_files: list[Path] = []


def _cleanup_temp_files():
    for p in _temp_files:
        try:
            if p.exists():
                p.unlink()
                logger.info(f"Cleaned up temp file: {p}")
        except Exception as e:
            logger.warning(f"Failed to clean up temp file {p}: {e}")


atexit.register(_cleanup_temp_files)


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
            _temp_files.append(dst)
            return dst

    copy_with_progress(src, dst, label, log_every_mb)
    _temp_files.append(dst)
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


@contextlib.contextmanager
def _local_infile_progress(label: str, log_every_mb: int):
    """Context manager that safely patches/restores pmc.open for LOCAL INFILE progress."""
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
    try:
        yield
    finally:
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
        return "DATE NULL"
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


def create_match_key_functions(conn):
    """Fix #12: Single source of truth for match key computation.
    Fix #7: Supports M/D/YYYY, YYYY-MM-DD, MM-DD-YYYY, and MM/DD/YY date formats.
    """
    exec_sql(conn, "DROP FUNCTION IF EXISTS parse_dob_year;")
    exec_sql(conn, """
CREATE FUNCTION parse_dob_year(dob_raw VARCHAR(20))
RETURNS CHAR(4) DETERMINISTIC NO SQL
BEGIN
  DECLARE trimmed VARCHAR(20);
  DECLARE yr CHAR(4);
  SET trimmed = TRIM(dob_raw);
  IF trimmed IS NULL OR trimmed = '' THEN RETURN NULL; END IF;
  -- Try M/D/YYYY
  SET yr = DATE_FORMAT(STR_TO_DATE(trimmed, '%c/%e/%Y'), '%Y');
  IF yr IS NOT NULL THEN RETURN yr; END IF;
  -- Try YYYY-MM-DD
  SET yr = DATE_FORMAT(STR_TO_DATE(trimmed, '%Y-%m-%d'), '%Y');
  IF yr IS NOT NULL THEN RETURN yr; END IF;
  -- Try MM-DD-YYYY
  SET yr = DATE_FORMAT(STR_TO_DATE(trimmed, '%m-%d-%Y'), '%Y');
  IF yr IS NOT NULL THEN RETURN yr; END IF;
  -- Try M/D/YY (2-digit year)
  SET yr = DATE_FORMAT(STR_TO_DATE(trimmed, '%c/%e/%y'), '%Y');
  IF yr IS NOT NULL THEN RETURN yr; END IF;
  RETURN NULL;
END;
""")

    exec_sql(conn, "DROP FUNCTION IF EXISTS make_match_key;")
    exec_sql(conn, """
CREATE FUNCTION make_match_key(
  fn VARCHAR(100), ln VARCHAR(100), zip VARCHAR(20), dob_raw VARCHAR(20)
) RETURNS VARCHAR(320) DETERMINISTIC NO SQL
BEGIN
  DECLARE yr CHAR(4);
  SET yr = parse_dob_year(dob_raw);
  IF yr IS NULL THEN RETURN NULL; END IF;
  RETURN CONCAT(
    LOWER(REGEXP_REPLACE(COALESCE(fn, ''), '[^A-Za-z]', '')),
    '|',
    LOWER(REGEXP_REPLACE(COALESCE(ln, ''), '[^A-Za-z]', '')),
    '|',
    LEFT(REGEXP_REPLACE(COALESCE(zip, ''), '[^0-9]', ''), 5),
    '|',
    yr
  );
END;
""")

    exec_sql(conn, "DROP FUNCTION IF EXISTS make_match_key_hash;")
    exec_sql(conn, """
CREATE FUNCTION make_match_key_hash(
  fn VARCHAR(100), ln VARCHAR(100), zip VARCHAR(20), dob_raw VARCHAR(20)
) RETURNS BINARY(16) DETERMINISTIC NO SQL
BEGIN
  DECLARE mk VARCHAR(320);
  SET mk = make_match_key(fn, ln, zip, dob_raw);
  IF mk IS NULL THEN RETURN NULL; END IF;
  RETURN UNHEX(MD5(mk));
END;
""")
    logger.info("Created stored functions: parse_dob_year, make_match_key, make_match_key_hash")


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
    ] + AUDIENCE_GROUP_TABLES:
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

    # Optimized indexes for common queries
    idx_parts = [
        "PRIMARY KEY (`StateVoterId`)",
        "KEY idx_mkh (`match_key_hash`)",
        "KEY idx_cd_aud (`CDName`, `origin`(100))",
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
    """Load causeway file, then compute match keys using stored function."""
    load_path = ensure_loadable_path(src_path, f"Causeway copy {src_path.name}", COPY_LOG_EVERY_MB_CAUSEWAY)
    psql = normalize_path_for_load(load_path)
    # Fix #1: Escape audience name for SQL safety
    audience_escaped = conn.escape(src_path.name).strip("'")

    hdr = read_csv_header(load_path)
    idx = {h: i for i, h in enumerate(hdr)}
    required = ["FirstName", "LastName", "PrimaryZip", "DOB"]
    for r in required:
        if r not in idx:
            die(f"Causeway file {src_path.name} missing required column: {r}")

    vars_list = ", ".join([f"@v{i+1}" for i in range(len(hdr))])

    def v(colname: str) -> str:
        return f"@v{idx[colname] + 1}"

    # Fix #2: Handle both \r\n (Windows) and \n (Unix) line endings
    sql = f"""
LOAD DATA LOCAL INFILE '{psql}'
INTO TABLE stg_causeway_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES
({vars_list})
SET
  FirstName  = NULLIF(TRIM({v('FirstName')}), ''),
  LastName   = NULLIF(TRIM({v('LastName')}), ''),
  PrimaryZip = NULLIF(TRIM({v('PrimaryZip')}), ''),
  DOB        = NULLIF(TRIM({v('DOB')}), ''),
  origin     = '{audience_escaped}';
"""

    # Fix #8: Use context manager for safe monkeypatch scoping
    with _local_infile_progress(f"LOCAL INFILE causeway {src_path.name}", LOCAL_INFILE_LOG_EVERY_MB_CAUSEWAY):
        with conn.cursor() as cur:
            cur.execute(sql)
            loaded = cur.rowcount if cur.rowcount is not None else 0
        logger.info(f"Causeway load {src_path.name}: {loaded:,} rows")

    # Fix #15: Validate row count
    if loaded == 0:
        logger.warning(f"  WARNING: Zero rows loaded from {src_path.name} - check file format")

    # Fix #12: Use stored function for match key computation (single source of truth)
    logger.info(f"  Computing keys...")
    exec_sql(
        conn,
        """
UPDATE stg_causeway_raw
SET
  match_key = make_match_key(FirstName, LastName, PrimaryZip, DOB),
  match_key_hash = make_match_key_hash(FirstName, LastName, PrimaryZip, DOB)
WHERE origin = %s AND match_key IS NULL;
""",
        (src_path.name,),
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
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES
({vars_list})
SET
  {set_clause};
"""
    # Fix #8: Use context manager for safe monkeypatch scoping
    with _local_infile_progress("LOCAL INFILE full voter", LOCAL_INFILE_LOG_EVERY_MB_FULL):
        with conn.cursor() as cur:
            cur.execute(sql)
            loaded = cur.rowcount if cur.rowcount is not None else 0
        logger.info(f"Full voter staging load: {loaded:,} rows")

    # Fix #15: Validate row count
    if loaded == 0:
        die("Zero rows loaded from full voter file - check file format and line endings")

    # Fix #12: Use stored function for match key (single source of truth)
    # Fix #7: Date format handling is inside the stored function (supports more formats)
    logger.info(f"Computing match keys for full voter (only for rows with DOB)...")
    exec_sql(
        conn,
        """
UPDATE stg_voter_raw
SET
  yob = parse_dob_year(TRIM(DOB)),
  match_key = make_match_key(FirstName, LastName, PrimaryZip, DOB),
  match_key_hash = make_match_key_hash(FirstName, LastName, PrimaryZip, DOB)
WHERE DOB IS NOT NULL AND TRIM(DOB) <> '';
"""
    )

    # Log statistics
    with_keys = fetch_one(conn, "SELECT COUNT(*) FROM stg_voter_raw WHERE match_key_hash IS NOT NULL;")[0]
    without_keys = fetch_one(conn, "SELECT COUNT(*) FROM stg_voter_raw WHERE match_key_hash IS NULL;")[0]
    logger.info(f"  - Rows with match keys: {with_keys:,}")
    logger.info(f"  - Rows without match keys (no/invalid DOB): {without_keys:,}")


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

    dest_cols = [qident(c) for c in sanitized] + ["`yob`", "`match_key`", "`match_key_hash`", "`origin`"]

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
            # Fix #7: Use COALESCE chain that matches all formats the stored function supports
            return ("COALESCE("
                    "STR_TO_DATE(TRIM(DOB), '%c/%e/%Y'), "
                    "STR_TO_DATE(TRIM(DOB), '%Y-%m-%d'), "
                    "STR_TO_DATE(TRIM(DOB), '%m-%d-%Y'), "
                    "STR_TO_DATE(TRIM(DOB), '%c/%e/%y'))")
        if col in ("RegistrationDate", "LastVoterActivity"):
            return (
                f"COALESCE("
                f"STR_TO_DATE(TRIM({qident(col)}), '%c/%e/%Y'), "
                f"STR_TO_DATE(TRIM({qident(col)}), '%Y-%m-%d'), "
                f"STR_TO_DATE(TRIM({qident(col)}), '%m-%d-%Y'), "
                f"STR_TO_DATE(TRIM({qident(col)}), '%c/%e/%y'))"
            )
        return f"NULLIF(TRIM({qident(col)}), '')"

    sel_cols = [_sel_expr(c) for c in sanitized]

    # FIXED: Insert ALL voters with valid StateVoterId, regardless of match_key_hash
    insert_sql = f"""
INSERT INTO voter_file (
  {", ".join(dest_cols)}
)
SELECT
  {", ".join(sel_cols)},
  yob,
  match_key,
  match_key_hash,
  NULL AS origin
FROM stg_voter_raw
WHERE
  TRIM(StateVoterId) <> '';
"""
    exec_sql(conn, insert_sql)

    fv = fetch_one(conn, "SELECT COUNT(*) FROM voter_file;")[0]
    fv_with_mk = fetch_one(conn, "SELECT COUNT(*) FROM voter_file WHERE match_key_hash IS NOT NULL;")[0]
    fv_no_mk = fv - fv_with_mk
    logger.info(f"voter_file total rows: {fv:,}")
    logger.info(f"  - with match keys: {fv_with_mk:,}")
    logger.info(f"  - without match keys (no/invalid DOB): {fv_no_mk:,}")

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

    # Fix #9: Log how many voters are excluded due to ambiguous (non-unique) match keys
    ambig_stats = fetch_one(conn, """
        SELECT
            COUNT(*) AS ambig_keys,
            IFNULL(SUM(cnt), 0) AS ambig_voters
        FROM fullvoter_mk_counts
        WHERE cnt > 1;
    """)
    ambig_keys, ambig_voters = ambig_stats[0], ambig_stats[1]
    if ambig_voters > 0:
        logger.warning(f"  Ambiguous match keys (cnt > 1): {ambig_keys:,} keys affecting {int(ambig_voters):,} voters")
        logger.warning(f"  These voters are EXCLUDED from audience matching (match_key shared by multiple voters)")
        # Log drop count for ambiguous matches
        exec_sql(conn, """
            INSERT IGNORE INTO pipeline_drop_counts (run_id, dataset, source_file, reason, dropped)
            VALUES (%s, 'fullvoter', 'fullnyvoter.csv', 'ambiguous_match_key', %s);
        """, (RUN_ID, int(ambig_voters)))

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

    # Fix #6: Strict truncation detection - check for exact limit hit, not 90% threshold
    max_origin_len = fetch_one(conn, "SELECT MAX(LENGTH(origin)) FROM voter_file WHERE origin IS NOT NULL;")[0]
    logger.info(f"  - max origin length: {max_origin_len} chars (limit: {GROUP_CONCAT_MAX_LEN})")
    if max_origin_len and max_origin_len >= GROUP_CONCAT_MAX_LEN - 1:
        truncated_count = fetch_one(conn, f"""
            SELECT COUNT(*) FROM voter_file
            WHERE origin IS NOT NULL AND LENGTH(origin) >= {GROUP_CONCAT_MAX_LEN - 1};
        """)[0]
        logger.error(f"  ERROR: {truncated_count:,} origin values likely TRUNCATED! "
                     f"Increase GROUP_CONCAT_MAX_LEN (currently {GROUP_CONCAT_MAX_LEN})")
    elif max_origin_len and max_origin_len >= GROUP_CONCAT_MAX_LEN * 0.8:
        logger.warning(f"  WARNING: Origin values approaching GROUP_CONCAT limit - consider increasing")

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

    # Fix #13: Removed unnecessary final ANALYZE TABLE - summary tables are not queried again


# =========================
# CENSUS SURNAME ETHNICITY
# =========================
ETHNICITY_COLS = ["pctwhite", "pctblack", "pctapi", "pctaian", "pct2prace", "pcthispanic"]
ETHNICITY_LABELS = {
    "pctwhite": "WHITE",
    "pctblack": "BLACK",
    "pctapi": "ASIAN_PI",
    "pctaian": "AIAN",
    "pct2prace": "MULTI",
    "pcthispanic": "HISPANIC",
}


def _parse_pct(val):
    if val is None or val.strip() == "" or "(S)" in val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _dominant_ethnicity(pcts):
    best_label, best_val = None, -1
    for col in ETHNICITY_COLS:
        v = pcts.get(col)
        if v is not None and v > best_val:
            best_val = v
            best_label = ETHNICITY_LABELS[col]
    return best_label


def ensure_census_surnames(conn):
    """Load ref_census_surnames table if it doesn't already exist with data."""
    row = fetch_one(conn, """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'ref_census_surnames';
    """, (DB_NAME,))
    if row[0] > 0:
        cnt = fetch_one(conn, "SELECT COUNT(*) FROM ref_census_surnames;")[0]
        if cnt > 0:
            logger.info(f"ref_census_surnames already loaded ({cnt:,} surnames), skipping")
            return

    if not CENSUS_SURNAMES_CSV.exists():
        logger.warning(f"Census surnames CSV not found: {CENSUS_SURNAMES_CSV}")
        logger.warning("Ethnicity tables will show UNKNOWN for all voters")
        logger.warning("Download from: https://www2.census.gov/topics/genealogy/2010surnames/names.zip")
        exec_sql(conn, "DROP TABLE IF EXISTS ref_census_surnames;")
        exec_sql(conn, """
CREATE TABLE ref_census_surnames (
  surname           VARCHAR(100) NOT NULL PRIMARY KEY,
  census_count      INT UNSIGNED NULL,
  pctwhite          DECIMAL(5,2) NULL,
  pctblack          DECIMAL(5,2) NULL,
  pctapi            DECIMAL(5,2) NULL,
  pctaian           DECIMAL(5,2) NULL,
  pct2prace         DECIMAL(5,2) NULL,
  pcthispanic       DECIMAL(5,2) NULL,
  dominant_ethnicity VARCHAR(30) NULL,
  KEY idx_dominant (dominant_ethnicity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")
        return

    logger.info(f"Loading census surname data from {CENSUS_SURNAMES_CSV}...")
    exec_sql(conn, "DROP TABLE IF EXISTS ref_census_surnames;")
    exec_sql(conn, """
CREATE TABLE ref_census_surnames (
  surname           VARCHAR(100) NOT NULL PRIMARY KEY,
  census_count      INT UNSIGNED NULL,
  pctwhite          DECIMAL(5,2) NULL,
  pctblack          DECIMAL(5,2) NULL,
  pctapi            DECIMAL(5,2) NULL,
  pctaian           DECIMAL(5,2) NULL,
  pct2prace         DECIMAL(5,2) NULL,
  pcthispanic       DECIMAL(5,2) NULL,
  dominant_ethnicity VARCHAR(30) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

    batch = []
    total = 0
    insert_sql = """
INSERT INTO ref_census_surnames
  (surname, census_count, pctwhite, pctblack, pctapi, pctaian, pct2prace, pcthispanic, dominant_ethnicity)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
    with open(CENSUS_SURNAMES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip().upper()
            count_val = row.get("count", "").replace(",", "")
            try:
                census_count = int(count_val)
            except (ValueError, TypeError):
                census_count = None

            pcts = {col: _parse_pct(row.get(col, "")) for col in ETHNICITY_COLS}
            dom = _dominant_ethnicity(pcts)
            batch.append((
                name, census_count,
                pcts["pctwhite"], pcts["pctblack"], pcts["pctapi"],
                pcts["pctaian"], pcts["pct2prace"], pcts["pcthispanic"],
                dom,
            ))
            if len(batch) >= 5000:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch = []

    if batch:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, batch)
        conn.commit()
        total += len(batch)

    exec_sql(conn, "ALTER TABLE ref_census_surnames ADD INDEX idx_dominant (dominant_ethnicity);")
    conn.commit()
    logger.info(f"Loaded {total:,} surnames into ref_census_surnames")


# =========================
# AUDIENCE GROUPS (MATERIALIZED)
# =========================

# Define the 12 audience groups: (table_name, audience_group_label, WHERE clause for bridge)
# Combined groups require all 3 turnout levels; individual groups require just 1
AUDIENCE_GROUPS = {
    # Combined (all turnout levels required)
    "NYS_HARD_GOP": {
        "label": "HARD GOP",
        "where": """
            EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND (b.audience LIKE 'HT HARD GOP%' OR b.audience LIKE 'MT HARD GOP%' OR b.audience LIKE 'LT HARD GOP%'))
        """,
    },
    "NYS_HARD_DEM": {
        "label": "HARD DEM",
        "where": """
            EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND (b.audience LIKE 'HT HARD DEM%' OR b.audience LIKE 'MT HARD DEM%' OR b.audience LIKE 'LT HARD DEM%'))
        """,
    },
    "NYS_SWING": {
        "label": "SWING",
        "where": """
            EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND (b.audience LIKE 'HT SWING %' OR b.audience LIKE 'MT SWING %' OR b.audience LIKE 'LT SWING %'))
        """,
    },
    # Individual turnout levels
    "NYS_HT_HARD_GOP": {
        "label": "HT HARD GOP",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'HT HARD GOP%')",
    },
    "NYS_MT_HARD_GOP": {
        "label": "MT HARD GOP",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'MT HARD GOP%')",
    },
    "NYS_LT_HARD_GOP": {
        "label": "LT HARD GOP",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'LT HARD GOP%')",
    },
    "NYS_HT_HARD_DEM": {
        "label": "HT HARD DEM",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'HT HARD DEM%')",
    },
    "NYS_MT_HARD_DEM": {
        "label": "MT HARD DEM",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'MT HARD DEM%')",
    },
    "NYS_LT_HARD_DEM": {
        "label": "LT HARD DEM",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'LT HARD DEM%')",
    },
    "NYS_HT_SWING": {
        "label": "HT SWING",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'HT SWING %')",
    },
    "NYS_MT_SWING": {
        "label": "MT SWING",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'MT SWING %')",
    },
    "NYS_LT_SWING": {
        "label": "LT SWING",
        "where": "EXISTS (SELECT 1 FROM voter_audience_bridge b WHERE b.StateVoterId = f.StateVoterId AND b.audience LIKE 'LT SWING %')",
    },
}

# Tables created by build_audience_groups (for cleanup in rebuild_tables)
AUDIENCE_GROUP_TABLES = list(AUDIENCE_GROUPS.keys()) + [
    "NYS_COUNTS_STATEWIDE",
    "NYS_COUNTS_BY_CD",
    "NYS_COUNTS_BY_SD",
    "NYS_COUNTS_BY_AD",
    "NYS_ETHNICITY_STATEWIDE",
    "NYS_ETHNICITY_BY_CD",
    "NYS_ETHNICITY_BY_SD",
    "NYS_ETHNICITY_BY_AD",
    "NYS_ETHNICITY_DETAIL",
]


def build_audience_groups(conn):
    """Materialize all audience group tables, count tables, and ethnicity tables."""

    # ---------------------------------------------------------------
    # Step 1: Materialize 12 audience group tables
    # ---------------------------------------------------------------
    logger.info("Building materialized audience group tables (12 groups)...")
    for tbl, cfg in AUDIENCE_GROUPS.items():
        logger.info(f"  Materializing {tbl} ({cfg['label']})...")
        exec_sql(conn, f"DROP TABLE IF EXISTS {qident(tbl)};")
        exec_sql(conn, f"""
CREATE TABLE {qident(tbl)} AS
SELECT f.*
FROM voter_file f
WHERE {cfg['where']};
""")
        # Add primary key + district indexes for fast lookups
        exec_sql(conn, f"ALTER TABLE {qident(tbl)} ADD PRIMARY KEY (StateVoterId);")
        exec_sql(conn, f"ALTER TABLE {qident(tbl)} ADD KEY idx_cd (CDName);")
        exec_sql(conn, f"ALTER TABLE {qident(tbl)} ADD KEY idx_sd (SDName);")
        exec_sql(conn, f"ALTER TABLE {qident(tbl)} ADD KEY idx_ld (LDName);")
        cnt = fetch_one(conn, f"SELECT COUNT(*) FROM {qident(tbl)};")[0]
        logger.info(f"    {tbl}: {cnt:,} voters")

    # ---------------------------------------------------------------
    # Step 2: Materialized count tables
    # ---------------------------------------------------------------
    logger.info("Building materialized count tables...")

    # Statewide counts
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_COUNTS_STATEWIDE;")
    statewide_sql = " UNION ALL ".join(
        f"SELECT '{cfg['label']}' AS audience_group, COUNT(*) AS voters FROM {qident(tbl)}"
        for tbl, cfg in AUDIENCE_GROUPS.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_COUNTS_STATEWIDE AS {statewide_sql};")
    exec_sql(conn, "ALTER TABLE NYS_COUNTS_STATEWIDE ADD KEY idx_grp (audience_group);")
    logger.info("  NYS_COUNTS_STATEWIDE built")

    # By CD
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_COUNTS_BY_CD;")
    cd_sql = " UNION ALL ".join(
        f"SELECT '{cfg['label']}' AS audience_group, CDName, COUNT(*) AS voters FROM {qident(tbl)} GROUP BY CDName"
        for tbl, cfg in AUDIENCE_GROUPS.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_COUNTS_BY_CD AS {cd_sql};")
    exec_sql(conn, "ALTER TABLE NYS_COUNTS_BY_CD ADD KEY idx_grp_cd (audience_group, CDName);")
    logger.info("  NYS_COUNTS_BY_CD built")

    # By SD
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_COUNTS_BY_SD;")
    sd_sql = " UNION ALL ".join(
        f"SELECT '{cfg['label']}' AS audience_group, SDName, COUNT(*) AS voters FROM {qident(tbl)} GROUP BY SDName"
        for tbl, cfg in AUDIENCE_GROUPS.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_COUNTS_BY_SD AS {sd_sql};")
    exec_sql(conn, "ALTER TABLE NYS_COUNTS_BY_SD ADD KEY idx_grp_sd (audience_group, SDName);")
    logger.info("  NYS_COUNTS_BY_SD built")

    # By AD (LDName)
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_COUNTS_BY_AD;")
    ad_sql = " UNION ALL ".join(
        f"SELECT '{cfg['label']}' AS audience_group, LDName, COUNT(*) AS voters FROM {qident(tbl)} GROUP BY LDName"
        for tbl, cfg in AUDIENCE_GROUPS.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_COUNTS_BY_AD AS {ad_sql};")
    exec_sql(conn, "ALTER TABLE NYS_COUNTS_BY_AD ADD KEY idx_grp_ld (audience_group, LDName);")
    logger.info("  NYS_COUNTS_BY_AD built")

    # ---------------------------------------------------------------
    # Step 3: Materialized ethnicity tables
    # ---------------------------------------------------------------
    logger.info("Building materialized ethnicity tables...")

    # Only use the 3 combined groups for ethnicity (HARD GOP, HARD DEM, SWING)
    eth_groups = {k: v for k, v in AUDIENCE_GROUPS.items() if k in ("NYS_HARD_GOP", "NYS_HARD_DEM", "NYS_SWING")}

    # Ethnicity statewide
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_ETHNICITY_STATEWIDE;")
    eth_sw_sql = " UNION ALL ".join(
        f"""SELECT '{cfg['label']}' AS audience_group,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM {qident(tbl)} v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY e.dominant_ethnicity"""
        for tbl, cfg in eth_groups.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_ETHNICITY_STATEWIDE AS {eth_sw_sql};")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_STATEWIDE ADD KEY idx_grp (audience_group, ethnicity);")
    logger.info("  NYS_ETHNICITY_STATEWIDE built")

    # Ethnicity by CD
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_ETHNICITY_BY_CD;")
    eth_cd_sql = " UNION ALL ".join(
        f"""SELECT '{cfg['label']}' AS audience_group, v.CDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM {qident(tbl)} v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.CDName, e.dominant_ethnicity"""
        for tbl, cfg in eth_groups.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_ETHNICITY_BY_CD AS {eth_cd_sql};")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_BY_CD ADD KEY idx_grp_cd (audience_group, CDName, ethnicity);")
    logger.info("  NYS_ETHNICITY_BY_CD built")

    # Ethnicity by SD
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_ETHNICITY_BY_SD;")
    eth_sd_sql = " UNION ALL ".join(
        f"""SELECT '{cfg['label']}' AS audience_group, v.SDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM {qident(tbl)} v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.SDName, e.dominant_ethnicity"""
        for tbl, cfg in eth_groups.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_ETHNICITY_BY_SD AS {eth_sd_sql};")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_BY_SD ADD KEY idx_grp_sd (audience_group, SDName, ethnicity);")
    logger.info("  NYS_ETHNICITY_BY_SD built")

    # Ethnicity by AD
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_ETHNICITY_BY_AD;")
    eth_ad_sql = " UNION ALL ".join(
        f"""SELECT '{cfg['label']}' AS audience_group, v.LDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM {qident(tbl)} v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.LDName, e.dominant_ethnicity"""
        for tbl, cfg in eth_groups.items()
    )
    exec_sql(conn, f"CREATE TABLE NYS_ETHNICITY_BY_AD AS {eth_ad_sql};")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_BY_AD ADD KEY idx_grp_ld (audience_group, LDName, ethnicity);")
    logger.info("  NYS_ETHNICITY_BY_AD built")

    # Ethnicity detail: per-voter ethnicity probabilities for ALL voters
    exec_sql(conn, "DROP TABLE IF EXISTS NYS_ETHNICITY_DETAIL;")
    exec_sql(conn, """
CREATE TABLE NYS_ETHNICITY_DETAIL AS
SELECT
  f.StateVoterId,
  f.FirstName,
  f.LastName,
  f.CDName,
  f.SDName,
  f.LDName,
  f.origin,
  e.pctwhite,
  e.pctblack,
  e.pctapi,
  e.pctaian,
  e.pct2prace,
  e.pcthispanic,
  e.dominant_ethnicity
FROM voter_file f
LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName);
""")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_DETAIL ADD PRIMARY KEY (StateVoterId);")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_DETAIL ADD KEY idx_eth (dominant_ethnicity);")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_DETAIL ADD KEY idx_cd_eth (CDName, dominant_ethnicity);")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_DETAIL ADD KEY idx_sd_eth (SDName, dominant_ethnicity);")
    exec_sql(conn, "ALTER TABLE NYS_ETHNICITY_DETAIL ADD KEY idx_ld_eth (LDName, dominant_ethnicity);")
    detail_cnt = fetch_one(conn, "SELECT COUNT(*) FROM NYS_ETHNICITY_DETAIL;")[0]
    matched_cnt = fetch_one(conn, "SELECT COUNT(*) FROM NYS_ETHNICITY_DETAIL WHERE dominant_ethnicity IS NOT NULL;")[0]
    logger.info(f"  NYS_ETHNICITY_DETAIL: {detail_cnt:,} voters ({matched_cnt:,} with ethnicity match)")

    # ---------------------------------------------------------------
    # Summary log
    # ---------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("AUDIENCE GROUP SUMMARY")
    logger.info("=" * 60)
    rows = fetch_all(conn, "SELECT audience_group, voters FROM NYS_COUNTS_STATEWIDE ORDER BY audience_group;")
    for grp, cnt in rows:
        logger.info(f"  {grp:20s}  {cnt:>10,}")

    logger.info("-" * 60)
    logger.info("ETHNICITY SUMMARY (combined groups)")
    logger.info("-" * 60)
    rows = fetch_all(conn, "SELECT audience_group, ethnicity, voters FROM NYS_ETHNICITY_STATEWIDE ORDER BY audience_group, voters DESC;")
    cur_grp = None
    for grp, eth, cnt in rows:
        if grp != cur_grp:
            logger.info(f"  {grp}:")
            cur_grp = grp
        logger.info(f"    {eth:12s}  {cnt:>10,}")


# =========================
# MAIN
# =========================
def main(rebuild_groups=False):
    logger.info("=" * 80)
    if rebuild_groups:
        logger.info("REBUILD GROUPS MODE - Re-materializing audience groups & ethnicity tables only")
    else:
        logger.info("OPTIMIZED NYS VOTER TAGGING PIPELINE (FIXED)")
        logger.info("HIGH-PERFORMANCE MODE: i7-12700K (12 cores) / 64GB RAM / MySQL 8.4")
        logger.info("FIXES: Load ALL voters + Multiple audiences in origin field")
    logger.info("=" * 80)
    logger.info(f"DB: {DB_NAME}")
    logger.info(f"Run id: {RUN_ID}")
    logger.info(f"Log: {LOG_FILE}")

    ensure_database()
    conn = connect_db()
    try:
        # Fix #11: Acquire advisory lock to prevent concurrent pipeline runs
        lock_result = fetch_one(conn, "SELECT GET_LOCK('nys_voter_pipeline', 0);")
        if not lock_result or lock_result[0] != 1:
            die("Another pipeline instance is already running. Exiting.")
        logger.info("Acquired advisory lock: nys_voter_pipeline")

        ensure_metadata_tables(conn)

        # ── REBUILD GROUPS ONLY ──────────────────────────────────────
        if rebuild_groups:
            # Verify core tables exist before proceeding
            for required_tbl in ("voter_file", "voter_audience_bridge", "causeway_norm"):
                cnt = fetch_one(conn, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s;", (DB_NAME, required_tbl))
                if not cnt or cnt[0] == 0:
                    die(f"Table {required_tbl} does not exist. Run a full pipeline first.")

            logger.info("Step 5: Ensuring census surname ethnicity data...")
            ensure_census_surnames(conn)

            logger.info("Step 6: Rebuilding materialized audience groups & ethnicity tables...")
            conn.autocommit(False)
            start_aud = time.time()
            try:
                build_audience_groups(conn)
                conn.commit()
                aud_time = time.time() - start_aud
                logger.info(f"[OK] Audience groups committed in {aud_time:.1f} seconds")
            except Exception:
                logger.error("Audience group build failed, rolling back...")
                conn.rollback()
                raise
            finally:
                conn.autocommit(True)

            logger.info("=" * 80)
            logger.info("[OK] Rebuild groups completed successfully")
            return

        # ── FULL PIPELINE ────────────────────────────────────────────
        logger.info(f"Full voter: {FULLVOTER_PATH}")
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

        logger.info("Computing input fingerprints (content hash)...")
        cw_fp = sha256_dir_fingerprint(causeway_files)
        fv_fp = sha256_file_fingerprint(FULLVOTER_PATH)
        logger.info(f"  Causeway hash: {cw_fp[:16]}...")
        logger.info(f"  Full voter hash: {fv_fp[:16]}...")

        last_cw = fetch_one(conn, "SELECT value FROM pipeline_metadata WHERE name='causeway_input_hash';")
        last_fv = fetch_one(conn, "SELECT value FROM pipeline_metadata WHERE name='fullvoter_input_hash';")
        last_cw = last_cw[0] if last_cw else None
        last_fv = last_fv[0] if last_fv else None

        if last_cw == cw_fp and last_fv == fv_fp:
            logger.info("[OK] Inputs unchanged, skipping rebuild.")
            logger.info("  To force rebuild, delete rows from pipeline_metadata table.")
            return

        # Fix #5: Clear stale fingerprints BEFORE starting work.
        # If we crash mid-pipeline, the missing fingerprints force a full re-run.
        exec_sql(conn, "DELETE FROM pipeline_metadata WHERE name IN ('causeway_input_hash', 'fullvoter_input_hash');")

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

        # Fix #12: Create stored functions for match key (single source of truth)
        logger.info("Step 0: Creating match key stored functions...")
        create_match_key_functions(conn)

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

        # Fix #10: Disable autocommit for transforms so we can roll back on failure
        logger.info("Step 4: Running optimized transforms (transactional)...")
        conn.autocommit(False)
        start_transform = time.time()
        try:
            run_transforms(conn, fullvoter_source_name=FULLVOTER_PATH.name, header=fv_header)
            conn.commit()
            transform_time = time.time() - start_transform
            logger.info(f"[OK] Transforms committed in {transform_time:.1f} seconds")
        except Exception:
            logger.error("Transform failed, rolling back...")
            conn.rollback()
            raise
        finally:
            conn.autocommit(True)

        # Step 5: Ensure census surname ethnicity data is loaded
        logger.info("Step 5: Ensuring census surname ethnicity data...")
        ensure_census_surnames(conn)

        # Step 6: Build materialized audience group + ethnicity tables
        logger.info("Step 6: Building materialized audience groups & ethnicity tables...")
        conn.autocommit(False)
        start_aud = time.time()
        try:
            build_audience_groups(conn)
            conn.commit()
            aud_time = time.time() - start_aud
            logger.info(f"[OK] Audience groups committed in {aud_time:.1f} seconds")
        except Exception:
            logger.error("Audience group build failed, rolling back...")
            conn.rollback()
            raise
        finally:
            conn.autocommit(True)

        # Fix #5: Write fingerprints ONLY after everything succeeds
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
        logger.info("Fingerprints saved - pipeline state is consistent")

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
        # Fix #11: Always release advisory lock
        try:
            exec_sql(conn, "SELECT RELEASE_LOCK('nys_voter_pipeline');")
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYS Voter Tagging Pipeline")
    parser.add_argument("--rebuild-groups", action="store_true",
                        help="Skip Steps 0-4; re-materialize audience groups & ethnicity tables only (fast)")
    args = parser.parse_args()

    start_time = time.time()
    try:
        main(rebuild_groups=args.rebuild_groups)
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Total runtime: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")