#!/usr/bin/env python3
"""
sync/aiven_sync.py - Push local MySQL databases to Aiven remote
================================================================
Supports syncing the primary voter database (nys_voter_tagging) with change
detection, plus full-database dumps for donor and CRM databases.

Change detection:  row count + CHECKSUM TABLE fingerprint stored in _sync_state.
If both match -> skip.  Any change -> full mysqldump re-sync.

Usage:
    python main.py sync                       # sync nys_voter_tagging (voter + summary)
    python main.py sync --tables voter        # voter_file only
    python main.py sync --tables summary      # count/summary tables only
    python main.py sync --full                # force re-sync even if fingerprint matches
    python main.py sync --all-databases       # sync ALL databases to Aiven
    python main.py sync --databases boe_donors National_Donors  # specific DBs only
"""

import os, sys, time, logging, re, subprocess, shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db import get_conn, get_aiven_conn, DB_USER, DB_PASSWORD

logger = logging.getLogger("aiven_sync")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)])

SOURCE_DB  = "nys_voter_tagging"
DUMP_FILE  = Path("D:/voter_file_dump.sql")
CHUNK_SIZE = 5_000
SUMMARY_TABLES = ["counts_ld_audience","counts_sd_audience","counts_cd_audience","counts_state_audience"]

# All databases available for sync (besides nys_voter_tagging which is the default)
EXTRA_DATABASES = ["boe_donors", "National_Donors", "cfb_donors", "crm_unified"]

def _find_mysql_bin(exe):
    for c in [rf"C:\Program Files\MySQL\MySQL Server 8.4\bin\{exe}",
               rf"C:\Program Files\MySQL\MySQL Server 8.0\bin\{exe}"]:
        if Path(c).exists(): return c
    found = shutil.which(exe)
    if found: return found
    raise FileNotFoundError(f"Cannot find {exe}. Add MySQL bin dir to PATH.")

MYSQLDUMP = _find_mysql_bin("mysqldump.exe")
MYSQL_CLI = _find_mysql_bin("mysql.exe")

SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS _sync_state (
    table_name   VARCHAR(100)  NOT NULL,
    last_sync    DATETIME      NOT NULL,
    rows_synced  BIGINT        DEFAULT 0,
    checksum     BIGINT        DEFAULT NULL,
    status       ENUM('running','complete','failed','skipped') DEFAULT 'running',
    PRIMARY KEY (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def get_columns(conn, table, database=SOURCE_DB):
    cur = conn.cursor()
    cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                (database, table))
    cols = [r[0] for r in cur.fetchall()]
    cur.close(); return cols

def get_create_table(local_conn, table):
    cur = local_conn.cursor()
    cur.execute(f"SHOW CREATE TABLE `{table}`")
    ddl = cur.fetchone()[1]; cur.close()
    ddl = re.sub(r'/\*![\s\S]*?PARTITION BY[\s\S]*?\*/', '', ddl, flags=re.IGNORECASE).strip()
    return ddl.replace('utf8mb4_0900_ai_ci', 'utf8mb4_unicode_ci')

def table_exists(conn, table):
    cur = conn.cursor(); cur.execute("SHOW TABLES LIKE %s", (table,))
    exists = cur.fetchone() is not None; cur.close(); return exists

def get_row_count(conn, table):
    cur = conn.cursor(); cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    count = cur.fetchone()[0]; cur.close(); return count

def get_checksum(conn, table):
    """CHECKSUM TABLE - fast CRC fingerprint for change detection."""
    cur = conn.cursor(); cur.execute(f"CHECKSUM TABLE `{table}`")
    result = cur.fetchone(); cur.close()
    return result[1] if result else None

def get_sync_state(aiven_conn, table):
    cur = aiven_conn.cursor()
    cur.execute("SELECT last_sync, rows_synced, checksum, status FROM _sync_state WHERE table_name=%s", (table,))
    row = cur.fetchone(); cur.close(); return row

def update_sync_state(aiven_conn, table, status, rows_synced=0, checksum=None):
    cur = aiven_conn.cursor()
    cur.execute("""INSERT INTO _sync_state (table_name, last_sync, rows_synced, checksum, status)
        VALUES (%s, NOW(), %s, %s, %s)
        ON DUPLICATE KEY UPDATE last_sync=NOW(), rows_synced=%s, checksum=%s, status=%s""",
        (table, rows_synced, checksum, status, rows_synced, checksum, status))
    aiven_conn.commit(); cur.close()

def voter_file_changed(local_conn, aiven_conn, force_full):
    """Returns (changed, local_rows, local_checksum). Compares row count + CRC."""
    if force_full:
        logger.info("[voter_file] --full flag set, forcing re-sync.")
        return True, get_row_count(local_conn, "voter_file"), get_checksum(local_conn, "voter_file")

    logger.info("[voter_file] Computing fingerprint (row count + checksum)...")
    t0 = time.time()
    local_rows = get_row_count(local_conn, "voter_file")
    local_csum = get_checksum(local_conn, "voter_file")
    logger.info(f"[voter_file] Local: {local_rows:,} rows, checksum={local_csum} ({time.time()-t0:.1f}s)")

    state = get_sync_state(aiven_conn, "voter_file")
    if state is None:
        logger.info("[voter_file] No previous sync record - first run, will sync.")
        return True, local_rows, local_csum

    _, synced_rows, synced_csum, status = state
    if status != "complete":
        logger.info(f"[voter_file] Last sync status={status}, will re-sync.")
        return True, local_rows, local_csum

    if synced_rows == local_rows and synced_csum == local_csum:
        logger.info(f"[voter_file] No change since last sync (rows={synced_rows:,}, checksum={synced_csum}). Skipping.")
        return False, local_rows, local_csum

    logger.info(f"[voter_file] Change detected: rows {synced_rows:,} -> {local_rows:,}, checksum {synced_csum} -> {local_csum}")
    return True, local_rows, local_csum


def sync_voter_file_via_dump(local_rows, local_csum, aiven_conn):
    """Dump voter_file locally then stream import to Aiven."""
    cfg = {
        "host":       os.getenv("AIVEN_HOST"),
        "port":       os.getenv("AIVEN_PORT", "28808"),
        "user":       os.getenv("AIVEN_USER", "avnadmin"),
        "password":   os.getenv("AIVEN_PASSWORD", ""),
        "db":         os.getenv("AIVEN_DB", "nys_voter_tagging"),
        "ssl_ca":     os.getenv("AIVEN_SSL_CA", ""),
        "local_pw":   DB_PASSWORD,
        "local_user": DB_USER,
    }
    update_sync_state(aiven_conn, "voter_file", "running")

    # Phase 1: dump
    logger.info(f"[voter_file] Phase 1: mysqldump -> {DUMP_FILE} ...")
    t0 = time.time()
    dump_cmd = [MYSQLDUMP, f"-u{cfg['local_user']}", f"-p{cfg['local_pw']}",
        "--single-transaction", "--quick", "--extended-insert",
        "--disable-keys", "--add-drop-table", "--skip-triggers",
        "--max-allowed-packet=256M", SOURCE_DB, "voter_file"]
    with open(DUMP_FILE, "w", encoding="utf-8") as f:
        result = subprocess.run(dump_cmd, stdout=f, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logger.error(f"[voter_file] mysqldump failed:\n{result.stderr}")
        update_sync_state(aiven_conn, "voter_file", "failed")
        raise RuntimeError("mysqldump failed")
    dump_mb = DUMP_FILE.stat().st_size / 1_048_576
    logger.info(f"[voter_file] Dump complete: {dump_mb:,.1f} MB in {(time.time()-t0)/60:.1f} min")

    # Phase 2: import
    logger.info("[voter_file] Phase 2: importing to Aiven ...")
    t1 = time.time()
    import_cmd = [MYSQL_CLI,
        f"--host={cfg['host']}", f"--port={cfg['port']}",
        f"--user={cfg['user']}", f"-p{cfg['password']}",
        f"--ssl-ca={cfg['ssl_ca']}", "--max-allowed-packet=256M",
        "--connect-timeout=60", cfg['db']]
    with open(DUMP_FILE, "r", encoding="utf-8") as f:
        result = subprocess.run(import_cmd, stdin=f, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logger.error(f"[voter_file] mysql import failed:\n{result.stderr}")
        update_sync_state(aiven_conn, "voter_file", "failed")
        raise RuntimeError("Aiven import failed")
    logger.info(f"[voter_file] Import complete in {(time.time()-t1)/60:.1f} min")

    try: DUMP_FILE.unlink(); logger.info("[voter_file] Removed temp dump file.")
    except Exception: pass

    update_sync_state(aiven_conn, "voter_file", "complete",
                      rows_synced=local_rows, checksum=local_csum)
    logger.info(f"[voter_file] Done - {local_rows:,} rows synced in {(time.time()-t0)/60:.1f} min total.")

def sync_summary_table(local_conn, aiven_conn, table):
    logger.info(f"[{table}] Syncing summary table (full replace)...")
    if not table_exists(local_conn, table):
        logger.warning(f"[{table}] Not found locally, skipping."); return

    ddl = get_create_table(local_conn, table)
    cur = aiven_conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute(ddl); aiven_conn.commit(); cur.close()

    cols = get_columns(local_conn, table, SOURCE_DB)
    col_list = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    local_cur = local_conn.cursor()
    local_cur.execute(f"SELECT {col_list} FROM `{table}`")
    rows = local_cur.fetchall(); local_cur.close()

    if rows:
        aiven_cur = aiven_conn.cursor()
        for i in range(0, len(rows), CHUNK_SIZE):
            aiven_cur.executemany(
                f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})",
                rows[i:i + CHUNK_SIZE])
        aiven_conn.commit(); aiven_cur.close()

    logger.info(f"[{table}] Done - {len(rows):,} rows synced.")
    update_sync_state(aiven_conn, table, "complete", len(rows))


def sync_full_database(database, force_full=False):
    """Dump an entire local database and import it to Aiven.

    Creates the database on Aiven if it doesn't exist, then streams the
    full mysqldump.  Collation is patched from 0900_ai_ci -> unicode_ci
    for Aiven MySQL 8.0 compatibility.
    """
    cfg = {
        "host":       os.getenv("AIVEN_HOST"),
        "port":       os.getenv("AIVEN_PORT", "28808"),
        "user":       os.getenv("AIVEN_USER", "avnadmin"),
        "password":   os.getenv("AIVEN_PASSWORD", ""),
        "ssl_ca":     os.getenv("AIVEN_SSL_CA", ""),
        "local_pw":   DB_PASSWORD,
        "local_user": DB_USER,
    }
    dump_path = Path(f"D:/{database}_dump.sql")

    logger.info(f"[{database}] ── Full database sync ──")

    # Create database on Aiven if needed
    logger.info(f"[{database}] Ensuring database exists on Aiven...")
    aiven_admin = get_aiven_conn(database=None)
    cur = aiven_admin.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    aiven_admin.commit()
    cur.close()
    aiven_admin.close()

    # Connect to the specific database on Aiven for sync state tracking
    aiven_conn = get_aiven_conn(database)
    cur = aiven_conn.cursor()
    cur.execute(SYNC_STATE_DDL)
    aiven_conn.commit()
    cur.close()

    # Phase 1: mysqldump the entire database
    logger.info(f"[{database}] Phase 1: mysqldump -> {dump_path} ...")
    t0 = time.time()
    dump_cmd = [MYSQLDUMP, f"-u{cfg['local_user']}", f"-p{cfg['local_pw']}",
        "--single-transaction", "--quick", "--extended-insert",
        "--disable-keys", "--add-drop-table", "--skip-triggers",
        "--max-allowed-packet=256M", "--databases", database]
    with open(dump_path, "w", encoding="utf-8") as f:
        result = subprocess.run(dump_cmd, stdout=f, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logger.error(f"[{database}] mysqldump failed:\n{result.stderr}")
        update_sync_state(aiven_conn, f"db:{database}", "failed")
        aiven_conn.close()
        raise RuntimeError(f"mysqldump failed for {database}")

    dump_mb = dump_path.stat().st_size / 1_048_576
    logger.info(f"[{database}] Dump complete: {dump_mb:,.1f} MB in {(time.time()-t0)/60:.1f} min")

    # Patch collation for Aiven compatibility
    logger.info(f"[{database}] Patching collation for Aiven 8.0 compatibility...")
    raw = dump_path.read_text(encoding="utf-8")
    raw = raw.replace("utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci")
    # Remove CREATE DATABASE / USE statements since we already created the DB
    raw = re.sub(r"CREATE DATABASE.*?;\n", "", raw)
    raw = re.sub(r"USE `.*?`;\n", "", raw)
    dump_path.write_text(raw, encoding="utf-8")

    # Phase 2: import to Aiven
    logger.info(f"[{database}] Phase 2: importing to Aiven ...")
    t1 = time.time()
    import_cmd = [MYSQL_CLI,
        f"--host={cfg['host']}", f"--port={cfg['port']}",
        f"--user={cfg['user']}", f"-p{cfg['password']}",
        f"--ssl-ca={cfg['ssl_ca']}", "--max-allowed-packet=256M",
        "--connect-timeout=60", database]
    with open(dump_path, "r", encoding="utf-8") as f:
        result = subprocess.run(import_cmd, stdin=f, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logger.error(f"[{database}] Aiven import failed:\n{result.stderr}")
        update_sync_state(aiven_conn, f"db:{database}", "failed")
        aiven_conn.close()
        raise RuntimeError(f"Aiven import failed for {database}")

    logger.info(f"[{database}] Import complete in {(time.time()-t1)/60:.1f} min")

    try: dump_path.unlink(); logger.info(f"[{database}] Removed temp dump file.")
    except Exception: pass

    update_sync_state(aiven_conn, f"db:{database}", "complete")
    elapsed = (time.time() - t0) / 60
    logger.info(f"[{database}] Done - full database synced in {elapsed:.1f} min total.")
    aiven_conn.close()


def main(tables="all", force_full=False, sync_databases=None):
    logger.info("=" * 60)
    logger.info(f"Aiven Sync  |  tables={tables}  force_full={force_full}")
    logger.info("=" * 60)

    if not os.getenv("AIVEN_HOST"):
        logger.error("AIVEN_HOST not set in .env - add Aiven credentials first.")
        sys.exit(1)

    logger.info("Connecting to local MySQL...")
    local_conn = get_conn(SOURCE_DB)
    logger.info("Connecting to Aiven MySQL...")
    try:
        aiven_conn = get_aiven_conn(SOURCE_DB)
    except Exception as e:
        logger.error(f"Aiven connection failed: {e}"); sys.exit(1)

    # Bootstrap _sync_state table on Aiven (adds checksum col if new schema)
    cur = aiven_conn.cursor()
    cur.execute(SYNC_STATE_DDL)
    # Add checksum column if upgrading from old schema
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_sync_state' AND COLUMN_NAME='checksum'
    """)
    if cur.fetchone()[0] == 0:
        cur.execute("ALTER TABLE _sync_state ADD COLUMN checksum BIGINT DEFAULT NULL AFTER rows_synced")
        cur.execute("ALTER TABLE _sync_state MODIFY COLUMN status ENUM('running','complete','failed','skipped') DEFAULT 'running'")
        logger.info("Migrated _sync_state schema (added checksum column).")
    aiven_conn.commit(); cur.close()

    t_total = time.time()

    if tables in ("all", "voter"):
        changed, local_rows, local_csum = voter_file_changed(local_conn, aiven_conn, force_full)
        if changed:
            sync_voter_file_via_dump(local_rows, local_csum, aiven_conn)
        else:
            update_sync_state(aiven_conn, "voter_file", "skipped",
                              rows_synced=local_rows, checksum=local_csum)

    if tables in ("all", "summary"):
        for t in SUMMARY_TABLES:
            try:
                sync_summary_table(local_conn, aiven_conn, t)
            except Exception as e:
                logger.error(f"[{t}] FAILED: {e}")

    elapsed = (time.time() - t_total) / 60
    logger.info("=" * 60)
    logger.info(f"nys_voter_tagging sync finished in {elapsed:.1f} min")
    logger.info("=" * 60)
    local_conn.close(); aiven_conn.close()

    # ── Extra databases ──────────────────────────────────────────────────
    if sync_databases:
        for db in sync_databases:
            logger.info("")
            try:
                sync_full_database(db, force_full=force_full)
            except Exception as e:
                logger.error(f"[{db}] FAILED: {e}")

        logger.info("=" * 60)
        logger.info("All database syncs complete.")
        logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--tables", choices=["all", "voter", "summary"], default="all")
    p.add_argument("--full", action="store_true", help="Force re-sync even if unchanged")
    p.add_argument("--all-databases", action="store_true",
                   help="Sync ALL databases (boe_donors, National_Donors, cfb_donors, crm_unified)")
    p.add_argument("--databases", nargs="+", metavar="DB",
                   help="Sync specific extra databases by name")
    args = p.parse_args()

    dbs = None
    if args.all_databases:
        dbs = list(EXTRA_DATABASES)
    elif args.databases:
        dbs = args.databases

    main(tables=args.tables, force_full=args.full, sync_databases=dbs)
