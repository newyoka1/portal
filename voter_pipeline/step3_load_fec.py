#!/usr/bin/env python3
"""
step3_load_fec.py - Load FEC individual contributions into National_Donors

Loads NY-state individual contributions, committee master, candidate master,
and committee-to-candidate disbursements (pas2).

Hash-based change detection: skips if extracted files are unchanged.

Called by: python main.py national-enrich --refresh
"""
import csv, os, time, sys, hashlib
from pathlib import Path
from datetime import datetime

# Allow import from project root (utils/db)
sys.path.insert(0, str(Path(__file__).parent))
from utils.db import get_conn

# Fix CSV field size limit for large FEC files
_maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(_maxInt)
        break
    except OverflowError:
        _maxInt = int(_maxInt / 10)

# Relative path — works regardless of where the repo lives
EXTRACT_DIR = Path(__file__).parent / "data" / "fec_downloads" / "extracted"


# ---------------------------------------------------------------------------
# Hash-based change detection
# ---------------------------------------------------------------------------
def fec_files_hash() -> str:
    """Cheap metadata hash of every extracted FEC text file."""
    h = hashlib.md5()
    for f in sorted(EXTRACT_DIR.rglob("*.txt")):
        stat = f.stat()
        h.update(f"{f.relative_to(EXTRACT_DIR)}:{stat.st_size}:{stat.st_mtime}".encode())
    return h.hexdigest()


def get_stored_fec_hash(cur):
    """Return the last stored hash for 'fec_raw', or None."""
    try:
        cur.execute("SHOW TABLES LIKE 'load_metadata'")
        if not cur.fetchone():
            return None
        cur.execute(
            "SELECT file_hash FROM load_metadata "
            "WHERE load_type='fec_raw' ORDER BY load_date DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def clear_fec_hash(cur, conn):
    """Remove hash so a failed load forces full rebuild on next run."""
    try:
        cur.execute("DELETE FROM load_metadata WHERE load_type='fec_raw'")
        conn.commit()
    except Exception:
        pass


def store_fec_hash(cur, conn, fhash, total_rows):
    """Record successful FEC load."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS load_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            load_type   VARCHAR(50),
            file_hash   VARCHAR(32) NOT NULL,
            row_count   INT,
            load_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX(load_type, load_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    cur.execute(
        "INSERT INTO load_metadata (load_type, file_hash, row_count) "
        "VALUES ('fec_raw', %s, %s)",
        (fhash, total_rows))
    conn.commit()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def parse_date(s):
    """Parse MMDDYYYY -> YYYY-MM-DD, return None if invalid."""
    if not s or len(s) != 8:
        return None
    try:
        return f"{s[4:]}-{s[:2]}-{s[2:4]}"
    except Exception:
        return None


def parse_name(n):
    """Split 'LAST, FIRST' -> (first, last). Returns (None, None) on failure."""
    if not n:
        return None, None
    p = n.split(',', 1)
    return (p[1].strip() if len(p) > 1 else None, p[0].strip() if p else None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # 10-year window: 6 even-year FEC cycles ending at current/next cycle
    current_year  = datetime.now().year
    current_cycle = current_year if current_year % 2 == 0 else current_year + 1
    CYCLES = [current_cycle - (i * 2) for i in range(6)]

    print(f"Cycles ({len(CYCLES)} total, ~10 years): {', '.join(map(str, CYCLES))}")

    # Connect without a default database so we can CREATE it if needed
    conn = get_conn(database=None, autocommit=False)
    cur  = conn.cursor()

    print("\nCreating National_Donors database...")
    cur.execute("CREATE DATABASE IF NOT EXISTS National_Donors CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    cur.execute("ALTER DATABASE National_Donors CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    cur.execute("USE National_Donors")

    print("Creating tables...")
    cur.execute("""
CREATE TABLE IF NOT EXISTS fec_contributions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    committee_id VARCHAR(9),
    contributor_last_name VARCHAR(100),
    contributor_first_name VARCHAR(100),
    contributor_city VARCHAR(100),
    contributor_state CHAR(2) DEFAULT 'NY',
    contributor_zip VARCHAR(10),
    contributor_zip5 VARCHAR(5) AS (SUBSTRING(contributor_zip,1,5)) STORED,
    contribution_amount DECIMAL(12,2),
    contribution_date DATE,
    transaction_id VARCHAR(20) UNIQUE,
    cycle SMALLINT,
    party_signal VARCHAR(20) DEFAULT NULL,
    INDEX idx_name (contributor_last_name, contributor_first_name),
    INDEX idx_zip5 (contributor_zip5),
    INDEX idx_committee (committee_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS fec_committees (
    committee_id VARCHAR(9) PRIMARY KEY,
    committee_name VARCHAR(200),
    party_affiliation VARCHAR(3),
    committee_type VARCHAR(1),
    classified_party VARCHAR(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS fec_candidates (
    candidate_id VARCHAR(9) PRIMARY KEY,
    candidate_name VARCHAR(200),
    party VARCHAR(3),
    office VARCHAR(1),
    INDEX idx_party (party)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS committee_to_candidate (
    committee_id VARCHAR(9),
    candidate_id VARCHAR(9),
    contribution_amount DECIMAL(12,2),
    transaction_date DATE,
    INDEX idx_committee (committee_id),
    INDEX idx_candidate (candidate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")

    conn.commit()
    print("Tables ready\n")

    # ---- Change detection: skip if nothing new ------------------------------
    current_fec_hash = fec_files_hash()
    stored_fec_hash  = get_stored_fec_hash(cur)

    if stored_fec_hash == current_fec_hash:
        cur.execute("SELECT COUNT(*) FROM fec_contributions")
        contrib_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fec_committees")
        comm_count = cur.fetchone()[0]
        if contrib_count > 0 and comm_count > 0:
            print("FEC data unchanged -- skipping load")
            print(f"  fec_contributions: {contrib_count:,} rows")
            print(f"  fec_committees:    {comm_count:,} rows")
            conn.close()
            return
        else:
            print("Hash matches but tables empty -- forcing reload")

    # Clear hash before load so a crash forces retry next run
    clear_fec_hash(cur, conn)
    print("Loading FEC data...\n")

    # ---- Load committees ----------------------------------------------------
    print("Loading committee master files...")
    cm_files = list(EXTRACT_DIR.glob("cm*/cm.txt"))
    if cm_files:
        for cm_file in cm_files:
            print(f"  {cm_file.parent.name}...")
            batch, cnt = [], 0
            with open(cm_file, 'r', encoding='latin-1', errors='replace') as f:
                for row in csv.reader(f, delimiter='|'):
                    if len(row) < 10:
                        continue
                    cnt += 1
                    batch.append((
                        row[0][:9],
                        row[1][:200],
                        row[10][:3] if len(row) > 10 else None,
                        row[9][:1],
                    ))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT IGNORE INTO fec_committees VALUES (%s,%s,%s,%s,%s)",
                            [b + (None,) for b in batch])
                        conn.commit()
                        batch = []
                if batch:
                    cur.executemany(
                        "INSERT IGNORE INTO fec_committees VALUES (%s,%s,%s,%s,%s)",
                        [b + (None,) for b in batch])
                    conn.commit()
            print(f"    {cnt:,} committees")
    else:
        print("  WARNING: No committee files found")

    # ---- Load candidates ----------------------------------------------------
    print("\nLoading candidate master files...")
    cn_files = list(EXTRACT_DIR.glob("cn*/cn.txt"))
    if cn_files:
        for cn_file in cn_files:
            print(f"  {cn_file.parent.name}...")
            batch, cnt = [], 0
            with open(cn_file, 'r', encoding='latin-1', errors='replace') as f:
                for row in csv.reader(f, delimiter='|'):
                    if len(row) < 5:
                        continue
                    cnt += 1
                    batch.append((row[0][:9], row[1][:200], row[2][:3], row[4][:1]))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT IGNORE INTO fec_candidates VALUES (%s,%s,%s,%s)",
                            batch)
                        conn.commit()
                        batch = []
                if batch:
                    cur.executemany(
                        "INSERT IGNORE INTO fec_candidates VALUES (%s,%s,%s,%s)",
                        batch)
                    conn.commit()
            print(f"    {cnt:,} candidates")
    else:
        print("  WARNING: No candidate files found")

    # ---- Load committee-to-candidate (pas2) ---------------------------------
    # itpas2.txt layout: col0=CMTE_ID, col16=CAND_ID, col13=DATE, col14=AMT
    print("\nLoading committee-to-candidate disbursements (pas2)...")
    cur.execute("TRUNCATE TABLE committee_to_candidate")
    pas2_files = sorted(EXTRACT_DIR.glob("pas2*/itpas2.txt"))
    if pas2_files:
        total_pas2 = 0
        for pas2_file in pas2_files:
            print(f"  {pas2_file.parent.name}...")
            batch, cnt = [], 0
            with open(pas2_file, 'r', encoding='latin-1', errors='replace') as f:
                for row in csv.reader(f, delimiter='|'):
                    if len(row) < 17:
                        continue
                    cmte_id = row[0][:9]  if row[0]  else None
                    cand_id = row[16][:9] if row[16] else None
                    if not cmte_id or not cand_id:
                        continue
                    try:
                        amt = float(row[14]) if row[14] else 0.0
                    except Exception:
                        amt = 0.0
                    if amt <= 0:
                        continue
                    cnt += 1
                    batch.append((
                        cmte_id, cand_id, amt,
                        parse_date(row[13] if len(row) > 13 else ''),
                    ))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT IGNORE INTO committee_to_candidate VALUES (%s,%s,%s,%s)",
                            batch)
                        conn.commit()
                        batch = []
            if batch:
                cur.executemany(
                    "INSERT IGNORE INTO committee_to_candidate VALUES (%s,%s,%s,%s)",
                    batch)
                conn.commit()
            total_pas2 += cnt
            print(f"    {cnt:,} disbursements")
        print(f"  Total: {total_pas2:,} committee-to-candidate records")
    else:
        print("  WARNING: No pas2 files found - run step1 with pas2 in cycle list")
        print("  Unknown committee reclassification will be skipped in step4")

    # ---- Load individual contributions (NY only) ----------------------------
    print("\nLoading individual contributions (NY only)...")
    total_loaded = 0

    for cycle in CYCLES:
        yr  = str(cycle)[-2:]
        itc = EXTRACT_DIR / f"indiv{yr}" / "itcont.txt"
        if not itc.exists():
            print(f"  {cycle}: File not found, skipping")
            continue

        print(f"\n{cycle} ({cycle-1}-{cycle})...")
        batch, ny, tot = [], 0, 0
        start = time.time()

        try:
            with open(itc, 'r', encoding='latin-1', errors='replace') as f:
                for row in csv.reader(f, delimiter='|'):
                    tot += 1
                    if len(row) < 21:
                        continue
                    if (row[9] or '').strip().upper() != 'NY':
                        continue
                    ny += 1

                    first, last = parse_name(row[7] if len(row) > 7 else '')
                    dt  = parse_date(row[13] if len(row) > 13 else '')
                    try:
                        amt = float(row[14]) if len(row) > 14 and row[14] else 0.0
                    except Exception:
                        amt = 0.0

                    if amt <= 0:
                        continue

                    batch.append((
                        row[0][:9]  if row[0]  else None,
                        last, first,
                        row[8][:100]  if len(row) > 8  else None,
                        'NY',
                        row[10][:10]  if len(row) > 10 else None,
                        amt, dt,
                        row[16][:20]  if len(row) > 16 else None,
                        cycle,
                    ))

                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT IGNORE INTO fec_contributions "
                            "(committee_id, contributor_last_name, contributor_first_name, "
                            " contributor_city, contributor_state, contributor_zip, "
                            " contribution_amount, contribution_date, transaction_id, cycle) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            batch)
                        conn.commit()
                        rate = ny / (time.time() - start) if (time.time() - start) > 0 else 0
                        print(f"\r  {ny:,} NY ({tot:,} scanned, {rate:.0f}/sec)",
                              end='', flush=True)
                        batch = []

            if batch:
                cur.executemany(
                    "INSERT IGNORE INTO fec_contributions "
                    "(committee_id, contributor_last_name, contributor_first_name, "
                    " contributor_city, contributor_state, contributor_zip, "
                    " contribution_amount, contribution_date, transaction_id, cycle) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batch)
                conn.commit()

            print(f"\n  {ny:,} NY contributions")
            total_loaded += ny

        except Exception as e:
            print(f"\n  Error loading {cycle}: {e}")
            print("  Continuing with next cycle...")

    # Store hash now that load succeeded
    store_fec_hash(cur, conn, current_fec_hash, total_loaded)
    conn.close()

    print("\n" + "=" * 70)
    print("LOAD COMPLETE")
    print("=" * 70)
    print(f"\nTotal NY contributions: {total_loaded:,}")
    print(f"Cycles: {', '.join(map(str, CYCLES))}")
    print("\nNext: python step4_classify_parties.py")


if __name__ == "__main__":
    main()
