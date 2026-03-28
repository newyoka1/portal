#!/usr/bin/env python3
"""
fec_ingest.py
=============
Downloads FEC individual contribution bulk files from fec.gov,
filters to NY contributors only, and loads into FEC_NEW database.

Cycles ingested: 2020, 2022, 2024 (configurable via CYCLES below)

FEC bulk file URL pattern:
  https://www.fec.gov/files/bulk-downloads/{year}/indiv{yy}.zip

FEC columns (21, pipe-delimited, no header):
  CMTE_ID, AMNDT_IND, RPT_TP, TRANSACTION_PGI, IMAGE_NUM,
  TRANSACTION_TP, ENTITY_TP, NAME, CITY, STATE, ZIP_CODE,
  EMPLOYER, OCCUPATION, TRANSACTION_DT, TRANSACTION_AMT,
  OTHER_ID, TRAN_ID, FILE_NUM, MEMO_CD, MEMO_TEXT, SUB_ID
"""

import os, sys, zipfile, urllib.request, tempfile, csv, time
sys.path.insert(0, r"C:\Users\georg_2r965zq\AppData\Roaming\Python\Python314\site-packages")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)
import mysql.connector

# ── Config ────────────────────────────────────────────────────────────────────
CYCLES = [2020, 2022, 2024]   # election cycles to ingest
TARGET_STATE = "NY"
TARGET_DB = "FEC_NEW"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fec_downloads")
BATCH_SIZE = 10000

FEC_COLUMNS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state", "zip_code",
    "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id"
]

def get_conn(database=None):
    kwargs = dict(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
    )
    if database:
        kwargs["database"] = database
    return mysql.connector.connect(**kwargs)

def setup_database():
    print(f"Setting up {TARGET_DB} database...")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    conn.commit()
    conn.close()

    conn = get_conn(TARGET_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fec_contributions (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            cycle         SMALLINT NOT NULL,
            cmte_id       VARCHAR(9),
            amndt_ind     VARCHAR(1),
            rpt_tp        VARCHAR(3),
            transaction_pgi VARCHAR(5),
            image_num     VARCHAR(18),
            transaction_tp  VARCHAR(3),
            entity_tp     VARCHAR(3),
            name          VARCHAR(200),
            city          VARCHAR(30),
            state         CHAR(2),
            zip_code      VARCHAR(9),
            employer      VARCHAR(38),
            occupation    VARCHAR(38),
            transaction_dt DATE,
            transaction_amt DECIMAL(14,2),
            other_id      VARCHAR(9),
            tran_id       VARCHAR(32),
            file_num      BIGINT,
            memo_cd       VARCHAR(1),
            memo_text     VARCHAR(100),
            sub_id        BIGINT,
            INDEX idx_state  (state),
            INDEX idx_zip    (zip_code),
            INDEX idx_name   (name(50)),
            INDEX idx_cycle  (cycle)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci
    """)

    # Summary table - one row per unique contributor per cycle
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fec_ny_summary (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
            name            VARCHAR(200),
            city            VARCHAR(30),
            zip_code        VARCHAR(9),
            employer        VARCHAR(38),
            occupation      VARCHAR(38),
            total_amt       DECIMAL(14,2),
            contribution_cnt INT,
            first_cycle     SMALLINT,
            last_cycle      SMALLINT,
            INDEX idx_zip   (zip_code),
            INDEX idx_name  (name(50))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci
    """)

    conn.commit()
    conn.close()
    print(f"  Database and tables ready.")

def is_valid_zip(path):
    """Check zip is complete by verifying end-of-central-directory record."""
    try:
        with zipfile.ZipFile(path, "r") as zf:
            return len(zf.namelist()) > 0
    except Exception:
        return False

def download_cycle(cycle):
    yy = str(cycle)[2:]
    url = f"https://www.fec.gov/files/bulk-downloads/{cycle}/indiv{yy}.zip"
    zip_path = os.path.join(DOWNLOAD_DIR, f"indiv{yy}.zip")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    if os.path.exists(zip_path):
        if is_valid_zip(zip_path):
            print(f"  {cycle}: Already downloaded and valid, skipping.")
            return zip_path
        else:
            print(f"  {cycle}: Existing file is corrupt, re-downloading...")
            os.remove(zip_path)

    print(f"  {cycle}: Downloading from {url}...")
    print(f"  (This may take several minutes - files are 500MB+)")

    tmp_path = zip_path + ".part"
    try:
        req = urllib.request.urlopen(url)
        total = int(req.headers.get("Content-Length", 0))
        downloaded = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        with open(tmp_path, "wb") as f:
            while True:
                chunk = req.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = min(100, downloaded * 100 / total)
                    mb = downloaded / 1024 / 1024
                    print(f"\r  Downloaded: {mb:.0f} MB ({pct:.0f}%)", end="", flush=True)
        print()
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise RuntimeError(f"Download failed: {e}")

    if not is_valid_zip(tmp_path):
        os.remove(tmp_path)
        raise RuntimeError(f"Downloaded file is not a valid zip: {url}")

    os.rename(tmp_path, zip_path)
    print(f"  {cycle}: Download complete and verified.")
    return zip_path

def parse_date(dt_str):
    """Convert MMDDYYYY to YYYY-MM-DD"""
    dt_str = dt_str.strip() if dt_str else ""
    if len(dt_str) == 8:
        try:
            return f"{dt_str[4:8]}-{dt_str[0:2]}-{dt_str[2:4]}"
        except:
            pass
    return None

def load_cycle(cycle, zip_path):
    print(f"  {cycle}: Loading NY contributions into {TARGET_DB}...")
    conn = get_conn(TARGET_DB)
    cur = conn.cursor()

    # Delete existing rows for this cycle first
    cur.execute("DELETE FROM fec_contributions WHERE cycle = %s", (cycle,))
    conn.commit()

    rows_loaded = 0
    rows_skipped = 0
    batch = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        txt_files = [f for f in zf.namelist() if f.endswith(".txt")]
        print(f"  Files in zip: {txt_files}")

        for txt_file in txt_files:
            print(f"  Processing {txt_file}...")
            with zf.open(txt_file) as f:
                for line in f:
                    try:
                        row = line.decode("latin-1").rstrip("\n").split("|")
                        if len(row) < 21:
                            rows_skipped += 1
                            continue

                        state = row[9].strip()
                        if state != TARGET_STATE:
                            continue

                        # Parse amount
                        try:
                            amt = float(row[14].strip()) if row[14].strip() else None
                        except:
                            amt = None

                        batch.append((
                            cycle,
                            row[0].strip() or None,   # cmte_id
                            row[1].strip() or None,   # amndt_ind
                            row[2].strip() or None,   # rpt_tp
                            row[3].strip() or None,   # transaction_pgi
                            row[4].strip() or None,   # image_num
                            row[5].strip() or None,   # transaction_tp
                            row[6].strip() or None,   # entity_tp
                            row[7].strip() or None,   # name
                            row[8].strip() or None,   # city
                            state,                    # state
                            row[10].strip()[:5] or None,  # zip_code (5 digits)
                            row[11].strip() or None,  # employer
                            row[12].strip() or None,  # occupation
                            parse_date(row[13]),       # transaction_dt
                            amt,                       # transaction_amt
                            row[15].strip() or None,  # other_id
                            row[16].strip() or None,  # tran_id
                            int(row[17].strip()) if row[17].strip().isdigit() else None,  # file_num
                            row[18].strip() or None,  # memo_cd
                            row[19].strip() or None,  # memo_text
                            int(row[20].strip()) if row[20].strip().isdigit() else None,  # sub_id
                        ))

                        if len(batch) >= BATCH_SIZE:
                            cur.executemany("""
                                INSERT INTO fec_contributions
                                (cycle,cmte_id,amndt_ind,rpt_tp,transaction_pgi,image_num,
                                 transaction_tp,entity_tp,name,city,state,zip_code,
                                 employer,occupation,transaction_dt,transaction_amt,
                                 other_id,tran_id,file_num,memo_cd,memo_text,sub_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """, batch)
                            conn.commit()
                            rows_loaded += len(batch)
                            print(f"\r  Loaded: {rows_loaded:,} rows", end="", flush=True)
                            batch = []
                    except Exception as e:
                        rows_skipped += 1
                        continue

        # Final batch
        if batch:
            cur.executemany("""
                INSERT INTO fec_contributions
                (cycle,cmte_id,amndt_ind,rpt_tp,transaction_pgi,image_num,
                 transaction_tp,entity_tp,name,city,state,zip_code,
                 employer,occupation,transaction_dt,transaction_amt,
                 other_id,tran_id,file_num,memo_cd,memo_text,sub_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch)
            conn.commit()
            rows_loaded += len(batch)

    print(f"\n  {cycle}: Loaded {rows_loaded:,} NY rows ({rows_skipped:,} skipped)")
    conn.close()
    return rows_loaded

def build_summary():
    print("\nBuilding fec_ny_summary (one row per unique contributor)...")
    conn = get_conn(TARGET_DB)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fec_ny_summary")
    cur.execute("""
        INSERT INTO fec_ny_summary
            (name, city, zip_code, employer, occupation,
             total_amt, contribution_cnt, first_cycle, last_cycle)
        SELECT
            name,
            city,
            LEFT(zip_code, 5) as zip_code,
            employer,
            occupation,
            SUM(transaction_amt)  as total_amt,
            COUNT(*)              as contribution_cnt,
            MIN(cycle)            as first_cycle,
            MAX(cycle)            as last_cycle
        FROM fec_contributions
        WHERE entity_tp = 'IND'
          AND transaction_amt > 0
        GROUP BY name, city, LEFT(zip_code, 5), employer, occupation
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM fec_ny_summary")
    count = cur.fetchone()[0]
    print(f"  Summary rows: {count:,}")
    conn.close()

def main():
    print("=" * 55)
    print("FEC Individual Contributions Ingest - NY Only")
    print("=" * 55)

    setup_database()

    total = 0
    for cycle in CYCLES:
        print(f"\n--- Cycle {cycle} ---")
        zip_path = download_cycle(cycle)
        total += load_cycle(cycle, zip_path)

    build_summary()

    # Final counts
    conn = get_conn(TARGET_DB)
    cur = conn.cursor()
    cur.execute("SELECT cycle, COUNT(*), SUM(transaction_amt) FROM fec_contributions GROUP BY cycle ORDER BY cycle")
    print("\n=== Summary by Cycle ===")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,} rows  ${row[2]:,.0f} total")
    conn.close()

    print(f"\nDone! Total NY rows loaded: {total:,}")

if __name__ == "__main__":
    main()

