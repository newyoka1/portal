#!/usr/bin/env python3
"""
fec_classify_committees.py
===========================
Downloads FEC committee master (cm.zip) and PAC->candidate disbursements (itpas2.zip),
infers party affiliation for unaffiliated PACs/JFCs, and tags every row in
fec_contributions with a resolved party.

Party resolution tiers:
  D_EXPLICIT   / R_EXPLICIT   -- committee file has party = D or R
  D_NAME       / R_NAME       -- committee name contains strong party keywords
  D_INFERRED   / R_INFERRED   -- 80%+ of disbursements went to D or R candidates
  D_MIXED      / R_MIXED      -- 60-79% lean
  MIXED                       -- genuinely split
  UNKNOWN                     -- no data to classify

Adds to fec_contributions:
  cmte_party          -- raw party from committee master
  cmte_party_resolved -- final resolved party signal
"""

import os, sys, zipfile, urllib.request
sys.path.insert(0, r"C:\Users\georg_2r965zq\AppData\Roaming\Python\Python314\site-packages")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)
import mysql.connector

TARGET_DB    = "FEC_NEW"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fec_downloads")

DEM_KEYWORDS = [
    "DEMOCRAT", "DEMOCRATIC", "DNC", "DCCC", "DSCC", "DLCC",
    "BIDEN", "HARRIS", "OBAMA", "CLINTON", "PELOSI", "SCHUMER",
    "ACT BLUE", "ACTBLUE", "EMILY", "MOVE ON", "MOVEON",
    "PROGRESSIVE", "INDIVISIBLE", "SWING LEFT", "HIGHER HEIGHTS",
    "VICTORY FUND"
]
REP_KEYWORDS = [
    "REPUBLICAN", "GOP", "RNC", "NRCC", "NRSC", "RSLC",
    "TRUMP", "MAGA", "SAVE AMERICA", "AMERICA FIRST",
    "TEA PARTY", "FREEDOM CAUCUS", "HERITAGE", "PATRIOT",
    "WIN RED", "WINRED", "REAGAN"
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


def progress(block_num, block_size, total_size):
    if total_size > 0:
        pct = min(100, block_num * block_size * 100 / total_size)
        mb = block_num * block_size / 1024 / 1024
        print(f"\r  Downloaded: {mb:.0f} MB ({pct:.0f}%)", end="", flush=True)


def download(url, dest):
    if os.path.exists(dest):
        print(f"  Already downloaded: {os.path.basename(dest)}")
        return
    print(f"  Downloading {os.path.basename(dest)}...")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    urllib.request.urlretrieve(url, dest, reporthook=progress)
    print()


def add_columns():
    conn = get_conn(TARGET_DB)
    cur = conn.cursor()
    for col, defn in [
        ("cmte_party",          "VARCHAR(3)  DEFAULT NULL"),
        ("cmte_party_resolved", "VARCHAR(15) DEFAULT NULL"),
    ]:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME='fec_contributions' AND COLUMN_NAME=%s
        """, (TARGET_DB, col))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE fec_contributions ADD COLUMN {col} {defn}")
            print(f"  Added column: {col}")
        else:
            print(f"  Column exists: {col}")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fec_committee_party (
            cmte_id             VARCHAR(9) PRIMARY KEY,
            cmte_name           VARCHAR(200),
            cmte_party_raw      VARCHAR(3),
            cmte_party_resolved VARCHAR(15),
            INDEX idx_resolved (cmte_party_resolved)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_0900_ai_ci
    """)
    conn.commit()
    conn.close()


def load_committee_master():
    url  = "https://www.fec.gov/files/bulk-downloads/data/cm.zip"
    dest = os.path.join(DOWNLOAD_DIR, "cm.zip")
    download(url, dest)
    print("  Parsing committee master...")
    committees = {}
    with zipfile.ZipFile(dest) as zf:
        for fname in zf.namelist():
            if fname.endswith(".txt"):
                with zf.open(fname) as f:
                    for line in f:
                        row = line.decode("latin-1").rstrip("\n").split("|")
                        if len(row) < 11:
                            continue
                        cmte_id   = row[0].strip()
                        cmte_name = row[1].strip()
                        party_raw = row[10].strip() if len(row) > 10 else ""
                        if cmte_id:
                            committees[cmte_id] = {"name": cmte_name, "party_raw": party_raw}
    print(f"  Loaded {len(committees):,} committees")
    return committees


def classify_by_name(name):
    name_up = name.upper()
    dem = any(k in name_up for k in DEM_KEYWORDS)
    rep = any(k in name_up for k in REP_KEYWORDS)
    if dem and not rep:
        return "D_NAME"
    if rep and not dem:
        return "R_NAME"
    return None


def build_disbursement_signals():
    cn_url  = "https://www.fec.gov/files/bulk-downloads/data/cn.zip"
    cn_dest = os.path.join(DOWNLOAD_DIR, "cn.zip")
    download(cn_url, cn_dest)
    print("  Parsing candidate master...")
    cand_party = {}
    with zipfile.ZipFile(cn_dest) as zf:
        for fname in zf.namelist():
            if fname.endswith(".txt"):
                with zf.open(fname) as f:
                    for line in f:
                        row = line.decode("latin-1").rstrip("\n").split("|")
                        if len(row) >= 6:
                            cand_id = row[0].strip()
                            party   = row[2].strip()
                            if cand_id and party:
                                cand_party[cand_id] = party

    it_url  = "https://www.fec.gov/files/bulk-downloads/data/itpas2.zip"
    it_dest = os.path.join(DOWNLOAD_DIR, "itpas2.zip")
    download(it_url, it_dest)
    print("  Building disbursement signals...")
    signals = {}
    with zipfile.ZipFile(it_dest) as zf:
        for fname in zf.namelist():
            if fname.endswith(".txt"):
                with zf.open(fname) as f:
                    for line in f:
                        try:
                            row = line.decode("latin-1").rstrip("\n").split("|")
                            if len(row) < 16:
                                continue
                            giver     = row[0].strip()
                            recipient = row[15].strip()
                            party = cand_party.get(recipient)
                            if not party or not giver:
                                continue
                            if giver not in signals:
                                signals[giver] = {"D": 0, "R": 0, "O": 0}
                            if party in ("DEM", "D"):
                                signals[giver]["D"] += 1
                            elif party in ("REP", "R"):
                                signals[giver]["R"] += 1
                            else:
                                signals[giver]["O"] += 1
                        except:
                            continue
    print(f"  Disbursement signals for {len(signals):,} committees")
    return signals


def resolve_party(cmte_id, name, party_raw, signals):
    if party_raw in ("DEM", "D"):
        return "D_EXPLICIT"
    if party_raw in ("REP", "R"):
        return "R_EXPLICIT"
    name_signal = classify_by_name(name)
    if name_signal:
        return name_signal
    sig = signals.get(cmte_id)
    if sig:
        d, r, o = sig["D"], sig["R"], sig["O"]
        total = d + r + o
        if total == 0:
            return "UNKNOWN"
        d_pct = d / total
        r_pct = r / total
        if d_pct >= 0.80:
            return "D_INFERRED"
        if r_pct >= 0.80:
            return "R_INFERRED"
        if d_pct >= 0.60:
            return "D_MIXED"
        if r_pct >= 0.60:
            return "R_MIXED"
        return "MIXED"
    return "UNKNOWN"


def write_classifications(committees, signals):
    print(f"\nClassifying {len(committees):,} committees...")
    conn = get_conn(TARGET_DB)
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE fec_committee_party")

    batch = []
    counts = {"D_EXPLICIT":0,"R_EXPLICIT":0,"D_NAME":0,"R_NAME":0,
              "D_INFERRED":0,"D_MIXED":0,"R_INFERRED":0,"R_MIXED":0,
              "MIXED":0,"UNKNOWN":0}

    for cmte_id, info in committees.items():
        resolved = resolve_party(cmte_id, info["name"], info["party_raw"], signals)
        batch.append((cmte_id, info["name"][:200], info["party_raw"] or None, resolved))
        counts[resolved] = counts.get(resolved, 0) + 1

        if len(batch) >= 5000:
            cur.executemany("""
                INSERT INTO fec_committee_party (cmte_id, cmte_name, cmte_party_raw, cmte_party_resolved)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    cmte_name=VALUES(cmte_name),
                    cmte_party_raw=VALUES(cmte_party_raw),
                    cmte_party_resolved=VALUES(cmte_party_resolved)
            """, batch)
            conn.commit()
            batch = []

    if batch:
        cur.executemany("""
            INSERT INTO fec_committee_party (cmte_id, cmte_name, cmte_party_raw, cmte_party_resolved)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                cmte_name=VALUES(cmte_name),
                cmte_party_raw=VALUES(cmte_party_raw),
                cmte_party_resolved=VALUES(cmte_party_resolved)
        """, batch)
        conn.commit()

    print("\n  Classification breakdown:")
    for k, v in counts.items():
        print(f"    {k:20s} {v:>8,}")

    print("\nTagging fec_contributions with resolved party...")
    cur.execute("""
        UPDATE fec_contributions fc
        JOIN fec_committee_party cp ON fc.cmte_id = cp.cmte_id
        SET fc.cmte_party          = cp.cmte_party_raw,
            fc.cmte_party_resolved = cp.cmte_party_resolved
    """)
    conn.commit()
    print(f"  Rows tagged: {cur.rowcount:,}")

    cur.execute("""
        SELECT cmte_party_resolved, COUNT(*), SUM(transaction_amt)
        FROM fec_contributions
        GROUP BY cmte_party_resolved
        ORDER BY COUNT(*) DESC
    """)
    print("\n  Contributions by resolved party:")
    for row in cur.fetchall():
        print(f"    {str(row[0]):20s}  {row[1]:>8,} rows  ${row[2] or 0:>14,.0f}")

    conn.close()


def main():
    print("=" * 55)
    print("FEC Committee Party Classification")
    print("=" * 55)
    add_columns()
    committees = load_committee_master()
    signals    = build_disbursement_signals()
    write_classifications(committees, signals)
    print("\nDone!")


if __name__ == "__main__":
    main()
