# -*- coding: utf-8 -*-
"""
add_donor_year_detail.py
Adds donor_last_date + per-year D/R/U donation amounts (2018-2024) to voter_file.
Reads credentials from .env in same directory.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import time
import os
from pathlib import Path

def load_env():
    env = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

def get_conn(env, db="nys_voter_tagging"):
    return get_conn('nys_voter_tagging'),
        port=int(env.get("MYSQL_PORT", 3306)),
        user=env.get("MYSQL_USER", "root"),
        password=env.get("MYSQL_PASSWORD", ""),
        database=db,
        charset="utf8mb4",
        connection_timeout=600
    )

def run(cur, sql, label=""):
    t = time.time()
    cur.execute(sql)
    elapsed = time.time() - t
    print(f"  [{elapsed:.1f}s] {label}")

def main():
    env = load_env()
    conn = get_conn(env)
    conn.autocommit = False
    cur = conn.cursor()

    print("=" * 60)
    print("add_donor_year_detail.py")
    print("Adds donor_last_date + 2018-2024 per-year D/R/U amounts")
    print("=" * 60)

    # ── Step 1: Add columns ──────────────────────────────────────
    print("\nStep 1: Adding columns to voter_file...")
    new_cols = [
        ("donor_last_date", "DATE DEFAULT NULL"),
        ("donor_D2018amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2019amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2020amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2021amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2022amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2023amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2024amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2018amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2019amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2020amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2021amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2022amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2023amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2024amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2018amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2019amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2020amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2021amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2022amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2023amt",  "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2024amt",  "DECIMAL(14,2) DEFAULT 0"),
    ]
    cur.execute("SHOW COLUMNS FROM voter_file")
    existing = {r[0] for r in cur.fetchall()}

    for col, dtype in new_cols:
        if col in existing:
            print(f"    {col} already exists, skipping")
            continue
        run(cur, f"ALTER TABLE voter_file ADD COLUMN {col} {dtype}", f"add {col}")
        conn.commit()

    # ── Step 2: Detect last-date source ──────────────────────────
    print("\nStep 2: Checking stg_donor_matchkeys...")
    cur.execute("SHOW TABLES IN donors_2024 LIKE 'stg_donor_matchkeys'")
    has_mk = cur.fetchone() is not None
    print(f"  stg_donor_matchkeys exists: {has_mk}")

    # ── Step 3: Build staging ────────────────────────────────────
    print("\nStep 3: Building staging table...")
    run(cur, "DROP TABLE IF EXISTS _donor_detail_stg", "drop old staging")
    run(cur, """
        CREATE TABLE _donor_detail_stg (
            sboeid    VARCHAR(50) PRIMARY KEY,
            last_date DATE,
            D2018amt  DECIMAL(14,2), D2019amt DECIMAL(14,2), D2020amt DECIMAL(14,2),
            D2021amt  DECIMAL(14,2), D2022amt DECIMAL(14,2), D2023amt DECIMAL(14,2),
            D2024amt  DECIMAL(14,2),
            R2018amt  DECIMAL(14,2), R2019amt DECIMAL(14,2), R2020amt DECIMAL(14,2),
            R2021amt  DECIMAL(14,2), R2022amt DECIMAL(14,2), R2023amt DECIMAL(14,2),
            R2024amt  DECIMAL(14,2),
            U2018amt  DECIMAL(14,2), U2019amt DECIMAL(14,2), U2020amt DECIMAL(14,2),
            U2021amt  DECIMAL(14,2), U2022amt DECIMAL(14,2), U2023amt DECIMAL(14,2),
            U2024amt  DECIMAL(14,2)
        ) ENGINE=InnoDB
    """, "create staging")

    run(cur, """
        INSERT INTO _donor_detail_stg
            (sboeid,
             D2018amt,D2019amt,D2020amt,D2021amt,D2022amt,D2023amt,D2024amt,
             R2018amt,R2019amt,R2020amt,R2021amt,R2022amt,R2023amt,R2024amt,
             U2018amt,U2019amt,U2020amt,U2021amt,U2022amt,U2023amt,U2024amt)
        SELECT
            sboeid,
            SUM(boe_D2018amt),SUM(boe_D2019amt),SUM(boe_D2020amt),
            SUM(boe_D2021amt),SUM(boe_D2022amt),SUM(boe_D2023amt),SUM(boe_D2024amt),
            SUM(boe_R2018amt),SUM(boe_R2019amt),SUM(boe_R2020amt),
            SUM(boe_R2021amt),SUM(boe_R2022amt),SUM(boe_R2023amt),SUM(boe_R2024amt),
            SUM(boe_U2018amt),SUM(boe_U2019amt),SUM(boe_U2020amt),
            SUM(boe_U2021amt),SUM(boe_U2022amt),SUM(boe_U2023amt),SUM(boe_U2024amt)
        FROM donors_2024.ProvenDonors2024_BOEReclassified
        WHERE sboeid IS NOT NULL AND sboeid != ''
        GROUP BY sboeid
    """, "aggregate per-year amounts")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM _donor_detail_stg")
    print(f"  Staging rows: {cur.fetchone()[0]:,}")

    # ── Step 4: Populate last_date ───────────────────────────────
    print("\nStep 4: Populating last donation date...")
    if has_mk:
        run(cur, """
            UPDATE _donor_detail_stg s
            JOIN (
                SELECT mk.sboeid, MAX(b.SCHED_DATE) AS last_dt
                FROM donors_2024.stg_donor_matchkeys mk
                JOIN donors_2024.boe_contributions_raw b ON b.match_key = mk.match_key
                GROUP BY mk.sboeid
            ) ld ON ld.sboeid = s.sboeid
            SET s.last_date = ld.last_dt
        """, "update last_date via stg_donor_matchkeys")
    else:
        # Derive from boe_contributions_raw via sboeid in ProvenDonors column SCHED_DATE
        run(cur, """
            UPDATE _donor_detail_stg s
            JOIN (
                SELECT p.sboeid, MAX(b.SCHED_DATE) AS last_dt
                FROM donors_2024.ProvenDonors2024_BOEReclassified p
                JOIN donors_2024.boe_contributions_raw b
                  ON b.FILING_SCHED_ABBREV IS NOT NULL
                 AND b.match_key = p.sboeid
                WHERE p.sboeid IS NOT NULL
                GROUP BY p.sboeid
            ) ld ON ld.sboeid = s.sboeid
            SET s.last_date = ld.last_dt
        """, "update last_date via direct sboeid match on boe_contributions_raw")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM _donor_detail_stg WHERE last_date IS NOT NULL")
    n_dated = cur.fetchone()[0]
    print(f"  Rows with last_date: {n_dated:,}")

    # ── Step 5: Update voter_file ─────────────────────────
    print("\nStep 5: Updating voter_file...")
    run(cur, """
        UPDATE voter_file f
        JOIN _donor_detail_stg s ON s.sboeid = f.StateVoterId
        SET
            f.donor_last_date = s.last_date,
            f.donor_D2018amt  = COALESCE(s.D2018amt, 0),
            f.donor_D2019amt  = COALESCE(s.D2019amt, 0),
            f.donor_D2020amt  = COALESCE(s.D2020amt, 0),
            f.donor_D2021amt  = COALESCE(s.D2021amt, 0),
            f.donor_D2022amt  = COALESCE(s.D2022amt, 0),
            f.donor_D2023amt  = COALESCE(s.D2023amt, 0),
            f.donor_D2024amt  = COALESCE(s.D2024amt, 0),
            f.donor_R2018amt  = COALESCE(s.R2018amt, 0),
            f.donor_R2019amt  = COALESCE(s.R2019amt, 0),
            f.donor_R2020amt  = COALESCE(s.R2020amt, 0),
            f.donor_R2021amt  = COALESCE(s.R2021amt, 0),
            f.donor_R2022amt  = COALESCE(s.R2022amt, 0),
            f.donor_R2023amt  = COALESCE(s.R2023amt, 0),
            f.donor_R2024amt  = COALESCE(s.R2024amt, 0),
            f.donor_U2018amt  = COALESCE(s.U2018amt, 0),
            f.donor_U2019amt  = COALESCE(s.U2019amt, 0),
            f.donor_U2020amt  = COALESCE(s.U2020amt, 0),
            f.donor_U2021amt  = COALESCE(s.U2021amt, 0),
            f.donor_U2022amt  = COALESCE(s.U2022amt, 0),
            f.donor_U2023amt  = COALESCE(s.U2023amt, 0),
            f.donor_U2024amt  = COALESCE(s.U2024amt, 0)
    """, "UPDATE voter_file")
    conn.commit()

    # ── Results ──────────────────────────────────────────────────
    print("\n=== RESULTS ===")
    cur.execute("""
        SELECT
            COUNT(CASE WHEN donor_last_date IS NOT NULL THEN 1 END) as with_date,
            MIN(donor_last_date) as earliest,
            MAX(donor_last_date) as latest,
            SUM(donor_D2024amt) as D2024,
            SUM(donor_R2024amt) as R2024,
            SUM(donor_U2024amt) as U2024
        FROM voter_file
    """)
    r = cur.fetchone()
    print(f"  Rows with last_date:  {r[0]:,}")
    print(f"  Date range:           {r[1]} to {r[2]}")
    print(f"  2024 D total:         ${r[3]:,.0f}" if r[3] else "  2024 D total: $0")
    print(f"  2024 R total:         ${r[4]:,.0f}" if r[4] else "  2024 R total: $0")
    print(f"  2024 U total:         ${r[5]:,.0f}" if r[5] else "  2024 U total: $0")

    run(cur, "DROP TABLE IF EXISTS _donor_detail_stg", "cleanup staging")
    conn.commit()
    cur.close()
    conn.close()
    print("\nDone! 22 columns added to voter_file:")
    print("  donor_last_date")
    print("  donor_D/R/U 2018-2024 (21 columns)")

if __name__ == "__main__":
    t = time.time()
    main()
    print(f"\nTotal time: {(time.time()-t)/60:.1f} min")