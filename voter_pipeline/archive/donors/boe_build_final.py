# -*- coding: utf-8 -*-
"""
Build ProvenDonors2024_BOEReclassified
======================================
Combines ProvenDonors2024OnePerInd (all original columns) with
boe_donor_signals (BOE transaction-level R/D/U by year).

Then copies the finished table to nys_voter_tagging database.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import time

TARGET_DB   = 'nys_voter_tagging'
TARGET_TABLE = 'ProvenDonors2024_BOEReclassified'
SOURCE_TABLE = 'ProvenDonors2024_BOEReclassified'

def run(cur, sql, label=""):
    t = time.time()
    cur.execute(sql)
    print(f"  {label} ({time.time()-t:.1f}s)")

def main():
    conn = get_conn('nys_voter_tagging')
    cur  = conn.cursor()

    print("=" * 60)
    print("Building ProvenDonors2024_BOEReclassified")
    print("=" * 60)

    # â”€â”€ Step 1: Build the merged table in nys_voter_tagging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nStep 1: Creating merged table in nys_voter_tagging...")
    run(cur, f"DROP TABLE IF EXISTS {SOURCE_TABLE}", "drop old")

    run(cur, f"""
        CREATE TABLE {SOURCE_TABLE} AS
        SELECT
            p.*,

            -- BOE lifetime totals
            COALESCE(b.boe_total_D_amt, 0) AS boe_total_D_amt,
            COALESCE(b.boe_total_R_amt, 0) AS boe_total_R_amt,
            COALESCE(b.boe_total_U_amt, 0) AS boe_total_U_amt,
            COALESCE(b.boe_total_D_cnt, 0) AS boe_total_D_cnt,
            COALESCE(b.boe_total_R_cnt, 0) AS boe_total_R_cnt,
            COALESCE(b.boe_total_U_cnt, 0) AS boe_total_U_cnt,

            -- BOE per-year D amounts
            COALESCE(b.boe_D2018amt, 0) AS boe_D2018amt,
            COALESCE(b.boe_D2019amt, 0) AS boe_D2019amt,
            COALESCE(b.boe_D2020amt, 0) AS boe_D2020amt,
            COALESCE(b.boe_D2021amt, 0) AS boe_D2021amt,
            COALESCE(b.boe_D2022amt, 0) AS boe_D2022amt,
            COALESCE(b.boe_D2023amt, 0) AS boe_D2023amt,
            COALESCE(b.boe_D2024amt, 0) AS boe_D2024amt,

            -- BOE per-year R amounts
            COALESCE(b.boe_R2018amt, 0) AS boe_R2018amt,
            COALESCE(b.boe_R2019amt, 0) AS boe_R2019amt,
            COALESCE(b.boe_R2020amt, 0) AS boe_R2020amt,
            COALESCE(b.boe_R2021amt, 0) AS boe_R2021amt,
            COALESCE(b.boe_R2022amt, 0) AS boe_R2022amt,
            COALESCE(b.boe_R2023amt, 0) AS boe_R2023amt,
            COALESCE(b.boe_R2024amt, 0) AS boe_R2024amt,

            -- BOE per-year U amounts
            COALESCE(b.boe_U2018amt, 0) AS boe_U2018amt,
            COALESCE(b.boe_U2019amt, 0) AS boe_U2019amt,
            COALESCE(b.boe_U2020amt, 0) AS boe_U2020amt,
            COALESCE(b.boe_U2021amt, 0) AS boe_U2021amt,
            COALESCE(b.boe_U2022amt, 0) AS boe_U2022amt,
            COALESCE(b.boe_U2023amt, 0) AS boe_U2023amt,
            COALESCE(b.boe_U2024amt, 0) AS boe_U2024amt,

            -- BOE per-year D counts
            COALESCE(b.boe_D2018cnt, 0) AS boe_D2018cnt,
            COALESCE(b.boe_D2019cnt, 0) AS boe_D2019cnt,
            COALESCE(b.boe_D2020cnt, 0) AS boe_D2020cnt,
            COALESCE(b.boe_D2021cnt, 0) AS boe_D2021cnt,
            COALESCE(b.boe_D2022cnt, 0) AS boe_D2022cnt,
            COALESCE(b.boe_D2023cnt, 0) AS boe_D2023cnt,
            COALESCE(b.boe_D2024cnt, 0) AS boe_D2024cnt,

            -- BOE per-year R counts
            COALESCE(b.boe_R2018cnt, 0) AS boe_R2018cnt,
            COALESCE(b.boe_R2019cnt, 0) AS boe_R2019cnt,
            COALESCE(b.boe_R2020cnt, 0) AS boe_R2020cnt,
            COALESCE(b.boe_R2021cnt, 0) AS boe_R2021cnt,
            COALESCE(b.boe_R2022cnt, 0) AS boe_R2022cnt,
            COALESCE(b.boe_R2023cnt, 0) AS boe_R2023cnt,
            COALESCE(b.boe_R2024cnt, 0) AS boe_R2024cnt,

            -- BOE per-year U counts
            COALESCE(b.boe_U2018cnt, 0) AS boe_U2018cnt,
            COALESCE(b.boe_U2019cnt, 0) AS boe_U2019cnt,
            COALESCE(b.boe_U2020cnt, 0) AS boe_U2020cnt,
            COALESCE(b.boe_U2021cnt, 0) AS boe_U2021cnt,
            COALESCE(b.boe_U2022cnt, 0) AS boe_U2022cnt,
            COALESCE(b.boe_U2023cnt, 0) AS boe_U2023cnt,
            COALESCE(b.boe_U2024cnt, 0) AS boe_U2024cnt,

            -- Derived: reclassified party signal
            CASE
                WHEN b.sboeid IS NULL THEN 'NO_BOE_MATCH'
                WHEN COALESCE(b.boe_total_D_amt,0) > COALESCE(b.boe_total_R_amt,0)
                 AND COALESCE(b.boe_total_D_amt,0) > COALESCE(b.boe_total_U_amt,0) THEN 'D'
                WHEN COALESCE(b.boe_total_R_amt,0) > COALESCE(b.boe_total_D_amt,0)
                 AND COALESCE(b.boe_total_R_amt,0) > COALESCE(b.boe_total_U_amt,0) THEN 'R'
                ELSE 'U'
            END AS boe_party_signal

        FROM ProvenDonors2024OnePerInd p
        LEFT JOIN boe_donor_signals b
            ON CONVERT(b.sboeid USING utf8mb4) COLLATE utf8mb4_unicode_ci
             = CONVERT(p.sboeid USING utf8mb4) COLLATE utf8mb4_unicode_ci
    """, "CREATE TABLE AS SELECT (may take a minute)")

    run(cur, f"ALTER TABLE {SOURCE_TABLE} ADD INDEX idx_sboeid (sboeid(20))", "add sboeid index")
    conn.commit()

    # â”€â”€ Quick stats â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    cur.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
    total = cur.fetchone()[0]
    print(f"\n  Total rows: {total:,}")

    cur.execute(f"""
        SELECT boe_party_signal, COUNT(*) as cnt,
               SUM(boe_total_D_amt + boe_total_R_amt + boe_total_U_amt) as dollars
        FROM {SOURCE_TABLE}
        GROUP BY boe_party_signal ORDER BY cnt DESC
    """)
    print("  Party signal breakdown:")
    for r in cur.fetchall():
        dollars = r[2] or 0
        print(f"    {r[0]:15s} {r[1]:,} donors   ${dollars:,.0f}")

    cur.close()
    conn.close()

    # â”€â”€ Step 2: Copy to nys_voter_tagging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print(f"\nStep 2: Copying to {TARGET_DB}.{TARGET_TABLE}...")
    conn2 = get_conn(TARGET_DB)
    cur2  = conn2.cursor()

    run(cur2, f"DROP TABLE IF EXISTS {TARGET_TABLE}", "drop old in target db")
    conn2.commit()

    # Use CREATE TABLE ... SELECT across databases
    run(cur2, f"""
        CREATE TABLE {TARGET_DB}.{TARGET_TABLE} AS
        SELECT * FROM nys_voter_tagging.{SOURCE_TABLE}
    """, "cross-db copy (this will take a few minutes)")

    run(cur2, f"ALTER TABLE {TARGET_TABLE} ADD INDEX idx_sboeid (sboeid(20))", "add index")
    conn2.commit()

    cur2.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    print(f"\n  Rows in {TARGET_DB}.{TARGET_TABLE}: {cur2.fetchone()[0]:,}")

    cur2.close()
    conn2.close()

    print(f"\nDone! Table available in both:")
    print(f"  nys_voter_tagging.{SOURCE_TABLE}")
    print(f"  {TARGET_DB}.{TARGET_TABLE}")

if __name__ == '__main__':
    t = time.time()
    main()
    print(f"\nTotal time: {(time.time()-t)/60:.1f} minutes")
