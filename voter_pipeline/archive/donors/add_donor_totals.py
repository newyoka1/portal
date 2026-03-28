# -*- coding: utf-8 -*-
"""
Add donor_D_amt, donor_R_amt, donor_U_amt to voter_file
Joins on StateVoterId = sboeid, aggregated from ProvenDonors2024_BOEReclassified
Only updates rows where a BOE match exists and amount > 0.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import time

def get_conn():
    return get_conn('nys_voter_tagging')

def run(cur, sql, label=""):
    t = time.time()
    cur.execute(sql)
    print(f"  {label} ({time.time()-t:.1f}s)")

def main():
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    print("=" * 60)
    print("Adding donor totals to voter_file")
    print("=" * 60)

    # Step 1: Add columns if not already there
    print("\nStep 1: Adding columns...")
    for col, dtype in [
        ("donor_D_amt", "DECIMAL(14,2) DEFAULT 0"),
        ("donor_R_amt", "DECIMAL(14,2) DEFAULT 0"),
        ("donor_U_amt", "DECIMAL(14,2) DEFAULT 0"),
    ]:
        try:
            run(cur, f"ALTER TABLE voter_file ADD COLUMN {col} {dtype}", f"add {col}")
            conn.commit()
        except Exception as e:
            print(f"  {col} already exists or error: {e}")
            conn.rollback()

    # Step 2: Build a staging aggregate from ProvenDonors (aggregated by sboeid
    # since ProvenDonors can have dupe sboeids)
    print("\nStep 2: Building aggregated donor totals staging table...")
    run(cur, "DROP TABLE IF EXISTS _donor_totals_stg", "drop old")
    run(cur, """
        CREATE TABLE _donor_totals_stg (
            sboeid      VARCHAR(50) PRIMARY KEY,
            total_D_amt DECIMAL(14,2),
            total_R_amt DECIMAL(14,2),
            total_U_amt DECIMAL(14,2)
        ) ENGINE=InnoDB
    """, "create staging")
    run(cur, """
        INSERT INTO _donor_totals_stg (sboeid, total_D_amt, total_R_amt, total_U_amt)
        SELECT
            sboeid,
            SUM(boe_total_D_amt) AS total_D_amt,
            SUM(boe_total_R_amt) AS total_R_amt,
            SUM(boe_total_U_amt) AS total_U_amt
        FROM nys_voter_tagging.ProvenDonors2024_BOEReclassified
        WHERE sboeid IS NOT NULL AND sboeid != ''
          AND (boe_total_D_amt > 0 OR boe_total_R_amt > 0 OR boe_total_U_amt > 0)
        GROUP BY sboeid
    """, "aggregate by sboeid")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM _donor_totals_stg")
    print(f"  Unique sboeids with amounts: {cur.fetchone()[0]:,}")

    # Step 3: Update voter_file via JOIN
    print("\nStep 3: Updating voter_file...")
    run(cur, """
        UPDATE voter_file f
        JOIN _donor_totals_stg d ON d.sboeid = f.StateVoterId
        SET
            f.donor_D_amt = d.total_D_amt,
            f.donor_R_amt = d.total_R_amt,
            f.donor_U_amt = d.total_U_amt
    """, "UPDATE (may take a minute on 6M rows)")
    conn.commit()

    # Step 4: Stats
    print("\n=== RESULTS ===")
    cur.execute("""
        SELECT
            COUNT(*) as updated_rows,
            SUM(donor_D_amt) as total_D,
            SUM(donor_R_amt) as total_R,
            SUM(donor_U_amt) as total_U,
            COUNT(CASE WHEN donor_D_amt > 0 OR donor_R_amt > 0 OR donor_U_amt > 0 THEN 1 END) as donors_tagged
        FROM voter_file
    """)
    r = cur.fetchone()
    print(f"  Total rows in table:  {r[0]:,}")
    print(f"  Rows with amounts:    {r[4]:,}")
    print(f"  Total D donated:      ${r[1]:,.0f}")
    print(f"  Total R donated:      ${r[2]:,.0f}")
    print(f"  Total U donated:      ${r[3]:,.0f}")

    # Clean up staging
    run(cur, "DROP TABLE IF EXISTS _donor_totals_stg", "drop staging")
    conn.commit()

    print("\nDone! voter_file now has columns: donor_D_amt, donor_R_amt, donor_U_amt")
    cur.close()
    conn.close()

if __name__ == '__main__':
    t = time.time()
    main()
    print(f"\nTotal time: {(time.time()-t)/60:.1f} minutes")