# -*- coding: utf-8 -*-
"""
BOE Reclassification - Resume from Step 4
Steps 1-3 already completed. Picks up from the aggregation.
Minor party mapping:
  1 = Democrat
  2 = Republican
  3 = Conservative  -> R
  4 = Liberal       -> D
  5 = Right to Life -> R
  6 = Working Families -> D
  7 = Independence  -> U
  9 = Green         -> D (left-leaning)
  11 = Women's Equality -> D
  Other/NULL        -> U
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import time

YEARS = list(range(2018, 2025))

def run(cur, sql, label=""):
    t = time.time()
    cur.execute(sql)
    elapsed = time.time() - t
    print(f"  {label} ({elapsed:.1f}s)")

def main():
    conn = get_conn()
    cur = conn.cursor()

    print("=" * 60)
    print("BOE Reclassification - Resume Step 4")
    print("=" * 60)

    # â”€â”€ Update stg_filer_party with proper minor party mapping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nUpdating filer party lookup with full party mapping...")
    run(cur, "DROP TABLE IF EXISTS stg_filer_party", "drop old")
    run(cur, """
        CREATE TABLE stg_filer_party (
            FILER_ID VARCHAR(20) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY,
            party    CHAR(1)
        ) ENGINE=InnoDB
    """, "create table")
    run(cur, """
        INSERT INTO stg_filer_party (FILER_ID, party)
        SELECT
            CONVERT(FILER_ID USING utf8mb4) COLLATE utf8mb4_0900_ai_ci,
            CASE
                WHEN MAX(FilerParty) = 1  THEN 'D'   -- Democrat
                WHEN MAX(FilerParty) = 2  THEN 'R'   -- Republican
                WHEN MAX(FilerParty) = 3  THEN 'R'   -- Conservative
                WHEN MAX(FilerParty) = 4  THEN 'D'   -- Liberal
                WHEN MAX(FilerParty) = 5  THEN 'R'   -- Right to Life
                WHEN MAX(FilerParty) = 6  THEN 'D'   -- Working Families
                WHEN MAX(FilerParty) = 9  THEN 'D'   -- Green
                WHEN MAX(FilerParty) = 11 THEN 'D'   -- Women's Equality
                ELSE 'U'
            END
        FROM boe_filer_registry
        GROUP BY FILER_ID
    """, "insert with full party mapping")
    conn.commit()

    cur.execute("SELECT party, COUNT(*) FROM stg_filer_party GROUP BY party ORDER BY party")
    print("  Party distribution after mapping:")
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,} committees")

    # â”€â”€ Step 4: Aggregate by donor / year / party â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nStep 4: Building per-donor per-year reclassified amounts...")
    run(cur, "DROP TABLE IF EXISTS stg_boe_donor_party_year", "drop old")
    run(cur, """
        CREATE TABLE stg_boe_donor_party_year (
            sboeid        VARCHAR(20),
            election_year SMALLINT,
            party         CHAR(1),
            total_amt     DECIMAL(14,2),
            num_contribs  INT,
            INDEX idx_sboe (sboeid),
            INDEX idx_year (election_year)
        ) ENGINE=InnoDB
    """, "create staging table")
    run(cur, """
        INSERT INTO stg_boe_donor_party_year (sboeid, election_year, party, total_amt, num_contribs)
        SELECT
            v.StateVoterId          AS sboeid,
            b.ELECTION_YEAR,
            COALESCE(fp.party, 'U') AS party,
            SUM(b.ORG_AMT)          AS total_amt,
            COUNT(*)                AS num_contribs
        FROM boe_contributions_raw b
        JOIN voter_file v
            ON UPPER(TRIM(b.FLNG_ENT_LAST_NAME))  = UPPER(TRIM(v.LastName))
            AND UPPER(TRIM(b.FLNG_ENT_FIRST_NAME)) = UPPER(TRIM(v.FirstName))
            AND LEFT(TRIM(b.FLNG_ENT_ZIP), 5)      = LEFT(v.PrimaryZip, 5)
        LEFT JOIN stg_filer_party fp
            ON fp.FILER_ID = CONVERT(b.FILER_ID USING utf8mb4) COLLATE utf8mb4_0900_ai_ci
        WHERE b.FILING_SCHED_ABBREV = 'A'
          AND b.ELECTION_YEAR BETWEEN 2018 AND 2024
        GROUP BY v.StateVoterId, b.ELECTION_YEAR, COALESCE(fp.party, 'U')
    """, "aggregate (may take several minutes)")
    conn.commit()

    cur.execute("SELECT COUNT(DISTINCT sboeid) FROM stg_boe_donor_party_year")
    print(f"  Matched donors: {cur.fetchone()[0]:,}")

    cur.execute("""
        SELECT party, SUM(total_amt), SUM(num_contribs)
        FROM stg_boe_donor_party_year
        GROUP BY party ORDER BY party
    """)
    print("  Totals by party in staging:")
    for r in cur.fetchall():
        print(f"    {r[0]}: ${r[1]:,.0f}  ({r[2]:,} contributions)")

    # â”€â”€ Step 5: Pivot to wide format â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nStep 5: Pivoting into boe_donor_signals...")
    run(cur, "DROP TABLE IF EXISTS boe_donor_signals", "drop old")

    year_cols = []
    for y in YEARS:
        for p in ['D', 'R', 'U']:
            year_cols.append(
                f"SUM(CASE WHEN election_year={y} AND party='{p}' THEN total_amt  ELSE 0 END) AS boe_{p}{y}amt"
            )
            year_cols.append(
                f"SUM(CASE WHEN election_year={y} AND party='{p}' THEN num_contribs ELSE 0 END) AS boe_{p}{y}cnt"
            )

    pivot_sql = """
        CREATE TABLE boe_donor_signals AS
        SELECT
            sboeid,
            SUM(CASE WHEN party='D' THEN total_amt   ELSE 0 END) AS boe_total_D_amt,
            SUM(CASE WHEN party='R' THEN total_amt   ELSE 0 END) AS boe_total_R_amt,
            SUM(CASE WHEN party='U' THEN total_amt   ELSE 0 END) AS boe_total_U_amt,
            SUM(CASE WHEN party='D' THEN num_contribs ELSE 0 END) AS boe_total_D_cnt,
            SUM(CASE WHEN party='R' THEN num_contribs ELSE 0 END) AS boe_total_R_cnt,
            SUM(CASE WHEN party='U' THEN num_contribs ELSE 0 END) AS boe_total_U_cnt,
            """ + ",\n            ".join(year_cols) + """
        FROM stg_boe_donor_party_year
        GROUP BY sboeid
    """
    run(cur, pivot_sql, "pivot to wide format")
    run(cur, "ALTER TABLE boe_donor_signals ADD INDEX idx_sboe (sboeid)", "add index")
    conn.commit()

    # â”€â”€ Step 6: Final stats â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n\n=== RESULTS ===")
    cur.execute("SELECT COUNT(*) FROM boe_donor_signals")
    print(f"Donors in reclassified table: {cur.fetchone()[0]:,}")

    cur.execute("""
        SELECT
            SUM(boe_total_D_amt) as D_dollars,
            SUM(boe_total_R_amt) as R_dollars,
            SUM(boe_total_U_amt) as U_dollars
        FROM boe_donor_signals
    """)
    if r and r[0] is not None:
        print(f"  D: ${r[0]:,.0f}   R: ${r[1]:,.0f}   Remaining U: ${r[2]:,.0f}")
    else:
        print("  No donor totals (boe_contributions_raw is empty)")

    cur.execute("""
        SELECT
            CASE
                WHEN boe_total_D_amt > boe_total_R_amt AND boe_total_D_amt > boe_total_U_amt THEN 'Majority D'
                WHEN boe_total_R_amt > boe_total_D_amt AND boe_total_R_amt > boe_total_U_amt THEN 'Majority R'
                ELSE 'Majority U'
            END AS classification,
            COUNT(*) as donors,
            SUM(boe_total_D_amt + boe_total_R_amt + boe_total_U_amt) as total_dollars
        FROM boe_donor_signals
        GROUP BY 1 ORDER BY donors DESC
    """)
    print("\nReclassification breakdown:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,} donors   ${row[2]:,.0f}")

    print("\nDone! Tables created:")
    print("  boe_donor_signals  -- per-donor BOE amounts by party/year (join on sboeid)")
    print("  stg_boe_donor_party_year   -- staging (safe to drop)")
    print("  stg_donor_matchkeys        -- staging (safe to drop)")
    print("  stg_filer_party            -- staging (safe to drop)")

    cur.close()
    conn.close()

if __name__ == '__main__':
    t = time.time()
    main()
    print(f"\nTotal time: {(time.time()-t)/60:.1f} minutes")
