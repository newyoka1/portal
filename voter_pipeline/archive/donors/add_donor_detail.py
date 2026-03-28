# -*- coding: utf-8 -*-
"""
donors/add_donor_detail.py
Add donor_last_date + per-year D/R/U amounts (2018-2024) to voter_file.
Joins on StateVoterId = sboeid via ProvenDonors2024_BOEReclassified.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.db import get_conn

def run(cur, sql, label=""):
    t = time.time()
    cur.execute(sql)
    print(f"  {label} ({time.time()-t:.1f}s)")

def main():
    conn = get_conn()
    cur = conn.cursor()
    print("=" * 60)
    print("Adding donor_last_date + per-year D/R/U to voter_file")
    print("=" * 60)

    # Step 1: Add columns
    print("\nStep 1: Adding columns...")
    new_cols = [
        ("donor_last_date","DATE DEFAULT NULL"),
        ("donor_D2018amt","DECIMAL(14,2) DEFAULT 0"),("donor_D2019amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2020amt","DECIMAL(14,2) DEFAULT 0"),("donor_D2021amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2022amt","DECIMAL(14,2) DEFAULT 0"),("donor_D2023amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_D2024amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2018amt","DECIMAL(14,2) DEFAULT 0"),("donor_R2019amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2020amt","DECIMAL(14,2) DEFAULT 0"),("donor_R2021amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2022amt","DECIMAL(14,2) DEFAULT 0"),("donor_R2023amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_R2024amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2018amt","DECIMAL(14,2) DEFAULT 0"),("donor_U2019amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2020amt","DECIMAL(14,2) DEFAULT 0"),("donor_U2021amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2022amt","DECIMAL(14,2) DEFAULT 0"),("donor_U2023amt","DECIMAL(14,2) DEFAULT 0"),
        ("donor_U2024amt","DECIMAL(14,2) DEFAULT 0"),
    ]
    cur.execute("SHOW COLUMNS FROM voter_file")
    existing = {r[0] for r in cur.fetchall()}
    for col, dtype in new_cols:
        if col in existing:
            print(f"    {col} already exists, skipping"); continue
        try:
            run(cur, f"ALTER TABLE voter_file ADD COLUMN {col} {dtype}", f"add {col}")
            conn.commit()
        except Exception as e:
            print(f"    ERROR {col}: {e}"); conn.rollback()

    # Step 2: Build staging
    print("\nStep 2: Building staging table...")
    cur.execute("SHOW TABLES IN nys_voter_tagging LIKE 'stg_donor_matchkeys'")
    has_matchkeys = cur.fetchone() is not None
    run(cur, "DROP TABLE IF EXISTS _donor_detail_stg", "drop old")
    run(cur, """
        CREATE TABLE _donor_detail_stg (
            sboeid VARCHAR(50) PRIMARY KEY, last_date DATE,
            D2018amt DECIMAL(14,2), D2019amt DECIMAL(14,2), D2020amt DECIMAL(14,2),
            D2021amt DECIMAL(14,2), D2022amt DECIMAL(14,2), D2023amt DECIMAL(14,2), D2024amt DECIMAL(14,2),
            R2018amt DECIMAL(14,2), R2019amt DECIMAL(14,2), R2020amt DECIMAL(14,2),
            R2021amt DECIMAL(14,2), R2022amt DECIMAL(14,2), R2023amt DECIMAL(14,2), R2024amt DECIMAL(14,2),
            U2018amt DECIMAL(14,2), U2019amt DECIMAL(14,2), U2020amt DECIMAL(14,2),
            U2021amt DECIMAL(14,2), U2022amt DECIMAL(14,2), U2023amt DECIMAL(14,2), U2024amt DECIMAL(14,2)
        ) ENGINE=InnoDB""", "create staging")
    run(cur, """
        INSERT INTO _donor_detail_stg
            (sboeid, D2018amt,D2019amt,D2020amt,D2021amt,D2022amt,D2023amt,D2024amt,
             R2018amt,R2019amt,R2020amt,R2021amt,R2022amt,R2023amt,R2024amt,
             U2018amt,U2019amt,U2020amt,U2021amt,U2022amt,U2023amt,U2024amt)
        SELECT sboeid,
            SUM(boe_D2018amt),SUM(boe_D2019amt),SUM(boe_D2020amt),SUM(boe_D2021amt),
            SUM(boe_D2022amt),SUM(boe_D2023amt),SUM(boe_D2024amt),
            SUM(boe_R2018amt),SUM(boe_R2019amt),SUM(boe_R2020amt),SUM(boe_R2021amt),
            SUM(boe_R2022amt),SUM(boe_R2023amt),SUM(boe_R2024amt),
            SUM(boe_U2018amt),SUM(boe_U2019amt),SUM(boe_U2020amt),SUM(boe_U2021amt),
            SUM(boe_U2022amt),SUM(boe_U2023amt),SUM(boe_U2024amt)
        FROM nys_voter_tagging.ProvenDonors2024_BOEReclassified
        WHERE sboeid IS NOT NULL AND sboeid != ''
        GROUP BY sboeid""", "insert per-year amounts")
    conn.commit()

    # Step 3: Last donation date
    print("\nStep 3: Populating last donation date...")
    if has_matchkeys:
        run(cur, """
            UPDATE _donor_detail_stg s
            JOIN (SELECT CONVERT(mk.sboeid USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS sboeid,
                         MAX(b.SCHED_DATE) AS last_dt
                  FROM nys_voter_tagging.stg_donor_matchkeys mk
                  JOIN nys_voter_tagging.boe_contributions_raw b
                    ON CONVERT(b.match_key USING utf8mb4) COLLATE utf8mb4_0900_ai_ci
                     = CONVERT(mk.match_key USING utf8mb4) COLLATE utf8mb4_0900_ai_ci
                  GROUP BY mk.sboeid) ld ON ld.sboeid = s.sboeid
            SET s.last_date = ld.last_dt""", "last_date via matchkeys")
    else:
        run(cur, """
            UPDATE _donor_detail_stg s
            JOIN (SELECT p.sboeid, MAX(b.SCHED_DATE) AS last_dt
                  FROM nys_voter_tagging.ProvenDonors2024_BOEReclassified p
                  JOIN nys_voter_tagging.boe_contributions_raw b
                    ON b.match_key = CONCAT(UPPER(p.LastName),'|',UPPER(p.FirstName),'|',p.PrimaryZip)
                  WHERE p.sboeid IS NOT NULL GROUP BY p.sboeid) ld ON ld.sboeid = s.sboeid
            SET s.last_date = ld.last_dt""", "last_date via name+zip fallback")
    conn.commit()

    # Step 4: Update voter file
    print("\nStep 4: Updating voter_file...")
    run(cur, """
        UPDATE voter_file f
        JOIN _donor_detail_stg s ON s.sboeid = f.StateVoterId
        SET f.donor_last_date=s.last_date,
            f.donor_D2018amt=COALESCE(s.D2018amt,0), f.donor_D2019amt=COALESCE(s.D2019amt,0),
            f.donor_D2020amt=COALESCE(s.D2020amt,0), f.donor_D2021amt=COALESCE(s.D2021amt,0),
            f.donor_D2022amt=COALESCE(s.D2022amt,0), f.donor_D2023amt=COALESCE(s.D2023amt,0),
            f.donor_D2024amt=COALESCE(s.D2024amt,0),
            f.donor_R2018amt=COALESCE(s.R2018amt,0), f.donor_R2019amt=COALESCE(s.R2019amt,0),
            f.donor_R2020amt=COALESCE(s.R2020amt,0), f.donor_R2021amt=COALESCE(s.R2021amt,0),
            f.donor_R2022amt=COALESCE(s.R2022amt,0), f.donor_R2023amt=COALESCE(s.R2023amt,0),
            f.donor_R2024amt=COALESCE(s.R2024amt,0),
            f.donor_U2018amt=COALESCE(s.U2018amt,0), f.donor_U2019amt=COALESCE(s.U2019amt,0),
            f.donor_U2020amt=COALESCE(s.U2020amt,0), f.donor_U2021amt=COALESCE(s.U2021amt,0),
            f.donor_U2022amt=COALESCE(s.U2022amt,0), f.donor_U2023amt=COALESCE(s.U2023amt,0),
            f.donor_U2024amt=COALESCE(s.U2024amt,0)""", "UPDATE voter_file")
    conn.commit()

    # Results
    print("\n=== RESULTS ===")
    cur.execute("""SELECT COUNT(CASE WHEN donor_last_date IS NOT NULL THEN 1 END),
        MIN(donor_last_date), MAX(donor_last_date),
        SUM(donor_D2022amt+donor_D2023amt+donor_D2024amt),
        SUM(donor_R2022amt+donor_R2023amt+donor_R2024amt)
        FROM voter_file""")
    r = cur.fetchone()
    print(f"  Voters with last_date:   {r[0]:,}")
    print(f"  Date range:              {r[1]} to {r[2]}")
    print(f"  D total 2022-2024:       ${r[3]:,.0f}")
    print(f"  R total 2022-2024:       ${r[4]:,.0f}")
    run(cur, "DROP TABLE IF EXISTS _donor_detail_stg", "cleanup")
    conn.commit(); cur.close(); conn.close()
    print("\nDone!")

if __name__ == '__main__':
    t = time.time()
    main()
    print(f"Total time: {(time.time()-t)/60:.1f} min")
