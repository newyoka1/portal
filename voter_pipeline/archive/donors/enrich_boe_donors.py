#!/usr/bin/env python3
"""
enrich_boe_donors.py
Enriches voter_file with BOE donor signals from politik1_nydata.nys_donors.
No ingest, no staging tables.
"""
import os, sys, time
sys.path.insert(0, r'C:\Users\georg_2r965zq\AppData\Roaming\Python\Python314\site-packages')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)
import mysql.connector

def t(cur, sql, label):
    s = time.time()
    cur.execute(sql)
    print(f"  {label} ({time.time()-s:.1f}s)")

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST","127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="nys_voter_tagging",
    connection_timeout=600
)
cur = conn.cursor()

print("="*60)
print("BOE Donor Enrichment (from politik1_nydata.nys_donors)")
print("="*60)

print("\nStep 1: Adding BOE donor columns to voter_file...")
columns = [
    ("boe_total_R_amt","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_total_U_amt","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_R_2021","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_R_2022","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_R_2023","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_U_2021","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_U_2022","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_U_2023","DECIMAL(14,2) DEFAULT NULL"),
    ("boe_party_signal","VARCHAR(1) DEFAULT NULL"),
]
for col, typedef in columns:
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='nys_voter_tagging' AND TABLE_NAME='voter_file' AND COLUMN_NAME=%s", (col,))
    if cur.fetchone()[0] == 0:
        cur.execute(f"ALTER TABLE voter_file ADD COLUMN `{col}` {typedef}")
        print(f"  Added: {col}")
    else:
        print(f"  Exists: {col}")
conn.commit()

print("\nStep 2: Clearing existing BOE values...")
t(cur, """
    UPDATE voter_file SET
        boe_total_R_amt=NULL, boe_total_U_amt=NULL,
        boe_R_2021=NULL, boe_R_2022=NULL, boe_R_2023=NULL,
        boe_U_2021=NULL, boe_U_2022=NULL, boe_U_2023=NULL,
        boe_party_signal=NULL
    WHERE boe_total_R_amt IS NOT NULL OR boe_total_U_amt IS NOT NULL
""", "cleared")
conn.commit()

print("\nStep 3: Joining nys_donors -> voter_file by sboeid...")
t(cur, """
    UPDATE voter_file v
    JOIN politik1_nydata.nys_donors d
        ON v.StateVoterId = d.sboeid
        AND d.sboeid IS NOT NULL AND d.sboeid != ''
    SET
        v.boe_total_R_amt = NULLIF(COALESCE(d.SumOfR2021amt,0)+COALESCE(d.SumOfR2022amt,0)+COALESCE(d.SumOfR2023amt,0),0),
        v.boe_total_U_amt = NULLIF(COALESCE(d.SumOfU2021amt,0)+COALESCE(d.SumOfU2022amt,0)+COALESCE(d.SumOfU2023amt,0),0),
        v.boe_R_2021 = NULLIF(d.SumOfR2021amt,0),
        v.boe_R_2022 = NULLIF(d.SumOfR2022amt,0),
        v.boe_R_2023 = NULLIF(d.SumOfR2023amt,0),
        v.boe_U_2021 = NULLIF(d.SumOfU2021amt,0),
        v.boe_U_2022 = NULLIF(d.SumOfU2022amt,0),
        v.boe_U_2023 = NULLIF(d.SumOfU2023amt,0)
""", "updated donor amounts")
conn.commit()
print(f"  Rows updated: {cur.rowcount:,}")

print("\nStep 4: Setting boe_party_signal...")
t(cur, """
    UPDATE voter_file SET
        boe_party_signal = CASE
            WHEN COALESCE(boe_total_R_amt,0) > COALESCE(boe_total_U_amt,0) THEN 'R'
            WHEN COALESCE(boe_total_U_amt,0) > COALESCE(boe_total_R_amt,0) THEN 'U'
            WHEN COALESCE(boe_total_R_amt,0) > 0 THEN 'R'
            ELSE NULL
        END
    WHERE boe_total_R_amt IS NOT NULL OR boe_total_U_amt IS NOT NULL
""", "party signals set")
conn.commit()

print("\n=== RESULTS ===")
cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_R_amt IS NOT NULL"); print(f"  Republican donors:        {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_U_amt IS NOT NULL"); print(f"  Unaffiliated donors:      {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_party_signal IS NOT NULL"); print(f"  Total BOE-matched voters: {cur.fetchone()[0]:,}")
conn.close()
print("\nDone.")
