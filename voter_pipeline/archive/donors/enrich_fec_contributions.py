#!/usr/bin/env python3
"""
enrich_fec_contributions.py
Adds f_total_contribution_amt + f_contribution_count to voter_file
from politik1_fec.boe_voter_with_contributions via sboeid.
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
print("FEC Contribution Enrichment")
print("="*60)

print("\nStep 1: Checking source data...")
cur.execute("SELECT COUNT(*) FROM politik1_fec.boe_voter_with_contributions")
src = cur.fetchone()[0]
print(f"  politik1_fec.boe_voter_with_contributions: {src:,} rows")
if src == 0:
    print("  WARNING: source table is empty, skipping")
    conn.close()
    exit(0)

print("\nStep 2: Adding FEC columns to voter_file (if needed)...")
for col, typedef in [
    ("f_total_contribution_amt","DECIMAL(14,2) DEFAULT NULL"),
    ("f_contribution_count","INT DEFAULT NULL"),
]:
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='nys_voter_tagging' AND TABLE_NAME='voter_file' AND COLUMN_NAME=%s", (col,))
    if cur.fetchone()[0] == 0:
        cur.execute(f"ALTER TABLE voter_file ADD COLUMN `{col}` {typedef}")
        print(f"  Added: {col}")
    else:
        print(f"  Exists: {col}")
conn.commit()

print("\nStep 3: Clearing existing FEC values...")
t(cur, """
    UPDATE voter_file
    SET f_total_contribution_amt=NULL, f_contribution_count=NULL
    WHERE f_total_contribution_amt IS NOT NULL OR f_contribution_count IS NOT NULL
""", "cleared")
conn.commit()

print("\nStep 4: Joining boe_voter_with_contributions -> voter_file...")
t(cur, """
    UPDATE voter_file v
    JOIN politik1_fec.boe_voter_with_contributions c ON v.StateVoterId = c.sboeid
    SET
        v.f_total_contribution_amt = c.total_contribution_amt,
        v.f_contribution_count     = c.contribution_count
""", "updated")
conn.commit()
print(f"  Rows updated: {cur.rowcount:,}")

print("\n=== RESULTS ===")
cur.execute("SELECT COUNT(*) FROM voter_file WHERE f_contribution_count > 0"); print(f"  Voters with FEC contributions: {cur.fetchone()[0]:,}")
cur.execute("SELECT SUM(f_total_contribution_amt) FROM voter_file WHERE f_total_contribution_amt > 0")
total = cur.fetchone()[0] or 0
print(f"  Total FEC amount: ${total:,.2f}")
conn.close()
print("\nDone.")