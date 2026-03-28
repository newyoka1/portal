#!/usr/bin/env python3
"""
enrich_boe_donor_totals.py
===========================
Adds BOE donor total columns to voter_file from boe_proven_donors.

Columns added:
  boe_total_D_amt, boe_total_R_amt, boe_total_U_amt
  boe_total_D_cnt, boe_total_R_cnt, boe_total_U_cnt
  boe_party_signal
"""

import os, sys
sys.path.insert(0, r"C:\Users\georg_2r965zq\AppData\Roaming\Python\Python314\site-packages")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)
import mysql.connector

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="nys_voter_tagging"
)
cur = conn.cursor()

# ── Step 1: Add columns if not already there ─────────────────────────────────
print("Step 1: Adding BOE donor columns to voter_file (if needed)...")

new_cols = [
    ("boe_total_D_amt",  "DECIMAL(36,2) DEFAULT NULL"),
    ("boe_total_R_amt",  "DECIMAL(36,2) DEFAULT NULL"),
    ("boe_total_U_amt",  "DECIMAL(36,2) DEFAULT NULL"),
    ("boe_total_D_cnt",  "DECIMAL(32,0) DEFAULT NULL"),
    ("boe_total_R_cnt",  "DECIMAL(32,0) DEFAULT NULL"),
    ("boe_total_U_cnt",  "DECIMAL(32,0) DEFAULT NULL"),
    ("boe_party_signal", "VARCHAR(12)   DEFAULT NULL"),
]

for col_name, col_def in new_cols:
    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'nys_voter_tagging'
          AND TABLE_NAME   = 'voter_file'
          AND COLUMN_NAME  = %s
    """, (col_name,))
    if cur.fetchone()[0] == 0:
        cur.execute(f"ALTER TABLE voter_file ADD COLUMN {col_name} {col_def}")
        print(f"  Added {col_name}")
    else:
        print(f"  {col_name} already exists, skipping")

conn.commit()

# ── Step 2: Populate via sboeid join ─────────────────────────────────────────
print("\nStep 2: Populating BOE donor columns via StateVoterId = sboeid join...")
cur.execute("""
    UPDATE voter_file v
    JOIN boe_proven_donors p
        ON CONVERT(v.StateVoterId USING utf8mb4) COLLATE utf8mb4_unicode_ci
         = CONVERT(p.sboeid       USING utf8mb4) COLLATE utf8mb4_unicode_ci
    SET
        v.boe_total_D_amt  = p.boe_total_D_amt,
        v.boe_total_R_amt  = p.boe_total_R_amt,
        v.boe_total_U_amt  = p.boe_total_U_amt,
        v.boe_total_D_cnt  = p.boe_total_D_cnt,
        v.boe_total_R_cnt  = p.boe_total_R_cnt,
        v.boe_total_U_cnt  = p.boe_total_U_cnt,
        v.boe_party_signal = p.boe_party_signal
""")
conn.commit()
print(f"  Rows updated: {cur.rowcount:,}")

# ── Summary ───────────────────────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_party_signal IS NOT NULL")
print(f"\nDone! Voters with BOE donor data: {cur.fetchone()[0]:,}")

conn.close()
