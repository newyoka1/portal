#!/usr/bin/env python3
"""
Fix FEC Collation Mismatch
===========================
Diagnoses and fixes collation conflicts between voter_file and National_Donors tables
"""

import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

def connect_db(database):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=False
    )

def main():
    print("=" * 80)
    print("FEC COLLATION DIAGNOSTIC & FIX")
    print("=" * 80)
    print()
    
    # Check voter_file collation
    print("Step 1: Checking StateVoterId collations...")
    conn = connect_db("nys_voter_tagging")
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = 'nys_voter_tagging' 
        AND TABLE_NAME = 'voter_file' 
        AND COLUMN_NAME = 'StateVoterId'
    """)
    voter_col = cur.fetchone()
    if voter_col:
        print(f"  voter_file.StateVoterId: {voter_col[2]}")
    else:
        print("  ERROR: voter_file.StateVoterId not found!")
        return
    
    # Check National_Donors collation
    cur.execute("""
        SELECT COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = 'National_Donors' 
        AND TABLE_NAME = 'ny_voters_with_donations' 
        AND COLUMN_NAME = 'StateVoterId'
    """)
    fec_col = cur.fetchone()
    if fec_col:
        print(f"  National_Donors.ny_voters_with_donations.StateVoterId: {fec_col[2]}")
    else:
        print("  ERROR: National_Donors.ny_voters_with_donations.StateVoterId not found!")
        return
    
    print()
    
    # Check if they match
    if voter_col[2] == fec_col[2]:
        print("  ✓ Collations match! No fix needed.")
        conn.close()
        return
    
    print(f"  ✗ MISMATCH DETECTED:")
    print(f"    voter_file uses: {voter_col[2]}")
    print(f"    National_Donors uses: {fec_col[2]}")
    print()
    
    # Ask user to confirm fix
    print("Step 2: Fix collation mismatch")
    print(f"  Will change National_Donors.ny_voters_with_donations.StateVoterId")
    print(f"  FROM: {fec_col[2]}")
    print(f"  TO:   {voter_col[2]}")
    print()
    
    response = input("  Apply fix? (y/N): ").strip().lower()
    if response != 'y':
        print("  Skipped. No changes made.")
        conn.close()
        return
    
    print()
    print("  Applying fix...")
    
    # Get the column type
    cur.execute("""
        SELECT COLUMN_TYPE 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = 'National_Donors' 
        AND TABLE_NAME = 'ny_voters_with_donations' 
        AND COLUMN_NAME = 'StateVoterId'
    """)
    col_type = cur.fetchone()[0]
    
    # Alter the column
    alter_sql = f"""
        ALTER TABLE National_Donors.ny_voters_with_donations 
        MODIFY COLUMN StateVoterId {col_type} 
        CHARACTER SET utf8mb4 
        COLLATE {voter_col[2]}
    """
    
    print(f"  Executing: ALTER TABLE ... MODIFY COLUMN StateVoterId ... COLLATE {voter_col[2]}")
    
    try:
        cur.execute(alter_sql)
        conn.commit()
        print("  ✓ Success!")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        conn.rollback()
        conn.close()
        return
    
    print()
    
    # Verify fix
    print("Step 3: Verifying fix...")
    cur.execute("""
        SELECT COLLATION_NAME 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = 'National_Donors' 
        AND TABLE_NAME = 'ny_voters_with_donations' 
        AND COLUMN_NAME = 'StateVoterId'
    """)
    new_collation = cur.fetchone()[0]
    print(f"  New collation: {new_collation}")
    
    if new_collation == voter_col[2]:
        print("  ✓ Collations now match!")
    else:
        print("  ✗ Still mismatched!")
    
    conn.close()
    
    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print("  You can now run: python main.py fec-enrich")
    print()

if __name__ == "__main__":
    main()
