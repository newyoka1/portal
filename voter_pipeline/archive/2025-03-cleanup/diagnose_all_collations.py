#!/usr/bin/env python3
"""
Comprehensive FEC Collation Check
==================================
Checks all string columns and table-level collations
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
    print("COMPREHENSIVE COLLATION DIAGNOSTIC")
    print("=" * 80)
    print()
    
    conn = connect_db("information_schema")
    cur = conn.cursor()
    
    # Check table-level collations
    print("Step 1: Table-level collations...")
    cur.execute("""
        SELECT TABLE_NAME, TABLE_COLLATION
        FROM TABLES
        WHERE TABLE_SCHEMA IN ('nys_voter_tagging', 'National_Donors')
        AND TABLE_NAME IN ('voter_file', 'ny_voters_with_donations')
    """)
    
    for table, collation in cur.fetchall():
        print(f"  {table:40} {collation}")
    print()
    
    # Check all string columns in both tables
    print("Step 2: All string columns in voter_file...")
    cur.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME
        FROM COLUMNS
        WHERE TABLE_SCHEMA = 'nys_voter_tagging'
        AND TABLE_NAME = 'voter_file'
        AND COLLATION_NAME IS NOT NULL
        ORDER BY COLUMN_NAME
    """)
    
    voter_cols = {}
    for col, typ, collation in cur.fetchall():
        voter_cols[col] = collation
        print(f"  {col:30} {typ:20} {collation}")
    print()
    
    print("Step 3: All string columns in ny_voters_with_donations...")
    cur.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME
        FROM COLUMNS
        WHERE TABLE_SCHEMA = 'National_Donors'
        AND TABLE_NAME = 'ny_voters_with_donations'
        AND COLLATION_NAME IS NOT NULL
        ORDER BY COLUMN_NAME
    """)
    
    fec_cols = {}
    mismatches = []
    for col, typ, collation in cur.fetchall():
        fec_cols[col] = collation
        print(f"  {col:30} {typ:20} {collation}")
        
        # Check for mismatch with voter_file
        if col in voter_cols and voter_cols[col] != collation:
            mismatches.append((col, voter_cols[col], collation))
    
    print()
    
    # Report mismatches
    if mismatches:
        print("Step 4: COLLATION MISMATCHES FOUND:")
        print()
        for col, voter_coll, fec_coll in mismatches:
            print(f"  Column: {col}")
            print(f"    voter_file:              {voter_coll}")
            print(f"    ny_voters_with_donations: {fec_coll}")
            print()
        
        print("  These mismatches could cause JOIN errors.")
        print()
        
        # Offer to fix
        response = input("  Fix all mismatches in ny_voters_with_donations? (y/N): ").strip().lower()
        if response == 'y':
            print()
            print("  Applying fixes...")
            
            conn_db = connect_db("National_Donors")
            cur_db = conn_db.cursor()
            
            for col, voter_coll, fec_coll in mismatches:
                # Get column type
                cur.execute("""
                    SELECT COLUMN_TYPE
                    FROM COLUMNS
                    WHERE TABLE_SCHEMA = 'National_Donors'
                    AND TABLE_NAME = 'ny_voters_with_donations'
                    AND COLUMN_NAME = %s
                """, (col,))
                col_type = cur.fetchone()[0]
                
                print(f"  Fixing {col}: {fec_coll} → {voter_coll}")
                
                alter_sql = f"""
                    ALTER TABLE ny_voters_with_donations
                    MODIFY COLUMN `{col}` {col_type}
                    CHARACTER SET utf8mb4
                    COLLATE {voter_coll}
                """
                
                try:
                    cur_db.execute(alter_sql)
                    conn_db.commit()
                    print(f"    ✓ Success")
                except Exception as e:
                    print(f"    ✗ Error: {e}")
                    conn_db.rollback()
            
            conn_db.close()
            print()
            print("  ✓ All fixes applied!")
    else:
        print("Step 4: No collation mismatches found!")
        print()
        print("  The error might be caused by:")
        print("  1. A temporary table created during the UPDATE")
        print("  2. An implicit string conversion in MySQL")
        print("  3. Database-level collation settings")
        print()
        print("  Try running the UPDATE with COLLATE clause:")
        print("  ON v.StateVoterId COLLATE utf8mb4_0900_ai_ci = fec.StateVoterId")
    
    conn.close()
    
    print()
    print("=" * 80)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
