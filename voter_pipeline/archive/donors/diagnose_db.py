#!/usr/bin/env python3
"""
Comprehensive database structure diagnostic
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

conn = get_conn()
cur = conn.cursor()

print("=" * 80)
print("DATABASE STRUCTURE DIAGNOSTIC")
print("=" * 80)

# Get current database name
cur.execute("SELECT DATABASE()")
db_name = cur.fetchone()[0]
print(f"Database: {db_name}")

# List all tables
print("\nStep 1: All tables in database:")
cur.execute("SHOW TABLES")
tables = [row[0] for row in cur.fetchall()]
for table in tables:
    print(f"  - {table}")

# Check if voter_file exists
print("\nStep 2: voter_file structure:")
if 'voter_file' in tables:
    cur.execute("DESCRIBE voter_file")
    print("  Column Name                Type                      Collation")
    print("  " + "-" * 70)
    for row in cur.fetchall():
        col_name = row[0]
        col_type = row[1]
        # Get collation for this column
        cur.execute("""
            SELECT COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'voter_file'
            AND COLUMN_NAME = %s
        """, (db_name, col_name))
        collation_result = cur.fetchone()
        collation = collation_result[0] if collation_result else "N/A"
        print(f"  {col_name:25} {col_type:25} {collation}")
else:
    print("  ❌ voter_file table not found!")

# Check if contributions_raw exists
print("\nStep 3: contributions_raw structure:")
if 'contributions_raw' in tables:
    cur.execute("DESCRIBE contributions_raw")
    print("  Column Name                Type                      Collation")
    print("  " + "-" * 70)
    for row in cur.fetchall():
        col_name = row[0]
        col_type = row[1]
        # Get collation for this column
        cur.execute("""
            SELECT COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'contributions_raw'
            AND COLUMN_NAME = %s
        """, (db_name, col_name))
        collation_result = cur.fetchone()
        collation = collation_result[0] if collation_result else "N/A"
        print(f"  {col_name:25} {col_type:25} {collation}")
else:
    print("  ❌ contributions_raw table not found!")

print("\n" + "=" * 80)

cur.close()
conn.close()
