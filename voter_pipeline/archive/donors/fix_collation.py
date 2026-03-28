#!/usr/bin/env python3
"""
Fix collation mismatch between nys_voter_tagging.voter_file and boe_donors.contributions_raw
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

print("=" * 80)
print("CROSS-DATABASE COLLATION FIX")
print("=" * 80)

# Connect and check both databases
conn = get_conn()
cur = conn.cursor()

# Step 1: Check voter_file collations (nys_voter_tagging)
print("\nStep 1: Checking nys_voter_tagging.voter_file collations...")
cur.execute("""
    SELECT COLUMN_NAME, COLLATION_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'nys_voter_tagging'
    AND TABLE_NAME = 'voter_file'
    AND COLUMN_NAME IN ('FirstName', 'LastName', 'MiddleName', 'PrimaryZip')
    ORDER BY COLUMN_NAME
""")

voter_cols = {}
for col_name, collation in cur.fetchall():
    voter_cols[col_name] = collation
    print(f"  {col_name:20} {collation}")

# Step 2: Check contributions_raw collations (boe_donors)
print("\nStep 2: Checking boe_donors.contributions_raw collations...")
cur.execute("""
    SELECT COLUMN_NAME, COLLATION_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'boe_donors'
    AND TABLE_NAME = 'contributions_raw'
    AND COLUMN_NAME IN ('first_name', 'last_name', 'middle_name', 'zip')
    ORDER BY COLUMN_NAME
""")

contrib_cols = {}
for col_name, collation in cur.fetchall():
    contrib_cols[col_name] = collation
    print(f"  {col_name:20} {collation}")

# Step 3: Identify and fix mismatches
print("\nStep 3: Fixing collation mismatches...")

# Target collation from voter_file (should be utf8mb4_0900_ai_ci)
target_collation = voter_cols.get('FirstName') or 'utf8mb4_0900_ai_ci'
print(f"  Target collation: {target_collation}")

# Fix contributions_raw columns
cur.execute("USE boe_donors")

fixes = [
    ('first_name', 'VARCHAR(100)'),
    ('last_name', 'VARCHAR(100)'),
    ('middle_name', 'VARCHAR(100)'),
    ('zip', 'VARCHAR(20)')
]

for col_name, col_type in fixes:
    current_collation = contrib_cols.get(col_name)
    
    if current_collation != target_collation:
        print(f"  Converting {col_name}: {current_collation} → {target_collation}")
        
        try:
            cur.execute(f"""
                ALTER TABLE contributions_raw
                MODIFY COLUMN {col_name} {col_type}
                CHARACTER SET utf8mb4 COLLATE {target_collation}
            """)
            conn.commit()
            print(f"    ✓ Success")
        except Exception as e:
            print(f"    ❌ Error: {e}")
            conn.rollback()
    else:
        print(f"  ✓ {col_name} already matches ({current_collation})")

# Step 4: Update the composite index
print("\nStep 4: Recreating composite index with correct collation...")
try:
    cur.execute("DROP INDEX idx_contrib_name_zip ON contributions_raw")
    print("  Dropped old index")
except:
    pass

try:
    cur.execute("CREATE INDEX idx_contrib_name_zip ON contributions_raw(last_name, first_name, zip)")
    print("  ✓ Created new index with correct collation")
except Exception as e:
    print(f"  ❌ Error: {e}")

conn.commit()

print("\n" + "=" * 80)
print("COLLATION FIX COMPLETE")
print("=" * 80)
print("\nNow run: python donors/boe_match_aggregate.py")

cur.close()
conn.close()
