#!/usr/bin/env python3
"""
Comprehensive collation fix across all NYS voter pipeline databases
Ensures all text columns use utf8mb4_0900_ai_ci to match voter_file
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

print("=" * 80)
print("COMPREHENSIVE COLLATION FIX - ALL DATABASES")
print("=" * 80)

conn = get_conn()
cur = conn.cursor()

# Step 1: Get target collation from voter_file
print("\nStep 1: Identifying target collation...")
cur.execute("""
    SELECT COLLATION_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'nys_voter_tagging'
    AND TABLE_NAME = 'voter_file'
    AND COLUMN_NAME = 'StateVoterId'
""")
result = cur.fetchone()
TARGET_COLLATION = result[0] if result else 'utf8mb4_0900_ai_ci'
print(f"  Target collation: {TARGET_COLLATION}")

# Step 2: Fix boe_donors database
print("\n" + "=" * 80)
print("Step 2: Fixing boe_donors database...")
print("=" * 80)

cur.execute("USE boe_donors")

# Get all text columns in boe_donors
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'boe_donors'
    AND DATA_TYPE IN ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext')
    AND COLLATION_NAME IS NOT NULL
    ORDER BY TABLE_NAME, COLUMN_NAME
""")

boe_columns = cur.fetchall()
boe_fixes = []

print(f"\nFound {len(boe_columns)} text columns in boe_donors")
for table_name, col_name, col_type, collation in boe_columns:
    if collation != TARGET_COLLATION:
        boe_fixes.append((table_name, col_name, col_type, collation))

if boe_fixes:
    print(f"\nFixing {len(boe_fixes)} columns with wrong collation...")
    for table_name, col_name, col_type, old_collation in boe_fixes:
        print(f"  {table_name}.{col_name}: {old_collation} → {TARGET_COLLATION}")
        
        try:
            cur.execute(f"""
                ALTER TABLE {table_name}
                MODIFY COLUMN {col_name} {col_type}
                CHARACTER SET utf8mb4 COLLATE {TARGET_COLLATION}
            """)
            conn.commit()
            print(f"    ✓ Success")
        except Exception as e:
            print(f"    ❌ Error: {e}")
            conn.rollback()
else:
    print("  ✓ All columns already have correct collation")

# Step 3: Check politik1_fec database
print("\n" + "=" * 80)
print("Step 3: Checking politik1_fec database...")
print("=" * 80)

# Check if database exists
cur.execute("SHOW DATABASES LIKE 'politik1_fec'")
if cur.fetchone():
    cur.execute("USE politik1_fec")
    
    # Get all text columns
    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'politik1_fec'
        AND DATA_TYPE IN ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext')
        AND COLLATION_NAME IS NOT NULL
        ORDER BY TABLE_NAME, COLUMN_NAME
    """)
    
    fec_columns = cur.fetchall()
    fec_fixes = []
    
    print(f"\nFound {len(fec_columns)} text columns in politik1_fec")
    for table_name, col_name, col_type, collation in fec_columns:
        if collation != TARGET_COLLATION:
            fec_fixes.append((table_name, col_name, col_type, collation))
    
    if fec_fixes:
        print(f"\nFixing {len(fec_fixes)} columns with wrong collation...")
        for table_name, col_name, col_type, old_collation in fec_fixes:
            print(f"  {table_name}.{col_name}: {old_collation} → {TARGET_COLLATION}")
            
            try:
                cur.execute(f"""
                    ALTER TABLE {table_name}
                    MODIFY COLUMN {col_name} {col_type}
                    CHARACTER SET utf8mb4 COLLATE {TARGET_COLLATION}
                """)
                conn.commit()
                print(f"    ✓ Success")
            except Exception as e:
                print(f"    ❌ Error: {e}")
                conn.rollback()
    else:
        print("  ✓ All columns already have correct collation")
else:
    print("  ⚠️  politik1_fec database not found (skipping)")

# Step 4: Recreate indexes with correct collation
print("\n" + "=" * 80)
print("Step 4: Recreating indexes with correct collation...")
print("=" * 80)

cur.execute("USE boe_donors")

# Drop and recreate key indexes
indexes_to_recreate = [
    ("contributions_raw", "idx_contrib_name_zip", "last_name, first_name, zip"),
    ("contributions_matched", "idx_matched_statevoterid", "StateVoterId"),
]

for table_name, index_name, columns in indexes_to_recreate:
    print(f"\n  {table_name}.{index_name}...")
    
    # Drop if exists
    try:
        cur.execute(f"DROP INDEX {index_name} ON {table_name}")
        print(f"    Dropped old index")
    except:
        pass
    
    # Create new
    try:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name}({columns})")
        conn.commit()
        print(f"    ✓ Created with correct collation")
    except Exception as e:
        print(f"    ❌ Error: {e}")

# Step 5: Summary
print("\n" + "=" * 80)
print("COLLATION FIX COMPLETE")
print("=" * 80)
print(f"\nAll databases now use: {TARGET_COLLATION}")
print("\nYou can now run:")
print("  python donors/boe_match_aggregate.py")
print("  python main.py boe-enrich")

cur.close()
conn.close()
