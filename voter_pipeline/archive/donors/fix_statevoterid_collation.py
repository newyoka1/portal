#!/usr/bin/env python3
"""
Fix StateVoterId collations in boe_donors tables to match voter_file
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

print("=" * 80)
print("FIXING STATEVOTERID COLLATIONS IN BOE_DONORS")
print("=" * 80)

conn = get_conn()
cur = conn.cursor()

# Get target collation from voter_file
cur.execute("""
    SELECT COLLATION_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'nys_voter_tagging'
    AND TABLE_NAME = 'voter_file'
    AND COLUMN_NAME = 'StateVoterId'
""")
target_collation = cur.fetchone()[0]
print(f"\nTarget collation: {target_collation}")

# Switch to boe_donors
cur.execute("USE boe_donors")

# Fix contributions_matched
print("\nFixing contributions_matched.StateVoterId...")
try:
    cur.execute(f"""
        ALTER TABLE contributions_matched
        MODIFY COLUMN StateVoterId VARCHAR(50)
        CHARACTER SET utf8mb4 COLLATE {target_collation}
    """)
    conn.commit()
    print("  ✓ Success")
except Exception as e:
    print(f"  ❌ Error: {e}")
    conn.rollback()

# Fix donor_summary
print("\nFixing donor_summary.StateVoterId...")
try:
    cur.execute(f"""
        ALTER TABLE donor_summary
        MODIFY COLUMN StateVoterId VARCHAR(50)
        CHARACTER SET utf8mb4 COLLATE {target_collation}
    """)
    conn.commit()
    print("  ✓ Success")
except Exception as e:
    print(f"  ❌ Error: {e}")
    conn.rollback()

print("\n" + "=" * 80)
print("COMPLETE - Now re-run: python donors/boe_match_aggregate.py")
print("=" * 80)

cur.close()
conn.close()
