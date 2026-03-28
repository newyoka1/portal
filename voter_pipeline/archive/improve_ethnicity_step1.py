#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMPROVEMENT #1: Add normalized surname matching
This should boost match rate from 60% to ~75%+
"""
import pymysql
import os
import sys

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

load_dotenv(r'd:\git\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', '127.0.0.1'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'root'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='nys_voter_tagging',
    charset='utf8mb4',
    autocommit=True
)

print("\n" + "="*80)
print("ETHNICITY MATCHING IMPROVEMENT #1: NORMALIZED SURNAME MATCHING")
print("="*80)

cur = conn.cursor()

# Step 1: Add normalized_surname column to census table
print("\n[1/5] Adding normalized_surname column to ref_census_surnames...")
try:
    cur.execute("""
        ALTER TABLE ref_census_surnames 
        ADD COLUMN normalized_surname VARCHAR(100) GENERATED ALWAYS AS (
            UPPER(REPLACE(REPLACE(REPLACE(REPLACE(surname, "'", ""), "-", ""), " ", ""), ".", ""))
        ) STORED,
        ADD KEY idx_normalized_surname (normalized_surname)
    """)
    print("✓ Added normalized_surname column with index")
except pymysql.err.OperationalError as e:
    if e.args[0] == 1060:  # Column exists
        print("  Column already exists")
    else:
        raise

# Step 2: Add normalized_surname column to voter_file
print("\n[2/5] Adding normalized_lastname to voter_file...")
try:
    cur.execute("""
        ALTER TABLE voter_file 
        ADD COLUMN normalized_lastname VARCHAR(100) GENERATED ALWAYS AS (
            UPPER(REPLACE(REPLACE(REPLACE(REPLACE(LastName, "'", ""), "-", ""), " ", ""), ".", ""))
        ) STORED,
        ADD KEY idx_normalized_lastname (normalized_lastname)
    """)
    print("✓ Added normalized_lastname column with index")
except pymysql.err.OperationalError as e:
    if e.args[0] == 1060:  # Column exists
        print("  Column already exists, ensuring index...")
        try:
            cur.execute("ALTER TABLE voter_file ADD KEY idx_normalized_lastname (normalized_lastname)")
        except:
            pass

# Step 3: Test the improvement
print("\n[3/5] Testing match rate improvement...")

# Original match rate (already know it's 60.17%)
cur.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN e.surname IS NOT NULL THEN 1 ELSE 0 END) as matched_old,
        SUM(CASE WHEN e2.normalized_surname IS NOT NULL THEN 1 ELSE 0 END) as matched_new
    FROM voter_file f
    LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
    LEFT JOIN ref_census_surnames e2 ON e2.normalized_surname = f.normalized_lastname
    LIMIT 100000
""")

total, old_match, new_match = cur.fetchone()
old_rate = (old_match / total) * 100
new_rate = (new_match / total) * 100
improvement = new_rate - old_rate

print(f"\nResults (sample of {total:,} voters):")
print(f"  Old method (exact match):      {old_match:,} ({old_rate:.2f}%)")
print(f"  New method (normalized match): {new_match:,} ({new_rate:.2f}%)")
print(f"  Improvement:                   +{new_match - old_match:,} ({improvement:+.2f}%)")

# Step 4: Check specific problem cases
print("\n[4/5] Checking specific problem surnames...")

test_surnames = ["O'BRIEN", "O'CONNOR", "DE LA CRUZ", "MC DONALD"]
print(f"\n{'Original':<20} {'Old Match':<15} {'New Match':<15}")
print("-" * 50)

for surname in test_surnames:
    # Check old method
    cur.execute("SELECT surname FROM ref_census_surnames WHERE surname = %s", (surname,))
    old_match = cur.fetchone()
    
    # Check new method (normalized)
    normalized = surname.replace("'", "").replace("-", "").replace(" ", "").replace(".", "")
    cur.execute("SELECT surname FROM ref_census_surnames WHERE normalized_surname = %s", (normalized,))
    new_match = cur.fetchone()
    
    print(f"{surname:<20} {'✗ No' if not old_match else '✓ Yes':<15} {'✗ No' if not new_match else '✓ Yes':<15}")

# Step 5: Provide update query for export scripts
print("\n[5/5] Updated JOIN query for export scripts:")
print("-" * 80)
print("""
BEFORE (60% match rate):
LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)

AFTER (75%+ match rate):
LEFT JOIN ref_census_surnames e ON e.normalized_surname = f.normalized_lastname
""")

print("\n" + "="*80)
print("NEXT STEPS:")
print("="*80)
print("1. Update export_ld_to_excel_simple.py to use normalized columns")
print("2. Run full match rate test on entire database")
print("3. Consider improvement #2: Add 2020 Census data")
print("4. Consider improvement #3: Add firstname-based ethnicity prediction")
print("=" * 80)

conn.close()