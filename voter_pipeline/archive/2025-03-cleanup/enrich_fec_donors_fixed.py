#!/usr/bin/env python3
"""
FEC Donor Enrichment Step
==========================
Adds FEC contribution data from National_Donors.ny_voters_with_donations
to the voter_file table.

This adds columns for:
- Democratic contributions (amount & count)
- Republican contributions (amount & count)
- Independent contributions (amount & count)
- Unknown contributions (amount & count)
- Total contributions (amount & count)
- is_fec_donor flag

Called by: python main.py fec-enrich
"""

import os
import sys
from pathlib import Path
import pymysql
from dotenv import load_dotenv

# Load .env
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
        autocommit=True
    )

def main():
    print("=" * 80)
    print("FEC DONOR ENRICHMENT - Integrating National_Donors data")
    print("=" * 80)
    print()
    
    # Check if National_Donors database exists
    print("Step 1: Checking for National_Donors database...")
    conn_sys = connect_db("information_schema")
    cur = conn_sys.cursor()
    cur.execute("SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME = 'National_Donors'")
    if not cur.fetchone():
        print("  ERROR: National_Donors database not found!")
        print("  Please run: python main.py national --full")
        print("  This will build the FEC donor matching table.")
        conn_sys.close()
        sys.exit(1)
    
    # Check if ny_voters_with_donations table exists
    cur.execute("""
        SELECT TABLE_NAME FROM TABLES 
        WHERE TABLE_SCHEMA = 'National_Donors' 
        AND TABLE_NAME = 'ny_voters_with_donations'
    """)
    if not cur.fetchone():
        print("  ERROR: ny_voters_with_donations table not found!")
        print("  Please run: python main.py national --full")
        conn_sys.close()
        sys.exit(1)
    
    conn_sys.close()
    print("  ✓ National_Donors database found")
    print()
    
    # Connect to voter database
    print("Step 2: Adding FEC columns to voter_file...")
    conn = connect_db("nys_voter_tagging")
    cur = conn.cursor()
    
    # Check if columns already exist
    cur.execute("SHOW COLUMNS FROM voter_file LIKE 'fec_%'")
    existing_cols = cur.fetchall()
    
    if len(existing_cols) > 0:
        print(f"  Found {len(existing_cols)} existing FEC columns")
        response = input("  Drop and recreate FEC columns? (y/N): ").strip().lower()
        if response == 'y':
            print("  Dropping existing FEC columns...")
            # Drop columns one by one for MySQL 8.4
            cols_to_drop = [
                'fec_total_amount', 'fec_total_count',
                'fec_democratic_amount', 'fec_democratic_count',
                'fec_republican_amount', 'fec_republican_count',
                'fec_independent_amount', 'fec_independent_count',
                'fec_unknown_amount', 'fec_unknown_count',
                'is_fec_donor'
            ]
            for col in cols_to_drop:
                try:
                    cur.execute(f"ALTER TABLE voter_file DROP COLUMN {col}")
                except:
                    pass
    
    # Add new columns (check each individually for MySQL 8.4 compatibility)
    print("  Adding FEC contribution columns...")
    
    columns_to_add = [
        ("fec_total_amount", "DECIMAL(14,2) DEFAULT 0"),
        ("fec_total_count", "INT DEFAULT 0"),
        ("fec_democratic_amount", "DECIMAL(14,2) DEFAULT 0"),
        ("fec_democratic_count", "INT DEFAULT 0"),
        ("fec_republican_amount", "DECIMAL(14,2) DEFAULT 0"),
        ("fec_republican_count", "INT DEFAULT 0"),
        ("fec_independent_amount", "DECIMAL(14,2) DEFAULT 0"),
        ("fec_independent_count", "INT DEFAULT 0"),
        ("fec_unknown_amount", "DECIMAL(14,2) DEFAULT 0"),
        ("fec_unknown_count", "INT DEFAULT 0"),
        ("is_fec_donor", "BOOLEAN DEFAULT FALSE"),
    ]
    
    added_count = 0
    for col_name, col_type in columns_to_add:
        cur.execute(f"SHOW COLUMNS FROM voter_file LIKE '{col_name}'")
        if not cur.fetchone():
            cur.execute(f"ALTER TABLE voter_file ADD COLUMN {col_name} {col_type}")
            added_count += 1
    
    # Add indexes
    cur.execute("SHOW INDEX FROM voter_file WHERE Key_name = 'idx_fec_donor'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE voter_file ADD INDEX idx_fec_donor (is_fec_donor)")
        added_count += 1
    
    cur.execute("SHOW INDEX FROM voter_file WHERE Key_name = 'idx_fec_total'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE voter_file ADD INDEX idx_fec_total (fec_total_amount)")
        added_count += 1
    
    if added_count > 0:
        print(f"  ✓ Added {added_count} new columns/indexes")
    else:
        print("  ✓ All FEC columns already exist")
    print()
    
    # Enrich data
    print("Step 3: Enriching voter_file with FEC data...")
    print("  (This may take 2-3 minutes for 13M voters)")
    
    import time
    start = time.time()
    
    cur.execute("""
        UPDATE nys_voter_tagging.voter_file v
        JOIN National_Donors.ny_voters_with_donations fec
          ON v.StateVoterId = fec.StateVoterId
        SET
          v.fec_total_amount = fec.total_donation_amount,
          v.fec_total_count = fec.total_donation_count,
          v.fec_democratic_amount = fec.democratic_amount,
          v.fec_democratic_count = fec.democratic_count,
          v.fec_republican_amount = fec.republican_amount,
          v.fec_republican_count = fec.republican_count,
          v.fec_independent_amount = fec.independent_amount,
          v.fec_independent_count = fec.independent_count,
          v.fec_unknown_amount = fec.unknown_amount,
          v.fec_unknown_count = fec.unknown_count,
          v.is_fec_donor = fec.is_fec_donor
    """)
    
    matched = cur.rowcount
    elapsed = time.time() - start
    
    print(f"  ✓ Enriched {matched:,} voters with FEC data ({elapsed:.1f}s)")
    print()
    
    # Summary stats
    print("Step 4: Summary statistics...")
    cur.execute("""
        SELECT 
            COUNT(*) as total_voters,
            SUM(is_fec_donor) as fec_donors,
            SUM(fec_democratic_amount) as dem_total,
            SUM(fec_republican_amount) as rep_total,
            SUM(fec_independent_amount) as ind_total,
            SUM(fec_total_amount) as overall_total
        FROM voter_file
    """)
    
    row = cur.fetchone()
    total, donors, dem, rep, ind, overall = row
    
    print(f"  Total voters: {int(total):,}")
    print(f"  FEC donors: {int(donors or 0):,} ({(donors or 0)/total*100:.2f}%)")
    print()
    print(f"  Total contributed: ${overall or 0:,.2f}")
    print(f"    Democratic:  ${dem or 0:,.2f}")
    print(f"    Republican:  ${rep or 0:,.2f}")
    print(f"    Independent: ${ind or 0:,.2f}")
    print()
    
    # Party breakdown
    print("  FEC Donors by registered party:")
    cur.execute("""
        SELECT 
            OfficialParty,
            COUNT(*) as donors,
            SUM(fec_total_amount) as total_amt
        FROM voter_file
        WHERE is_fec_donor = TRUE
        GROUP BY OfficialParty
        ORDER BY donors DESC
        LIMIT 10
    """)
    
    for party, count, amt in cur.fetchall():
        print(f"    {party or 'Unknown':20} {int(count):6,} donors  ${amt or 0:12,.2f}")
    
    conn.close()
    
    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print("  voter_file now includes FEC contribution data")
    print("  Ready for export with: python main.py export --ld XX")
    print()

if __name__ == "__main__":
    main()
