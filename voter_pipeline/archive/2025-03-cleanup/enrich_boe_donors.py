#!/usr/bin/env python3
"""
BOE Donor Enrichment Step
==========================
Adds NYS Board of Elections campaign finance data to voter_file.

This adds columns for:
- Democratic donations (amount by year: 2018-2024)
- Republican donations (amount by year: 2018-2024)
- Unaffiliated donations (amount by year: 2018-2024)
- Total amounts: boe_total_D_amt, boe_total_R_amt, boe_total_U_amt
- Donor lists: boe_Alist, boe_Blist, boe_ClistDEM
- Contact: boe_email

Called by: python main.py boe-enrich
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
    print("BOE DONOR ENRICHMENT - Integrating NYS Campaign Finance Data")
    print("=" * 80)
    print()
    
    # Check if donors_2024 database exists
    print("Step 1: Checking for BOE donor database...")
    conn_sys = connect_db("information_schema")
    cur = conn_sys.cursor()
    cur.execute("SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME = 'donors_2024'")
    if not cur.fetchone():
        print("  ERROR: donors_2024 database not found!")
        print("  Please run: python main.py donors")
        print("  This will build the BOE donor tables.")
        conn_sys.close()
        sys.exit(1)
    
    # Check for ProvenDonors table
    cur.execute("""
        SELECT TABLE_NAME FROM TABLES 
        WHERE TABLE_SCHEMA = 'donors_2024' 
        AND (TABLE_NAME LIKE 'ProvenDonors%' OR TABLE_NAME LIKE 'boe_%')
    """)
    donor_tables = cur.fetchall()
    if len(donor_tables) == 0:
        print("  ERROR: No ProvenDonors tables found in donors_2024!")
        print("  Please run: python main.py donors")
        conn_sys.close()
        sys.exit(1)
    
    conn_sys.close()
    print(f"  ✓ Found {len(donor_tables)} BOE donor tables")
    print()
    
    # Connect to voter database
    print("Step 2: Adding BOE columns to voter_file...")
    conn = connect_db("nys_voter_tagging")
    cur = conn.cursor()
    
    # Check if columns already exist
    cur.execute("SHOW COLUMNS FROM voter_file LIKE 'boe_%'")
    existing_cols = cur.fetchall()
    
    if len(existing_cols) > 0:
        print(f"  Found {len(existing_cols)} existing BOE columns")
        response = input("  Drop and recreate BOE columns? (y/N): ").strip().lower()
        if response == 'y':
            print("  Dropping existing BOE columns...")
            cols_to_drop = [
                'boe_total_D_amt', 'boe_total_R_amt', 'boe_total_U_amt',
                'boe_D_2018', 'boe_D_2019', 'boe_D_2020', 'boe_D_2021', 'boe_D_2022', 'boe_D_2023', 'boe_D_2024',
                'boe_R_2018', 'boe_R_2019', 'boe_R_2020', 'boe_R_2021', 'boe_R_2022', 'boe_R_2023', 'boe_R_2024',
                'boe_U_2018', 'boe_U_2019', 'boe_U_2020', 'boe_U_2021', 'boe_U_2022', 'boe_U_2023', 'boe_U_2024',
                'boe_Alist', 'boe_Blist', 'boe_ClistDEM', 'boe_email'
            ]
            for col in cols_to_drop:
                try:
                    cur.execute(f"ALTER TABLE voter_file DROP COLUMN {col}")
                except:
                    pass
    
    # Add new columns
    print("  Adding BOE contribution columns...")
    
    columns_to_add = [
        # Totals by party
        ("boe_total_D_amt", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_total_R_amt", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_total_U_amt", "DECIMAL(14,2) DEFAULT 0"),
        # Democratic by year
        ("boe_D_2018", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2019", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2020", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2021", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2022", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2023", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_D_2024", "DECIMAL(14,2) DEFAULT 0"),
        # Republican by year
        ("boe_R_2018", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2019", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2020", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2021", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2022", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2023", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_R_2024", "DECIMAL(14,2) DEFAULT 0"),
        # Unaffiliated by year
        ("boe_U_2018", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2019", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2020", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2021", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2022", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2023", "DECIMAL(14,2) DEFAULT 0"),
        ("boe_U_2024", "DECIMAL(14,2) DEFAULT 0"),
        # Donor lists and contact
        ("boe_Alist", "BOOLEAN DEFAULT FALSE"),
        ("boe_Blist", "BOOLEAN DEFAULT FALSE"),
        ("boe_ClistDEM", "BOOLEAN DEFAULT FALSE"),
        ("boe_email", "VARCHAR(255) DEFAULT NULL"),
    ]
    
    added_count = 0
    for col_name, col_type in columns_to_add:
        cur.execute(f"SHOW COLUMNS FROM voter_file LIKE '{col_name}'")
        if not cur.fetchone():
            cur.execute(f"ALTER TABLE voter_file ADD COLUMN {col_name} {col_type}")
            added_count += 1
    
    # Add index
    cur.execute("SHOW INDEX FROM voter_file WHERE Key_name = 'idx_boe_donor'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE voter_file ADD INDEX idx_boe_donor (boe_total_D_amt, boe_total_R_amt, boe_total_U_amt)")
        added_count += 1
    
    if added_count > 0:
        print(f"  ✓ Added {added_count} new columns/indexes")
    else:
        print("  ✓ All BOE columns already exist")
    print()
    
    # Find the BOE donor table
    print("Step 3: Finding BOE donor table...")
    conn_donors = connect_db("donors_2024")
    cur_donors = conn_donors.cursor()
    
    # Try to find the main donor table
    table_name = None
    for candidate in ['ProvenDonors2024_BOEReclassified', 'boe_proven_donors', 'ProvenDonors2024']:
        cur_donors.execute(f"SHOW TABLES LIKE '{candidate}'")
        if cur_donors.fetchone():
            table_name = candidate
            break
    
    if not table_name:
        print("  ERROR: Could not find BOE donor table!")
        print("  Looking for: ProvenDonors2024_BOEReclassified, boe_proven_donors, or ProvenDonors2024")
        conn_donors.close()
        conn.close()
        sys.exit(1)
    
    print(f"  ✓ Using table: donors_2024.{table_name}")
    
    # Check what columns are available
    cur_donors.execute(f"DESCRIBE {table_name}")
    available_cols = [row[0] for row in cur_donors.fetchall()]
    print(f"  ✓ Table has {len(available_cols)} columns")
    print()
    
    # Enrich data
    print("Step 4: Enriching voter_file with BOE data...")
    print("  (This may take 3-5 minutes for 13M voters)")
    
    import time
    start = time.time()
    
    # Build dynamic SET clause based on available columns
    set_clauses = []
    
    # Totals
    if 'DEM_TOTAL' in available_cols:
        set_clauses.append("v.boe_total_D_amt = COALESCE(boe.DEM_TOTAL, 0)")
    if 'REP_TOTAL' in available_cols:
        set_clauses.append("v.boe_total_R_amt = COALESCE(boe.REP_TOTAL, 0)")
    if 'UNA_TOTAL' in available_cols:
        set_clauses.append("v.boe_total_U_amt = COALESCE(boe.UNA_TOTAL, 0)")
    
    # By year (2018-2024)
    for year in range(2018, 2025):
        for party, prefix in [('D', 'DEM'), ('R', 'REP'), ('U', 'UNA')]:
            col = f"{prefix}_{year}"
            if col in available_cols:
                set_clauses.append(f"v.boe_{party}_{year} = COALESCE(boe.{col}, 0)")
    
    # Donor lists
    if 'A_LIST_DONOR' in available_cols:
        set_clauses.append("v.boe_Alist = COALESCE(boe.A_LIST_DONOR, 0)")
    if 'B_LIST_DONOR' in available_cols:
        set_clauses.append("v.boe_Blist = COALESCE(boe.B_LIST_DONOR, 0)")
    if 'C_LIST_DEM' in available_cols:
        set_clauses.append("v.boe_ClistDEM = COALESCE(boe.C_LIST_DEM, 0)")
    
    # Email
    if 'EMAIL' in available_cols:
        set_clauses.append("v.boe_email = boe.EMAIL")
    
    if not set_clauses:
        print("  ERROR: No matching columns found in BOE donor table!")
        conn_donors.close()
        conn.close()
        sys.exit(1)
    
    set_clause = ",\n          ".join(set_clauses)
    
    sql = f"""
        UPDATE nys_voter_tagging.voter_file v
        JOIN donors_2024.{table_name} boe
          ON v.StateVoterId COLLATE utf8mb4_unicode_ci = boe.StateVoterID COLLATE utf8mb4_unicode_ci
        SET
          {set_clause}
    """
    
    cur.execute(sql)
    matched = cur.rowcount
    elapsed = time.time() - start
    
    print(f"  ✓ Enriched {matched:,} voters with BOE data ({elapsed:.1f}s)")
    print()
    
    # Summary stats
    print("Step 5: Summary statistics...")
    cur.execute("""
        SELECT 
            COUNT(*) as total_voters,
            SUM(CASE WHEN boe_total_D_amt > 0 OR boe_total_R_amt > 0 OR boe_total_U_amt > 0 THEN 1 ELSE 0 END) as boe_donors,
            SUM(boe_total_D_amt) as dem_total,
            SUM(boe_total_R_amt) as rep_total,
            SUM(boe_total_U_amt) as una_total,
            SUM(boe_total_D_amt + boe_total_R_amt + boe_total_U_amt) as overall_total
        FROM voter_file
    """)
    
    row = cur.fetchone()
    total, donors, dem, rep, una, overall = row
    
    print(f"  Total voters: {int(total):,}")
    print(f"  BOE donors: {int(donors or 0):,} ({(donors or 0)/total*100:.2f}%)")
    print()
    print(f"  Total contributed: ${overall or 0:,.2f}")
    print(f"    Democratic:   ${dem or 0:,.2f}")
    print(f"    Republican:   ${rep or 0:,.2f}")
    print(f"    Unaffiliated: ${una or 0:,.2f}")
    print()
    
    # Party breakdown
    print("  BOE Donors by registered party:")
    cur.execute("""
        SELECT 
            OfficialParty,
            COUNT(*) as donors,
            SUM(boe_total_D_amt + boe_total_R_amt + boe_total_U_amt) as total_amt
        FROM voter_file
        WHERE boe_total_D_amt > 0 OR boe_total_R_amt > 0 OR boe_total_U_amt > 0
        GROUP BY OfficialParty
        ORDER BY donors DESC
        LIMIT 10
    """)
    
    for party, count, amt in cur.fetchall():
        print(f"    {party or 'Unknown':20} {int(count):6,} donors  ${amt or 0:12,.2f}")
    
    conn_donors.close()
    conn.close()
    
    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print("  voter_file now includes BOE contribution data")
    print("  Ready for export with: python main.py export --ld XX")
    print()

if __name__ == "__main__":
    main()
