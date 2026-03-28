#!/usr/bin/env python3
"""
create_unified_voter_donor_table.py
Creates ONE table with all voter data + donation columns appended
"""
import pymysql
import csv
import os
from dotenv import load_dotenv
from pathlib import Path
import time

load_dotenv()

VOTER_CSV = Path(r"D:\git\nys-voter-pipeline\data\full voter 2025\fullnyvoter.csv")

print("\nStarting unified voter-donor table creation...")
print("This will take 10-15 minutes to load 13M voters\n")

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
)

cur = conn.cursor()

# Create database
print("Creating National_Donors database...")
cur.execute("CREATE DATABASE IF NOT EXISTS National_Donors CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
cur.execute("USE National_Donors")
print("✓ Database ready\n")

# Create unified table
print("Creating ny_voters_with_donations table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS ny_voters_with_donations (
        StateVoterId VARCHAR(50) PRIMARY KEY,
        LastName VARCHAR(100),
        FirstName VARCHAR(100),
        MiddleName VARCHAR(100),
        PrimaryCity VARCHAR(100),
        PrimaryZip VARCHAR(10),
        Zip5 VARCHAR(5) AS (SUBSTRING(PrimaryZip, 1, 5)) STORED,
        OfficialParty VARCHAR(30),
        CountyName VARCHAR(50),
        CDName VARCHAR(10),
        LDName VARCHAR(10),
        SDName VARCHAR(10),
        
        -- DONATION COLUMNS
        total_donation_amount DECIMAL(14,2) DEFAULT NULL,
        total_donation_count INT DEFAULT NULL,
        democratic_amount DECIMAL(14,2) DEFAULT NULL,
        democratic_count INT DEFAULT NULL,
        republican_amount DECIMAL(14,2) DEFAULT NULL,
        republican_count INT DEFAULT NULL,
        independent_amount DECIMAL(14,2) DEFAULT NULL,
        independent_count INT DEFAULT NULL,
        unknown_amount DECIMAL(14,2) DEFAULT NULL,
        unknown_count INT DEFAULT NULL,
        is_fec_donor BOOLEAN DEFAULT FALSE,
        
        INDEX idx_name (LastName, FirstName),
        INDEX idx_zip5 (Zip5),
        INDEX idx_party (OfficialParty),
        INDEX idx_donor (is_fec_donor)
    ) ENGINE=InnoDB
""")
print("✓ Table created with donation columns\n")
conn.commit()

# Check if already loaded
cur.execute("SELECT COUNT(*) FROM ny_voters_with_donations")
existing = cur.fetchone()[0]

if existing > 0:
    print(f"Found {existing:,} existing voters")
    response = input("Clear and reload? (y/N): ").strip().lower()
    if response != 'y':
        print("Keeping existing data")
        conn.close()
        exit(0)
    cur.execute("TRUNCATE TABLE ny_voters_with_donations")
    conn.commit()

# Load CSV
print(f"Loading voters from: {VOTER_CSV}")
print(f"File size: {VOTER_CSV.stat().st_size / 1024 / 1024:.1f} MB\n")

insert_sql = """
    INSERT INTO ny_voters_with_donations (
        StateVoterId, LastName, FirstName, MiddleName,
        PrimaryCity, PrimaryZip, OfficialParty, CountyName,
        CDName, LDName, SDName
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

batch = []
count = 0
start = time.time()

with open(VOTER_CSV, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    for row in reader:
        count += 1
        
        batch.append((
            row.get('StateVoterId'),
            row.get('LastName'),
            row.get('FirstName'),
            row.get('MiddleName'),
            row.get('PrimaryCity'),
            row.get('PrimaryZip'),
            row.get('OfficialParty'),
            row.get('CountyName'),
            row.get('CDName'),
            row.get('LDName'),
            row.get('SDName')
        ))
        
        if len(batch) >= 5000:
            cur.executemany(insert_sql, batch)
            conn.commit()
            elapsed = time.time() - start
            rate = count / elapsed if elapsed > 0 else 0
            print(f"\rProgress: {count:,} voters loaded ({rate:.0f} rec/sec)", end='', flush=True)
            batch = []
    
    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()

elapsed = time.time() - start
print(f"\n\n✓ Loaded {count:,} voters in {elapsed:.1f}s ({count/elapsed:.0f} rec/sec)\n")

# Append donations from politik1_fec
print("Checking for FEC donation data...")
try:
    cur.execute("SELECT COUNT(*) FROM politik1_fec.boe_voter_with_contributions WHERE total_contribution_amt > 0")
    donors = cur.fetchone()[0]
    print(f"✓ Found {donors:,} FEC donors\n")
    
    print("Appending donation totals...")
    cur.execute("""
        UPDATE ny_voters_with_donations v
        JOIN politik1_fec.boe_voter_with_contributions f ON v.StateVoterId = f.sboeid
        SET 
            v.total_donation_amount = f.total_contribution_amt,
            v.total_donation_count = f.contribution_count,
            v.is_fec_donor = TRUE
        WHERE f.total_contribution_amt > 0
    """)
    conn.commit()
    print(f"✓ Updated {cur.rowcount:,} voters with donation data\n")
    
    # Stats
    cur.execute("SELECT COUNT(*), SUM(total_donation_amount) FROM ny_voters_with_donations WHERE is_fec_donor = TRUE")
    donor_count, total_amt = cur.fetchone()
    print(f"=== SUMMARY ===")
    print(f"Total voters: {count:,}")
    print(f"FEC donors: {donor_count:,}")
    print(f"Total donated: ${total_amt:,.2f}")
    print(f"Average: ${total_amt/donor_count:,.2f}\n")
    
except Exception as e:
    print(f"No FEC data found: {e}")
    print("Donation columns will remain NULL until FEC data is loaded\n")

conn.close()

print("="*70)
print("COMPLETE - Table created: National_Donors.ny_voters_with_donations")
print("="*70)
print("\nColumns include:")
print("  ✓ StateVoterId (unique key)")
print("  ✓ Voter data (name, party, districts, etc.)")
print("  ✓ total_donation_amount & total_donation_count")
print("  ✓ democratic_amount & democratic_count (ready)")
print("  ✓ republican_amount & republican_count (ready)")
print("  ✓ independent_amount & independent_count (ready)")
print("  ✓ unknown_amount & unknown_count (ready)")
print("\nQuery: SELECT * FROM National_Donors.ny_voters_with_donations WHERE is_fec_donor = TRUE;")
print("="*70)
