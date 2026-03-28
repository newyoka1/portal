#!/usr/bin/env python3
"""
Load BOE ProvenDonors data into boe_donors database
"""
import os
import sys
import pymysql
import csv
import time
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

PROVEN_DONORS_CSV = r"D:\git\nys-voter-pipeline\data\boe_donors\ProvenDonors2024OnePerInd.csv"

print("=" * 80)
print("LOADING BOE PROVENDONORS DATA")
print("=" * 80)
print()

# Create boe_donors database
print("Step 1: Creating boe_donors database...")
conn = pymysql.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASSWORD,
    charset="utf8mb4", autocommit=True
)
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS boe_donors CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
print("  ? Database created")
conn.close()
print()

# Connect to boe_donors database
print("Step 2: Checking if ProvenDonors table exists...")
conn = pymysql.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASSWORD,
    database="boe_donors",
    charset="utf8mb4", autocommit=True,
    local_infile=True
)
cur = conn.cursor()

# Check if table exists and has data
cur.execute("SHOW TABLES LIKE 'ProvenDonors2024'")
table_exists = cur.fetchone()

if table_exists:
    cur.execute("SELECT COUNT(*) FROM ProvenDonors2024")
    existing_count = cur.fetchone()[0]
    
    if existing_count > 0:
        print(f"  Found existing ProvenDonors2024 table with {existing_count:,} records")
        response = input("  Reload data? (y/N): ").strip().lower()
        if response != 'y':
            print("  Skipping load - using existing data")
            print()
            print("=" * 80)
            print("COMPLETE!")
            print("=" * 80)
            print()
            print(f"BOE data already loaded: {existing_count:,} donors")
            print()
            print("Next steps:")
            print("  1. python main.py boe-enrich")
            print("  2. python main.py export --ld 63")
            print()
            conn.close()
            sys.exit(0)

print("  Creating ProvenDonors table...")

# Drop existing table
cur.execute("DROP TABLE IF EXISTS ProvenDonors2024")

# Create table with all columns from ProvenDonors CSV
cur.execute("""
CREATE TABLE ProvenDonors2024 (
    StateVoterID VARCHAR(30),
    voterparty VARCHAR(10),
    LASTNAME VARCHAR(100),
    FIRSTNAME VARCHAR(100),
    ZIPCODE VARCHAR(10),
    Countyname VARCHAR(50),
    adval VARCHAR(10),
    sdval VARCHAR(10),
    cdval VARCHAR(10),
    email VARCHAR(255),
    
    -- Democratic amounts by year
    D2018amt DECIMAL(14,2) DEFAULT 0,
    D2019amt DECIMAL(14,2) DEFAULT 0,
    D2020amt DECIMAL(14,2) DEFAULT 0,
    D2021amt DECIMAL(14,2) DEFAULT 0,
    D2022amt DECIMAL(14,2) DEFAULT 0,
    D2023amt DECIMAL(14,2) DEFAULT 0,
    D2024amt DECIMAL(14,2) DEFAULT 0,
    
    -- Republican amounts by year
    R2018amt DECIMAL(14,2) DEFAULT 0,
    R2019amt DECIMAL(14,2) DEFAULT 0,
    R2020amt DECIMAL(14,2) DEFAULT 0,
    R2021amt DECIMAL(14,2) DEFAULT 0,
    R2022amt DECIMAL(14,2) DEFAULT 0,
    R2023amt DECIMAL(14,2) DEFAULT 0,
    R2024amt DECIMAL(14,2) DEFAULT 0,
    
    -- Unaffiliated amounts by year
    U2018amt DECIMAL(14,2) DEFAULT 0,
    U2019amt DECIMAL(14,2) DEFAULT 0,
    U2020amt DECIMAL(14,2) DEFAULT 0,
    U2021amt DECIMAL(14,2) DEFAULT 0,
    U2022amt DECIMAL(14,2) DEFAULT 0,
    U2023amt DECIMAL(14,2) DEFAULT 0,
    U2024amt DECIMAL(14,2) DEFAULT 0,
    
    -- Democratic counts by year
    D2018cnt INT DEFAULT 0,
    D2019cnt INT DEFAULT 0,
    D2020cnt INT DEFAULT 0,
    D2021cnt INT DEFAULT 0,
    D2022cnt INT DEFAULT 0,
    D2023cnt INT DEFAULT 0,
    D2024cnt INT DEFAULT 0,
    
    -- Republican counts by year
    R2018cnt INT DEFAULT 0,
    R2019cnt INT DEFAULT 0,
    R2020cnt INT DEFAULT 0,
    R2021cnt INT DEFAULT 0,
    R2022cnt INT DEFAULT 0,
    R2023cnt INT DEFAULT 0,
    R2024cnt INT DEFAULT 0,
    
    -- Unaffiliated counts by year
    U2018cnt INT DEFAULT 0,
    U2019cnt INT DEFAULT 0,
    U2020cnt INT DEFAULT 0,
    U2021cnt INT DEFAULT 0,
    U2022cnt INT DEFAULT 0,
    U2023cnt INT DEFAULT 0,
    U2024cnt INT DEFAULT 0,
    
    -- Flags
    ContribToRep BOOLEAN DEFAULT FALSE,
    ContribToDem BOOLEAN DEFAULT FALSE,
    ContribToUnk BOOLEAN DEFAULT FALSE,
    Alist BOOLEAN DEFAULT FALSE,
    Blist BOOLEAN DEFAULT FALSE,
    Clist BOOLEAN DEFAULT FALSE,
    ClistDEM BOOLEAN DEFAULT FALSE,
    BlistDEM BOOLEAN DEFAULT FALSE,
    
    -- Totals
    DEM_TOTAL DECIMAL(14,2) AS (D2018amt + D2019amt + D2020amt + D2021amt + D2022amt + D2023amt + D2024amt) STORED,
    REP_TOTAL DECIMAL(14,2) AS (R2018amt + R2019amt + R2020amt + R2021amt + R2022amt + R2023amt + R2024amt) STORED,
    UNA_TOTAL DECIMAL(14,2) AS (U2018amt + U2019amt + U2020amt + U2021amt + U2022amt + U2023amt + U2024amt) STORED,
    
    -- List flags
    A_LIST_DONOR BOOLEAN AS (Alist) STORED,
    B_LIST_DONOR BOOLEAN AS (Blist) STORED,
    C_LIST_DEM BOOLEAN AS (ClistDEM) STORED,
    
    PRIMARY KEY (StateVoterID),
    INDEX idx_name (LASTNAME, FIRSTNAME),
    INDEX idx_totals (DEM_TOTAL, REP_TOTAL, UNA_TOTAL)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
""")
print("  ? Table created")
print()

# Load data using LOAD DATA LOCAL INFILE for speed
print("Step 3: Loading ProvenDonors CSV...")
print(f"  File: {PROVEN_DONORS_CSV}")

csv_path = PROVEN_DONORS_CSV.replace("\\", "/")

try:
    start = time.time()
    cur.execute(f"""
        LOAD DATA LOCAL INFILE '{csv_path}'
        INTO TABLE ProvenDonors2024
        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
    """)
    elapsed = time.time() - start
    
    cur.execute("SELECT COUNT(*) FROM ProvenDonors2024")
    count = cur.fetchone()[0]
    
    print(f"  ? Loaded {count:,} donors ({elapsed:.1f}s)")
    
except Exception as e:
    print(f"  ERROR: {e}")
    print()
    print("  Falling back to slower CSV import method...")
    
    # Fallback: Load via Python CSV reader
    cur.execute("TRUNCATE TABLE ProvenDonors2024")
    
    batch = []
    count = 0
    start = time.time()
    
    with open(PROVEN_DONORS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            batch.append(tuple(row.get(col, 0) for col in [
                'sboeid', 'voterparty', 'LASTNAME', 'FIRSTNAME', 'ZIPCODE',
                'Countyname', 'adval', 'sdval', 'cdval', 'email',
                'D2018amt', 'D2019amt', 'D2020amt', 'D2021amt', 'D2022amt', 'D2023amt', 'D2024amt',
                'R2018amt', 'R2019amt', 'R2020amt', 'R2021amt', 'R2022amt', 'R2023amt', 'R2024amt',
                'U2018amt', 'U2019amt', 'U2020amt', 'U2021amt', 'U2022amt', 'U2023amt', 'U2024amt',
                'D2018cnt', 'D2019cnt', 'D2020cnt', 'D2021cnt', 'D2022cnt', 'D2023cnt', 'D2024cnt',
                'R2018cnt', 'R2019cnt', 'R2020cnt', 'R2021cnt', 'R2022cnt', 'R2023cnt', 'R2024cnt',
                'U2018cnt', 'U2019cnt', 'U2020cnt', 'U2021cnt', 'U2022cnt', 'U2023cnt', 'U2024cnt',
                'ContribToRep', 'ContribToDem', 'ContribToUnk',
                'Alist', 'Blist', 'Clist', 'ClistDEM', 'BlistDEM'
            ]))
            
            if len(batch) >= 5000:
                placeholders = ','.join(['%s'] * 72)
                cur.executemany(f"INSERT INTO ProvenDonors2024 VALUES ({placeholders})", batch)
                print(f"\r  Loaded {count:,} donors...", end='', flush=True)
                batch = []
        
        if batch:
            placeholders = ','.join(['%s'] * 72)
            cur.executemany(f"INSERT INTO ProvenDonors2024 VALUES ({placeholders})", batch)
    
    elapsed = time.time() - start
    print(f"\r  ? Loaded {count:,} donors ({elapsed:.1f}s)")

conn.close()

print()
print("=" * 80)
print("COMPLETE!")
print("=" * 80)
print()
print("BOE data loaded into: boe_donors.ProvenDonors2024")
print()
print("Next steps:")
print("  1. python main.py boe-enrich")
print("  2. python main.py export --ld 63")
print()

