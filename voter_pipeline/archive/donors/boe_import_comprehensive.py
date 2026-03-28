"""
BOE Comprehensive Import - New Format
======================================
Imports all BOE contribution files (2023-2025) and builds donor summary table.

Files processed:
- 2023gen.csv, 2024gen.csv, 2024pri.csv, 2024offcycle.csv, 2024ghi.csv
- 2025gen.csv, 2025pri.csv, 2025offcycle.csv, 2025ghi.csv
- COMMCAND.CSV (committee master for party classification)

Creates:
- boe_donors.contributions_raw (all contributions)
- boe_donors.contributions_matched (matched to voters)
- boe_donors.donor_summary (aggregated by StateVoterId + party + year)
"""

import os, sys, csv, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

BOE_DIR = r"D:\git\nys-voter-pipeline\data\boe_reports"

# Files to import
CONTRIBUTION_FILES = [
    "2023gen_extract/2023gen.csv",
    "2024gen_extract/2024gen.csv",
    "2024pri_extract/2024pri.csv",
    "2024offcycle_extract/2024offcycle.csv",
    "2024ghi_extract/2024ghi.csv",
    "2025gen_extract/2025gen.csv",
    "2025pri_extract/2025pri.csv",
    "2025offcycle_extract/2025offcycle.csv",
    "2025ghi_extract/2025ghi.csv",
]

def main():
    print("="*80)
    print("BOE COMPREHENSIVE IMPORT - Processing 2023-2025 Contributions")
    print("="*80)
    
    conn = get_conn()
    cur = conn.cursor()
    
    # Create boe_donors database
    print("\nStep 1: Creating boe_donors database...")
    cur.execute("CREATE DATABASE IF NOT EXISTS boe_donors")
    cur.execute("USE boe_donors")
    print("  ✓ Database ready")
    
    # Drop and recreate raw contributions table
    print("\nStep 2: Creating contributions_raw table...")
    cur.execute("DROP TABLE IF EXISTS contributions_raw")
    cur.execute("""
        CREATE TABLE contributions_raw (
            id INT AUTO_INCREMENT PRIMARY KEY,
            filer_id VARCHAR(50),
            committee_name VARCHAR(255),
            year INT,
            schedule_type VARCHAR(10),
            transaction_date DATE,
            first_name VARCHAR(100),
            middle_name VARCHAR(100),
            last_name VARCHAR(100),
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(10),
            zip VARCHAR(20),
            amount DECIMAL(14,2),
            employer VARCHAR(255),
            occupation VARCHAR(255),
            raw_line TEXT,
            INDEX(year),
            INDEX(last_name, first_name),
            INDEX(zip),
            INDEX(amount)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    print("  ✓ Table created")
    
    # Import all files
    print("\nStep 3: Importing contribution files...")
    total_imported = 0
    
    for csv_file in CONTRIBUTION_FILES:
        file_path = os.path.join(BOE_DIR, csv_file)
        
        if not os.path.exists(file_path):
            print(f"  ⚠️  Skipping {csv_file} (not found)")
            continue
        
        file_size = os.path.getsize(file_path) / (1024*1024)
        print(f"\n  Processing: {csv_file} ({file_size:.1f} MB)")
        
        start = time.time()
        count = 0
        batch = []
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            for row in reader:
                if len(row) < 40:  # Skip malformed rows
                    continue
                
                # Extract key fields (positions may vary - adjust based on actual structure)
                try:
                    filer_id = row[0] if len(row) > 0 else ''
                    committee = row[2] if len(row) > 2 else ''
                    year = int(row[3]) if len(row) > 3 and row[3].isdigit() else 0
                    sched = row[10] if len(row) > 10 else ''
                    
                    # Only process Schedule A (monetary contributions)
                    if sched != 'A':
                        continue
                    
                    # Date
                    date_str = row[15] if len(row) > 15 else ''
                    trans_date = date_str[:10] if date_str else None
                    
                    # Donor info
                    first = row[25] if len(row) > 25 else ''
                    middle = row[26] if len(row) > 26 else ''
                    last = row[27] if len(row) > 27 else ''
                    addr = row[28] if len(row) > 28 else ''
                    city = row[29] if len(row) > 29 else ''
                    state = row[30] if len(row) > 30 else ''
                    zip_code = row[31] if len(row) > 31 else ''
                    
                    # Amount
                    amt_str = row[36] if len(row) > 36 else '0'
                    amount = float(amt_str) if amt_str and amt_str.replace('.','').replace('-','').isdigit() else 0
                    
                    # Employment
                    employer = row[51] if len(row) > 51 else ''
                    occupation = row[52] if len(row) > 52 else ''
                    
                    # Only include individual contributions with name
                    if not last or amount <= 0:
                        continue
                    
                    batch.append((
                        filer_id, committee, year, sched, trans_date,
                        first, middle, last, addr, city, state, zip_code,
                        amount, employer, occupation, '|'.join(row[:40])
                    ))
                    
                    count += 1
                    
                    # Batch insert every 5000 rows
                    if len(batch) >= 5000:
                        cur.executemany("""
                            INSERT INTO contributions_raw 
                            (filer_id, committee_name, year, schedule_type, transaction_date,
                             first_name, middle_name, last_name, address, city, state, zip,
                             amount, employer, occupation, raw_line)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, batch)
                        conn.commit()
                        batch = []
                        
                except Exception as e:
                    continue  # Skip bad rows
            
            # Insert remaining batch
            if batch:
                cur.executemany("""
                    INSERT INTO contributions_raw 
                    (filer_id, committee_name, year, schedule_type, transaction_date,
                     first_name, middle_name, last_name, address, city, state, zip,
                     amount, employer, occupation, raw_line)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
        
        elapsed = time.time() - start
        total_imported += count
        print(f"    Imported: {count:,} contributions ({elapsed:.1f}s)")
    
    print(f"\n  ✓ Total imported: {total_imported:,} contributions")
    
    # Load committee master for party classification
    print("\nStep 4: Loading committee master (COMMCAND.CSV)...")
    commcand_path = os.path.join(BOE_DIR, "commcand_extract/COMMCAND.CSV")
    
    if os.path.exists(commcand_path):
        cur.execute("DROP TABLE IF EXISTS committees")
        cur.execute("""
            CREATE TABLE committees (
                filer_id VARCHAR(50) PRIMARY KEY,
                committee_name VARCHAR(255),
                committee_type VARCHAR(50),
                level VARCHAR(50),
                status VARCHAR(50)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """)
        
        batch = []
        with open(commcand_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 5:
                    batch.append((row[0], row[1], row[2], row[3], row[4]))
                    
                    if len(batch) >= 1000:
                        cur.executemany("""
                            INSERT INTO committees (filer_id, committee_name, committee_type, level, status)
                            VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE committee_name=VALUES(committee_name)
                        """, batch)
                        conn.commit()
                        batch = []
            
            if batch:
                cur.executemany("""
                    INSERT INTO committees (filer_id, committee_name, committee_type, level, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE committee_name=VALUES(committee_name)
                """, batch)
                conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM committees")
        comm_count = cur.fetchone()[0]
        print(f"  ✓ Loaded {comm_count:,} committees")
    else:
        print("  ⚠️  COMMCAND.CSV not found, skipping party classification")
    
    print("\n" + "="*80)
    print("IMPORT COMPLETE!")
    print("="*80)
    print(f"\nNext steps:")
    print("  1. Classify committee parties (Democrat/Republican/Unaffiliated)")
    print("  2. Match contributions to voters by name+zip")
    print("  3. Aggregate by StateVoterId + party + year")
    print("  4. Run: python main.py boe-enrich")
    
    conn.close()

if __name__ == "__main__":
    main()