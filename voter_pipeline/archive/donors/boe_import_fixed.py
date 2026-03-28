"""
BOE Import - FIXED Name Parsing
================================
The BOE CSV has ONE name field (col25) that needs to be parsed.
Not separate first/middle/last fields!
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BOE_DIR = r"D:\git\nys-voter-pipeline\data\boe_reports"

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

def parse_name(full_name):
    """Parse full name into first, middle, last"""
    if not full_name or not full_name.strip():
        return '', '', ''
    
    # Skip obvious organizations
    org_keywords = ['COMMITTEE', 'PARTY', 'PAC', 'CORP', 'LLC', 'INC', 'ASSOCIATION', 
                    'UNION', 'FUND', 'GROUP', 'NETWORK', 'COALITION']
    name_upper = full_name.upper()
    if any(kw in name_upper for kw in org_keywords):
        return '', '', ''  # Skip organizations
    
    # Split name
    parts = full_name.strip().split()
    
    if len(parts) == 0:
        return '', '', ''
    elif len(parts) == 1:
        return '', '', parts[0]  # Just last name
    elif len(parts) == 2:
        return parts[0], '', parts[1]  # First, Last
    elif len(parts) == 3:
        return parts[0], parts[1], parts[2]  # First, Middle, Last
    else:
        # 4+ parts: First, Middle(s), Last
        return parts[0], ' '.join(parts[1:-1]), parts[-1]

def classify_party(name):
    """Enhanced party classification"""
    n = name.upper()
    
    # Strong indicators
    if any(x in n for x in ['DEMOCRATIC', 'DEMOCRAT ', ' DEM ', 'DNC', 'DCCC', 'WORKING FAMILIES', 'WFP']):
        return 'D'
    if any(x in n for x in ['REPUBLICAN', ' REP ', ' GOP', 'RNC', 'NRCC', 'CONSERVATIVE', ' CON ']):
        return 'R'
    
    # Known politicians
    if any(x in n for x in ['HOCHUL', 'SCHUMER', 'GILLIBRAND', 'BIDEN', 'CLINTON']):
        return 'D'
    if any(x in n for x in ['TRUMP', 'STEFANIK', 'ZELDIN', 'MOLINARO']):
        return 'R'
    
    # Ideological
    if any(x in n for x in ['PROGRESSIVE', 'LABOR', 'UNION', 'PLANNED PARENTHOOD']):
        return 'D'
    if any(x in n for x in ['TAXPAYER', 'PRO-LIFE', 'CHAMBER OF COMMERCE', 'NRA']):
        return 'R'
    
    return 'U'

def main():
    import pymysql
    from dotenv import load_dotenv
    load_dotenv()
    
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        local_infile=True,
        charset="utf8mb4",
        autocommit=True
    )
    cur = conn.cursor()
    
    print("="*80)
    print("BOE IMPORT - FIXED NAME PARSING")
    print("="*80)
    
    # Create database
    print("\nStep 1: Creating boe_donors database...")
    cur.execute("CREATE DATABASE IF NOT EXISTS boe_donors")
    cur.execute("USE boe_donors")
    
    # Drop and recreate
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
            state VARCHAR(50),
            zip VARCHAR(20),
            amount DECIMAL(14,2),
            employer VARCHAR(255),
            occupation VARCHAR(255),
            party CHAR(1) DEFAULT 'U',
            INDEX(year),
            INDEX(last_name, first_name),
            INDEX(zip),
            INDEX(amount),
            INDEX(party)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    
    # Import using Python csv.reader (slow but correct)
    print("\nStep 3: Importing contributions...")
    import csv
    total_loaded = 0
    
    for csv_file in CONTRIBUTION_FILES:
        file_path = os.path.join(BOE_DIR, csv_file)
        
        if not os.path.exists(file_path):
            print(f"  ⚠️  Skipping {csv_file} (not found)")
            continue
        
        file_size = os.path.getsize(file_path) / (1024*1024)
        print(f"\n  Processing: {csv_file} ({file_size:.1f} MB)")
        
        count = 0
        batch = []
        start = time.time()
        
        # Diagnostic counters
        total_rows = 0
        sched_a_rows = 0
        individual_rows = 0
        valid_amount_rows = 0
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            for row in reader:
                total_rows += 1
                
                if len(row) < 40:
                    continue
                
                try:
                    # Only Schedule A (contributions received)
                    sched = row[10] if len(row) > 10 else ''
                    if sched != 'A':
                        continue
                    sched_a_rows += 1
                    
                    # Basic fields
                    filer_id = row[0] if len(row) > 0 else ''
                    committee = row[2] if len(row) > 2 else ''
                    year = int(row[3]) if len(row) > 3 and row[3].isdigit() else 0
                    
                    if year < 2018 or year > 2025:
                        continue
                    
                    # Date
                    trans_date = row[15][:10] if len(row) > 15 else None
                    
                    # Check contributor type (column 18, index 17)
                    contributor_type = row[17] if len(row) > 17 else ''
                    
                    # For individuals, use columns 26-28 directly
                    if contributor_type == 'Individual':
                        individual_rows += 1
                        first = row[25] if len(row) > 25 else ''
                        middle = row[26] if len(row) > 26 else ''
                        last = row[27] if len(row) > 27 else ''
                    else:
                        # For organizations, skip (we only want individuals)
                        continue
                    
                    # Skip if no last name
                    if not last or not last.strip():
                        continue
                    
                    # Address fields
                    addr = row[28] if len(row) > 28 else ''
                    city = row[29] if len(row) > 29 else ''
                    state = row[30] if len(row) > 30 else ''
                    zip_code = row[31] if len(row) > 31 else ''
                    
                    # Amount
                    amt_str = row[36] if len(row) > 36 else '0'
                    amount = float(amt_str) if amt_str and amt_str.replace('.','').replace('-','').isdigit() else 0
                    
                    if amount <= 0:
                        continue
                    valid_amount_rows += 1
                    
                    # Employment
                    employer = row[51] if len(row) > 51 else ''
                    occupation = row[52] if len(row) > 52 else ''
                    
                    # Classify party
                    party = classify_party(committee)
                    
                    batch.append((
                        filer_id, committee, year, 'A', trans_date,
                        first, middle, last, addr, city, state, zip_code,
                        amount, employer, occupation, party
                    ))
                    
                    count += 1
                    
                    if len(batch) >= 5000:
                        cur.executemany("""
                            INSERT INTO contributions_raw 
                            (filer_id, committee_name, year, schedule_type, transaction_date,
                             first_name, middle_name, last_name, address, city, state, zip,
                             amount, employer, occupation, party)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, batch)
                        batch = []
                        
                except Exception:
                    continue
            
            # Insert remaining
            if batch:
                cur.executemany("""
                    INSERT INTO contributions_raw 
                    (filer_id, committee_name, year, schedule_type, transaction_date,
                     first_name, middle_name, last_name, address, city, state, zip,
                     amount, employer, occupation, party)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
        
        elapsed = time.time() - start
        total_loaded += count
        print(f"    Total rows: {total_rows:,}")
        print(f"    Schedule A: {sched_a_rows:,}")
        print(f"    Individuals: {individual_rows:,}")
        print(f"    Valid amount: {valid_amount_rows:,}")
        print(f"    Imported: {count:,} contributions ({elapsed:.1f}s)")
    
    # Stats
    print(f"\n  ✓ Total loaded: {total_loaded:,} contributions")
    
    cur.execute("""
        SELECT party, COUNT(*), SUM(amount) 
        FROM contributions_raw 
        GROUP BY party
    """)
    print("\n  Party breakdown:")
    for party, count, total in cur.fetchall():
        party_name = {'D': 'Democrat', 'R': 'Republican', 'U': 'Unaffiliated'}.get(party, party)
        print(f"    {party_name:15} {int(count):8,} contributions  ${total or 0:12,.2f}")
    
    # Sample names
    print("\n  Sample contributors:")
    cur.execute("SELECT first_name, middle_name, last_name, city FROM contributions_raw LIMIT 5")
    for first, middle, last, city in cur.fetchall():
        print(f"    {first} {middle} {last} ({city})")
    
    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)
    print("\nNext: python donors/boe_match_aggregate.py")
    
    conn.close()

if __name__ == "__main__":
    main()