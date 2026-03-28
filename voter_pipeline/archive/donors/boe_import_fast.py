"""
BOE Fast Import - Using MySQL LOAD DATA LOCAL INFILE
====================================================
10-100x faster than Python csv.reader!

Strategy:
1. Use LOAD DATA LOCAL INFILE to bulk load entire CSV (fast)
2. Filter to Schedule A and valid contributions in SQL (still fast)
3. Match and aggregate (same as before)

Expected: ~2-5 minutes instead of 30 minutes
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

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

def main():
    print("="*80)
    print("BOE FAST IMPORT - Using MySQL LOAD DATA")
    print("="*80)
    
    # Enable local_infile for LOAD DATA
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
        local_infile=True,  # CRITICAL: Enable LOAD DATA LOCAL INFILE
        charset="utf8mb4",
        autocommit=True
    )
    cur = conn.cursor()
    
    # Create database
    print("\nStep 1: Creating boe_donors database...")
    cur.execute("CREATE DATABASE IF NOT EXISTS boe_donors")
    cur.execute("USE boe_donors")
    print("  ✓ Database ready")
    
    # Create staging table for raw load
    print("\nStep 2: Creating staging table...")
    cur.execute("DROP TABLE IF EXISTS contributions_staging")
    cur.execute("""
        CREATE TABLE contributions_staging (
            col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT,
            col5 TEXT, col6 TEXT, col7 TEXT, col8 TEXT,
            col9 TEXT, col10 TEXT, col11 TEXT, col12 TEXT,
            col13 TEXT, col14 TEXT, col15 TEXT, col16 TEXT,
            col17 TEXT, col18 TEXT, col19 TEXT, col20 TEXT,
            col21 TEXT, col22 TEXT, col23 TEXT, col24 TEXT,
            col25 TEXT, col26 TEXT, col27 TEXT, col28 TEXT,
            col29 TEXT, col30 TEXT, col31 TEXT, col32 TEXT,
            col33 TEXT, col34 TEXT, col35 TEXT, col36 TEXT,
            col37 TEXT, col38 TEXT, col39 TEXT, col40 TEXT,
            col41 TEXT, col42 TEXT, col43 TEXT, col44 TEXT,
            col45 TEXT, col46 TEXT, col47 TEXT, col48 TEXT,
            col49 TEXT, col50 TEXT, col51 TEXT, col52 TEXT,
            col53 TEXT, col54 TEXT, col55 TEXT, col56 TEXT,
            col57 TEXT, col58 TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    
    # Create final contributions table
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
    print("  ✓ Tables created")
    
    # Import each file using LOAD DATA
    print("\nStep 3: Bulk loading CSV files...")
    total_loaded = 0
    
    for csv_file in CONTRIBUTION_FILES:
        file_path = os.path.join(BOE_DIR, csv_file)
        
        if not os.path.exists(file_path):
            print(f"  ⚠️  Skipping {csv_file} (not found)")
            continue
        
        file_size = os.path.getsize(file_path) / (1024*1024)
        print(f"\n  Loading: {csv_file} ({file_size:.1f} MB)")
        
        # Clear staging
        cur.execute("TRUNCATE TABLE contributions_staging")
        
        # LOAD DATA - super fast!
        start = time.time()
        load_sql = f"""
            LOAD DATA LOCAL INFILE '{file_path.replace(chr(92), '/')}'
            INTO TABLE contributions_staging
            FIELDS TERMINATED BY ',' 
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
        """
        
        try:
            cur.execute(load_sql)
            conn.commit()
            
            cur.execute("SELECT COUNT(*) FROM contributions_staging")
            raw_count = cur.fetchone()[0]
            
            # Filter and insert into contributions_raw
            insert_sql = """
                INSERT INTO contributions_raw 
                (filer_id, committee_name, year, schedule_type, transaction_date,
                 first_name, middle_name, last_name, address, city, state, zip,
                 amount, employer, occupation)
                SELECT 
                    col1,  -- filer_id
                    col3,  -- committee_name
                    CAST(col4 AS UNSIGNED),  -- year
                    col11, -- schedule_type
                    STR_TO_DATE(SUBSTRING(col16, 1, 10), '%Y-%m-%d'),  -- transaction_date
                    col26, -- first_name
                    col27, -- middle_name
                    col28, -- last_name
                    col29, -- address
                    col30, -- city
                    col31, -- state
                    col32, -- zip
                    CAST(NULLIF(col37, '') AS DECIMAL(14,2)),  -- amount
                    col52, -- employer
                    col53  -- occupation
                FROM contributions_staging
                WHERE col11 = 'A'  -- Schedule A only
                  AND col28 IS NOT NULL  -- Must have last name
                  AND col28 != ''
                  AND CAST(NULLIF(col37, '') AS DECIMAL(14,2)) > 0  -- Amount > 0
                  AND CAST(col4 AS UNSIGNED) BETWEEN 2018 AND 2025
            """
            
            cur.execute(insert_sql)
            conn.commit()
            
            filtered_count = cur.rowcount
            elapsed = time.time() - start
            total_loaded += filtered_count
            
            print(f"    Raw rows: {raw_count:,}")
            print(f"    Filtered: {filtered_count:,} contributions")
            print(f"    Time: {elapsed:.1f}s ({int(filtered_count/elapsed):,} rows/sec)")
            
        except Exception as e:
            print(f"    ERROR: {e}")
            print(f"    Falling back to Python csv.reader for this file...")
            
            # ACTUAL FALLBACK - Use Python CSV reader
            import csv
            fallback_count = 0
            batch = []
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                
                for row in reader:
                    if len(row) < 40:
                        continue
                    
                    try:
                        # Only Schedule A
                        if len(row) > 10 and row[10] != 'A':
                            continue
                        
                        # Extract fields
                        filer_id = row[0] if len(row) > 0 else ''
                        committee = row[2] if len(row) > 2 else ''
                        year = int(row[3]) if len(row) > 3 and row[3].isdigit() else 0
                        
                        if year < 2018 or year > 2025:
                            continue
                        
                        trans_date = row[15][:10] if len(row) > 15 else None
                        first = row[25] if len(row) > 25 else ''
                        middle = row[26] if len(row) > 26 else ''
                        last = row[27] if len(row) > 27 else ''
                        addr = row[28] if len(row) > 28 else ''
                        city = row[29] if len(row) > 29 else ''
                        state = row[30] if len(row) > 30 else ''
                        zip_code = row[31] if len(row) > 31 else ''
                        
                        amt_str = row[36] if len(row) > 36 else '0'
                        amount = float(amt_str) if amt_str and amt_str.replace('.','').replace('-','').isdigit() else 0
                        
                        employer = row[51] if len(row) > 51 else ''
                        occupation = row[52] if len(row) > 52 else ''
                        
                        if not last or amount <= 0:
                            continue
                        
                        # Classify party
                        party = classify_party(committee)
                        
                        batch.append((
                            filer_id, committee, year, 'A', trans_date,
                            first, middle, last, addr, city, state, zip_code,
                            amount, employer, occupation, party
                        ))
                        
                        fallback_count += 1
                        
                        if len(batch) >= 5000:
                            cur.executemany("""
                                INSERT INTO contributions_raw 
                                (filer_id, committee_name, year, schedule_type, transaction_date,
                                 first_name, middle_name, last_name, address, city, state, zip,
                                 amount, employer, occupation, party)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, batch)
                            conn.commit()
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
                    conn.commit()
            
            total_loaded += fallback_count
            print(f"    Fallback imported: {fallback_count:,} contributions")
    
    # Classify parties
    print("\nStep 4: Classifying committee parties...")
    
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
    
    cur.execute("SELECT DISTINCT committee_name FROM contributions_raw")
    committees = cur.fetchall()
    
    for (comm,) in committees:
        party = classify_party(comm)
        cur.execute("UPDATE contributions_raw SET party=%s WHERE committee_name=%s", (party, comm))
    
    conn.commit()
    
    # Drop staging table
    cur.execute("DROP TABLE contributions_staging")
    
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
    
    print("\n" + "="*80)
    print("IMPORT COMPLETE!")
    print("="*80)
    print("\nNext: python donors/boe_match_aggregate.py")
    
    conn.close()

if __name__ == "__main__":
    main()