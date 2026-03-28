#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create and populate ref_census_surnames table from Census Bureau API
Uses the Census Bureau's Surname List (2010) which provides ethnicity probabilities
"""

import pymysql
import requests
import os
import time
import sys

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Load environment
load_dotenv('D:\\git\\.env')

# Database config
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': 'nys_voter_tagging',
    'charset': 'utf8mb4'
}

# Census Bureau Surname List API
CENSUS_API_URL = "https://api.census.gov/data/2010/surname"

def create_table(conn):
    """Create the ref_census_surnames table"""
    cursor = conn.cursor()
    
    drop_sql = "DROP TABLE IF EXISTS ref_census_surnames"
    
    create_sql = """
    CREATE TABLE ref_census_surnames (
        surname VARCHAR(100) PRIMARY KEY,
        count INT,
        surname_rank INT,
        pct_white DECIMAL(6,3),
        pct_black DECIMAL(6,3),
        pct_api DECIMAL(6,3),
        pct_aian DECIMAL(6,3),
        pct_2prace DECIMAL(6,3),
        pct_hispanic DECIMAL(6,3),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_surname (surname)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    
    print("Creating ref_census_surnames table...")
    cursor.execute(drop_sql)
    cursor.execute(create_sql)
    conn.commit()
    print("✓ Table created successfully")


def fetch_census_data():
    """Fetch surname data from Census Bureau API"""
    print("\nFetching surname data from Census Bureau API...")
    print("This may take a few minutes...")
    
    # Request all fields
    params = {
        'get': 'NAME,COUNT,RANK,PCTWHITE,PCTBLACK,PCTAPI,PCTAIAN,PCT2PRACE,PCTHISPANIC',
        'for': 'us:*'
    }
    
    try:
        response = requests.get(CENSUS_API_URL, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        # First row is headers
        headers = data[0]
        rows = data[1:]
        
        print(f"✓ Retrieved {len(rows):,} surnames from Census Bureau")
        
        return headers, rows
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching data from Census API: {e}")
        print("\nTrying alternative method...")
        return fetch_census_data_alternative()


def fetch_census_data_alternative():
    """Alternative method: fetch from Census FTP site"""
    print("Fetching from Census FTP file...")
    
    # Census maintains surname files on their FTP
    ftp_url = "https://www2.census.gov/topics/genealogy/2010surnames/names.zip"
    
    try:
        import io
        import zipfile
        import csv
        
        print("Downloading surname data file...")
        response = requests.get(ftp_url, timeout=120)
        response.raise_for_status()
        
        # Extract ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Look for the CSV file
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise Exception("No CSV file found in ZIP")
            
            csv_file = csv_files[0]
            print(f"Extracting {csv_file}...")
            
            with z.open(csv_file) as f:
                # Read CSV
                text_data = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.reader(text_data)
                
                headers = next(reader)  # First row is headers
                rows = list(reader)
                
        print(f"✓ Retrieved {len(rows):,} surnames from Census file")
        return headers, rows
        
    except Exception as e:
        print(f"✗ Error with alternative method: {e}")
        return None, None


def insert_data(conn, headers, rows):
    """Insert surname data into database"""
    if not rows:
        print("✗ No data to insert")
        return
    
    cursor = conn.cursor()
    
    insert_sql = """
    INSERT INTO ref_census_surnames 
    (surname, count, surname_rank, pct_white, pct_black, pct_api, pct_aian, pct_2prace, pct_hispanic)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    print("\nInserting data into database...")
    
    # Map headers to positions
    header_map = {h.upper(): i for i, h in enumerate(headers)}
    
    batch_size = 1000
    inserted = 0
    batch = []
    
    for row in rows:
        try:
            surname = row[header_map.get('NAME', 0)]
            count = int(row[header_map.get('COUNT', 1)])
            rank = int(row[header_map.get('RANK', 2)])
            pct_white = float(row[header_map.get('PCTWHITE', 3)])
            pct_black = float(row[header_map.get('PCTBLACK', 4)])
            pct_api = float(row[header_map.get('PCTAPI', 5)])
            pct_aian = float(row[header_map.get('PCTAIAN', 6)])
            pct_2prace = float(row[header_map.get('PCT2PRACE', 7)])
            pct_hispanic = float(row[header_map.get('PCTHISPANIC', 8)])
            
            batch.append((
                surname, count, rank, 
                pct_white, pct_black, pct_api, 
                pct_aian, pct_2prace, pct_hispanic
            ))
            
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"  Inserted {inserted:,} / {len(rows):,} surnames...", end='\r')
                batch = []
                
        except (ValueError, IndexError) as e:
            print(f"\n  Warning: Skipping invalid row: {e}")
            continue
    
    # Insert remaining batch
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)
    
    print(f"\n✓ Successfully inserted {inserted:,} surnames")


def main():
    print("=" * 80)
    print("  CENSUS SURNAME ETHNICITY TABLE CREATOR")
    print("  Building from U.S. Census Bureau Surname List (2010)")
    print("=" * 80)
    
    # Connect to database
    try:
        print("\nConnecting to MySQL database...")
        conn = pymysql.connect(**DB_CONFIG)
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return
    
    try:
        # Create table
        create_table(conn)
        
        # Fetch Census data
        headers, rows = fetch_census_data()
        
        if rows:
            # Insert data
            insert_data(conn, headers, rows)
            
            # Show sample
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""
                SELECT surname, count, pct_white, pct_black, pct_hispanic 
                FROM ref_census_surnames 
                ORDER BY surname_rank 
                LIMIT 10
            """)
            
            print("\n" + "=" * 80)
            print("Sample data (top 10 most common surnames):")
            print("=" * 80)
            for row in cursor.fetchall():
                print(f"{row['surname']:15} Count: {row['count']:>8,}  "
                      f"White: {row['pct_white']:>5.1f}%  "
                      f"Black: {row['pct_black']:>5.1f}%  "
                      f"Hispanic: {row['pct_hispanic']:>5.1f}%")
            
            print("\n✓ Table created and populated successfully!")
            print("\nYou can now run your export script again.")
            
        else:
            print("\n✗ Failed to retrieve Census data")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()