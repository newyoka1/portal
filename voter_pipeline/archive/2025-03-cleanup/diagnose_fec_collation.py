"""
Diagnose the exact collation mismatch causing the FEC enrichment error
"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_PORT')),
        charset='utf8mb4'
    )

conn = connect_db()
cursor = conn.cursor()

print("="*80)
print("DIAGNOSING FEC ENRICHMENT COLLATION ISSUE")
print("="*80)

# Check voter_file columns used in JOIN
print("\n1. nys_voter_tagging.voter_file JOIN columns:")
cursor.execute("""
    SELECT COLUMN_NAME, COLLATION_NAME, COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'nys_voter_tagging'
      AND TABLE_NAME = 'voter_file'
      AND COLUMN_NAME IN ('StateVoterId', 'first_name', 'last_name', 'zip5', 'firstname', 'lastname', 'rzip5')
    ORDER BY COLUMN_NAME
""")

for row in cursor.fetchall():
    col, collation, typ = row
    print(f"  {col:20} {typ:20} {collation}")

# Check what tables exist in fec_new
print("\n2. Tables in fec_new database:")
try:
    cursor.execute("USE fec_new")
    cursor.execute("SHOW TABLES")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"  Found {len(tables)} tables: {', '.join(tables[:10])}")
    
    # Check donor_summary table (commonly used for matching)
    if 'donor_summary' in tables:
        print("\n3. fec_new.donor_summary JOIN columns:")
        cursor.execute("""
            SELECT COLUMN_NAME, COLLATION_NAME, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'fec_new'
              AND TABLE_NAME = 'donor_summary'
              AND COLLATION_NAME IS NOT NULL
            ORDER BY COLUMN_NAME
        """)
        
        for row in cursor.fetchall():
            col, collation, typ = row
            print(f"  {col:30} {typ:20} {collation}")
    else:
        print("\n3. 'donor_summary' table NOT FOUND in fec_new")
        print(f"   Available tables: {', '.join(tables)}")

except Exception as e:
    print(f"\n  ERROR accessing fec_new: {e}")

# Check the actual error line from enrich_fec_donors.py
print("\n4. Checking the specific JOIN in enrich_fec_donors.py...")
print("   Looking at line 155 UPDATE statement...")

# Try to find which table it's actually joining
try:
    cursor.execute("USE fec_new")
    cursor.execute("SHOW TABLES LIKE '%donor%'")
    donor_tables = [t[0] for t in cursor.fetchall()]
    print(f"   Donor-related tables in fec_new: {donor_tables}")
except:
    pass

cursor.close()
conn.close()

print("\n" + "="*80)
print("DIAGNOSIS COMPLETE")
print("="*80)
