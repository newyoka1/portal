import pymysql, os
from dotenv import load_dotenv
load_dotenv('D:\\git\\nys-voter-pipeline\\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT',3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD')
)

cur = conn.cursor()

print("Checking for boe_donors database...")
cur.execute("SHOW DATABASES LIKE 'boe_donors'")
if cur.fetchone():
    print("✓ boe_donors database exists\n")
    
    cur.execute("USE boe_donors")
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    
    print(f"Tables in boe_donors ({len(tables)}):")
    for table in tables:
        print(f"  - {table[0]}")
        cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cur.fetchone()[0]
        print(f"    ({count:,} rows)")
else:
    print("✗ boe_donors database NOT found!")
    print("\nChecking voter_file for existing BOE data...")
    cur.execute("USE nys_voter_tagging")
    cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_R_amt > 0 OR boe_total_D_amt > 0")
    donors = cur.fetchone()[0]
    print(f"  Voters with BOE data: {donors:,}")
    
    if donors > 0:
        print("\n  BOE columns exist but boe_donors database is missing!")
        print("  The enrichment has already been run.")

conn.close()
