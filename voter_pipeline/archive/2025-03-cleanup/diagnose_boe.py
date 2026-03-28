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

print("="*70)
print("BOE DONORS DIAGNOSTIC")
print("="*70)

# Check boe_donors database
cur.execute("USE boe_donors")
cur.execute("SHOW TABLES")
tables = cur.fetchall()

print(f"\nTables in boe_donors database: {len(tables)}")
for table in tables:
    table_name = table[0]
    print(f"\n  Table: {table_name}")
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    print(f"    Rows: {count:,}")
    
    # Check if it has StateVoterID column
    cur.execute(f"SHOW COLUMNS FROM {table_name} LIKE '%VoterID%'")
    voter_id_col = cur.fetchall()
    if voter_id_col:
        print(f"    Voter ID column: {voter_id_col[0][0]}")
    
    # Show first few column names
    cur.execute(f"DESCRIBE {table_name}")
    cols = cur.fetchall()
    print(f"    Columns ({len(cols)}): {', '.join([c[0] for c in cols[:8]])}...")

# Check voter_file BOE data
print("\n" + "="*70)
print("VOTER_FILE BOE DATA")
print("="*70)

cur.execute("USE nys_voter_tagging")

cur.execute("""
    SELECT 
        SUM(CASE WHEN boe_total_R_amt > 0 THEN 1 ELSE 0 END) as r_count,
        SUM(CASE WHEN boe_total_D_amt > 0 THEN 1 ELSE 0 END) as d_count,
        SUM(CASE WHEN boe_total_U_amt > 0 THEN 1 ELSE 0 END) as u_count,
        SUM(boe_total_R_amt) as r_amt,
        SUM(boe_total_D_amt) as d_amt,
        SUM(boe_total_U_amt) as u_amt
    FROM voter_file
""")

result = cur.fetchone()
r_cnt, d_cnt, u_cnt, r_amt, d_amt, u_amt = result

print(f"\nBOE Donors in voter_file:")
print(f"  Republican:   {int(r_cnt or 0):8,} donors   ${r_amt or 0:12,.2f}")
print(f"  Democrat:     {int(d_cnt or 0):8,} donors   ${d_amt or 0:12,.2f}")
print(f"  Unaffiliated: {int(u_cnt or 0):8,} donors   ${u_amt or 0:12,.2f}")

total_donors = int(r_cnt or 0) + int(d_cnt or 0) + int(u_cnt or 0)
total_amt = (r_amt or 0) + (d_amt or 0) + (u_amt or 0)

print(f"\n  TOTAL:        {total_donors:8,} donors   ${total_amt:12,.2f}")

if total_donors == 0:
    print("\n⚠️  BOE COLUMNS EXIST BUT HAVE NO DATA!")
    print("   Need to run: python main.py boe-enrich")
else:
    print("\n✓ BOE data exists in voter_file")

# Check a sample voter with BOE data
if total_donors > 0:
    print("\nSample BOE donor:")
    cur.execute("""
        SELECT StateVoterId, FirstName, LastName, 
               boe_total_R_amt, boe_total_D_amt, boe_total_U_amt
        FROM voter_file 
        WHERE boe_total_R_amt > 0 OR boe_total_D_amt > 0 OR boe_total_U_amt > 0
        LIMIT 1
    """)
    sample = cur.fetchone()
    if sample:
        print(f"  ID: {sample[0]}")
        print(f"  Name: {sample[1]} {sample[2]}")
        print(f"  R: ${sample[3] or 0:.2f}  D: ${sample[4] or 0:.2f}  U: ${sample[5] or 0:.2f}")

conn.close()
