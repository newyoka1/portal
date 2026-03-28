import pymysql, os
from dotenv import load_dotenv
load_dotenv('D:\\git\\nys-voter-pipeline\\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT',3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='nys_voter_tagging'
)

cur = conn.cursor()

print("="*60)
print("BOE DONOR DATA CHECK")
print("="*60)

# Check for BOE columns
cur.execute("SHOW COLUMNS FROM voter_file WHERE Field LIKE 'boe_%'")
boe_cols = cur.fetchall()
print(f"\nBOE columns in voter_file: {len(boe_cols)}")
for col in boe_cols[:10]:  # Show first 10
    print(f"  - {col[0]}")
if len(boe_cols) > 10:
    print(f"  ... and {len(boe_cols)-10} more")

# Check if any voters have BOE data
print("\nBOE Data Check:")
cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_R_amt > 0")
r_donors = cur.fetchone()[0]
print(f"  Republican donors: {r_donors:,}")

cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_D_amt > 0")
d_donors = cur.fetchone()[0]
print(f"  Democrat donors: {d_donors:,}")

cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_U_amt > 0")
u_donors = cur.fetchone()[0]
print(f"  Unaffiliated donors: {u_donors:,}")

total_donors = r_donors + d_donors + u_donors
print(f"\n  TOTAL BOE donors: {total_donors:,}")

if total_donors == 0:
    print("\n  ⚠️  BOE columns exist but have NO DATA!")
    print("  Need to run enrichment.")
else:
    print("\n  ✓ BOE data exists in voter_file")

conn.close()
