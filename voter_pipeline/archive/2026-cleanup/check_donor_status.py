import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='nys_voter_tagging'
)
cur = conn.cursor()

# Check boe_proven_donors
cur.execute("SHOW TABLES LIKE 'boe_proven_donors'")
if cur.fetchone():
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_D_amt > 0")
    d = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_R_amt > 0")
    r = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_U_amt > 0")
    u = cur.fetchone()[0]
    print(f"boe_proven_donors: {total:,} rows")
    print(f"  D donors: {d:,}")
    print(f"  R donors: {r:,}")
    print(f"  U donors: {u:,}")
else:
    print("boe_proven_donors table NOT FOUND")

# Check voter_file BOE columns
cur.execute("SHOW COLUMNS FROM voter_file LIKE 'boe_total_%'")
cols = [c[0] for c in cur.fetchall()]
print(f"\nvoter_file BOE columns: {cols if cols else 'NONE'}")

if cols:
    cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_D_amt > 0")
    vd = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM voter_file WHERE boe_total_R_amt > 0")
    vr = cur.fetchone()[0]
    print(f"voter_file enriched - D: {vd:,}  R: {vr:,}")

conn.close()
