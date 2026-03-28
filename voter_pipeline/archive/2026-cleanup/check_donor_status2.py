import pymysql, dotenv, os, sys
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST","localhost"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="nys_voter_tagging"
)
cur = conn.cursor()

out = []

cur.execute("SHOW TABLES LIKE 'boe_proven_donors'")
if cur.fetchone():
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors")
    out.append(f"boe_proven_donors: {cur.fetchone()[0]:,} rows")
    for p in ["D","R","U"]:
        cur.execute(f"SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_{p}_amt > 0")
        out.append(f"  {p} donors: {cur.fetchone()[0]:,}")
else:
    out.append("boe_proven_donors: NOT FOUND")

cur.execute("SHOW COLUMNS FROM voter_file LIKE 'boe_total_%'")
cols = [c[0] for c in cur.fetchall()]
out.append(f"voter_file BOE cols: {cols if cols else 'NONE'}")
if cols:
    for p in ["D","R","U"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM voter_file WHERE boe_total_{p}_amt > 0")
            out.append(f"  voter_file {p} enriched: {cur.fetchone()[0]:,}")
        except: pass

conn.close()
with open("logs\\donor_status.txt","w") as f:
    f.write("\n".join(out))
