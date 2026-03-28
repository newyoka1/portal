import os, sys
sys.path.insert(0, r"D:\git\nys-voter-pipeline")
from dotenv import load_dotenv
load_dotenv(r"D:\git\nys-voter-pipeline\.env")
import mysql.connector

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST","127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER","root"),
    password=os.getenv("MYSQL_PASSWORD",""),
    database="nys_voter_tagging"
)
cur = conn.cursor()
lines = []

# Check all LDName variations to see what's in the DB
cur.execute("""
    SELECT LDName, COUNT(*) cnt 
    FROM voter_file 
    WHERE LDName LIKE '%63%' OR LDName = '63'
    GROUP BY LDName ORDER BY cnt DESC
""")
lines.append("=== LDName values containing 63 ===")
for r in cur.fetchall(): lines.append(f"  LDName='{r[0]}'  count={r[1]:,}")

# Also check matched vs unmatched for LD 63
cur.execute("""
    SELECT 
        COUNT(*) AS total,
        SUM(CASE WHEN origin IS NOT NULL AND TRIM(origin)!='' THEN 1 ELSE 0 END) AS matched,
        SUM(CASE WHEN origin IS NULL OR TRIM(origin)='' THEN 1 ELSE 0 END) AS unmatched
    FROM voter_file
    WHERE LDName = '63'
""")
r = cur.fetchone()
lines.append(f"\n=== LD 63 breakdown ===")
lines.append(f"  Total:     {r[0]:,}")
lines.append(f"  Matched:   {r[1]:,}")
lines.append(f"  Unmatched: {r[2]:,}")

cur.close(); conn.close()

log = r"D:\git\nys-voter-pipeline\logs\ld_check.log"
with open(log, "w") as f: f.write("\n".join(lines))