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

# Total voters in table
cur.execute("SELECT COUNT(*) FROM voter_file")
lines.append(f"Total rows in voter_file: {cur.fetchone()[0]:,}")

# Check for NULL LDName
cur.execute("SELECT COUNT(*) FROM voter_file WHERE LDName IS NULL OR TRIM(LDName)=''")
lines.append(f"Rows with NULL/empty LDName:    {cur.fetchone()[0]:,}")

# Top 10 LDs by count to see distribution
cur.execute("""
    SELECT LDName, COUNT(*) cnt FROM voter_file
    GROUP BY LDName ORDER BY cnt DESC LIMIT 10
""")
lines.append("\nTop 10 LDs by voter count:")
for r in cur.fetchall(): lines.append(f"  LD {r[0]:<6}  {r[1]:,}")

# Check if LD 63 voters exist with different LDName format
cur.execute("""
    SELECT DISTINCT LDName FROM voter_file 
    WHERE LDName IS NOT NULL 
    ORDER BY CAST(LDName AS UNSIGNED)
    LIMIT 20
""")
lines.append("\nSample LDName values (first 20):")
for r in cur.fetchall(): lines.append(f"  '{r[0]}'")

cur.close(); conn.close()

log = r"D:\git\nys-voter-pipeline\logs\total_check.log"
with open(log, "w") as f: f.write("\n".join(lines))