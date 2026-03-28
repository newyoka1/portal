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
cur.execute("SELECT ModeledEthnicity, COUNT(*) cnt FROM voter_file GROUP BY ModeledEthnicity ORDER BY cnt DESC")
lines.append("=== ModeledEthnicity distribution ===")
for r in cur.fetchall(): lines.append(str(r))

lines.append("\n=== census sample ===")
cur.execute("SELECT * FROM ref_census_surnames LIMIT 3")
for r in cur.fetchall(): lines.append(str(r))

out = "\n".join(lines)
with open(r"D:\git\nys-voter-pipeline\logs\census_check.log", "w") as f:
    f.write(out)

cur.close(); conn.close()