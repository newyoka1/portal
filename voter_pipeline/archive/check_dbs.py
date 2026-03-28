import os, sys
sys.path.insert(0, r"D:\git\nys-voter-pipeline")
from dotenv import load_dotenv
load_dotenv(r"D:\git\nys-voter-pipeline\.env")
import mysql.connector

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST","127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER","root"),
    password=os.getenv("MYSQL_PASSWORD","")
)
cur = conn.cursor()

lines = []

# All databases
cur.execute("SHOW DATABASES")
lines.append("=== DATABASES ===")
dbs = [r[0] for r in cur.fetchall() if r[0] not in ("information_schema","performance_schema","mysql","sys")]
for db in dbs:
    lines.append(f"  {db}")

# Tables + row counts per db
for db in dbs:
    lines.append(f"\n=== {db} TABLES ===")
    cur.execute(f"""
        SELECT table_name, table_rows, 
               ROUND((data_length+index_length)/1024/1024,1) AS size_mb
        FROM information_schema.tables
        WHERE table_schema = '{db}'
        ORDER BY table_rows DESC
    """)
    for r in cur.fetchall():
        lines.append(f"  {r[0]:<45} rows~{r[1]:>10,}  {r[2]} MB")

cur.close(); conn.close()

with open(r"D:\git\nys-voter-pipeline\logs\db_check.log", "w") as f:
    f.write("\n".join(str(l) for l in lines))