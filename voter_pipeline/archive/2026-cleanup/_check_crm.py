import pymysql
from dotenv import load_dotenv
import os

load_dotenv(r"D:\git\nys-voter-pipeline\.env")

conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="crm_unified",
    charset="utf8mb4"
)
cur = conn.cursor()

# Show all tables
cur.execute("SHOW TABLES")
tables = [r[0] for r in cur.fetchall()]
print("TABLES:", tables)

# For each table, show columns that might contain email/name/address
for t in tables:
    cur.execute(f"SHOW COLUMNS FROM `{t}`")
    cols = cur.fetchall()
    print(f"\n--- {t} ({len(cols)} cols) ---")
    for c in cols:
        print(f"  {c[0]:40s} {c[1]}")

# Check row counts
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM `{t}`")
    cnt = cur.fetchone()[0]
    print(f"\n{t}: {cnt:,} rows")

conn.close()
