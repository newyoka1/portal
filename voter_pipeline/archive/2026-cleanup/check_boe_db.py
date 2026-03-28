import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST","localhost"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="boe_donors"
)
cur = conn.cursor()
cur.execute("SHOW TABLES")
tables = [r[0] for r in cur.fetchall()]
print("TABLES:", tables)
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM `{t}`")
    cnt = cur.fetchone()[0]
    cur.execute(f"SHOW COLUMNS FROM `{t}`")
    cols = [c[0] for c in cur.fetchall()]
    print(f"\n{t}: {cnt:,} rows")
    print(f"  cols: {cols}")
    cur.execute(f"SELECT * FROM `{t}` LIMIT 1")
    print(f"  sample: {cur.fetchone()}")
conn.close()
