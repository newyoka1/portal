import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST","localhost"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD")
)
cur = conn.cursor()
cur.execute("SHOW DATABASES")
dbs = [r[0] for r in cur.fetchall()]
print("DATABASES:", dbs)

# Check each for boe_donors
for db in dbs:
    if db in ("information_schema","performance_schema","mysql","sys"): continue
    try:
        cur.execute(f"SHOW TABLES IN `{db}` LIKE 'boe_donors'")
        if cur.fetchone():
            cur.execute(f"SELECT COUNT(*) FROM `{db}`.boe_donors")
            cnt = cur.fetchone()[0]
            cur.execute(f"SHOW COLUMNS FROM `{db}`.boe_donors")
            cols = [c[0] for c in cur.fetchall()]
            print(f"\nFOUND: {db}.boe_donors  ({cnt:,} rows)")
            print(f"  cols: {cols}")
            cur.execute(f"SELECT * FROM `{db}`.boe_donors LIMIT 1")
            print(f"  sample: {cur.fetchone()}")
    except Exception as e:
        print(f"  {db}: {e}")
conn.close()
