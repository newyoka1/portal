import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host="127.0.0.1",
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD")
)
cur = conn.cursor()
cur.execute("SHOW DATABASES")
dbs = [r[0] for r in cur.fetchall()]
print("DBS:", dbs)
for db in dbs:
    if db in ("information_schema","performance_schema","mysql","sys"): continue
    try:
        cur.execute(f"SHOW TABLES IN `{db}`")
        tables = [r[0] for r in cur.fetchall()]
        fec = [t for t in tables if any(k in t.lower() for k in ["fec","federal","indiv","cand","cmte","pac"])]
        if fec:
            print(f"\n{db}: {fec}")
            for t in fec:
                cur.execute(f"SELECT COUNT(*) FROM `{db}`.`{t}`")
                cnt = cur.fetchone()[0]
                cur.execute(f"SHOW COLUMNS FROM `{db}`.`{t}`")
                cols = [c[0] for c in cur.fetchall()]
                print(f"  {t}: {cnt:,} rows")
                print(f"  cols: {cols}")
                cur.execute(f"SELECT * FROM `{db}`.`{t}` LIMIT 1")
                print(f"  sample: {cur.fetchone()}")
    except Exception as e:
        print(f"  {db}: {e}")
conn.close()
