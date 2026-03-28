import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(host="127.0.0.1", port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"))
cur = conn.cursor()

# Check what election results tables exist
cur.execute("SHOW DATABASES")
dbs = [r[0] for r in cur.fetchall()]
print("DBS:", dbs)

for db in dbs:
    if db in ("information_schema","performance_schema","mysql","sys","world"): continue
    try:
        cur.execute(f"SHOW TABLES IN `{db}`")
        tables = [r[0] for r in cur.fetchall()]
        elec = [t for t in tables if any(k in t.lower() for k in ["result","elect","vote","mayor","candidate","race","precinct","returns"])]
        if elec:
            print(f"\n{db}: {elec}")
    except: pass

conn.close()
