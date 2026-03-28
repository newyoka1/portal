import pymysql, os
from dotenv import load_dotenv
load_dotenv()

conn = pymysql.connect(
    host=os.getenv("DB_HOST","localhost"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=int(os.getenv("DB_PORT","3306"))
)
cur = conn.cursor()
cur.execute("SHOW DATABASES")
dbs = [r[0] for r in cur.fetchall()]
print("Databases:", dbs)

for db in dbs:
    if db in ("information_schema","mysql","performance_schema","sys"):
        continue
    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND COLUMN_NAME LIKE %s
        ORDER BY TABLE_NAME
    """, (db, "%email%"))
    rows = cur.fetchall()
    if rows:
        print(f"\n{db}:")
        for tbl, col in rows:
            try:
                cur.execute(f"SELECT COUNT(*) FROM `{db}`.`{tbl}` WHERE `{col}` IS NOT NULL AND `{col}` != ''")
                cnt = cur.fetchone()[0]
                print(f"  {tbl}.{col} -> {cnt} non-empty")
            except Exception as e:
                print(f"  {tbl}.{col} -> ERROR: {e}")
conn.close()
