import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(host="127.0.0.1", port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"), database="boe_donors")
cur = conn.cursor()

# What is "GUS FOR NY"?
cur.execute("""
    SELECT DISTINCT filer, party, year, city, state,
           COUNT(*) as cnt, SUM(amount) as total
    FROM contributions
    WHERE filer LIKE '%GUS%' AND filer LIKE '%NY%'
    GROUP BY filer, party, year, city, state
    ORDER BY year DESC
""")
for r in cur.fetchall():
    print(r)

# Also check the raw BOE tables in boe_donors for candidate info
cur.execute("SHOW TABLES")
print("\nTables:", [r[0] for r in cur.fetchall()])

# Check raw_state_candidate for Gus
cur.execute("""
    SELECT c00, c01, c02, c03, c04, c05, c06 FROM boe_donors.raw_state_candidate
    WHERE c03 LIKE '%GUS%' OR c02 LIKE '%GUS%'
    LIMIT 10
""")
for r in cur.fetchall(): print("state:", r)

conn.close()
