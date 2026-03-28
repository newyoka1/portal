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

# Search for Mills as a contributor
cur.execute("""
    SELECT year, party, filer, date, first, middle, last, address, city, state, zip5, amount
    FROM contributions
    WHERE (last LIKE 'MILLS' OR last LIKE 'Mill%')
      AND first LIKE 'MICHAEL%'
      AND state = 'NY'
    ORDER BY date DESC
    LIMIT 50
""")
rows = cur.fetchall()
out = [f"Mills as contributor (NY, Michael Mills): {len(rows)} records"]
for r in rows:
    out.append(f"  {r[0]} | {r[2][:40]} | {r[3]} | ${r[11]} | {r[6]}, {r[4]} | {r[7]}, {r[8]} {r[10]}")

# Also search Canandaigua specifically
cur.execute("""
    SELECT year, party, filer, date, first, middle, last, address, city, state, zip5, amount
    FROM contributions
    WHERE last LIKE 'MILLS'
      AND first LIKE 'MICHAEL%'
      AND city LIKE '%CANANDAIGUA%'
    ORDER BY date DESC
    LIMIT 20
""")
rows2 = cur.fetchall()
out.append(f"\nMills in Canandaigua specifically: {len(rows2)} records")
for r in rows2:
    out.append(f"  {r[0]} | {r[2][:40]} | {r[3]} | ${r[11]} | {r[6]}, {r[4]} | {r[7]}")

conn.close()
with open("logs\\mills_donor_check.txt","w") as f:
    f.write("\n".join(out))
