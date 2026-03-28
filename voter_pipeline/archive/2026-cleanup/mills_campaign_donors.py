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

# Find Mills campaign filer name variants
cur.execute("""
    SELECT DISTINCT filer, COUNT(*) as cnt, SUM(amount) as total
    FROM contributions
    WHERE filer LIKE '%MILLS%' AND filer LIKE '%SENATE%'
    GROUP BY filer
    ORDER BY cnt DESC
""")
rows = cur.fetchall()
out = ["=== MILLS SENATE COMMITTEE FILER NAMES ==="]
for r in rows:
    out.append(f"  {r[0]}  |  {r[1]} contribs  |  ${r[2]:.2f}")

# Pull all donors TO the Mills campaign
cur.execute("""
    SELECT year, date, first, middle, last, address, city, state, zip5, amount, party
    FROM contributions
    WHERE filer LIKE '%MILLS%' AND filer LIKE '%SENATE%'
    ORDER BY amount DESC, date ASC
""")
donors = cur.fetchall()
out.append(f"\n=== DONORS TO MILLS SENATE CAMPAIGN: {len(donors)} records ===")
total = 0
for r in donors:
    total += float(r[9])
    name = f"{r[2]} {r[3]+' ' if r[3] else ''}{r[4]}"
    out.append(f"  {r[1]} | ${r[9]:>8.2f} | {r[10]} | {name.strip():<30} | {r[5]}, {r[6]} {r[8]}")

out.append(f"\nTOTAL: ${total:,.2f} across {len(donors)} contributions")

conn.close()
with open("logs\\mills_campaign_donors.txt","w") as f:
    f.write("\n".join(out))
