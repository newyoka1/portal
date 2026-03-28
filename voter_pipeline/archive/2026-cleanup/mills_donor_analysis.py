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

out = []

# 1. Geographic breakdown
cur.execute("""
    SELECT state, COUNT(*) as cnt, SUM(amount) as total
    FROM contributions
    WHERE filer = 'Mills for NY Senate'
    GROUP BY state ORDER BY total DESC
""")
out.append("=== BY STATE ===")
for r in cur.fetchall():
    out.append(f"  {r[0] or 'NULL':5} | {r[1]:4} contribs | ${float(r[2]):>9,.2f}")

# 2. Georgia specifically - how much and who are the big ones
cur.execute("""
    SELECT first, last, city, zip5, SUM(amount) as total, COUNT(*) as cnt
    FROM contributions
    WHERE filer = 'Mills for NY Senate' AND state = 'GA'
    GROUP BY first, last, city, zip5
    ORDER BY total DESC
""")
out.append("\n=== GEORGIA DONORS ===")
ga_total = 0
for r in cur.fetchall():
    ga_total += float(r[4])
    out.append(f"  ${float(r[4]):>8,.2f} ({r[5]}x) | {r[0]} {r[1]}, {r[2]}")
out.append(f"  GA TOTAL: ${ga_total:,.2f}")

# 3. Out-of-state total vs in-state
cur.execute("""
    SELECT
      SUM(CASE WHEN state='NY' THEN amount ELSE 0 END) as ny_total,
      SUM(CASE WHEN state='NY' THEN 1 ELSE 0 END) as ny_cnt,
      SUM(CASE WHEN state!='NY' THEN amount ELSE 0 END) as oos_total,
      SUM(CASE WHEN state!='NY' THEN 1 ELSE 0 END) as oos_cnt
    FROM contributions
    WHERE filer = 'Mills for NY Senate'
""")
r = cur.fetchone()
out.append(f"\n=== IN-STATE vs OUT-OF-STATE ===")
out.append(f"  NY:           {r[1]:4} contribs | ${float(r[0]):>9,.2f}")
out.append(f"  Out-of-state: {r[3]:4} contribs | ${float(r[2]):>9,.2f}")

# 4. Top donors summary (over $200)
cur.execute("""
    SELECT first, last, city, state, SUM(amount) as total, COUNT(*) as cnt
    FROM contributions
    WHERE filer = 'Mills for NY Senate'
    GROUP BY first, last, city, state
    HAVING total >= 200
    ORDER BY total DESC
""")
out.append("\n=== TOP DONORS ($200+) ===")
for r in cur.fetchall():
    out.append(f"  ${float(r[4]):>8,.2f} ({r[5]}x) | {r[0]} {r[1]}, {r[2]}, {r[3]}")

# 5. Matching fund progress - count unique in-district contributors >= $10
# District 54 counties: Ontario, Livingston, Wayne + parts of Monroe
# Zip codes or just NY for now - count all NY donors >= $10 as proxy
cur.execute("""
    SELECT COUNT(DISTINCT CONCAT(first, last, address)) as unique_donors,
           SUM(CASE WHEN amount >= 10 AND amount <= 250 THEN amount ELSE 0 END) as matchable_amt
    FROM contributions
    WHERE filer = 'Mills for NY Senate' AND state = 'NY'
      AND amount >= 10
""")
r = cur.fetchone()
out.append(f"\n=== MATCHING FUND PROXY (NY donors >= $10) ===")
out.append(f"  Unique NY donors >= $10: {r[0]}")
out.append(f"  Matchable amount (NY, $10-$250): ${float(r[1]):,.2f}")

# 6. Mills' own giving history from Canandaigua address - summary
cur.execute("""
    SELECT year, filer, date, amount, party
    FROM contributions
    WHERE last = 'MILLS' AND first LIKE 'MICHAEL%'
      AND city LIKE '%CANANDAIGUA%'
    ORDER BY date DESC
""")
out.append("\n=== MILLS' OWN GIVING (Canandaigua address) ===")
total = 0
for r in cur.fetchall():
    total += float(r[3])
    out.append(f"  {r[2]} | ${float(r[3]):>7,.2f} | {r[1][:45]}")
out.append(f"  TOTAL: ${total:,.2f}")

conn.close()
result = "\n".join(out)
with open("logs\\mills_donor_analysis.txt","w") as f:
    f.write(result)
print(result)
