import pymysql, os
from dotenv import load_dotenv
load_dotenv('D:\\git\\nys-voter-pipeline\\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT',3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='boe_donors'
)

cur = conn.cursor()

print("="*60)
print("CHECKING CONTRIBUTIONS_RAW TABLE")
print("="*60)

# Total count
cur.execute("SELECT COUNT(*) FROM contributions_raw")
total = cur.fetchone()[0]
print(f"\nTotal rows: {total:,}")

# By year
print("\nBy year:")
cur.execute("SELECT year, COUNT(*) FROM contributions_raw GROUP BY year ORDER BY year")
for year, count in cur.fetchall():
    print(f"  {year}: {count:,}")

# By party
print("\nBy party:")
cur.execute("SELECT party, COUNT(*), SUM(amount) FROM contributions_raw GROUP BY party")
for party, count, amt in cur.fetchall():
    party_name = {'D': 'Democrat', 'R': 'Republican', 'U': 'Unaffiliated'}.get(party, party)
    print(f"  {party_name}: {count:,} (${amt or 0:,.2f})")

# Sample rows
print("\nSample rows:")
cur.execute("SELECT first_name, last_name, city, amount FROM contributions_raw LIMIT 5")
for first, last, city, amt in cur.fetchall():
    print(f"  {first} {last} ({city}) - ${amt}")

conn.close()
