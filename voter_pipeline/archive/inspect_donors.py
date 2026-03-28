import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('donors_2024')
cur = conn.cursor()

cur.execute("DESCRIBE ProvenDonors2024OnePerInd")
print("ProvenDonors2024OnePerInd columns:")
for r in cur.fetchall():
    print(f"  {r[0]:35s} {r[1]}")

cur.execute("SELECT COUNT(*) FROM ProvenDonors2024OnePerInd WHERE PartyCode = 'U'")
print(f"\nU donors: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM ProvenDonors2024OnePerInd")
print(f"Total donors: {cur.fetchone()[0]:,}")

cur.execute("SELECT * FROM ProvenDonors2024OnePerInd WHERE PartyCode = 'U' LIMIT 2")
cols = [d[0] for d in cur.description]
print(f"\nColumns: {cols}")
for row in cur.fetchall():
    for k, v in zip(cols, row):
        print(f"  {k}: {v}")
    print()

cur.close()
conn.close()