import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
my = get_conn('donors_2024')
cur = my.cursor()

# Check duplicates
cur.execute("""
  SELECT COUNT(*) as cnt, sboeid 
  FROM ProvenDonors2024OnePerInd 
  WHERE sboeid IS NOT NULL AND sboeid != ''
  GROUP BY sboeid HAVING cnt > 1 
  ORDER BY cnt DESC LIMIT 10
""")
print("Top duplicate sboeid entries:")
for r in cur.fetchall(): print(f"  {r}")

cur.execute("""
  SELECT COUNT(*) FROM (
    SELECT sboeid FROM ProvenDonors2024OnePerInd 
    WHERE sboeid IS NOT NULL AND sboeid != ''
    GROUP BY sboeid HAVING COUNT(*) > 1
  ) x
""")
print(f"Total duplicated sboeids: {cur.fetchone()[0]:,}")
my.close()