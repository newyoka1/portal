import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('donors_2024')
cur = my.cursor()

# What does U mean in context of COMMCAND?
print("=== COMMCAND - Committee/Filer types that might map to U ===")
cur.execute("""
  SELECT COMMITTEE_TYPE_DESC, FILER_TYPE_DESC, COUNT(*) as cnt
  FROM COMMCAND20240221
  GROUP BY COMMITTEE_TYPE_DESC, FILER_TYPE_DESC
  ORDER BY cnt DESC
  LIMIT 30
""")
for r in cur.fetchall(): print(f"  {r}")

# Look at the FilerParty values in COMMCAND
print("\n=== FilerParty values in COMMCAND ===")
cur.execute("""
  SELECT FilerParty, COUNT(*) as cnt
  FROM COMMCAND20240221
  GROUP BY FilerParty ORDER BY cnt DESC
""")
for r in cur.fetchall(): print(f"  FilerParty={r[0]} -> {r[1]} committees")

# What party values exist in COMMCAND
print("\n=== Officeval/party cross in COMMCAND sample ===")
cur.execute("""
  SELECT FILER_NAME, COMMITTEE_TYPE_DESC, OFFICE_DESC, FilerParty, Officeval
  FROM COMMCAND20240221
  WHERE FilerParty IS NULL OR FilerParty = 0
  LIMIT 20
""")
for r in cur.fetchall(): print(f"  {r}")

# Check how many people have ONLY U donations vs mixed
print("\n=== Donor breakdown: pure U vs mixed ===")
cur.execute("""
  SELECT 
    SUM(CASE WHEN total_rep=0 AND total_dem=0 AND total_oth>0 THEN 1 ELSE 0 END) as pure_other,
    SUM(CASE WHEN total_rep>0 AND total_dem=0 AND total_oth=0 THEN 1 ELSE 0 END) as pure_rep,
    SUM(CASE WHEN total_rep=0 AND total_dem>0 AND total_oth=0 THEN 1 ELSE 0 END) as pure_dem,
    SUM(CASE WHEN total_oth>0 THEN 1 ELSE 0 END) as has_other,
    COUNT(*) as total
  FROM donor_party_totals
""")
r = cur.fetchone()
print(f"  Pure Other only donors : {r[0]:,}")
print(f"  Pure Rep only donors   : {r[1]:,}")
print(f"  Pure Dem only donors   : {r[2]:,}")
print(f"  Has any Other donation : {r[3]:,}")
print(f"  Total donors           : {r[4]:,}")

# Top U donors - what are their names, maybe they're PACs or nonpartisan
print("\n=== Top 20 donors by total_oth ===")
cur.execute("""
  SELECT FULLNAME, CITY, voterparty, total_rep, total_dem, total_oth, grand_total
  FROM donor_party_totals ORDER BY total_oth DESC LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {str(r[1]):<18} {str(r[2]):<5} REP:${r[3]:>8,.0f} DEM:${r[4]:>8,.0f} OTH:${r[5]:>10,.0f}")

my.close()