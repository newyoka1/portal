import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('donors_2024')
cur = my.cursor()

# Decode FilerParty values in COMMCAND
print("=== FilerParty value mapping ===")
cur.execute("""
  SELECT FilerParty, COMPLIANCE_TYPE_DESC, COUNT(*) as cnt
  FROM COMMCAND20240221
  GROUP BY FilerParty, COMPLIANCE_TYPE_DESC
  ORDER BY FilerParty, cnt DESC
""")
for r in cur.fetchall(): print(f"  FilerParty={r[0]} | {r[1]} | {r[2]} committees")

# Sample party 1 and 2 to confirm R vs D
print("\n=== Sample FilerParty=1 (should be REP?) ===")
cur.execute("SELECT FILER_NAME, OFFICE_DESC, COMMITTEE_TYPE_DESC, FilerParty FROM COMMCAND20240221 WHERE FilerParty=1 LIMIT 10")
for r in cur.fetchall(): print(f"  {r}")

print("\n=== Sample FilerParty=2 (should be DEM?) ===")
cur.execute("SELECT FILER_NAME, OFFICE_DESC, COMMITTEE_TYPE_DESC, FilerParty FROM COMMCAND20240221 WHERE FilerParty=2 LIMIT 10")
for r in cur.fetchall(): print(f"  {r}")

print("\n=== Sample FilerParty=3 ===")
cur.execute("SELECT FILER_NAME, OFFICE_DESC, COMMITTEE_TYPE_DESC, FilerParty FROM COMMCAND20240221 WHERE FilerParty=3 LIMIT 10")
for r in cur.fetchall(): print(f"  {r}")

print("\n=== Sample FilerParty=4 ===")
cur.execute("SELECT FILER_NAME, OFFICE_DESC, COMMITTEE_TYPE_DESC, FilerParty FROM COMMCAND20240221 WHERE FilerParty=4 LIMIT 10")
for r in cur.fetchall(): print(f"  {r}")

my.close()