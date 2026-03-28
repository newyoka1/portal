import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('donors_2024')
cur = my.cursor()

# What tables do we have?
cur.execute("SHOW TABLES")
print("=== Tables in donors_2024 ===")
for r in cur.fetchall(): print(f"  {r[0]}")

# Does COMMCAND have a FILER_ID that could link to transactions?
print("\n=== COMMCAND sample - key linking fields ===")
cur.execute("SELECT FILER_ID, FILER_NAME, COMMITTEE_TYPE_DESC, FilerParty, Officeval FROM COMMCAND20240221 LIMIT 10")
for r in cur.fetchall(): print(f"  {r}")

# Does ProvenDonors have any FILER_ID or committee reference column?
print("\n=== ProvenDonors columns that might link to committees ===")
cur.execute("SHOW COLUMNS FROM ProvenDonors2024OnePerInd")
cols = [r[0] for r in cur.fetchall()]
print(f"  All columns: {cols}")

# Check if there's any filer/committee reference in ProvenDonors
filer_like = [c for c in cols if any(x in c.lower() for x in ['filer','commit','trans','receipt','contrib'])]
print(f"\n  Possible transaction/filer columns: {filer_like}")

my.close()