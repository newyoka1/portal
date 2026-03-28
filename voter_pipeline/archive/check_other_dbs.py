import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('nys_voter_tagging')
cur = my.cursor()

# Check politik1_nydata for contribution transaction tables
print("=== Tables in politik1_nydata ===")
cur.execute("SHOW TABLES IN politik1_nydata")
for r in cur.fetchall(): print(f"  {r[0]}")

print("\n=== Tables in politik1_fec ===")
cur.execute("SHOW TABLES IN politik1_fec")
for r in cur.fetchall(): print(f"  {r[0]}")

my.close()