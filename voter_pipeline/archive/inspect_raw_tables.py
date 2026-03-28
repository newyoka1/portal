import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

my = get_conn('nys_voter_tagging')
cur = my.cursor()

# Check nys_donors
print("=== politik1_nydata.nys_donors ===")
cur.execute("SELECT COUNT(*) FROM politik1_nydata.nys_donors")
print(f"  Row count: {cur.fetchone()[0]:,}")
cur.execute("DESCRIBE politik1_nydata.nys_donors")
cols = cur.fetchall()
print(f"  Columns: {[c[0] for c in cols]}")
cur.execute("SELECT * FROM politik1_nydata.nys_donors LIMIT 3")
rows = cur.fetchall()
col_names = [c[0] for c in cols]
for row in rows:
    for c, v in zip(col_names, row):
        if v not in (None, '', 0): print(f"    {c}: {v}")
    print("    ---")

# Check voter_donations_matched
print("\n=== politik1_fec.voter_donations_matched ===")
cur.execute("SELECT COUNT(*) FROM politik1_fec.voter_donations_matched")
print(f"  Row count: {cur.fetchone()[0]:,}")
cur.execute("DESCRIBE politik1_fec.voter_donations_matched")
cols2 = cur.fetchall()
print(f"  Columns: {[c[0] for c in cols2]}")
cur.execute("SELECT * FROM politik1_fec.voter_donations_matched LIMIT 2")
rows2 = cur.fetchall()
col_names2 = [c[0] for c in cols2]
for row in rows2:
    for c, v in zip(col_names2, row):
        if v not in (None, '', 0): print(f"    {c}: {v}")
    print("    ---")

# Check stg_boe_voter_contrib
print("\n=== politik1_fec.stg_boe_voter_contrib ===")
cur.execute("SELECT COUNT(*) FROM politik1_fec.stg_boe_voter_contrib")
print(f"  Row count: {cur.fetchone()[0]:,}")
cur.execute("DESCRIBE politik1_fec.stg_boe_voter_contrib")
cols3 = cur.fetchall()
print(f"  Columns: {[c[0] for c in cols3]}")

my.close()