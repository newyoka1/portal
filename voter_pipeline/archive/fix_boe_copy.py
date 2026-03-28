import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

print("Connecting...")
conn = get_conn("nys_voter_tagging", autocommit=True, timeout=3600)
cur = conn.cursor()

print("Dropping empty boe_contributions_raw from nys_voter_tagging...")
cur.execute("DROP TABLE IF EXISTS `nys_voter_tagging`.`boe_contributions_raw`")
print("Dropped.")

print("Copying structure...")
cur.execute("CREATE TABLE `nys_voter_tagging`.`boe_contributions_raw` LIKE `donors_2024`.`boe_contributions_raw`")
print("Structure created.")

print("Copying 3.3M rows (this will take a few minutes)...")
t = time.time()
cur.execute("INSERT INTO `nys_voter_tagging`.`boe_contributions_raw` SELECT * FROM `donors_2024`.`boe_contributions_raw`")
elapsed = time.time() - t

cur.execute("SELECT COUNT(*) FROM `nys_voter_tagging`.`boe_contributions_raw`")
tgt = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM `donors_2024`.`boe_contributions_raw`")
src = cur.fetchone()[0]

print(f"Done in {elapsed:.1f}s")
print(f"Source rows: {src:,}")
print(f"Target rows: {tgt:,}")
print(f"Match: {'YES' if src == tgt else 'NO - MISMATCH!'}")

cur.close(); conn.close()