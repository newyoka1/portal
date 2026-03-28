import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

conn = get_conn("nys_voter_tagging", autocommit=False)
cur = conn.cursor()

src = "donors_2024.boe_contributions_raw"
tgt = "nys_voter_tagging.boe_contributions_raw"

cur.execute("SELECT COUNT(*) FROM donors_2024.boe_contributions_raw")
src_cnt = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
tgt_cnt = cur.fetchone()[0]

print(f"Source: {src_cnt:,} rows")
print(f"Target: {tgt_cnt:,} rows (currently empty)")

if tgt_cnt > 0:
    print("Target already has data - aborting.")
    cur.close(); conn.close()
    sys.exit(0)

print(f"\nCopying {src_cnt:,} rows in batches...")
BATCH = 100000
offset = 0
total = 0
t = time.time()

while True:
    cur.execute(f"""
        INSERT INTO nys_voter_tagging.boe_contributions_raw
        SELECT * FROM donors_2024.boe_contributions_raw
        LIMIT {BATCH} OFFSET {offset}
    """)
    affected = cur.rowcount
    conn.commit()
    total += affected
    print(f"  {total:,} / {src_cnt:,} rows ({time.time()-t:.0f}s)")
    if affected < BATCH:
        break
    offset += BATCH

cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
final = cur.fetchone()[0]
print(f"\nDone. {final:,} rows in nys_voter_tagging.boe_contributions_raw")
if final == src_cnt:
    print("Row counts match - SUCCESS")
else:
    print(f"WARNING: mismatch! src={src_cnt:,} tgt={final:,}")

cur.close(); conn.close()