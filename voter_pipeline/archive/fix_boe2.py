import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

log_path = r"D:\git\nys-voter-pipeline\logs\boe_copy.log"
lines = []

def log(msg):
    lines.append(msg)
    with open(log_path, "w") as f:
        f.write("\n".join(lines))

conn = get_conn("nys_voter_tagging", autocommit=False)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM donors_2024.boe_contributions_raw")
src_cnt = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
tgt_cnt = cur.fetchone()[0]

log(f"Source: {src_cnt:,} rows")
log(f"Target: {tgt_cnt:,} rows")

if tgt_cnt > 0:
    log("Target already has data - aborting.")
    cur.close(); conn.close()
    sys.exit(0)

log(f"Copying {src_cnt:,} rows in batches of 100,000...")
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
    log(f"  {total:,} / {src_cnt:,}  ({time.time()-t:.0f}s)")
    if affected < BATCH:
        break
    offset += BATCH

cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
final = cur.fetchone()[0]
log(f"Final count: {final:,}")
log("SUCCESS" if final == src_cnt else f"MISMATCH src={src_cnt:,} tgt={final:,}")
cur.close(); conn.close()