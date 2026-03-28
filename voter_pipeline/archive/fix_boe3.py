import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

log_path = r"D:\git\nys-voter-pipeline\logs\boe_copy.log"

def log(msg):
    with open(log_path, "a") as f:
        f.write(msg + "\n")

log(f"\n=== New run: {time.strftime('%H:%M:%S')} ===")

conn = get_conn("nys_voter_tagging", autocommit=True, timeout=3600)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
tgt = cur.fetchone()[0]
log(f"Target currently: {tgt:,} rows")

if tgt > 0:
    log("Already has data, aborting.")
    cur.close(); conn.close()
    sys.exit(0)

cur.execute("SELECT COUNT(*) FROM donors_2024.boe_contributions_raw")
src = cur.fetchone()[0]
log(f"Source: {src:,} rows - starting single bulk INSERT...")

t = time.time()
# Single bulk INSERT - fastest, no OFFSET scanning
cur.execute("""
    INSERT INTO nys_voter_tagging.boe_contributions_raw
    SELECT * FROM donors_2024.boe_contributions_raw
""")
elapsed = time.time() - t
log(f"INSERT done in {elapsed:.0f}s")

cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
final = cur.fetchone()[0]
log(f"Final: {final:,} rows")
log("SUCCESS" if final == src else f"MISMATCH src={src:,} tgt={final:,}")
cur.close(); conn.close()