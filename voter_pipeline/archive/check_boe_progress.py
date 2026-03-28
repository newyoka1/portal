import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

conn = get_conn("nys_voter_tagging")
cur = conn.cursor()

# Check current row count in target
cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
tgt = cur.fetchone()[0]

# Check for running queries
cur.execute("SHOW PROCESSLIST")
procs = cur.fetchall()

lines = [f"nys_voter_tagging.boe_contributions_raw current rows: {tgt:,}", "\nRunning processes:"]
for p in procs:
    lines.append(str(p))

with open(r"D:\git\nys-voter-pipeline\logs\boe_progress.log", "w") as f:
    f.write("\n".join(str(l) for l in lines))
cur.close(); conn.close()