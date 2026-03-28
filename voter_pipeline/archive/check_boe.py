import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

conn = get_conn("nys_voter_tagging")
cur = conn.cursor()

lines = []

# Row count in source
cur.execute("SELECT COUNT(*) FROM donors_2024.boe_contributions_raw")
src_cnt = cur.fetchone()[0]
lines.append(f"donors_2024.boe_contributions_raw rows: {src_cnt:,}")

# Does it already exist in target?
cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='nys_voter_tagging' AND table_name='boe_contributions_raw'")
exists = cur.fetchone()[0]
lines.append(f"boe_contributions_raw exists in nys_voter_tagging: {exists}")

if exists:
    cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
    tgt_cnt = cur.fetchone()[0]
    lines.append(f"nys_voter_tagging.boe_contributions_raw rows: {tgt_cnt:,}")

# Check structure differences
lines.append("\n--- donors_2024.boe_contributions_raw columns ---")
cur.execute("DESCRIBE donors_2024.boe_contributions_raw")
for r in cur.fetchall():
    lines.append(f"  {r}")

if exists:
    lines.append("\n--- nys_voter_tagging.boe_contributions_raw columns ---")
    cur.execute("DESCRIBE nys_voter_tagging.boe_contributions_raw")
    for r in cur.fetchall():
        lines.append(f"  {r}")

# Try the copy and catch the error
if not exists:
    lines.append("\n--- Attempting copy to catch error ---")
    try:
        cur.execute("CREATE TABLE nys_voter_tagging.boe_contributions_raw LIKE donors_2024.boe_contributions_raw")
        conn.commit()
        lines.append("CREATE LIKE: OK")
    except Exception as e:
        lines.append(f"CREATE LIKE error: {e}")
        conn.rollback()

cur.close(); conn.close()

with open(r"D:\git\nys-voter-pipeline\logs\boe_check.log", "w") as f:
    f.write("\n".join(str(l) for l in lines))
print("done")