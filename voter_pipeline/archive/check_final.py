import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn
conn = get_conn("nys_voter_tagging")
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.boe_contributions_raw")
tgt = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM donors_2024.boe_contributions_raw")
src = cur.fetchone()[0]
with open(r"D:\git\nys-voter-pipeline\logs\boe_final.log","w") as f:
    f.write(f"src: {src:,}\ntgt: {tgt:,}\nmatch: {src==tgt}")
cur.close(); conn.close()