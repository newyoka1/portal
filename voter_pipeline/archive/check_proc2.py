import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn
conn = get_conn("nys_voter_tagging")
cur = conn.cursor()
cur.execute("SHOW PROCESSLIST")
lines = []
for p in cur.fetchall():
    lines.append(str(p))
with open(r"D:\git\nys-voter-pipeline\logs\proc2.log","w") as f:
    f.write("\n".join(lines))
cur.close(); conn.close()