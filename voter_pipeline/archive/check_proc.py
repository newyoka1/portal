import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn
conn = get_conn("nys_voter_tagging")
cur = conn.cursor()
cur.execute("SHOW PROCESSLIST")
with open(r"D:\git\nys-voter-pipeline\logs\proc.log","w") as f:
    for p in cur.fetchall():
        f.write(str(p)+"\n")
cur.close(); conn.close()