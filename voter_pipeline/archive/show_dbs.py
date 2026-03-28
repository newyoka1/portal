import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('nys_voter_tagging')
cur = conn.cursor()
cur.execute("SHOW DATABASES")
print("Databases:")
for r in cur.fetchall():
    print(f"  {r[0]}")
cur.close()
conn.close()