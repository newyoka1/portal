import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('nys_voter_tagging')
cur = conn.cursor()

cur.execute("SHOW TABLES IN nys_voter_tagging")
print("Tables in nys_voter_tagging:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Also check if ProvenDonors exists there already
cur.execute("""
    SELECT table_name, table_rows, ROUND(data_length/1024/1024,1) as data_mb
    FROM information_schema.tables
    WHERE table_schema = 'nys_voter_tagging'
    ORDER BY data_length DESC
""")
print("\nWith sizes:")
for r in cur.fetchall():
    print(f"  {r[0]:50s} ~{r[1]:,} rows  {r[2]} MB")

cur.close()
conn.close()