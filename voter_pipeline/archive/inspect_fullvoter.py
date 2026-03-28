import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('nys_voter_tagging')
cur = conn.cursor()

# voter_file columns
cur.execute("DESCRIBE nys_voter_tagging.voter_file")
cols = cur.fetchall()
print("voter_file columns:")
for r in cols:
    print(f"  {r[0]:35s} {r[1]}")

# Check if sboeid exists and sample values
cur.execute("SELECT sboeid, COUNT(*) FROM nys_voter_tagging.voter_file WHERE sboeid IS NOT NULL AND sboeid != '' GROUP BY 1 LIMIT 3")
print("\nSample sboeid values in voter_file:")
for r in cur.fetchall():
    print(f"  '{r[0]}' ({r[1]} rows)")

cur.execute("SELECT COUNT(*), SUM(sboeid IS NOT NULL AND sboeid != '') FROM nys_voter_tagging.voter_file")
r = cur.fetchone()
print(f"\nTotal rows: {r[0]:,}  |  Rows with sboeid: {r[1]:,}")

# Check if donor columns already exist
cur.execute("SHOW COLUMNS FROM nys_voter_tagging.voter_file LIKE 'boe%'")
existing = cur.fetchall()
print(f"\nExisting boe_ columns: {[r[0] for r in existing]}")

cur.execute("SHOW COLUMNS FROM nys_voter_tagging.voter_file LIKE 'donor%'")
existing2 = cur.fetchall()
print(f"Existing donor_ columns: {[r[0] for r in existing2]}")

# Sample sboeid from ProvenDonors to confirm format matches
cur.execute("SELECT sboeid FROM nys_voter_tagging.ProvenDonors2024_BOEReclassified WHERE sboeid IS NOT NULL LIMIT 3")
print("\nSample sboeid in ProvenDonors2024_BOEReclassified:")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

cur.close()
conn.close()