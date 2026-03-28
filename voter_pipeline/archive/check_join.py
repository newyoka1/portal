import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('nys_voter_tagging')
cur = conn.cursor()

# Sample StateVoterId from fullnyvoter
cur.execute("SELECT StateVoterId FROM nys_voter_tagging.voter_file WHERE StateVoterId IS NOT NULL AND StateVoterId != '' LIMIT 5")
print("Sample StateVoterId in voter_file:")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

# Sample sboeid from ProvenDonors
cur.execute("SELECT sboeid FROM nys_voter_tagging.ProvenDonors2024_BOEReclassified WHERE sboeid IS NOT NULL AND sboeid != '' LIMIT 5")
print("\nSample sboeid in ProvenDonors2024_BOEReclassified:")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

# Test overlap
cur.execute("""
    SELECT COUNT(DISTINCT f.StateVoterId)
    FROM nys_voter_tagging.voter_file f
    JOIN nys_voter_tagging.ProvenDonors2024_BOEReclassified p
      ON f.StateVoterId = p.sboeid
    WHERE p.boe_total_D_amt > 0 OR p.boe_total_R_amt > 0 OR p.boe_total_U_amt > 0
""")
print(f"\nMatching donors between tables: {cur.fetchone()[0]:,}")

cur.close()
conn.close()