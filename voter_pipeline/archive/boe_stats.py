import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
conn = get_conn('donors_2024')
cur = conn.cursor()

cur.execute("SELECT COUNT(*), SUM(ORG_AMT) FROM boe_contributions_raw")
cnt, total = cur.fetchone()
print(f"Total rows: {cnt:,}  |  Total dollars: ${total:,.0f}")

cur.execute("""
    SELECT COALESCE(f.party, 'U') as party,
           COUNT(*) as txns,
           SUM(b.ORG_AMT) as dollars
    FROM boe_contributions_raw b
    LEFT JOIN (
        SELECT CONVERT(FILER_ID USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS FILER_ID,
               CASE FilerParty WHEN 1 THEN 'D' WHEN 2 THEN 'R' ELSE 'U' END as party
        FROM boe_filer_registry
    ) f ON CONVERT(b.FILER_ID USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = f.FILER_ID
    GROUP BY party ORDER BY dollars DESC
""")
print("\nBy party:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,} transactions   ${r[2]:,.0f}")

cur.execute("""
    SELECT ELECTION_YEAR, COUNT(*), SUM(ORG_AMT)
    FROM boe_contributions_raw
    GROUP BY ELECTION_YEAR ORDER BY ELECTION_YEAR
""")
print("\nBy year:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,} transactions   ${r[2]:,.0f}")

cur.execute("""
    SELECT SOURCE_FILE, COUNT(*), SUM(ORG_AMT)
    FROM boe_contributions_raw
    GROUP BY SOURCE_FILE
""")
print("\nBy source file:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,} rows   ${r[2]:,.0f}")

cur.close()
conn.close()