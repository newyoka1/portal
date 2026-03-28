import os
﻿import mysql.connector
conn = get_conn('donors_2024')
cur = conn.cursor()
cur.execute("SHOW COLUMNS FROM donors_2024.ProvenDonors2024_BOEReclassified LIKE \"boe_%amt\"")
print("Per-year cols:")
for r in cur.fetchall():
    print(" ", r[0])
cur.execute("SELECT MIN(SCHED_DATE), MAX(SCHED_DATE) FROM donors_2024.boe_contributions_raw WHERE SCHED_DATE IS NOT NULL")
r = cur.fetchone()
print("Date range:", r[0], "to", r[1])
cur.execute("SHOW TABLES IN donors_2024 LIKE \"stg_donor_matchkeys\"")
r2 = cur.fetchone()
print("stg_donor_matchkeys exists:", r2 is not None)
cur.close()
conn.close()