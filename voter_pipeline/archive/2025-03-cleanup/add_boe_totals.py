import pymysql, dotenv, os
dotenv.load_dotenv()

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='nys_voter_tagging'
)
cur = conn.cursor()

print("Adding total columns to boe_proven_donors...")

# Add total amount columns
total_cols = [
    ('boe_total_D_amt', 'DECIMAL(12,2) DEFAULT NULL'),
    ('boe_total_R_amt', 'DECIMAL(12,2) DEFAULT NULL'),
    ('boe_total_U_amt', 'DECIMAL(12,2) DEFAULT NULL'),
    ('boe_total_D_cnt', 'SMALLINT DEFAULT NULL'),
    ('boe_total_R_cnt', 'SMALLINT DEFAULT NULL'),
    ('boe_total_U_cnt', 'SMALLINT DEFAULT NULL'),
    ('boe_party_signal', 'VARCHAR(12) DEFAULT NULL'),
]

for col_name, col_def in total_cols:
    try:
        cur.execute(f"ALTER TABLE boe_proven_donors ADD COLUMN {col_name} {col_def}")
        print(f"  Added {col_name}")
    except Exception as e:
        if '1060' in str(e):  # Duplicate column
            print(f"  {col_name} already exists")
        else:
            print(f"  Error adding {col_name}: {e}")

conn.commit()

print("\nComputing totals from yearly amounts...")

# Update D totals
cur.execute("""
    UPDATE boe_proven_donors SET
        boe_total_D_amt = COALESCE(D2018amt,0) + COALESCE(D2019amt,0) + COALESCE(D2020amt,0) +
                          COALESCE(D2021amt,0) + COALESCE(D2022amt,0) + COALESCE(D2023amt,0) + COALESCE(D2024amt,0),
        boe_total_D_cnt = COALESCE(D2018cnt,0) + COALESCE(D2019cnt,0) + COALESCE(D2020cnt,0) +
                          COALESCE(D2021cnt,0) + COALESCE(D2022cnt,0) + COALESCE(D2023cnt,0) + COALESCE(D2024cnt,0)
""")
print(f"  D totals: {cur.rowcount:,} rows")

# Update R totals
cur.execute("""
    UPDATE boe_proven_donors SET
        boe_total_R_amt = COALESCE(R2018amt,0) + COALESCE(R2019amt,0) + COALESCE(R2020amt,0) +
                          COALESCE(R2021amt,0) + COALESCE(R2022amt,0) + COALESCE(R2023amt,0) + COALESCE(R2024amt,0),
        boe_total_R_cnt = COALESCE(R2018cnt,0) + COALESCE(R2019cnt,0) + COALESCE(R2020cnt,0) +
                          COALESCE(R2021cnt,0) + COALESCE(R2022cnt,0) + COALESCE(R2023cnt,0) + COALESCE(R2024cnt,0)
""")
print(f"  R totals: {cur.rowcount:,} rows")

# Update U totals
cur.execute("""
    UPDATE boe_proven_donors SET
        boe_total_U_amt = COALESCE(U2018amt,0) + COALESCE(U2019amt,0) + COALESCE(U2020amt,0) +
                          COALESCE(U2021amt,0) + COALESCE(U2022amt,0) + COALESCE(U2023amt,0) + COALESCE(U2024amt,0),
        boe_total_U_cnt = COALESCE(U2018cnt,0) + COALESCE(U2019cnt,0) + COALESCE(U2020cnt,0) +
                          COALESCE(U2021cnt,0) + COALESCE(U2022cnt,0) + COALESCE(U2023cnt,0) + COALESCE(U2024cnt,0)
""")
print(f"  U totals: {cur.rowcount:,} rows")

# Set party signal
cur.execute("""
    UPDATE boe_proven_donors SET
        boe_party_signal = CASE
            WHEN boe_total_D_amt > boe_total_R_amt AND boe_total_D_amt > boe_total_U_amt THEN 'D'
            WHEN boe_total_R_amt > boe_total_D_amt AND boe_total_R_amt > boe_total_U_amt THEN 'R'
            WHEN boe_total_U_amt > boe_total_D_amt AND boe_total_U_amt > boe_total_R_amt THEN 'U'
            WHEN boe_total_D_amt = boe_total_R_amt AND boe_total_D_amt > 0 THEN 'Mixed'
            ELSE 'Unknown'
        END
    WHERE boe_total_D_amt > 0 OR boe_total_R_amt > 0 OR boe_total_U_amt > 0
""")
print(f"  Party signals: {cur.rowcount:,} rows")

conn.commit()

# Summary
cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_D_amt > 0")
d_count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_R_amt > 0")
r_count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM boe_proven_donors WHERE boe_total_U_amt > 0")
u_count = cur.fetchone()[0]

print(f"\n=== SUMMARY ===")
print(f"Donors with D contributions: {d_count:,}")
print(f"Donors with R contributions: {r_count:,}")
print(f"Donors with U contributions: {u_count:,}")

conn.close()
