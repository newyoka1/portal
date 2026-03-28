#!/usr/bin/env python3
import pymysql
import os

load_dotenv(r'd:\git\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', '127.0.0.1'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'root'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='nys_voter_tagging',
    charset='utf8mb4'
)

cur = conn.cursor()

print("\n" + "="*80)
print("CENSUS SURNAME ETHNICITY TABLE ANALYSIS")
print("="*80)

# Table structure
cur.execute('DESCRIBE ref_census_surnames')
print("\nTable Structure:")
print("-"*80)
for row in cur.fetchall():
    print(f"  {row[0]:<30} {row[1]:<20} {row[2]}")

# Total count
cur.execute('SELECT COUNT(*) FROM ref_census_surnames')
total = cur.fetchone()[0]
print(f"\nTotal surnames in table: {total:,}")

# Sample data
cur.execute("""
    SELECT surname, dominant_ethnicity, pct_white, pct_black, pct_hispanic, pct_api 
    FROM ref_census_surnames 
    ORDER BY surname_rank 
    LIMIT 15
""")
print("\nSample Data (Top 15 surnames by frequency):")
print("-"*80)
print(f"{'Surname':<15} {'Dominant':<25} {'White%':>7} {'Black%':>7} {'Hisp%':>7} {'API%':>7}")
print("-"*80)
for row in cur.fetchall():
    print(f"{row[0]:<15} {row[1]:<25} {row[2]:>6.1f}% {row[3]:>6.1f}% {row[4]:>6.1f}% {row[5]:>6.1f}%")

# Check match rate
print("\n" + "="*80)
print("MATCH RATE ANALYSIS")
print("="*80)

cur.execute("""
    SELECT 
        COUNT(*) as total_voters,
        SUM(CASE WHEN e.surname IS NOT NULL THEN 1 ELSE 0 END) as matched,
        ROUND(SUM(CASE WHEN e.surname IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as match_rate
    FROM voter_file f
    LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
""")
total, matched, rate = cur.fetchone()
print(f"\nTotal voters: {total:,}")
print(f"Matched to census: {matched:,}")
print(f"Match rate: {rate}%")
print(f"Unmatched: {total - matched:,} ({100-rate:.2f}%)")

# Distribution of unmatched surnames
cur.execute("""
    SELECT 
        UPPER(LastName) as surname,
        COUNT(*) as voter_count
    FROM voter_file f
    LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
    WHERE e.surname IS NULL
    GROUP BY UPPER(LastName)
    ORDER BY voter_count DESC
    LIMIT 20
""")

print("\nTop 20 Unmatched Surnames (not in census table):")
print("-"*80)
print(f"{'Surname':<20} {'Voter Count':>15}")
print("-"*80)
for row in cur.fetchall():
    print(f"{row[0]:<20} {row[1]:>15,}")

conn.close()