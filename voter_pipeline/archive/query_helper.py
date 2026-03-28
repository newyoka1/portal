#!/usr/bin/env python3
"""
Quick Query Helper - Interactive tool to explore your data

This tool helps you discover what's available and generates SQL queries for you.
"""

import os
import pymysql
from pathlib import Path

DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")


def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )


def main():
    conn = connect_db()

    print("=" * 80)
    print("QUERY HELPER - Discover Your Data")
    print("=" * 80)

    # Get available districts
    print("\n1. Available Districts:")
    print("-" * 80)

    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT LDName FROM voter_file WHERE LDName IS NOT NULL ORDER BY LDName;")
        lds = [row[0] for row in cur.fetchall()]
        print(f"Legislative Districts (LD): {len(lds)} districts")
        print(f"   First 10: {', '.join(lds[:10])}")

        cur.execute("SELECT DISTINCT SDName FROM voter_file WHERE SDName IS NOT NULL ORDER BY SDName;")
        sds = [row[0] for row in cur.fetchall()]
        print(f"State Senate Districts (SD): {len(sds)} districts")
        print(f"   First 10: {', '.join(sds[:10])}")

        cur.execute("SELECT DISTINCT CDName FROM voter_file WHERE CDName IS NOT NULL ORDER BY CDName;")
        cds = [row[0] for row in cur.fetchall()]
        print(f"Congressional Districts (CD): {len(cds)} districts")
        print(f"   All: {', '.join(cds)}")

    # Get audience stats
    print("\n2. Audience Statistics:")
    print("-" * 80)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT audience) FROM voter_audience_bridge;")
        total_audiences = cur.fetchone()[0]
        print(f"Total unique audiences: {total_audiences}")

        # Count by turnout level
        cur.execute("""
            SELECT
                CASE
                    WHEN audience LIKE 'HT %' THEN 'HT'
                    WHEN audience LIKE 'MT %' THEN 'MT'
                    WHEN audience LIKE 'LT %' THEN 'LT'
                    ELSE 'OTHER'
                END AS turnout_level,
                COUNT(DISTINCT audience) AS count
            FROM voter_audience_bridge
            GROUP BY turnout_level
            ORDER BY turnout_level;
        """)
        for level, count in cur.fetchall():
            print(f"   {level}: {count} audiences")

        # Count by lean
        cur.execute("""
            SELECT
                CASE
                    WHEN audience LIKE '%HARD GOP%' THEN 'HARD GOP'
                    WHEN audience LIKE '%HARD DEM%' THEN 'HARD DEM'
                    WHEN audience LIKE '%SWING%' THEN 'SWING'
                    WHEN audience LIKE '%LEAN GOP%' THEN 'LEAN GOP'
                    WHEN audience LIKE '%LEAN DEM%' THEN 'LEAN DEM'
                    ELSE 'OTHER'
                END AS lean_type,
                COUNT(DISTINCT audience) AS count
            FROM voter_audience_bridge
            GROUP BY lean_type
            ORDER BY count DESC;
        """)
        print("\n   By political lean:")
        for lean, count in cur.fetchall():
            print(f"   {lean}: {count} audiences")

    # Top audiences by voter count
    print("\n3. Top 20 Audiences by Voter Count:")
    print("-" * 80)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT audience, COUNT(DISTINCT StateVoterId) AS voters
            FROM voter_audience_bridge
            GROUP BY audience
            ORDER BY voters DESC
            LIMIT 20;
        """)
        for i, (aud, count) in enumerate(cur.fetchall(), 1):
            print(f"{i:3d}. {aud:55s}  {count:>10,} voters")

    # Sample queries for user
    print("\n4. Sample Queries You Can Run:")
    print("-" * 80)

    # Pick a sample LD
    sample_ld = lds[0] if lds else "063"

    print(f"\n-- Get all audiences in LD {sample_ld}:")
    print(f"""
SELECT b.audience, COUNT(DISTINCT f.StateVoterId) AS voters
FROM voter_audience_bridge b
INNER JOIN voter_file f ON f.StateVoterId = b.StateVoterId
WHERE f.LDName = '{sample_ld}'
GROUP BY b.audience
ORDER BY voters DESC;
""")

    print(f"\n-- Get ethnicity breakdown for LD {sample_ld}:")
    print(f"""
SELECT
    COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
    COUNT(*) AS voters
FROM voter_file f
LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
WHERE f.LDName = '{sample_ld}'
GROUP BY e.dominant_ethnicity
ORDER BY voters DESC;
""")

    print(f"\n-- Get HT HARD GOP voters in LD {sample_ld}:")
    print(f"""
SELECT f.*
FROM voter_file f
INNER JOIN voter_audience_bridge b ON b.StateVoterId = f.StateVoterId
WHERE b.audience LIKE 'HT HARD GOP%'
  AND f.LDName = '{sample_ld}'
LIMIT 100;
""")

    print("\n5. Recommended Next Steps:")
    print("-" * 80)
    print(f"""
# List all audiences for LD {sample_ld}:
python audience_analytics.py --ld "{sample_ld}" --list-audiences

# Get ethnicity breakdown for specific audience in LD {sample_ld}:
python audience_analytics.py --ld "{sample_ld}" --audience "HT HARD GOP INDV NYS_001.csv" --ethnicity

# Build materialized tables for all HARD GOP audiences:
python build_causeway_audience_tables.py --pattern "HARD GOP"

# Export all data for LD {sample_ld} to CSV:
python audience_analytics.py --ld "{sample_ld}" --export-csv
""")

    print("=" * 80)
    conn.close()


if __name__ == "__main__":
    main()