#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Add dominant_ethnicity column to ref_census_surnames table
Determines which ethnicity has the highest percentage for each surname
"""

import pymysql
import os
import sys

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv('D:\\git\\.env')

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': 'nys_voter_tagging',
    'charset': 'utf8mb4'
}

print("Connecting to database...")
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# Add the dominant_ethnicity column
print("Adding dominant_ethnicity column...")
try:
    cursor.execute("""
        ALTER TABLE ref_census_surnames 
        ADD COLUMN dominant_ethnicity VARCHAR(40) AFTER pct_hispanic
    """)
    conn.commit()
    print("✓ Column added")
except pymysql.err.OperationalError as e:
    if e.args[0] == 1060:  # Duplicate column
        print("  Column already exists, modifying length...")
        cursor.execute("""
            ALTER TABLE ref_census_surnames 
            MODIFY COLUMN dominant_ethnicity VARCHAR(40)
        """)
        conn.commit()
        print("✓ Column length updated")
    else:
        raise

# Calculate and populate dominant ethnicity
print("\nCalculating dominant ethnicity for each surname...")
print("This will take a few moments...")

cursor.execute("""
    UPDATE ref_census_surnames
    SET dominant_ethnicity = 
        CASE 
            WHEN pct_white >= GREATEST(pct_black, pct_api, pct_aian, pct_2prace, pct_hispanic) 
                THEN 'WHITE'
            WHEN pct_black >= GREATEST(pct_white, pct_api, pct_aian, pct_2prace, pct_hispanic) 
                THEN 'BLACK'
            WHEN pct_hispanic >= GREATEST(pct_white, pct_black, pct_api, pct_aian, pct_2prace) 
                THEN 'HISPANIC'
            WHEN pct_api >= GREATEST(pct_white, pct_black, pct_aian, pct_2prace, pct_hispanic) 
                THEN 'ASIAN/PACIFIC ISLANDER'
            WHEN pct_aian >= GREATEST(pct_white, pct_black, pct_api, pct_2prace, pct_hispanic) 
                THEN 'AMERICAN INDIAN/ALASKA NATIVE'
            WHEN pct_2prace >= GREATEST(pct_white, pct_black, pct_api, pct_aian, pct_hispanic) 
                THEN 'TWO OR MORE RACES'
            ELSE 'UNKNOWN'
        END
""")
conn.commit()

print(f"✓ Updated {cursor.rowcount:,} surnames with dominant ethnicity")

# Show sample
cursor.execute("""
    SELECT surname, dominant_ethnicity, pct_white, pct_black, pct_hispanic, pct_api
    FROM ref_census_surnames
    ORDER BY surname_rank
    LIMIT 15
""")

print("\n" + "="*80)
print("Sample data with dominant ethnicity:")
print("="*80)
print(f"{'Surname':<15} {'Dominant':<25} {'White%':>7} {'Black%':>7} {'Hisp%':>7} {'API%':>7}")
print("-"*80)
for row in cursor.fetchall():
    print(f"{row[0]:<15} {row[1]:<25} {row[2]:>6.1f}% {row[3]:>6.1f}% {row[4]:>6.1f}% {row[5]:>6.1f}%")

print("\n✓ Dominant ethnicity column added and populated successfully!")
print("Your export script should now work correctly.")

conn.close()