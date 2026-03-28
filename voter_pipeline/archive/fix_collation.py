#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix collation mismatch in ref_census_surnames table
"""

import pymysql
import os
import sys

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

print("Fixing collation to match voter_file table...")
cursor.execute("""
    ALTER TABLE ref_census_surnames 
    CONVERT TO CHARACTER SET utf8mb4 
    COLLATE utf8mb4_0900_ai_ci
""")
conn.commit()

print("✓ Collation updated successfully!")
print("\nYour export script should now work correctly.")

conn.close()