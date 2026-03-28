#!/usr/bin/env python3
import os
import pymysql


conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password=os.getenv('MYSQL_PASSWORD'),
    database='NYS_VOTER_TAGGING'
)

cursor = conn.cursor()
cursor.execute("SELECT DISTINCT LDName FROM voter_file WHERE LDName IS NOT NULL ORDER BY LDName LIMIT 20")
lds = cursor.fetchall()

print("First 20 Legislative Districts in database:")
for ld in lds:
    print(f"  {ld[0]}")

cursor.execute("SELECT COUNT(*) FROM voter_file WHERE LDName = '063'")
count1 = cursor.fetchone()[0]
print(f"\nVoters with LDName = '063': {count1}")

cursor.execute("SELECT COUNT(*) FROM voter_file WHERE LDName = 'AD 063'")
count2 = cursor.fetchone()[0]
print(f"Voters with LDName = 'AD 063': {count2}")

cursor.execute("SELECT COUNT(*) FROM voter_file WHERE LDName LIKE '%63%'")
count3 = cursor.fetchone()[0]
print(f"Voters with LDName containing '63': {count3}")

conn.close()