import pymysql
import os

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password=MYSQL_PASSWORD,
    database='NYS_VOTER_TAGGING'
)

cursor = conn.cursor()
cursor.execute('SHOW COLUMNS FROM voter_file')
columns = cursor.fetchall()

print("Columns in voter_file:")
for col in columns[:30]:
    print(f"  {col[0]} ({col[1]})")

conn.close()