"""
Quick check of key databases
"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    port=int(os.getenv('MYSQL_PORT'))
)
cursor = conn.cursor()

# Check which FEC databases exist
cursor.execute("SHOW DATABASES LIKE '%fec%'")
fec_dbs = [db[0] for db in cursor.fetchall()]

print("FEC Databases found:")
for db in fec_dbs:
    print(f"  - {db}")

# Check which have tables with spaces
print("\nChecking for tables with spaces:")
for db in fec_dbs:
    cursor.execute(f"USE `{db}`")
    cursor.execute("SHOW TABLES")
    tables = [t[0] for t in cursor.fetchall()]
    space_tables = [t for t in tables if ' ' in t]
    if space_tables:
        print(f"  {db}:")
        for t in space_tables:
            print(f"    - '{t}'")

cursor.close()
conn.close()
