"""
Check which databases actually exist and have space issues
"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_PORT')),
        charset='utf8mb4'
    )

conn = connect_db()
cursor = conn.cursor()

# Get all databases
cursor.execute("SHOW DATABASES")
all_dbs = [db[0] for db in cursor.fetchall()]

print("="*80)
print("ALL DATABASES ON SERVER")
print("="*80)

# Key patterns to look for
key_patterns = ['fec', 'voter', 'boe', 'donor', 'nydata']

for db in sorted(all_dbs):
    is_system = db in ('information_schema', 'mysql', 'performance_schema', 'sys')
    is_key = any(pattern in db.lower() for pattern in key_patterns)
    marker = "⭐" if is_key and not is_system else "  "
    print(f"{marker} {db}")

cursor.close()
conn.close()

print("\n" + "="*80)
print("⭐ = Likely used by nys-voter-pipeline")
print("="*80)
