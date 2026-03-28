import pymysql, os
from dotenv import load_dotenv
load_dotenv('D:\\git\\nys-voter-pipeline\\.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT',3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD')
)

cur = conn.cursor()

print("Enabling LOAD DATA LOCAL INFILE on MySQL server...")
cur.execute("SET GLOBAL local_infile = 1")
print("✓ Enabled on server")

cur.execute("SHOW VARIABLES LIKE 'local_infile'")
result = cur.fetchone()
print(f"\nCurrent setting: {result[1]}")

conn.close()
print("\nNow run: python donors/boe_import_fast.py")
