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
cur.execute("SHOW VARIABLES LIKE 'local_infile'")
result = cur.fetchone()

print("="*60)
print("MySQL LOAD DATA LOCAL INFILE Check")
print("="*60)
print(f"\nSetting: {result[0]}")
print(f"Value: {result[1]}")

if result[1] == 'ON':
    print("\n✅ ENABLED - You can use the fast import!")
else:
    print("\n⚠️  DISABLED - Need to enable it first")
    print("\nTo enable, run:")
    print("  SET GLOBAL local_infile = 1;")

conn.close()
