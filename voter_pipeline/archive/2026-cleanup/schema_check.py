import os
os.chdir(r"D:\git\nys-voter-pipeline")
from dotenv import load_dotenv; load_dotenv()
import mysql.connector

conn = mysql.connector.connect(host="127.0.0.1", port=3306, user="root", password=os.environ.get("MYSQL_PASSWORD",""))
cur = conn.cursor()

cur.execute("DESCRIBE boe_donors.contributions")
cols = cur.fetchall()
for c in cols:
    print(c)

print("\n--- SAMPLE ROW ---")
cur.execute("SELECT * FROM boe_donors.contributions WHERE filer='Mills for NY Senate' LIMIT 1")
row = cur.fetchone()
print(row)
conn.close()
