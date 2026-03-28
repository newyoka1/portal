import os, sys
os.chdir(r'D:\git\nys-voter-pipeline')
from dotenv import load_dotenv
load_dotenv()
import mysql.connector

conn = mysql.connector.connect(
    host='127.0.0.1', port=3306, user='root',
    password=os.environ.get('MYSQL_PASSWORD','')
)
cur = conn.cursor()
cur.execute('SHOW DATABASES')
rows = cur.fetchall()
for r in rows:
    print(r)
conn.close()
