import os, sys
os.chdir(r"D:\git\nys-voter-pipeline")
sys.path.insert(0, r"D:\git\nys-voter-pipeline")
from dotenv import load_dotenv
load_dotenv()
import mysql.connector

conn = mysql.connector.connect(
    host=os.environ["AIVEN_HOST"],
    port=int(os.environ["AIVEN_PORT"]),
    user=os.environ["AIVEN_USER"],
    password=os.environ["AIVEN_PASSWORD"],
    database=os.environ["AIVEN_DB"],
    ssl_ca=os.environ["AIVEN_SSL_CA"],
    ssl_verify_cert=True
)
cur = conn.cursor()
cur.execute("SHOW TABLES")
tables = [t[0] for t in cur.fetchall()]
conn.close()

with open(r"D:\git\nys-voter-pipeline\test_out.txt","w") as f:
    f.write("TABLES:\n" + "\n".join(tables))
