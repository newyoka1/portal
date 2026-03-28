import os, sys
os.chdir(r"D:\git\nys-voter-pipeline")
from dotenv import load_dotenv
load_dotenv()
import mysql.connector

conn = mysql.connector.connect(
    host=os.environ["AIVEN_HOST"], port=int(os.environ["AIVEN_PORT"]),
    user=os.environ["AIVEN_USER"], password=os.environ["AIVEN_PASSWORD"],
    database=os.environ["AIVEN_DB"], ssl_ca=os.environ["AIVEN_SSL_CA"], ssl_verify_cert=True
)
cur = conn.cursor()

# Check local MySQL for donor/contribution tables
out = ["=== Aiven nys_voter_tagging tables: done above ===\n"]
out.append("Checking for donor-related databases on local MySQL...")

# Try local connection
try:
    local = mysql.connector.connect(host="127.0.0.1", port=3306, user="root", password=os.environ.get("MYSQL_PASSWORD",""))
    lc = local.cursor()
    lc.execute("SHOW DATABASES")
    dbs = [r[0] for r in lc.fetchall()]
    out.append("LOCAL DATABASES: " + ", ".join(dbs))
    for db in dbs:
        lc.execute(f"SHOW TABLES FROM `{db}`")
        tbls = [r[0] for r in lc.fetchall()]
        donor_tbls = [t for t in tbls if any(k in t.lower() for k in ["donor","contrib","boe","cfb","fec","campaign","finance"])]
        if donor_tbls:
            out.append(f"  {db}: {donor_tbls}")
    local.close()
except Exception as e:
    out.append(f"Local connection error: {e}")

conn.close()
with open(r"D:\git\nys-voter-pipeline\test_out.txt","w") as f:
    f.write("\n".join(out))
