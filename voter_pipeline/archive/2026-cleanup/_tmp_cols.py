import pymysql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymysql.connect(host=os.getenv('MYSQL_HOST','127.0.0.1'), port=int(os.getenv('MYSQL_PORT','3306')),
    user=os.getenv('MYSQL_USER','root'), password=os.getenv('MYSQL_PASSWORD'), database='nys_voter_tagging')
cur = conn.cursor()
cur.execute("SHOW COLUMNS FROM voter_file")
cols = [r[0] for r in cur.fetchall()]
with open(r'D:\git\nys-voter-pipeline\_tmp_cols_out.txt', 'w') as f:
    for c in cols:
        f.write(c + '\n')
print("Done - wrote", len(cols), "columns")
conn.close()
