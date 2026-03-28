import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(host="127.0.0.1", port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"), database="nys_voter_tagging")
cur = conn.cursor()
cur.execute("SHOW COLUMNS FROM voter_file")
cols = [r[0] for r in cur.fetchall()]
print(cols)
conn.close()
