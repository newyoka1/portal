import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'), database='nys_voter_tagging', charset='utf8mb4')
cur = conn.cursor()
cur.execute("SELECT FirstName, LastName, Email, ResAddressLine1, ResCity, ResZip5, Phone, CountyName FROM voter_file WHERE UPPER(LastName) = 'ROSENBERG' AND UPPER(FirstName) LIKE 'SID%%' LIMIT 20")
rows = cur.fetchall()
with open(r'D:\git\nys-voter-pipeline\_tmp_output.txt', 'w', encoding='utf-8') as f:
    f.write(f'Found {len(rows)} results\n')
    for r in rows:
        f.write(str(r) + '\n')
conn.close()
