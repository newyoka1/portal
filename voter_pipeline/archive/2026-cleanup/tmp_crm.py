import pymysql, os, json
from dotenv import load_dotenv
load_dotenv('D:/git/nys-voter-pipeline/.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST'), port=int(os.getenv('MYSQL_PORT')),
    user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
    database='crm_unified'
)
cur = conn.cursor()

cur.execute('SHOW TABLES')
tables = [r[0] for r in cur.fetchall()]

out = []
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM `{t}`')
    cnt = cur.fetchone()[0]
    cur.execute(f'SHOW COLUMNS FROM `{t}`')
    cols = cur.fetchall()
    out.append(f'\n=== {t}  ({cnt:,} rows) ===')
    for c in cols:
        out.append(f'  {c[0]:40s} {c[1]}')
    cur.execute(f'SELECT * FROM `{t}` LIMIT 2')
    rows = cur.fetchall()
    col_names = [c[0] for c in cols]
    for i, row in enumerate(rows):
        out.append(f'  --- sample {i+1} ---')
        for cn, val in zip(col_names, row):
            if val is not None and str(val).strip():
                out.append(f'    {cn}: {val}')

result = '\n'.join(out)
with open('D:/git/nys-voter-pipeline/tmp_output.txt', 'w', encoding='utf-8') as f:
    f.write(result)
conn.close()
