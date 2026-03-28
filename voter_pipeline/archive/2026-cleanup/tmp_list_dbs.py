import pymysql, os
from dotenv import load_dotenv
load_dotenv('D:/git/nys-voter-pipeline/.env')

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST'), port=int(os.getenv('MYSQL_PORT')),
    user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD')
)
cur = conn.cursor()

# Get all non-system databases and their tables
cur.execute("SHOW DATABASES")
dbs = [r[0] for r in cur.fetchall() if r[0] not in ('information_schema','mysql','performance_schema','sys')]

output = []
for db in dbs:
    output.append(f'\n=== {db} ===')
    cur.execute(f'SHOW TABLES FROM `{db}`')
    tables = [t[0] for t in cur.fetchall()]
    for t in tables:
        cur.execute(f'SELECT COUNT(*) FROM `{db}`.`{t}`')
        cnt = cur.fetchone()[0]
        output.append(f'  {t}  ({cnt:,} rows)')
        # Check if table has email column
        cur.execute(f"SHOW COLUMNS FROM `{db}`.`{t}` LIKE '%%email%%'")
        ecols = cur.fetchall()
        if ecols:
            output.append(f'    ^ HAS EMAIL COLUMNS: {[c[0] for c in ecols]}')

conn.close()

result = '\n'.join(output)
print(result)
with open('D:/git/nys-voter-pipeline/tmp_output.txt', 'w') as f:
    f.write(result)
