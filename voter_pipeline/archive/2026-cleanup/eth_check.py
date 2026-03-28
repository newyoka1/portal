import os, sys, mysql.connector
from dotenv import load_dotenv
load_dotenv(r'D:\git\nys-voter-pipeline\.env')
try:
    conn = mysql.connector.connect(host=os.getenv('MYSQL_HOST','127.0.0.1'), port=int(os.getenv('MYSQL_PORT',3306)), user=os.getenv('MYSQL_USER','root'), password=os.getenv('MYSQL_PASSWORD',''), database='nys_voter_tagging')
    cur = conn.cursor()
    cur.execute('SELECT ModeledEthnicity, COUNT(*) cnt FROM voter_file GROUP BY ModeledEthnicity ORDER BY cnt DESC')
    rows = cur.fetchall()
    total = sum(r[1] for r in rows)
    lines = []
    lines.append(f'{"Ethnicity":<25} {"Count":>12} {"Pct":>7}')
    lines.append('-'*48)
    for eth, cnt in rows:
        lines.append(f'{str(eth):<25} {cnt:>12,} {cnt/total*100:>6.2f}%')
    lines.append(f'{"TOTAL":<25} {total:>12,}')
    out = chr(10).join(lines)
    with open(r'D:\git\nys-voter-pipeline\logs\eth_result.txt', 'w') as f:
        f.write(out)
    cur.close(); conn.close()
except Exception as e:
    with open(r'D:\git\nys-voter-pipeline\logs\eth_result.txt', 'w') as f:
        f.write(f'ERROR: {e}')
