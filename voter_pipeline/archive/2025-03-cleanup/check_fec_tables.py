"""
Check collations on the actual FEC tables being used
"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    port=int(os.getenv('MYSQL_PORT')),
    charset='utf8mb4'
)
cursor = conn.cursor()

print("="*80)
print("CHECKING FEC_NEW TABLE COLLATIONS")
print("="*80)

cursor.execute("USE fec_new")

for table in ['fec_committee_party', 'fec_contributions', 'fec_ny_summary']:
    print(f"\n{table}:")
    print("-"*80)
    
    cursor.execute(f"""
        SELECT COLUMN_NAME, COLLATION_NAME, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'fec_new'
          AND TABLE_NAME = '{table}'
          AND COLLATION_NAME IS NOT NULL
        ORDER BY COLUMN_NAME
    """)
    
    wrong_collation = []
    for col, collation, typ in cursor.fetchall():
        status = "✓" if collation == 'utf8mb4_0900_ai_ci' else "✗"
        print(f"  {status} {col:30} {typ:20} {collation}")
        if collation != 'utf8mb4_0900_ai_ci':
            wrong_collation.append((col, collation))
    
    if wrong_collation:
        print(f"\n  ⚠ {len(wrong_collation)} columns need fixing")
    else:
        print(f"\n  ✓ All columns correct")

cursor.close()
conn.close()
