import pyodbc
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn

ACCESS_DB  = r'D:\2024 Donors.accdb'
MYSQL_DB   = 'donors_2024'
TBL        = 'ProvenDonors2024OnePerInd'

print("Connecting...")
acc = pyodbc.connect(rf'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={ACCESS_DB};')
my  = get_conn('donors_2024')
my_cur = my.cursor()
my_cur.execute(f'USE `{MYSQL_DB}`;')

# Read data using SELECT * -- get col names from cursor.description
print("Fetching data from Access (this may take a moment)...")
cur = acc.cursor()
cur.execute(f'SELECT * FROM [{TBL}]')
col_names = [d[0] for d in cur.description]
rows      = cur.fetchall()
print(f"  Fetched {len(rows)} rows, {len(col_names)} columns")
print(f"  Columns: {col_names}")

# Build CREATE TABLE - everything as TEXT to avoid type guessing issues
def safe(name):
    return f'`{name}`'

col_defs = [f'  {safe(c)} TEXT' for c in col_names]
ddl = f'CREATE TABLE IF NOT EXISTS {safe(TBL)} (\n' + ',\n'.join(col_defs) + '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'

my_cur.execute(f'DROP TABLE IF EXISTS {safe(TBL)};')
my_cur.execute(ddl)
my.commit()
print("  Table created.")

# Insert in batches
placeholders = ', '.join(['%s'] * len(col_names))
col_list     = ', '.join(safe(c) for c in col_names)
insert_sql   = f'INSERT IGNORE INTO {safe(TBL)} ({col_list}) VALUES ({placeholders})'

inserted = 0
errors   = 0
batch_size = 500
for i in range(0, len(rows), batch_size):
    batch = []
    for row in rows[i:i+batch_size]:
        clean = []
        for v in row:
            if isinstance(v, bytes):
                try:    v = v.decode('utf-8', errors='replace')
                except: v = None
            elif v is not None:
                v = str(v) if not isinstance(v, (int, float)) else v
            clean.append(v)
        batch.append(tuple(clean))
    try:
        my_cur.executemany(insert_sql, batch)
        my.commit()
        inserted += len(batch)
        print(f"  Inserted {inserted}/{len(rows)} rows...", end='\r')
    except Exception as e:
        errors += 1
        print(f"\n  Batch error at row {i}: {e}")
        for single in batch:
            try:
                my_cur.execute(insert_sql, single)
                my.commit()
                inserted += 1
            except:
                errors += 1

print(f"\n  Done: {inserted} rows inserted, {errors} errors")
print("Migration of ProvenDonors2024OnePerInd complete!")
acc.close()
my.close()