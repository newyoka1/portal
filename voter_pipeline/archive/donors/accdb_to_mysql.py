import pyodbc
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import re

ACCESS_DB = r'D:\2024 Donors.accdb'
MYSQL_DB   = 'nys_voter_tagging'

# ── type map ────────────────────────────────────────────────────────────────
def access_to_mysql_type(type_name, size):
    t = type_name.upper()
    if t == 'COUNTER':   return 'INT AUTO_INCREMENT'
    if t == 'INTEGER':   return 'INT'
    if t == 'BYTE':      return 'TINYINT'
    if t == 'DOUBLE':    return 'DOUBLE'
    if t == 'CURRENCY':  return 'DECIMAL(19,4)'
    if t == 'BIT':       return 'TINYINT(1)'
    if t == 'LONGCHAR':  return 'LONGTEXT'
    if t == 'DATETIME':  return 'DATETIME'
    if t == 'DATE':      return 'DATE'
    if t == 'VARCHAR':
        s = int(size) if size else 255
        if s > 65535: return 'LONGTEXT'
        if s > 255:   return f'VARCHAR({s})'
        return f'VARCHAR({s})'
    return 'TEXT'

def safe_col(name):
    return f'`{name}`'

def safe_table(name):
    return f'`{name}`'

# ── connect Access ───────────────────────────────────────────────────────────
print("Connecting to Access...")
acc = pyodbc.connect(
    rf'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={ACCESS_DB};'
)
acc_cur = acc.cursor()

# ── connect / create MySQL DB ────────────────────────────────────────────────
print("Connecting to MySQL...")
my = get_conn('nys_voter_tagging')
my_cur = my.cursor()
my_cur.execute(f'CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;')
my_cur.execute(f'USE `{MYSQL_DB}`;')
my.commit()

# ── get tables ───────────────────────────────────────────────────────────────
tables = [row.table_name for row in acc_cur.tables(tableType='TABLE')]
print(f"\nFound {len(tables)} tables: {tables}\n")

for tbl in tables:
    print(f"=== Processing: {tbl} ===")

    try:
        cols_info = list(acc.cursor().columns(table=tbl))
    except Exception as e:
        print(f"  WARNING: Could not read columns for '{tbl}': {e}")
        continue

    if not cols_info:
        print(f"  WARNING: No columns found, skipping.")
        continue

    col_defs = []
    col_names = []
    pk_col = None

    for col in cols_info:
        cname = col.column_name
        ctype = access_to_mysql_type(col.type_name, col.column_size)
        col_names.append(cname)
        if 'AUTO_INCREMENT' in ctype:
            col_defs.append(f'  {safe_col(cname)} INT AUTO_INCREMENT PRIMARY KEY')
            pk_col = cname
        else:
            col_defs.append(f'  {safe_col(cname)} {ctype}')

    ddl = f'CREATE TABLE IF NOT EXISTS {safe_table(tbl)} (\n' + ',\n'.join(col_defs) + '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n'

    my_cur.execute(f'DROP TABLE IF EXISTS {safe_table(tbl)};')
    try:
        my_cur.execute(ddl)
        my.commit()
        print(f"  OK: Table created ({len(col_names)} columns)")
    except Exception as e:
        print(f"  ERROR: CREATE TABLE failed: {e}")
        print(f"  DDL:\n{ddl}")
        continue

    try:
        data_cur = acc.cursor()
        data_cur.execute(f'SELECT * FROM [{tbl}]')
        rows = data_cur.fetchall()
    except Exception as e:
        print(f"  WARNING: Could not read data: {e}")
        continue

    if not rows:
        print(f"  OK: No data rows.")
        continue

    placeholders = ', '.join(['%s'] * len(col_names))
    col_list     = ', '.join(safe_col(c) for c in col_names)
    insert_sql   = f'INSERT IGNORE INTO {safe_table(tbl)} ({col_list}) VALUES ({placeholders})'

    batch_size = 1000
    inserted   = 0
    errors     = 0
    for i in range(0, len(rows), batch_size):
        batch = []
        for row in rows[i:i+batch_size]:
            clean = []
            for v in row:
                if isinstance(v, bytes):
                    try:    v = v.decode('utf-8', errors='replace')
                    except: v = None
                clean.append(v)
            batch.append(tuple(clean))
        try:
            my_cur.executemany(insert_sql, batch)
            my.commit()
            inserted += len(batch)
        except Exception as e:
            errors += 1
            print(f"  WARNING: Batch error at row {i}: {e}")
            for single in batch:
                try:
                    my_cur.execute(insert_sql, single)
                    my.commit()
                    inserted += 1
                except:
                    errors += 1

    print(f"  OK: {inserted} rows inserted, {errors} errors")

print("\nMigration complete! Database: nys_voter_tagging")
acc.close()
my.close()