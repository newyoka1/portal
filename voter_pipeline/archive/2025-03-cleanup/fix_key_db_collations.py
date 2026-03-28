"""
Fix collations for the 3 key databases used by nys-voter-pipeline
Focuses ONLY on: boe_donors, fec_new, nys_voter_tagging
"""
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_PORT')),
        charset='utf8mb4',
        autocommit=False
    )

KEY_DATABASES = ['boe_donors', 'fec_new', 'nys_voter_tagging']
TARGET_COLLATION = 'utf8mb4_0900_ai_ci'  # MySQL 8.4 default

def get_column_definition(cursor, db, table, column):
    """Get full column definition for ALTER statement"""
    cursor.execute(f"USE `{db}`")
    cursor.execute(f"SHOW FULL COLUMNS FROM `{table}` WHERE Field = %s", (column,))
    row = cursor.fetchone()
    
    if not row:
        return None
    
    # row: Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
    field_type = row[1]
    collation = row[2]
    nullable = "NULL" if row[3] == "YES" else "NOT NULL"
    default = f"DEFAULT '{row[5]}'" if row[5] is not None and row[5] != 'NULL' else ""
    if row[5] is None and row[3] == "YES":
        default = "DEFAULT NULL"
    extra = row[6] if row[6] else ""
    
    # Build full definition
    definition = f"{field_type}"
    if collation and 'utf8mb4' in str(collation):
        definition += f" CHARACTER SET utf8mb4 COLLATE {TARGET_COLLATION}"
    definition += f" {nullable}"
    if default:
        definition += f" {default}"
    if extra:
        definition += f" {extra}"
    
    return definition.strip(), collation

def main():
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*80)
    print("FIXING COLLATIONS FOR KEY DATABASES")
    print("="*80)
    print(f"Target collation: {TARGET_COLLATION}")
    print()
    
    total_fixed = 0
    total_errors = 0
    
    for db in KEY_DATABASES:
        try:
            cursor.execute(f"USE `{db}`")
        except:
            print(f"\n[{db}] - DATABASE NOT FOUND, skipping")
            continue
            
        print(f"\n{'='*80}")
        print(f"DATABASE: {db}")
        print(f"{'='*80}")
        
        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        
        db_fixed = 0
        db_errors = 0
        
        for table in tables:
            # Get text columns with wrong collation
            cursor.execute(f"""
                SELECT COLUMN_NAME, COLLATION_NAME, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND COLLATION_NAME IS NOT NULL
                  AND COLLATION_NAME != %s
            """, (db, table, TARGET_COLLATION))
            
            columns = cursor.fetchall()
            
            if not columns:
                continue
                
            print(f"\n  Table: {table} ({len(columns)} columns to fix)")
            
            for col_name, current_collation, col_type in columns:
                try:
                    col_def, _ = get_column_definition(cursor, db, table, col_name)
                    if col_def:
                        sql = f"ALTER TABLE `{table}` MODIFY COLUMN `{col_name}` {col_def}"
                        cursor.execute(sql)
                        conn.commit()
                        print(f"    ✓ {col_name}: {current_collation} → {TARGET_COLLATION}")
                        db_fixed += 1
                    else:
                        print(f"    ✗ {col_name}: Could not get column definition")
                        db_errors += 1
                except Exception as e:
                    conn.rollback()
                    print(f"    ✗ {col_name}: {str(e)[:80]}")
                    db_errors += 1
        
        print(f"\n  Summary: {db_fixed} fixed, {db_errors} errors")
        total_fixed += db_fixed
        total_errors += db_errors
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    print("COLLATION FIX COMPLETE")
    print("="*80)
    print(f"Total columns fixed: {total_fixed}")
    print(f"Total errors: {total_errors}")
    
    if total_errors > 0:
        print("\nNote: Some errors are expected for system columns or special data types.")
    
    print("\nYou can now re-run: python main.py --verbose national-enrich")

if __name__ == "__main__":
    main()
