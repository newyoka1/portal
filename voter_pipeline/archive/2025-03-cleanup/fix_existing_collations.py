#!/usr/bin/env python3
"""
Fix Existing Database Collations
==================================
Converts National_Donors and boe_donors databases to utf8mb4_0900_ai_ci
"""

import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

TARGET_COLLATION = "utf8mb4_0900_ai_ci"

def connect_db(database=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=False
    )

def fix_database_collation(db_name):
    """Fix database-level collation"""
    print(f"\n{'='*80}")
    print(f"FIXING DATABASE: {db_name}")
    print('='*80)
    
    conn = connect_db()
    cur = conn.cursor()
    
    # Check if database exists
    cur.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s", (db_name,))
    if not cur.fetchone():
        print(f"  Database '{db_name}' not found - skipping")
        conn.close()
        return
    
    # Get current database collation
    cur.execute("""
        SELECT DEFAULT_COLLATION_NAME 
        FROM information_schema.SCHEMATA 
        WHERE SCHEMA_NAME = %s
    """, (db_name,))
    current_collation = cur.fetchone()[0]
    
    print(f"  Current collation: {current_collation}")
    
    if current_collation == TARGET_COLLATION:
        print(f"  ✓ Already using {TARGET_COLLATION}")
        conn.close()
        return
    
    # Alter database collation
    print(f"  Changing database collation to {TARGET_COLLATION}...")
    try:
        cur.execute(f"ALTER DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE {TARGET_COLLATION}")
        conn.commit()
        print("  ✓ Database collation updated")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        conn.rollback()
        conn.close()
        return
    
    # Get all tables in the database
    cur.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_TYPE = 'BASE TABLE'
    """, (db_name,))
    tables = [row[0] for row in cur.fetchall()]
    
    if not tables:
        print("  No tables found")
        conn.close()
        return
    
    print(f"\n  Found {len(tables)} tables to fix:")
    
    # Fix each table
    conn.close()
    conn = connect_db(db_name)
    cur = conn.cursor()
    
    for table in tables:
        print(f"\n  Table: {table}")
        
        # Get all string columns
        cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND COLLATION_NAME IS NOT NULL
            ORDER BY ORDINAL_POSITION
        """, (db_name, table))
        
        string_cols = cur.fetchall()
        
        if not string_cols:
            print("    No string columns - skipping")
            continue
        
        # Check if any columns need fixing
        needs_fix = [col for col in string_cols if col[2] != TARGET_COLLATION]
        
        if not needs_fix:
            print(f"    ✓ All columns already use {TARGET_COLLATION}")
            continue
        
        print(f"    Fixing {len(needs_fix)} columns...")
        
        # Fix each column
        for col_name, col_type, current_coll, is_nullable in needs_fix:
            null_spec = "NULL" if is_nullable == "YES" else "NOT NULL"
            
            alter_sql = f"""
                ALTER TABLE `{table}` 
                MODIFY COLUMN `{col_name}` {col_type} 
                CHARACTER SET utf8mb4 
                COLLATE {TARGET_COLLATION} 
                {null_spec}
            """
            
            try:
                cur.execute(alter_sql)
                conn.commit()
                print(f"      ✓ {col_name}: {current_coll} → {TARGET_COLLATION}")
            except Exception as e:
                print(f"      ✗ {col_name}: ERROR - {e}")
                conn.rollback()
        
        # Convert table default collation
        try:
            cur.execute(f"ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE {TARGET_COLLATION}")
            conn.commit()
            print(f"    ✓ Table default collation updated")
        except Exception as e:
            print(f"    ✗ Table conversion error: {e}")
            conn.rollback()
    
    conn.close()
    print(f"\n  ✓ Database {db_name} collation fixes complete!")

def main():
    print("=" * 80)
    print("FIX EXISTING DATABASE COLLATIONS")
    print("=" * 80)
    print(f"Target collation: {TARGET_COLLATION}")
    print()
    
    # Fix both databases
    databases = ["National_Donors", "boe_donors"]
    
    for db in databases:
        fix_database_collation(db)
    
    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print()
    print("All existing databases and tables now use utf8mb4_0900_ai_ci")
    print("All future table creation will use this collation")
    print()
    print("You can now run: python main.py fec-enrich")
    print()

if __name__ == "__main__":
    main()
