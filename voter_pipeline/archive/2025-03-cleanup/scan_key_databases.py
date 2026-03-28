"""
Scan ONLY the 3 key databases for space issues
"""
import pymysql
import os
import json
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        port=int(os.getenv('MYSQL_PORT')),
        charset='utf8mb4'
    )

# Only these 3 databases
KEY_DATABASES = ['boe_donors', 'fec_new', 'nys_voter_tagging']

def main():
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*80)
    print("SCANNING 3 KEY DATABASES FOR SPACES")
    print("="*80)
    
    all_issues = []
    
    for db in KEY_DATABASES:
        print(f"\n[{db}]")
        
        try:
            cursor.execute(f"USE `{db}`")
            cursor.execute("SHOW TABLES")
            tables = [t[0] for t in cursor.fetchall()]
            
            db_issues = []
            
            for table in tables:
                # Check if table name has spaces
                if ' ' in table:
                    db_issues.append({
                        'type': 'table',
                        'database': db,
                        'table': table,
                        'new_name': table.replace(' ', '_')
                    })
                    print(f"  TABLE: '{table}' -> '{table.replace(' ', '_')}'")
                
                # Check columns for spaces
                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()
                for col in columns:
                    col_name = col[0]
                    if ' ' in col_name:
                        db_issues.append({
                            'type': 'column',
                            'database': db,
                            'table': table,
                            'column': col_name,
                            'new_name': col_name.replace(' ', '_')
                        })
                        print(f"  COLUMN: {table}.'{col_name}' -> '{col_name.replace(' ', '_')}'")
            
            if db_issues:
                all_issues.extend(db_issues)
            else:
                print(f"  OK - No issues found")
                
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print("\n" + "="*80)
    print(f"TOTAL ISSUES FOUND: {len(all_issues)}")
    print("="*80)
    
    # Save to file
    with open('space_issues_key_dbs.json', 'w') as f:
        json.dump(all_issues, f, indent=2)
    
    if all_issues:
        print("\nDetails saved to: space_issues_key_dbs.json")
    else:
        print("\nALL CLEAN - No spaces found in key databases!")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
