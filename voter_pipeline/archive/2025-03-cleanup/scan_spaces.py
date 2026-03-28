"""
Scan all databases for tables and columns with spaces
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
        charset='utf8mb4'
    )

def main():
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get all databases
    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall() 
                 if db[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
    
    print("="*80)
    print("SCANNING FOR TABLES AND COLUMNS WITH SPACES")
    print("="*80)
    
    all_issues = []
    
    for db in databases:
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
        
        if db_issues:
            all_issues.extend(db_issues)
            print(f"\n[{db}]")
            for issue in db_issues:
                if issue['type'] == 'table':
                    print(f"  TABLE: '{issue['table']}' → '{issue['new_name']}'")
                else:
                    print(f"  COLUMN: {issue['table']}.'{issue['column']}' → '{issue['new_name']}'")
    
    print("\n" + "="*80)
    print(f"TOTAL ISSUES FOUND: {len(all_issues)}")
    print("="*80)
    
    # Save to file for processing
    import json
    with open('space_issues.json', 'w') as f:
        json.dump(all_issues, f, indent=2)
    print("\nDetails saved to: space_issues.json")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
