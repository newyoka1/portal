"""
Generate SQL rename statements for all tables/columns with spaces
"""
import json

with open('space_issues.json', 'r') as f:
    issues = json.load(f)

# Group by database
by_database = {}
for issue in issues:
    db = issue['database']
    if db not in by_database:
        by_database[db] = {'tables': [], 'columns': []}
    
    if issue['type'] == 'table':
        by_database[db]['tables'].append(issue)
    else:
        by_database[db]['columns'].append(issue)

# Generate SQL
sql_output = []

for db, data in sorted(by_database.items()):
    sql_output.append(f"-- ========================================")
    sql_output.append(f"-- DATABASE: {db}")
    sql_output.append(f"-- ========================================")
    sql_output.append(f"USE `{db}`;")
    sql_output.append("")
    
    # Rename tables first
    if data['tables']:
        sql_output.append("-- RENAME TABLES")
        for t in data['tables']:
            sql_output.append(f"RENAME TABLE `{t['table']}` TO `{t['new_name']}`;")
        sql_output.append("")
    
    # Then rename columns
    if data['columns']:
        sql_output.append("-- RENAME COLUMNS")
        # Group by table
        by_table = {}
        for c in data['columns']:
            table = c['table']
            if table not in by_table:
                by_table[table] = []
            by_table[table].append(c)
        
        for table, columns in sorted(by_table.items()):
            sql_output.append(f"\n-- Table: {table}")
            for c in columns:
                # Need to preserve column type - will do this dynamically
                sql_output.append(f"-- ALTER TABLE `{table}` CHANGE `{c['column']}` `{c['new_name']}` <TYPE>;")
    
    sql_output.append("\n")

# Save
with open('rename_spaces.sql', 'w') as f:
    f.write('\n'.join(sql_output))

print("SQL script generated: rename_spaces.sql")
print(f"\nSummary by database:")
for db, data in sorted(by_database.items()):
    print(f"  {db}: {len(data['tables'])} tables, {len(data['columns'])} columns")
