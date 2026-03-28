"""
Search all Python files for references to tables/columns with spaces
"""
import os
import re
import json

# Load the issues
with open('space_issues.json', 'r') as f:
    issues = json.load(f)

# Extract unique table/column names
table_names = set()
column_names = set()

for issue in issues:
    if issue['type'] == 'table':
        table_names.add(issue['table'])
    else:
        column_names.add(issue['column'])

print("="*80)
print("SEARCHING FOR REFERENCES IN PYTHON CODE")
print("="*80)

# Search all .py files
references = []

def search_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            file_refs = []
            
            # Search for table references
            for table in table_names:
                if table in content:
                    # Find line numbers
                    for i, line in enumerate(content.split('\n'), 1):
                        if table in line:
                            file_refs.append({
                                'file': filepath,
                                'line': i,
                                'type': 'table',
                                'name': table,
                                'context': line.strip()[:100]
                            })
            
            # Search for column references (quoted)
            for col in column_names:
                # Look for quoted references
                patterns = [
                    f'"{col}"',
                    f"'{col}'",
                    f"`{col}`",
                    f'[{col}]'  # Pandas style
                ]
                for pattern in patterns:
                    if pattern in content:
                        for i, line in enumerate(content.split('\n'), 1):
                            if pattern in line:
                                file_refs.append({
                                    'file': filepath,
                                    'line': i,
                                    'type': 'column',
                                    'name': col,
                                    'context': line.strip()[:100]
                                })
                                break
            
            return file_refs
    except Exception as e:
        return []

# Walk through project directory
for root, dirs, files in os.walk('.'):
    # Skip certain directories
    if any(skip in root for skip in ['.git', '.venv', '__pycache__', 'node_modules']):
        continue
    
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            refs = search_file(filepath)
            if refs:
                references.extend(refs)

# Print results
if references:
    print(f"\nFOUND {len(references)} REFERENCES:\n")
    
    # Group by file
    by_file = {}
    for ref in references:
        file = ref['file']
        if file not in by_file:
            by_file[file] = []
        by_file[file].append(ref)
    
    for file, refs in sorted(by_file.items()):
        print(f"\n{file}:")
        for ref in refs:
            print(f"  Line {ref['line']:4}: [{ref['type'].upper():6}] {ref['name']}")
            print(f"           {ref['context']}")
else:
    print("\n✓ NO REFERENCES FOUND - Safe to rename!")
    print("\nThis means the Python code does not reference tables/columns with spaces.")
    print("However, you should still review the SQL script before executing.")

print("\n" + "="*80)
