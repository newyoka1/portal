import csv

file_path = r'D:\git\nys-voter-pipeline\data\boe_reports\2024gen_extract\2024gen.csv'

print("Checking Schedule A contributor types...")
print("="*80)

contributor_types = {}
schedule_a_count = 0

with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
    reader = csv.reader(f)
    
    for row in reader:
        if len(row) > 10 and row[10] == 'A':
            schedule_a_count += 1
            
            # Column 18 (index 17) = contributor type
            cont_type = row[17] if len(row) > 17 else ''
            contributor_types[cont_type] = contributor_types.get(cont_type, 0) + 1

print(f"Total Schedule A rows: {schedule_a_count:,}\n")
print("Contributor types breakdown:")
for cont_type, count in sorted(contributor_types.items(), key=lambda x: -x[1]):
    print(f"  '{cont_type}': {count:,} rows ({count/schedule_a_count*100:.1f}%)")
