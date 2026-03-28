import csv

file_path = r'D:\git\nys-voter-pipeline\data\boe_reports\2024gen_extract\2024gen.csv'

print("Analyzing BOE CSV structure...")
print("="*80)

with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
    reader = csv.reader(f)
    
    for i, row in enumerate(reader):
        if i >= 10:  # First 10 rows
            break
        
        print(f"\nRow {i+1} - Total fields: {len(row)}")
        print(f"  Col 1 (filer_id): {row[0] if len(row) > 0 else ''}")
        print(f"  Col 3 (committee): {row[2] if len(row) > 2 else ''}")
        print(f"  Col 4 (year): {row[3] if len(row) > 3 else ''}")
        print(f"  Col 11 (schedule): {row[10] if len(row) > 10 else ''}")
        print(f"  Col 25 (entity name): {row[24] if len(row) > 24 else ''}")
        print(f"  Col 26 (first?): {row[25] if len(row) > 25 else ''}")
        print(f"  Col 27 (middle?): {row[26] if len(row) > 26 else ''}")
        print(f"  Col 28 (last?): {row[27] if len(row) > 27 else ''}")
        print(f"  Col 37 (amount): {row[36] if len(row) > 36 else ''}")
        
print("\n" + "="*80)
print("Checking Schedule types:")
with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
    reader = csv.reader(f)
    schedules = {}
    
    for row in reader:
        if len(row) > 10:
            sched = row[10]
            schedules[sched] = schedules.get(sched, 0) + 1
    
    for sched, count in sorted(schedules.items()):
        print(f"  Schedule {sched}: {count:,} rows")
