import csv
import os

boe_dir = r'D:\git\nys-voter-pipeline\data\boe_reports'
files = [
    '2024gen_extract/2024gen.csv',
    '2024pri_extract/2024pri.csv',
    '2025gen_extract/2025gen.csv'
]

for file in files:
    path = os.path.join(boe_dir, file)
    if os.path.exists(path):
        print(f"\n{file}:")
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            rows = list(reader)[:5]
            
            if rows:
                print(f"  Columns: {len(rows[0])}")
                print(f"  Sample row 1: {rows[0][:10]}...")
                if len(rows) > 1:
                    print(f"  Sample row 2: {rows[1][:10]}...")
