import csv

file_path = r'D:\git\nys-voter-pipeline\data\boe_reports\2024gen_extract\2024gen.csv'

print("Looking for Schedule A rows specifically...")
print("="*80)

found = 0
with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
    reader = csv.reader(f)
    
    for row in reader:
        if len(row) > 10 and row[10] == 'A':
            found += 1
            if found <= 10:  # Show first 10 Schedule A rows
                print(f"\nSchedule A Row {found}:")
                print(f"  Committee: {row[2] if len(row) > 2 else ''}")
                print(f"  Year: {row[3] if len(row) > 3 else ''}")
                print(f"  Col 18 (contributor type): {row[17] if len(row) > 17 else ''}")
                print(f"  Col 25 (entity name): {row[24] if len(row) > 24 else ''}")
                print(f"  Col 26 (first): {row[25] if len(row) > 25 else ''}")
                print(f"  Col 27 (middle): {row[26] if len(row) > 26 else ''}")
                print(f"  Col 28 (last): {row[27] if len(row) > 27 else ''}")
                print(f"  Col 29 (address): {row[28] if len(row) > 28 else ''}")
                print(f"  Col 30 (city): {row[29] if len(row) > 29 else ''}")
                print(f"  Col 32 (zip): {row[31] if len(row) > 31 else ''}")
                print(f"  Col 37 (amount): {row[36] if len(row) > 36 else ''}")
            
            if found >= 10:
                break

print(f"\n\nTotal Schedule A rows found: {found}")
