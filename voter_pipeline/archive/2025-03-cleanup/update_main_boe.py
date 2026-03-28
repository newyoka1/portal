#!/usr/bin/env python3
"""
Update main.py to add boe-enrich command
"""
import re

MAIN_PY = r"D:\git\nys-voter-pipeline\main.py"

# Read current main.py
with open(MAIN_PY, 'r', encoding='utf-8') as f:
    content = f.read()

# Add boe-enrich parser after fec-enrich
if 'boe-enrich' not in content:
    pattern = r'(    # fec-enrich\s+sub\.add_parser\("fec-enrich"[^\n]+\n)'
    replacement = r'\1\n    # boe-enrich\n    sub.add_parser("boe-enrich", help="Enrich voter_file with BOE donor data from donors_2024")\n'
    content = re.sub(pattern, replacement, content)
    print("? Added boe-enrich parser")
else:
    print("  boe-enrich parser already exists")

# Add boe-enrich command handler after fec-enrich
if 'elif args.command == "boe-enrich":' not in content:
    pattern = r'(    elif args\.command == "fec-enrich":\s+run\("pipeline/enrich_fec_donors\.py"\)\n)'
    replacement = r'''\1
    elif args.command == "boe-enrich":
        run("pipeline/enrich_boe_donors.py")

'''
    content = re.sub(pattern, replacement, content)
    print("? Added boe-enrich command handler")
else:
    print("  boe-enrich command handler already exists")

# Write back
with open(MAIN_PY, 'w', encoding='utf-8') as f:
    f.write(content)

print("\n? main.py updated successfully")
print("\nNow you can run:")
print("  python main.py boe-enrich")
