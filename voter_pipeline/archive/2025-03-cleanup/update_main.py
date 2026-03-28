#!/usr/bin/env python3
"""
Update main.py to add fec-enrich command
"""
import re

MAIN_PY = r"D:\git\nys-voter-pipeline\main.py"

# Read current main.py
with open(MAIN_PY, 'r', encoding='utf-8') as f:
    content = f.read()

# Add fec-enrich parser
if 'fec-enrich' not in content:
    # Find the donors parser section
    pattern = r'(    # donors\s+sub\.add_parser\("donors"[^\n]+\n)'
    replacement = r'\1\n    # fec-enrich\n    sub.add_parser("fec-enrich", help="Enrich voter_file with FEC donor data from National_Donors")\n'
    content = re.sub(pattern, replacement, content)
    print("✓ Added fec-enrich parser")
else:
    print("  fec-enrich parser already exists")

# Add fec-enrich command handler
if 'elif args.command == "fec-enrich":' not in content:
    # Find the donors command section
    pattern = r'(    elif args\.command == "donors":\s+print\("Running donor pipeline[^\n]+\n[^\n]+\n[^\n]+\n)'
    replacement = r'''\1
    elif args.command == "fec-enrich":
        run("pipeline/enrich_fec_donors.py")

'''
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    print("✓ Added fec-enrich command handler")
else:
    print("  fec-enrich command handler already exists")

# Write back
with open(MAIN_PY, 'w', encoding='utf-8') as f:
    f.write(content)

print("\n✓ main.py updated successfully")
print("\nNow you can run:")
print("  python main.py fec-enrich")
