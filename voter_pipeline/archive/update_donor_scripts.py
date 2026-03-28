"""
Patches all donor scripts to use nys_voter_tagging instead of donors_2024.
"""
import os, re

base = r"D:\git\nys-voter-pipeline"
files_to_patch = [
    r"donors\boe_ingest.py",
    r"donors\boe_reclassify.py",
    r"donors\boe_build_final.py",
    r"donors\add_donor_detail.py",
    r"donors\add_donor_totals.py",
    r"donors\accdb_to_mysql.py",
]

for rel in files_to_patch:
    path = os.path.join(base, rel)
    with open(path, "r", encoding="utf-8") as f:
        original = f.read()

    updated = original.replace("donors_2024", "nys_voter_tagging")

    changes = original.count("donors_2024")
    if changes == 0:
        print(f"  No changes needed: {rel}")
        continue

    with open(path, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"  Patched {changes} reference(s): {rel}")

print("\nDone. Verifying no donors_2024 references remain...")
for rel in files_to_patch:
    path = os.path.join(base, rel)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    remaining = content.count("donors_2024")
    if remaining > 0:
        print(f"  WARNING: {remaining} remaining in {rel}")
    else:
        print(f"  OK: {rel}")