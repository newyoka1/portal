"""
patch_add_donor_columns.py
--------------------------
One-time patch: adds donor_D_amt, donor_R_amt, donor_U_amt columns
to export_ld_to_excel_simple.py and adds a ModeledEthnicity column
to the per-audience tabs.

Run once from D:\git:
    python patch_add_donor_columns.py

Creates a backup at export_ld_to_excel_simple.py.bak before patching.
"""

import re
import shutil
from pathlib import Path

SCRIPT = Path(r"D:\git\export_ld_to_excel_simple.py")
BACKUP = SCRIPT.with_suffix(".py.bak")

ALREADY_PATCHED_MARKER = "donor_D_amt"


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def save(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def backup(src, dst):
    shutil.copy2(src, dst)
    print(f"Backup created: {dst}")


def patch_select_query(content):
    if ALREADY_PATCHED_MARKER in content:
        print("  checkmark SELECT query already contains donor columns -- skipping.")
        return content, False

    pattern = re.compile(
        r'([ \t]+v\.\w+)\n([ \t]+FROM voter_file)',
        re.MULTILINE
    )
    match = pattern.search(content)
    if not match:
        print("  X Could not find v.<col>\\n    FROM voter_file pattern.")
        print("    Manually add donor columns to the SELECT query.")
        return content, False

    last_col_line = match.group(1)
    from_line     = match.group(2)
    replacement = (
        f"{last_col_line},\n"
        f"    v.donor_D_amt,\n"
        f"    v.donor_R_amt,\n"
        f"    v.donor_U_amt,\n"
        f"    v.ModeledEthnicity\n"
        f"{from_line}"
    )
    new_content = content[:match.start()] + replacement + content[match.end():]
    print("  OK Donor + ModeledEthnicity columns added to SELECT query.")
    return new_content, True


def patch_headers(content):
    if "Donor Dem $" in content:
        print("  checkmark Header row already contains donor headers -- skipping.")
        return content, False

    pattern = re.compile(
        r"((?:headers|HEADERS|cols|columns)\s*=\s*\[.*?'StateVoterId'.*?\])",
        re.DOTALL
    )
    match = pattern.search(content)
    if not match:
        pattern = re.compile(
            r"((?:headers|HEADERS|cols|columns)\s*=\s*\(.*?'StateVoterId'.*?\))",
            re.DOTALL
        )
        match = pattern.search(content)

    if not match:
        print("  X Could not find header list containing 'StateVoterId'.")
        print("    Manually add: 'Donor Dem $', 'Donor Rep $', 'Donor Unk $', 'ModeledEthnicity'")
        return content, False

    original = match.group(1)
    closing = ']' if original.rstrip().endswith(']') else ')'
    insert_idx = original.rfind(closing)
    new_headers = ",\n    'Donor Dem $', 'Donor Rep $', 'Donor Unk $', 'ModeledEthnicity'"
    patched = original[:insert_idx] + new_headers + "\n" + original[insert_idx:]
    new_content = content.replace(original, patched, 1)
    print("  OK Donor + ModeledEthnicity headers added to header list.")
    return new_content, True


def patch_summary_query(content):
    if content.count(ALREADY_PATCHED_MARKER) >= 2:
        print("  checkmark Summary query already patched -- skipping.")
        return content, False

    occurrences = [m.start() for m in re.finditer(r'FROM voter_file', content)]
    if len(occurrences) < 2:
        print("  ~ Only one voter_file query -- summary may not need patching.")
        return content, False

    second_start = occurrences[1]
    before = content[:second_start]
    lines = before.split('\n')

    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if re.match(r'^v\.\w+$', stripped):
            lines[i] = lines[i] + ","
            insert_pos = i + 1
            new_lines = [
                "    v.donor_D_amt,",
                "    v.donor_R_amt,",
                "    v.donor_U_amt,",
                "    v.ModeledEthnicity",
            ]
            for j, nl in enumerate(new_lines):
                lines.insert(insert_pos + j, nl)
            new_content = '\n'.join(lines) + content[second_start:]
            print("  OK Donor + ModeledEthnicity columns added to Summary query.")
            return new_content, True

    print("  ~ Could not auto-patch Summary query.")
    return content, False


def main():
    if not SCRIPT.exists():
        print(f"ERROR: Script not found at {SCRIPT}")
        return

    backup(SCRIPT, BACKUP)
    content = load(SCRIPT)
    print(f"\nPatching: {SCRIPT}\n")

    changed = False

    print("-- Patch 1: Main SELECT query --")
    content, ok = patch_select_query(content)
    changed = changed or ok

    print("\n-- Patch 2: Header row --")
    content, ok = patch_headers(content)
    changed = changed or ok

    print("\n-- Patch 3: Summary tab query --")
    content, ok = patch_summary_query(content)
    changed = changed or ok

    if changed:
        save(SCRIPT, content)
        print(f"\nDone. File saved: {SCRIPT}")
        print(f"Backup at: {BACKUP}")
    else:
        print("\nNo changes made (already patched or patterns not found).")

    print("\n-- Next steps --")
    print("1. python build_modeled_ethnicity.py --dry-run")
    print("2. python build_modeled_ethnicity.py   (full run, 15-30 min)")
    print("3. python export_ld_to_excel_simple.py --ld 63")


if __name__ == "__main__":
    main()
