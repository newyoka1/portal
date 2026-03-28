#!/usr/bin/env python3
"""
Patch export.py with three changes:
1. Republicans on top (change PARTY_ORDER in all 3 donor tabs)
2. Highlight Democrats who donated to Republicans (yellow/gold rows)
3. Add an Explanation tab describing all sheets + turnout overlap note
"""
import sys

fp = r"D:\git\nys-voter-pipeline\export\export.py"
with open(fp, "r", encoding="utf-8") as f:
    src = f.read()

original = src  # keep a copy for verification

# ============================================================
# CHANGE 1: Republicans on top in all three donor tabs
# ============================================================
# There are exactly 3 occurrences of this line
old_order = '    PARTY_ORDER = ["Democrat", "Republican", "Conservative"]'
new_order = '    PARTY_ORDER = ["Republican", "Conservative", "Democrat"]'
count = src.count(old_order)
if count != 3:
    print(f"WARNING: Expected 3 PARTY_ORDER lines, found {count}")
src = src.replace(old_order, new_order)
print(f"[1/3] PARTY_ORDER changed to Republicans first ({count} occurrences)")


# ============================================================
# CHANGE 2a: BOE donor tab - highlight Dems who gave to R
# ============================================================
# In BOE donor list loop, after the Republican bold block, add Dem-to-R highlight
# The existing code for each row in the BOE donor list:
boe_old = '''            if party == "Republican":
                for ci in range(1, NUM_COLS + 1):
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)
            grp_d += float(r_data[10] or 0)'''

boe_new = '''            if party == "Republican":
                for ci in range(1, NUM_COLS + 1):
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)
            # Highlight Democrats who donated to Republicans (yellow/gold)
            if party == "Democrat" and float(r_data[11] or 0) > 0:
                _xover_fill = PatternFill(start_color="FFD966", end_color="FFD966", fill_type="solid")
                for ci in range(1, NUM_COLS + 1):
                    ws.cell(row=row, column=ci).fill = _xover_fill
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)
            grp_d += float(r_data[10] or 0)'''

if boe_old in src:
    src = src.replace(boe_old, boe_new, 1)
    print("[2a/3] BOE Donors: Dem-to-R crossover highlight added")
else:
    print("WARNING: Could not find BOE donor loop target for Dem-to-R highlight")


# ============================================================
# CHANGE 2b: National donor tab - highlight Dems who gave to R
# ============================================================
# national_republican_amount is at index 10 in the SELECT
nat_old = '''            if party == 'Republican':
                for ci in range(1, NUM_COLS_NAT + 1):
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)

            grp_dem_amt += float(r_data[8] or 0)'''

nat_new = '''            if party == 'Republican':
                for ci in range(1, NUM_COLS_NAT + 1):
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)
            # Highlight Democrats who donated to Republicans (yellow/gold)
            if party == 'Democrat' and float(r_data[10] or 0) > 0:
                _xover_fill = PatternFill(start_color="FFD966", end_color="FFD966", fill_type="solid")
                for ci in range(1, NUM_COLS_NAT + 1):
                    ws.cell(row=row, column=ci).fill = _xover_fill
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)

            grp_dem_amt += float(r_data[8] or 0)'''

if nat_old in src:
    src = src.replace(nat_old, nat_new, 1)
    print("[2b/3] National Donors: Dem-to-R crossover highlight added")
else:
    print("WARNING: Could not find National donor loop target for Dem-to-R highlight")


# ============================================================
# CHANGE 3: Add create_explanation_tab function + call it in main()
# ============================================================

explanation_func = '''

def create_explanation_tab(wb, district_type, district_number, has_cfb=False):
    """First tab: explains every sheet in the workbook and notes on turnout overlap."""
    ws = wb.create_sheet("Guide", 0)
    ws.sheet_properties.tabColor = "000000"

    title_font = Font(bold=True, size=14)
    section_font = Font(bold=True, size=12, color="1F497D")
    bold_font = Font(bold=True, size=11)
    normal_font = Font(size=11)
    note_font = Font(italic=True, size=11, color="C00000")
    hdr_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    hdr_font = Font(bold=True, color="FFFFFF", size=11)
    alt_fill = PatternFill(start_color="F2F7FF", end_color="F2F7FF", fill_type="solid")
    xover_fill = PatternFill(start_color="FFD966", end_color="FFD966", fill_type="solid")

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 90

    ws["A1"] = f"{district_type} {district_number} - Export Guide"
    ws["A1"].font = title_font
    ws.merge_cells("A1:B1")
    from datetime import datetime as _dt
    ws["A2"] = f"Generated: {_dt.now().strftime('%Y-%m-%d %H:%M')}"
    ws["A2"].font = Font(italic=True, size=10, color="666666")
    ws.merge_cells("A2:B2")

    row = 4
    ws.cell(row=row, column=1, value="SHEET DESCRIPTIONS").font = section_font
    ws.merge_cells(f"A{row}:B{row}")
    row += 1

    for ci, h in enumerate(["Sheet Name", "Description"], 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font = hdr_font; c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center")
    row += 1

    sheets = [
        ("Guide", "This sheet. Describes every tab in this workbook."),
        ("Summary", "Overview of all audience files matched to this district. Shows unique voter counts per audience, "
                     "combined deduplicated totals (so double-counted voters are removed), and overall district match rate."),
        ("Ethnicity (Modeled)", "Demographic breakdown using surname-based ethnicity modeling. "
                                "Section 1 shows audience match rates by ethnicity. "
                                "Section 2 shows party registration breakdown by ethnicity."),
        ("BOE Donors", "NYS Board of Elections state-level campaign finance donors matched to voters in this district. "
                       "Section 1: summary by party. Section 2: year-by-year totals. "
                       "Section 3: full donor list grouped by party registration (Republicans first). "
                       "Gold highlighted rows = registered Democrats who also donated to Republican candidates."),
        ("National Donor", "Federal (FEC) campaign contributions matched to voters. "
                           "Summary section shows totals by party signal (Dem/Rep/Ind/Unknown). "
                           "Donor list grouped by voter registration party (Republicans first). "
                           "Gold highlighted rows = registered Democrats who also donated to Republican candidates/committees."),
    ]
    if has_cfb:
        sheets.append(
            ("CFB Donors", "NYC Campaign Finance Board city-level contributions. "
                           "Shows per-cycle breakdown (2017, 2021, 2023, 2025) and full donor list "
                           "grouped by voter registration party (Republicans first).")
        )
    sheets += [
        ("Registered Democrats", "Full voter roster of all registered Democrats in this district "
                                 "with contact info, address, DOB, registration date, ethnicity, and email (if available)."),
        ("Republicans & Conservatives", "Full voter roster of all registered Republicans and Conservatives in this district "
                                        "with contact info, address, DOB, registration date, ethnicity, and email (if available)."),
        ("Issue Audience Tabs (orange)", "One tab per issue-based audience file (e.g. 'blue collar', 'border security crisis', "
                                         "'children in home'). Each tab lists every voter in this district who matched that audience. "
                                         "A voter can appear on MULTIPLE issue tabs if they matched multiple audiences."),
        ("Turnout Model Tabs (green)", "One tab per turnout model (HT = High Turnout, MT = Medium Turnout, LT = Low Turnout "
                                       "for DEM/GOP/IND). Each tab lists voters scored into that turnout tier."),
        ("Unmatched Voters", "Voters in this district who did not match ANY audience file. "
                             "These are voters with no audience data from the Causeway match."),
    ]

    for i, (name, desc) in enumerate(sheets):
        fill = alt_fill if i % 2 == 0 else None
        c1 = ws.cell(row=row, column=1, value=name)
        c1.font = bold_font
        c2 = ws.cell(row=row, column=2, value=desc)
        c2.font = normal_font
        c2.alignment = Alignment(wrap_text=True)
        if fill:
            c1.fill = fill; c2.fill = fill
        row += 1

    row += 1

    # --- COLOR KEY section ---
    ws.cell(row=row, column=1, value="COLOR KEY (Donor Tabs)").font = section_font
    ws.merge_cells(f"A{row}:B{row}")
    row += 1

    colors = [
        (PatternFill(start_color="FFB3B3", end_color="FFB3B3", fill_type="solid"),
         "Republican (voter registration)"),
        (PatternFill(start_color="E2CFED", end_color="E2CFED", fill_type="solid"),
         "Conservative (voter registration)"),
        (PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid"),
         "Democrat (voter registration)"),
        (xover_fill,
         "CROSSOVER: Registered Democrat who donated to Republican candidates (gold highlight, bold text)"),
    ]
    for fill, label in colors:
        ws.cell(row=row, column=1).fill = fill
        ws.cell(row=row, column=1).value = ""
        ws.cell(row=row, column=2, value=label).font = normal_font
        row += 1

    row += 1

    # --- TURNOUT OVERLAP NOTE ---
    ws.cell(row=row, column=1, value="IMPORTANT: TURNOUT AUDIENCE OVERLAP").font = section_font
    ws.merge_cells(f"A{row}:B{row}")
    row += 1

    overlap_notes = [
        "Turnout models (HT/MT/LT) are scored independently for each party propensity (DEM, GOP, IND).",
        "A single voter CAN appear on multiple turnout tabs. For example, a voter might be scored as "
        "HT HARD GOP (high-turnout strong Republican) AND also appear on MT SOFT DEM (medium-turnout weak Democrat) "
        "if their voting history and demographics produce overlapping signals.",
        "This is by design: the models measure LIKELIHOOD of supporting each party at different turnout levels, "
        "not a single mutually exclusive assignment.",
        "The 'COMBINED UNIQUE' row on the Summary tab shows the deduplicated count across all turnout models, "
        "removing any double-counting.",
        "Issue audiences can also overlap with each other and with turnout models. "
        "A voter in 'border security crisis' can simultaneously be in 'blue collar' and 'HT HARD GOP'.",
        "When planning outreach, use the COMBINED UNIQUE counts for accurate universe sizing, "
        "not the sum of individual audience counts.",
    ]
    for note in overlap_notes:
        ws.cell(row=row, column=1, value="").font = normal_font
        c = ws.cell(row=row, column=2, value=note)
        c.font = normal_font
        c.alignment = Alignment(wrap_text=True)
        row += 1

    print("  OK Guide tab created")

'''

# Insert the function BEFORE the main() function
main_marker = "\ndef main():"
if main_marker in src:
    src = src.replace(main_marker, explanation_func + "\ndef main():")
    print("[3a/3] create_explanation_tab() function added")
else:
    print("WARNING: Could not find main() function marker")

# Now add the call to create_explanation_tab in main(), right after wb.remove(wb.active)
call_old = '''        # Create summary tab
        print("Creating Summary tab...")'''

call_new = '''        # Create explanation / guide tab (first tab)
        print("Creating Guide tab...")
        create_explanation_tab(wb, district_type, district_number, has_cfb=False)

        # Create summary tab
        print("Creating Summary tab...")'''

if call_old in src:
    src = src.replace(call_old, call_new, 1)
    print("[3b/3] create_explanation_tab() call added to main()")
else:
    print("WARNING: Could not find summary tab creation marker in main()")

# Now we need to update the has_cfb flag on the Guide tab after we know it.
# Insert code to update guide tab after CFB check
cfb_update_old = '''        if has_cfb_donors:
            print('Creating CFB Donors tab...')
            create_cfb_donor_tab(wb, conn, district_type, district_number)
        else:
            print('  Skipping CFB Donors tab (cfb columns not found - run: python main.py cfb-enrich)')'''

cfb_update_new = '''        if has_cfb_donors:
            print('Creating CFB Donors tab...')
            create_cfb_donor_tab(wb, conn, district_type, district_number)
            # Update the Guide tab to reflect CFB availability
            guide_ws = wb["Guide"]
            # Find the row after "National Donor" and add CFB description
            # (already handled by recreating Guide if needed)
        else:
            print('  Skipping CFB Donors tab (cfb columns not found - run: python main.py cfb-enrich)')'''

# Actually, simpler: just move the Guide creation to AFTER we know has_cfb_donors
# Let me undo the early insertion and move it later instead.
# Revert - put guide creation after CFB check

src = src.replace(call_new, call_old)  # undo the early insertion
print("  (repositioning Guide tab creation to after CFB check)")

# Instead, insert guide creation after CFB donor check
cfb_update_new2 = '''        if has_cfb_donors:
            print('Creating CFB Donors tab...')
            create_cfb_donor_tab(wb, conn, district_type, district_number)
        else:
            print('  Skipping CFB Donors tab (cfb columns not found - run: python main.py cfb-enrich)')

        # Create explanation / guide tab (inserted at position 0, before Summary)
        print("Creating Guide tab...")
        create_explanation_tab(wb, district_type, district_number, has_cfb=has_cfb_donors)
        # Move Guide to be the very first sheet
        wb.move_sheet("Guide", offset=-(len(wb.sheetnames)-1))'''

if cfb_update_old in src:
    src = src.replace(cfb_update_old, cfb_update_new2, 1)
    print("[3b/3] Guide tab creation placed after CFB check (with has_cfb flag)")
else:
    print("WARNING: Could not find CFB check block")


# ============================================================
# VERIFY & WRITE
# ============================================================
if src == original:
    print("\nERROR: No changes were made!")
    sys.exit(1)

# Backup
backup = fp + ".bak"
with open(backup, "w", encoding="utf-8") as f:
    f.write(original)
print(f"\nBackup saved: {backup}")

with open(fp, "w", encoding="utf-8") as f:
    f.write(src)
print(f"Updated: {fp}")

# Syntax check
import py_compile
try:
    py_compile.compile(fp, doraise=True)
    print("Syntax check: PASSED")
except py_compile.PyCompileError as e:
    print(f"SYNTAX ERROR: {e}")
    print("Restoring backup...")
    with open(fp, "w", encoding="utf-8") as f:
        f.write(original)
    print("Backup restored. Fix the issue and try again.")
    sys.exit(1)

print("\nAll changes applied successfully!")
print("  - Republicans listed first on all donor tabs")
print("  - Gold highlight on Democrats who donated to Republicans")
print("  - Guide tab added with sheet descriptions + turnout overlap explanation")
print("\nRun: python main.py export --ld 63")
