#!/usr/bin/env python3
"""
Update export.py to show detailed FEC party breakdown
"""

EXPORT_PY = r"D:\git\nys-voter-pipeline\export\export.py"
BACKUP_PY = r"D:\git\nys-voter-pipeline\export\export.py.bak2"

# Read current export.py
with open(EXPORT_PY, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the create_national_donor_tab function
start_idx = None
end_idx = None

for i, line in enumerate(lines):
    if 'def create_national_donor_tab(' in line:
        start_idx = i
    if start_idx is not None and i > start_idx and (line.startswith('def ') and 'create_national_donor_tab' not in line):
        end_idx = i
        break

if start_idx is None:
    print("ERROR: Could not find create_national_donor_tab function")
    exit(1)

if end_idx is None:
    end_idx = len(lines)

print(f"Found create_national_donor_tab at lines {start_idx+1} to {end_idx}")

# Create backup
import shutil
shutil.copy(EXPORT_PY, BACKUP_PY)
print(f"✓ Backup created: {BACKUP_PY}")

# New enhanced function
new_function = '''def create_national_donor_tab(wb, conn, district_type, district_number):
    """National (FEC) donor tab with detailed party contribution breakdown."""
    col = {"LD": "LDName", "SD": "SDName", "CD": "CDName"}[district_type]
    ws = wb.create_sheet("National Donor")

    hdr_font = Font(bold=True, color="FFFFFF", size=11)
    hdr_fill = PatternFill(start_color="203864", end_color="203864", fill_type="solid")
    sec_font = Font(bold=True, size=12, color="1F497D")
    sec_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    num_fmt  = "#,##0"
    amt_fmt  = "$#,##0.00"
    pct_fmt  = "0.0%"

    ws["A1"] = f"{district_type} {district_number} - National Donor Analysis (FEC)"
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells("A1:M1")
    ws["A2"] = f"Source: FEC Federal Contributions (2020-2024) | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ws["A2"].font = Font(italic=True, size=10, color="666666")
    ws.merge_cells("A2:M2")

    row = 4
    ws.cell(row=row, column=1, value="SUMMARY - Contributions by Party Signal").font = sec_font
    for ci in range(1, 9): ws.cell(row=row, column=ci).fill = sec_fill
    row += 1

    # Enhanced summary with party breakdown
    sum_hdrs = ["Party Signal", "Donors", "% of District", "Total $", "Avg $", "Contributions", "Avg/Donor"]
    for ci, h in enumerate(sum_hdrs, 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font = hdr_font; c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center")
    row += 1

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM voter_file WHERE {col}=%s", (district_number,))
        total_voters = cur.fetchone()[0]
        
        # Get totals for each party signal
        for party_label, amt_col, cnt_col in [
            ("Democratic", "fec_democratic_amount", "fec_democratic_count"),
            ("Republican", "fec_republican_amount", "fec_republican_count"),
            ("Independent", "fec_independent_amount", "fec_independent_count"),
            ("Unknown", "fec_unknown_amount", "fec_unknown_count")
        ]:
            cur.execute(f"""
                SELECT 
                    COUNT(DISTINCT StateVoterId),
                    COALESCE(SUM({amt_col}), 0),
                    COALESCE(AVG({amt_col}), 0),
                    COALESCE(SUM({cnt_col}), 0)
                FROM voter_file 
                WHERE {col}=%s AND {cnt_col} > 0
            """, (district_number,))
            r = cur.fetchone()
            donors = int(r[0] or 0)
            total = float(r[1] or 0)
            avg_amt = float(r[2] or 0)
            contributions = int(r[3] or 0)
            
            vals = [
                party_label,
                donors,
                donors/total_voters if total_voters else 0,
                total,
                avg_amt,
                contributions,
                contributions/donors if donors else 0
            ]
            for ci, v in enumerate(vals, 1):
                cell = ws.cell(row=row, column=ci, value=v)
                if ci == 2: cell.number_format = num_fmt
                if ci == 3: cell.number_format = pct_fmt
                if ci in (4, 5): cell.number_format = amt_fmt
                if ci in (6, 7): cell.number_format = num_fmt
            row += 1
        
        # Overall total row
        cur.execute(f"""
            SELECT 
                COUNT(DISTINCT StateVoterId),
                COALESCE(SUM(fec_total_amount), 0),
                COALESCE(AVG(fec_total_amount), 0),
                COALESCE(SUM(fec_total_count), 0)
            FROM voter_file
            WHERE {col}=%s AND is_fec_donor = TRUE
        """, (district_number,))
        r = cur.fetchone()
        all_donors = int(r[0] or 0)
        all_total = float(r[1] or 0)
        all_avg = float(r[2] or 0)
        all_contribs = int(r[3] or 0)
        
        ws.cell(row=row, column=1, value="TOTAL (all parties)").font = Font(bold=True)
        vals = [
            all_donors,
            all_donors/total_voters if total_voters else 0,
            all_total,
            all_avg,
            all_contribs,
            all_contribs/all_donors if all_donors else 0
        ]
        for ci, v in enumerate(vals, 2):
            cell = ws.cell(row=row, column=ci, value=v)
            cell.font = Font(bold=True)
            if ci == 2: cell.number_format = num_fmt
            if ci == 3: cell.number_format = pct_fmt
            if ci in (4, 5): cell.number_format = amt_fmt
            if ci in (6, 7): cell.number_format = num_fmt
    
    row += 2

    # Donor list with party breakdown columns
    ws.cell(row=row, column=1, value="DONOR LIST - Detailed Contribution Breakdown").font = sec_font
    for ci in range(1, 20): ws.cell(row=row, column=ci).fill = sec_fill
    row += 1

    list_hdrs = ["StateVoterId", "FirstName", "LastName", "Address", "City", "ZIP",
                 "Email", "Phone", "Reg Party",
                 "Dem $", "Dem #", "Rep $", "Rep #", "Ind $", "Ind #",
                 "Total $", "Total #", "LD", "SD"]
    for ci, h in enumerate(list_hdrs, 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font = hdr_font; c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center")
    row += 1

    D_row_fill = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")
    R_row_fill = PatternFill(start_color="FFB3B3", end_color="FFB3B3", fill_type="solid")

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT StateVoterId, FirstName, LastName,
                   PrimaryAddress1, PrimaryCity, PrimaryZip,
                   boe_email, PrimaryPhone, OfficialParty,
                   COALESCE(fec_democratic_amount, 0), COALESCE(fec_democratic_count, 0),
                   COALESCE(fec_republican_amount, 0), COALESCE(fec_republican_count, 0),
                   COALESCE(fec_independent_amount, 0), COALESCE(fec_independent_count, 0),
                   COALESCE(fec_total_amount, 0), COALESCE(fec_total_count, 0),
                   LDName, SDName
            FROM voter_file
            WHERE {col}=%s AND is_fec_donor = TRUE
            ORDER BY OfficialParty, fec_total_amount DESC
            LIMIT 200000
        """, (district_number,))
        donors = cur.fetchall()

    PARTY_ORDER = ["Democrat", "Republican", "Conservative"]
    def party_sort_key(rec):
        p = rec[8] or ""  # OfficialParty is now at index 8
        try: return (PARTY_ORDER.index(p), 0)
        except ValueError: return (len(PARTY_ORDER), 0)
    donors = sorted(donors, key=party_sort_key)

    sec_hdr_font = Font(bold=True, size=11, color="FFFFFF")
    D_hdr_fill   = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    R_hdr_fill   = PatternFill(start_color="C00000", end_color="C00000", fill_type="solid")
    CON_hdr_fill = PatternFill(start_color="7030A0", end_color="7030A0", fill_type="solid")
    OTH_hdr_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
    CON_row_fill = PatternFill(start_color="E2CFED", end_color="E2CFED", fill_type="solid")

    # Group donors by party with subtotals
    from itertools import groupby
    for party, group_iter in groupby(donors, key=lambda r: r[8] or 'Other'):
        group_rows = list(group_iter)
        if party == 'Democrat':       hfill = D_hdr_fill;   row_fill = D_row_fill
        elif party == 'Republican':   hfill = R_hdr_fill;   row_fill = R_row_fill
        elif party == 'Conservative': hfill = CON_hdr_fill; row_fill = CON_row_fill
        else:                         hfill = OTH_hdr_fill; row_fill = None

        # Party header row
        lbl = ws.cell(row=row, column=1, value=f'--- {party.upper()} ---')
        lbl.font = sec_hdr_font; lbl.fill = hfill
        for ci in range(2, 20): ws.cell(row=row, column=ci).fill = hfill
        row += 1

        grp_dem_amt = grp_rep_amt = grp_ind_amt = grp_total_amt = 0.0
        grp_dem_cnt = grp_rep_cnt = grp_ind_cnt = grp_total_cnt = 0
        
        for r_data in group_rows:
            for ci, v in enumerate(r_data, 1):
                cell = ws.cell(row=row, column=ci, value=v)
                if row_fill: cell.fill = row_fill
                if ci in (10, 12, 14, 16): cell.number_format = amt_fmt  # Amount columns
                if ci in (11, 13, 15, 17): cell.number_format = num_fmt  # Count columns
            
            if party == 'Republican':
                for ci in range(1, 20):
                    ws.cell(row=row, column=ci).font = Font(bold=True, size=11)
            
            grp_dem_amt += float(r_data[9] or 0)
            grp_dem_cnt += int(r_data[10] or 0)
            grp_rep_amt += float(r_data[11] or 0)
            grp_rep_cnt += int(r_data[12] or 0)
            grp_ind_amt += float(r_data[13] or 0)
            grp_ind_cnt += int(r_data[14] or 0)
            grp_total_amt += float(r_data[15] or 0)
            grp_total_cnt += int(r_data[16] or 0)
            row += 1

        # Subtotal row
        sub_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')
        sub_font = Font(bold=True, italic=True)
        ws.cell(row=row, column=1, value=f'  {party} SUBTOTAL ({len(group_rows):,} donors)').font = sub_font
        ws.cell(row=row, column=1).fill = sub_fill
        
        # Subtotal values
        subtotal_vals = [
            (10, grp_dem_amt, amt_fmt), (11, grp_dem_cnt, num_fmt),
            (12, grp_rep_amt, amt_fmt), (13, grp_rep_cnt, num_fmt),
            (14, grp_ind_amt, amt_fmt), (15, grp_ind_cnt, num_fmt),
            (16, grp_total_amt, amt_fmt), (17, grp_total_cnt, num_fmt)
        ]
        for col_idx, val, fmt in subtotal_vals:
            cell = ws.cell(row=row, column=col_idx, value=val)
            cell.number_format = fmt
            cell.font = sub_font
            cell.fill = sub_fill
        
        # Fill other cells in subtotal row
        for ci in [2,3,4,5,6,7,8,9,18,19]:
            ws.cell(row=row, column=ci).fill = sub_fill
        row += 2

    auto_adjust_columns(ws)
    print(f"  OK National tab: {len(donors):,} FEC donors with party breakdown")

'''

# Replace the function
new_lines = lines[:start_idx] + [new_function] + lines[end_idx:]

# Write updated file
with open(EXPORT_PY, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("✓ export.py updated successfully")
print("\nChanges:")
print("  - Enhanced summary section showing D/R/I/Unknown breakdowns")
print("  - Added Dem $, Dem #, Rep $, Rep #, Ind $, Ind # columns to donor list")
print("  - Shows total contributions and average per donor")
print("\nBackup saved to: export.py.bak2")
