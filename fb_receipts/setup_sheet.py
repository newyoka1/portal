"""
Sets up Google Sheet layout:
- Settings rows at top of Sheet 1 (rows 1-4)
- Blank separator row (row 5)
- Client table below (row 6+)
- Dropdowns on active, schedule, and settings value cells
- Deletes the separate Settings tab if it exists
"""
from dotenv import load_dotenv
load_dotenv()

import os
import gspread
from google.oauth2.service_account import Credentials

creds = Credentials.from_service_account_file(
    os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
ws1 = sheet.sheet1
ws1_id = ws1.id

SCHEDULE_OPTIONS = [
    "weekly_monday", "weekly_tuesday", "weekly_wednesday",
    "weekly_thursday", "weekly_friday", "weekly_saturday", "weekly_sunday",
    "monthly_1", "monthly_7", "monthly_14", "monthly_15", "monthly_28",
]
TIME_OPTIONS = [f"{h:02d}:00" for h in range(6, 21)]  # 06:00 – 20:00
ACTIVE_OPTIONS = ["yes", "no"]

# ── Read existing data ─────────────────────────────────────────────────────────
existing = ws1.get_all_values()

# Preserve current settings values if already set
settings_values = {
    "admin_email": "",
    "notify_email": "George@politikanyc.com",
    "schedule_time": "09:00",
    "default_schedule": "weekly_friday",
}
for row in existing:
    key = row[0].strip().lower() if row else ""
    if key in settings_values and len(row) >= 2 and row[1].strip():
        settings_values[key] = row[1].strip()

# Find the client table rows (skip settings rows at top)
client_rows = []
for row in existing:
    first = row[0].strip().lower() if row else ""
    if first == "client_name":
        client_rows.append(row)
    elif client_rows:
        client_rows.append(row)

if not client_rows:
    client_rows = [["client_name", "ad_account_id", "email", "active", "schedule"]]

# Ensure client header has 'schedule' column
client_header = client_rows[0]
if "schedule" not in [h.lower() for h in client_header]:
    client_header.append("schedule")
    for row in client_rows[1:]:
        row.append("weekly_friday")

# ── Build new sheet layout ─────────────────────────────────────────────────────
# Row 1: admin_email
# Row 2: notify_email
# Row 3: schedule_time  [dropdown]
# Row 4: default_schedule  [dropdown]
# Row 5: blank separator
# Row 6: client headers
# Row 7+: client data  [active + schedule dropdowns]
new_data = [
    ["admin_email",      settings_values.get("admin_email", "")],
    ["notify_email",     settings_values.get("notify_email", "George@politikanyc.com")],
    ["schedule_time",    settings_values.get("schedule_time", "09:00")],
    ["default_schedule", settings_values.get("default_schedule", "weekly_friday")],
    [],  # blank separator
] + client_rows

max_cols = max(len(r) for r in new_data)
padded = [r + [""] * (max_cols - len(r)) for r in new_data]

ws1.clear()
# RAW preserves account IDs as strings (prevents scientific notation on large numbers)
ws1.update(values=padded, range_name="A1", value_input_option="RAW")
print(f"Wrote {len(new_data)} rows to Sheet 1 ({len(client_rows) - 1} clients).")

# ── Apply dropdown validations ─────────────────────────────────────────────────
def validation_request(sheet_id, col, row_start, row_end, options):
    return {
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_start,
                "endRowIndex": row_end,
                "startColumnIndex": col,
                "endColumnIndex": col + 1,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": v} for v in options],
                },
                "showCustomUi": True,
                "strict": False,
            },
        }
    }

def clear_validation(sheet_id, col):
    """Remove all validation from an entire column."""
    return {
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1000,
                "startColumnIndex": col,
                "endColumnIndex": col + 1,
            }
            # No "rule" key = clears all validation in range
        }
    }

# Client table: header at row index 5 (0-based) = row 6 in sheet
client_header_lower = [h.strip().lower() for h in client_rows[0]]
client_table_start = 5   # 0-based index of header row
data_row_start = client_table_start + 1
data_row_end   = client_table_start + len(client_rows)

requests = []

# Clear old validation from columns we'll set
for col_name in ("active", "schedule"):
    if col_name in client_header_lower:
        requests.append(clear_validation(ws1_id, client_header_lower.index(col_name)))
requests.append(clear_validation(ws1_id, 1))  # settings value column

# Settings dropdowns (column B, rows 3 and 4)
requests += [
    validation_request(ws1_id, 1, 2, 3, TIME_OPTIONS),      # row 3: schedule_time
    validation_request(ws1_id, 1, 3, 4, SCHEDULE_OPTIONS),  # row 4: default_schedule
]

# Client dropdowns
if "active" in client_header_lower:
    active_col = client_header_lower.index("active")
    requests.append(validation_request(ws1_id, active_col, data_row_start, data_row_end, ACTIVE_OPTIONS))

if "schedule" in client_header_lower:
    schedule_col = client_header_lower.index("schedule")
    requests.append(validation_request(ws1_id, schedule_col, data_row_start, data_row_end, SCHEDULE_OPTIONS))

sheet.batch_update({"requests": requests})
print(f"Applied {len(requests)} formatting/validation request(s).")

# ── Delete Settings tab if it exists ──────────────────────────────────────────
try:
    ws_s = sheet.worksheet("Settings")
    sheet.del_worksheet(ws_s)
    print("Deleted 'Settings' tab.")
except gspread.exceptions.WorksheetNotFound:
    pass

print("\nDone. Sheet layout:")
print("  Row 1: admin_email")
print("  Row 2: notify_email")
print("  Row 3: schedule_time  [dropdown]")
print("  Row 4: default_schedule  [dropdown]")
print("  Row 5: (blank separator)")
print("  Row 6: client table headers")
print("  Row 7+: client data  [active + schedule dropdowns]")
print("\nTo sync Windows Task Scheduler tasks: python main.py --sync-scheduler")
