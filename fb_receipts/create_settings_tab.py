"""One-time script to create the Settings tab in the Google Sheet."""
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os

load_dotenv()

creds = Credentials.from_service_account_file(
    os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))

# Create Settings worksheet
try:
    ws = sheet.add_worksheet(title="Settings", rows=20, cols=2)
    print("Created 'Settings' tab.")
except gspread.exceptions.APIError as e:
    if "already exists" in str(e):
        ws = sheet.worksheet("Settings")
        print("'Settings' tab already exists — updating.")
    else:
        raise

rows = [
    ["setting", "value"],
    ["schedule_time", "09:00"],
    ["notify_email", "George@politikanyc.com"],
    ["default_schedule", "weekly_friday"],
]
ws.update("A1", rows)
print("Settings tab populated:")
for r in rows[1:]:
    print(f"  {r[0]:<20} {r[1]}")
