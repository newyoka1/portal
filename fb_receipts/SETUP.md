# Facebook Ads Receipt Automation — Setup Guide

## What This Does

Automatically pulls Facebook Ads billing data for all your clients and emails each client their receipt as a PDF attachment.

**Flow:**
1. Reads your client list (name, ad account ID, email) from a Google Sheet
2. Fetches spend data and billing PDFs from the Meta Marketing API for each client
3. Emails each client their receipt with the PDF attached

**Scripts you interact with:**

| Script | What it does |
|---|---|
| `python main.py` | Main script — fetch receipts and send emails |
| `python populate_sheet.py` | One-time helper — auto-fills your Google Sheet with all ad accounts from Meta |

**`src/` folder** — internal modules, you don't run these directly:

| File | Role |
|---|---|
| `config.py` | Loads settings from `.env` |
| `orchestrator.py` | Coordinates the full workflow |
| `meta_client.py` | Fetches spend data and ad images from Meta Graph API |
| `sheets_client.py` | Reads client mappings from Google Sheets |
| `email_service.py` | Sends emails via Gmail |
| `facebook_downloader.py` | Downloads real Facebook billing PDFs using a browser session |
| `pdf_generator.py` | Generates a fallback PDF from spend data when Facebook PDFs aren't available |

---

## Prerequisites
- Python 3.11+
- A Meta Business Manager account with ad accounts
- A Google Cloud project with Sheets API and Gmail API enabled
- A Gmail account (or Google Workspace)

---

## Step 1: Install Dependencies

```bash
cd facebook-receipt-automation
python -m venv .venv
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # Mac / Linux
pip install -r requirements.txt
playwright install chromium  # required for Facebook PDF download
```

---

## Step 2: Meta (Facebook) API Setup

1. Go to [Meta for Developers](https://developers.facebook.com/) and create an App (type: Business)
2. Add the **Marketing API** product to your app
3. In [Business Settings](https://business.facebook.com/settings/):
   - Go to **Users > System Users**
   - Create a System User (Admin role)
   - Click **Generate Token** — select these permissions:
     - `ads_read`
     - `business_management`
     - `read_insights`
   - Copy the token
4. Find your **Business Manager ID**: Business Settings > Business Info > Business Manager ID
5. If you have multiple businesses, note all their IDs

---

## Step 3: Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use existing)
3. Enable these APIs:
   - Google Sheets API
   - Gmail API
4. Create a **Service Account**:
   - IAM & Admin > Service Accounts > Create
   - Download the JSON key file
   - Save it as `credentials/service_account.json`
5. **For Gmail sending**, the simplest approach is a **Gmail App Password**:
   - Go to [Google Account > Security](https://myaccount.google.com/security)
   - Enable 2-Step Verification (required)
   - Go to App Passwords > Generate one for "Mail"
   - Copy the 16-character password

---

## Step 4: Google Sheet Setup

1. Create a new Google Sheet
2. Set up the first worksheet with these exact column headers in row 1:

| client_name | ad_account_id | email | active |
|---|---|---|---|
| Acme Corp | 123456789 | billing@acme.com | yes |
| Big Widget | 987654321 | finance@bigwidget.io | yes |

   - `ad_account_id`: numeric ID only, without the `act_` prefix
   - `email`: one address, or multiple comma-separated (e.g. `a@co.com, b@co.com`)
   - `active`: `yes` to include, `no` to skip

   **Shortcut:** After completing Steps 2 and 5, run this to auto-populate the sheet with all your ad accounts (all set to `active=no` — flip to `yes` for each client you want to send to):
   ```bash
   python populate_sheet.py
   ```

3. **Share the sheet** with your service account email (found in the JSON key file, looks like `name@project.iam.gserviceaccount.com`) — give it **Editor** access
4. Copy the Sheet ID from the URL: `docs.google.com/spreadsheets/d/{SHEET_ID}/edit`

---

## Step 5: Configure .env

```bash
cp .env.example .env
```

Fill in your values:
- `META_ACCESS_TOKEN` — System User token from Step 2
- `META_BUSINESS_IDS` — comma-separated Business Manager IDs
- `GOOGLE_SERVICE_ACCOUNT_FILE` — path to your JSON key (default: `credentials/service_account.json`)
- `GOOGLE_SHEET_ID` — from Step 4
- `GMAIL_SENDER_EMAIL` — your Gmail address
- `GMAIL_APP_PASSWORD` — from Step 3

---

## Step 6: Log into Facebook (first run only)

The tool downloads real Facebook billing PDFs using a saved browser session. Before the first run:

```bash
python main.py --login
```

A browser window will open. Log in to Facebook, then press ENTER in the terminal. Your session is saved to `fb_session.json` and reused on all future runs.

> If you skip this step, the tool will still work — it falls back to generating a spend-summary PDF from the Meta API instead of attaching the real Facebook receipt.

---

## Usage

```bash
# Verify API access — list all ad accounts
python main.py --list-accounts

# Dry run — fetch receipts and log, but don't send emails
python main.py --dry-run

# Run once — fetch and email receipts for the last 35 days
python main.py

# Run for the period since the last successful send
python main.py --since-last-send

# Custom date range
python main.py --start-date 2026-02-01 --end-date 2026-02-28

# Skip real Facebook PDF download (use generated PDFs instead)
python main.py --no-fb-pdfs

# Show when the script last ran and what period it covered
python main.py --last-run

# Start the auto-scheduler (runs in foreground — use a process manager or cron for production)
python main.py --schedule
```

---

## Troubleshooting

- **"Invalid OAuth access token"**: Your Meta token may have expired. System User tokens should be long-lived, but regenerate one in Business Settings > System Users if needed.
- **"Insufficient permission"**: Make sure the System User has access to the ad accounts and the token includes `ads_read` and `business_management` permissions.
- **Gmail "Less secure apps" error**: Use an App Password (Step 3), not your regular Gmail password.
- **Google Sheet "not found"**: Make sure you shared the sheet with the service account email and gave it Editor access.
- **Facebook PDFs not downloading**: Re-run `python main.py --login` — your session may have expired. The tool will fall back to generated PDFs automatically.
- **No receipts found**: Run `python main.py --list-accounts` first to confirm API access, then check that the ad account IDs in the sheet match what's listed.
