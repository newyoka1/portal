# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Facebook Ads Receipt Automation is a Python tool that fetches Meta Ads billing data and emails each client their monthly receipt as a PDF. It reads a client list (name, ad account ID, email) from a Google Sheet, downloads real billing PDFs via a saved Playwright browser session, and falls back to generating a spend-summary PDF via the Meta API if the Facebook PDF is unavailable.

## Commands

```bash
# Install dependencies
python -m venv .venv
source .venv/bin/activate          # Mac/Linux
.venv\Scripts\activate             # Windows
pip install -r requirements.txt
playwright install chromium        # required for Facebook PDF download

# First-time Facebook login (saves session to fb_session.json)
python main.py --login

# Verify API access — list all ad accounts
python main.py --list-accounts

# Dry run — fetch receipts but do not send emails
python main.py --dry-run

# Run once — fetch and send receipts for the last 35 days
python main.py

# Run for the period since the last successful send
python main.py --since-last-send

# Custom date range
python main.py --start-date 2026-02-01 --end-date 2026-02-28

# Skip real Facebook PDF download (use generated PDFs instead)
python main.py --no-fb-pdfs

# Show last run info
python main.py --last-run

# Start the auto-scheduler (runs in foreground)
python main.py --schedule

# One-time helper: auto-fill Google Sheet with all ad accounts from Meta
python populate_sheet.py
```

There is no formal test or lint suite.

## Architecture

### Entry Point

`main.py` — parses CLI args and delegates to `src/orchestrator.py`.

### Module Roles (`src/`)

| File | Role |
|---|---|
| `config.py` | Loads settings from `.env` |
| `orchestrator.py` | Coordinates the full workflow |
| `meta_client.py` | Fetches spend data and ad images from Meta Graph API |
| `sheets_client.py` | Reads client mappings from Google Sheets |
| `email_service.py` | Sends emails via Gmail SMTP using App Password |
| `facebook_downloader.py` | Downloads real billing PDFs using a saved Playwright browser session |
| `pdf_generator.py` | Generates a fallback PDF from Meta API spend data |

### Data Flow

```
Google Sheet (client list: name, ad_account_id, email, active)
  → sheets_client.py reads active rows
  → meta_client.py fetches spend data per ad account
  → facebook_downloader.py downloads real Facebook billing PDF (Playwright)
      OR pdf_generator.py generates fallback spend-summary PDF
  → email_service.py emails each client their PDF
  → last_run.json updated with run timestamp and date range
```

### Key Files

- `fb_session.json` — Saved Playwright browser session (do not commit)
- `last_run.json` — Tracks the last successful send date range
- `receipts/` — Local PDF storage
- `credentials/` — Google service account JSON key (do not commit)

## Configuration

Copy `.env.example` to `.env` and fill in:

```
META_ACCESS_TOKEN=...              # Meta System User token (ads_read, business_management, read_insights)
META_BUSINESS_IDS=...              # Comma-separated Business Manager IDs
GOOGLE_SERVICE_ACCOUNT_FILE=credentials/service_account.json
GOOGLE_SHEET_ID=...                # From the Sheet URL
GMAIL_SENDER_EMAIL=...             # Gmail address
GMAIL_APP_PASSWORD=...             # 16-char Gmail App Password (not account password)
```

### Google Sheet Format

First worksheet, row 1 headers:

| client_name | ad_account_id | email | active |
|---|---|---|---|

- `ad_account_id`: numeric only, no `act_` prefix
- `email`: single address or comma-separated
- `active`: `yes` to include, `no` to skip
