"""
Entry point: run once or start the scheduler.

Usage:
    python main.py                          # Run once (last 35 days, with real FB PDFs)
    python main.py --since-last-send        # Run for period since last successful send
    python main.py --start-date 2026-03-01  # Explicit start date
    python main.py --end-date   2026-03-17  # Explicit end date (default: today)
    python main.py --dry-run                # Fetch + log but don't send emails
    python main.py --no-fb-pdfs             # Skip FB browser download, use generated PDFs
    python main.py --login                  # Save Facebook session (run before first use)
    python main.py --schedule               # Start weekly scheduler (runs in foreground)
    python main.py --list-accounts          # List all ad accounts across your businesses
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import schedule

from src.config import SCHEDULE_FREQUENCY, SCHEDULE_DAY, SCHEDULE_TIME
from src.orchestrator import Orchestrator
from src.meta_client import MetaClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("receipt_automation.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Last-run tracking ──────────────────────────────────────────────────────────
LAST_RUN_FILE = Path("last_run.json")


def _load_last_run() -> dict | None:
    """Return the saved last-run record, or None if it doesn't exist."""
    if LAST_RUN_FILE.exists():
        try:
            with open(LAST_RUN_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read %s: %s", LAST_RUN_FILE, e)
    return None


def _save_last_run(start_date: datetime, end_date: datetime) -> None:
    """Persist the start/end dates of the run that just completed."""
    record = {
        "last_run_at": datetime.now().isoformat(),
        "period_start": start_date.strftime("%Y-%m-%d"),
        "period_end": end_date.strftime("%Y-%m-%d"),
    }
    try:
        with open(LAST_RUN_FILE, "w") as f:
            json.dump(record, f, indent=2)
        logger.info("Saved run record to %s: %s to %s", LAST_RUN_FILE,
                    record["period_start"], record["period_end"])
    except Exception as e:
        logger.warning("Could not save last-run record: %s", e)


# ── Core runner ───────────────────────────────────────────────────────────────

def fb_login():
    """Open a browser for the user to log in and save the session."""
    from playwright.sync_api import sync_playwright
    from src.facebook_downloader import SESSION_FILE

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto("https://www.facebook.com/login")
        print("\n" + "=" * 60)
        print("  Log into Facebook in the browser window that just opened.")
        print("  Once fully logged in, come back here and press ENTER.")
        print("=" * 60 + "\n")
        input("Press ENTER after logging in > ")
        ctx.storage_state(path=str(SESSION_FILE))
        browser.close()
    print(f"Session saved to {SESSION_FILE}. You can now run the script normally.")


def run_once(
    dry_run: bool = False,
    use_fb_pdfs: bool = True,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    since_last_send: bool = False,
    save_run_record: bool = True,
):
    """
    Fetch receipts and email them to clients.

    Date-range logic (in priority order):
      1. Explicit --start-date / --end-date  (always respected if provided)
      2. --since-last-send                   (reads last_run.json)
      3. Default: last 35 days
    """
    # Resolve end date first
    if end_date is None:
        end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)

    # Resolve start date
    if start_date is None:
        if since_last_send:
            record = _load_last_run()
            if record:
                # Start the day after the last period ended
                last_end = datetime.strptime(record["period_end"], "%Y-%m-%d")
                start_date = last_end + timedelta(days=1)
                logger.info(
                    "Using --since-last-send: period %s → %s",
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                )
            else:
                logger.warning(
                    "No last-run record found — defaulting to last 35 days"
                )
                start_date = datetime.now() - timedelta(days=35)
        else:
            start_date = datetime.now() - timedelta(days=35)

    orch = Orchestrator()
    results = orch.run(
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
        use_fb_pdfs=use_fb_pdfs,
    )

    print(f"\nResults: {results}")

    # Persist run record after a real (non-dry) run that sent or found receipts
    if save_run_record and not dry_run:
        if results.get("sent", 0) > 0 or results.get("no_receipts", 0) > 0:
            _save_last_run(start_date, end_date)

    return results


def list_accounts():
    """List all ad accounts across all business managers."""
    client = MetaClient()
    accounts = client.get_all_ad_accounts()
    if not accounts:
        print("No ad accounts found. Check your META_ACCESS_TOKEN and META_BUSINESS_IDS.")
        return

    print(f"\nFound {len(accounts)} ad account(s):\n")
    print(f"{'Account ID':<20} {'Name':<35} {'Business':<25} {'Currency'}")
    print("-" * 100)
    for acc in accounts:
        print(
            f"{acc.get('account_id', 'N/A'):<20} "
            f"{acc.get('name', 'N/A'):<35} "
            f"{acc.get('business_name', 'N/A'):<25} "
            f"{acc.get('currency', 'N/A')}"
        )


def show_last_run():
    """Print the last-run record."""
    record = _load_last_run()
    if record:
        print(f"\nLast run:")
        print(f"  Ran at:        {record.get('last_run_at', 'N/A')}")
        print(f"  Period start:  {record.get('period_start', 'N/A')}")
        print(f"  Period end:    {record.get('period_end', 'N/A')}")
    else:
        print("No last-run record found (last_run.json does not exist yet).")


# ── Scheduler ─────────────────────────────────────────────────────────────────

def start_scheduler():
    """Start the background scheduler (uses --since-last-send logic each run)."""
    logger.info(
        "Starting scheduler: %s on day '%s' at %s",
        SCHEDULE_FREQUENCY, SCHEDULE_DAY, SCHEDULE_TIME,
    )

    def scheduled_run():
        logger.info("Scheduler triggered — running with --since-last-send logic")
        run_once(since_last_send=True)

    if SCHEDULE_FREQUENCY == "daily":
        schedule.every().day.at(SCHEDULE_TIME).do(scheduled_run)
    elif SCHEDULE_FREQUENCY == "weekly":
        getattr(schedule.every(), SCHEDULE_DAY.lower()).at(SCHEDULE_TIME).do(scheduled_run)
    elif SCHEDULE_FREQUENCY == "monthly":
        def monthly_check():
            if datetime.now().day == int(SCHEDULE_DAY):
                scheduled_run()
        schedule.every().day.at(SCHEDULE_TIME).do(monthly_check)
    else:
        logger.error("Unknown schedule frequency: %s", SCHEDULE_FREQUENCY)
        return

    print(f"Scheduler running ({SCHEDULE_FREQUENCY}). Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Facebook Ads Receipt Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          Run for last 35 days
  python main.py --since-last-send        Run for period since last successful send
  python main.py --start-date 2026-03-01  Run from March 1 to today
  python main.py --start-date 2026-03-01 --end-date 2026-03-15
  python main.py --dry-run --since-last-send
  python main.py --schedule               Start weekly auto-sender
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch receipts but don't send emails (also skips saving last_run.json)",
    )
    parser.add_argument(
        "--since-last-send",
        action="store_true",
        help="Use period since last successful send (reads last_run.json)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="Explicit start date for receipt period",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="Explicit end date for receipt period (default: today)",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Start the scheduler (runs in foreground, uses --since-last-send each time)",
    )
    parser.add_argument(
        "--list-accounts",
        action="store_true",
        help="List all ad accounts across your businesses",
    )
    parser.add_argument(
        "--last-run",
        action="store_true",
        help="Show the last-run record (last_run.json)",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Open browser to log into Facebook and save session",
    )
    parser.add_argument(
        "--no-fb-pdfs",
        action="store_true",
        help="Skip real Facebook PDF download; use generated PDFs instead",
    )
    args = parser.parse_args()

    # Parse explicit dates
    start_date: datetime | None = None
    end_date: datetime | None = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            parser.error(f"Invalid --start-date '{args.start_date}' — use YYYY-MM-DD format")

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            parser.error(f"Invalid --end-date '{args.end_date}' — use YYYY-MM-DD format")

    if start_date and end_date and start_date > end_date:
        parser.error("--start-date must be before --end-date")

    # Dispatch
    if args.login:
        fb_login()
    elif args.list_accounts:
        list_accounts()
    elif args.last_run:
        show_last_run()
    elif args.schedule:
        start_scheduler()
    else:
        run_once(
            dry_run=args.dry_run,
            use_fb_pdfs=not args.no_fb_pdfs,
            start_date=start_date,
            end_date=end_date,
            since_last_send=args.since_last_send,
        )


if __name__ == "__main__":
    main()
