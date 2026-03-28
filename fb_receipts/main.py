"""
Entry point: run once or start the scheduler.

Usage:
    python main.py                          # Run once (last 7 days)
    python main.py --since-last-send        # Run for period since last successful send
    python main.py --start-date 2026-03-01  # Explicit start date
    python main.py --end-date   2026-03-17  # Explicit end date (default: today)
    python main.py --dry-run                # Fetch + log but don't send emails
    python main.py --resend                 # Resend even if already sent for this period
    python main.py --schedule               # Start weekly scheduler (runs in foreground)
    python main.py --list-accounts          # List all ad accounts across your businesses
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import schedule

from src.config import SCHEDULE_FREQUENCY, SCHEDULE_DAY, SCHEDULE_TIME
from src.orchestrator import Orchestrator
from src.meta_client import MetaClient
from src.activity_logger import ActivityRun, show_history

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

def run_once(
    dry_run: bool = False,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    since_last_send: bool = False,
    save_run_record: bool = True,
    resend: bool = False,
    account_id: str | None = None,
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
                start_date = datetime.now() - timedelta(days=7)
        else:
            start_date = datetime.now() - timedelta(days=7)

    activity = ActivityRun(start_date, end_date, dry_run=dry_run)

    orch = Orchestrator()
    results = orch.run(
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
        resend=resend,
        account_id=account_id,
        activity=activity,
        manual=True,  # bypass schedule/sent-log checks for manual runs
    )

    print(f"\nResults: {results}")

    activity.finish(results)
    activity.save()

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


# ── Windows Task Scheduler ────────────────────────────────────────────────────

TASK_NAME = "FacebookReceiptAutomation"

DAY_MAP = {
    "monday": "MON", "tuesday": "TUE", "wednesday": "WED",
    "thursday": "THU", "friday": "FRI", "saturday": "SAT", "sunday": "SUN",
}


def setup_windows_scheduler():
    """Register a Windows Task Scheduler job to run daily."""
    project_dir = Path(__file__).resolve().parent
    python_exe = Path(sys.executable).resolve()
    main_script = project_dir / "main.py"
    log_file = project_dir / "receipt_automation.log"

    # Read schedule_time from DB settings if available, fall back to .env
    time_str = SCHEDULE_TIME
    try:
        from src.db_client import DbClient
        db_settings = DbClient().get_settings()
        if db_settings.get("schedule_time"):
            time_str = db_settings["schedule_time"]
            print(f"Using schedule_time from DB: {time_str}")
    except Exception as e:
        print(f"Could not read DB settings ({e}) — using .env value: {time_str}")

    # cmd wrapper so we can set the working directory
    command = (
        f'cmd /c "cd /d {project_dir} && '
        f'"{python_exe}" "{main_script}" --since-last-send '
        f'>> "{log_file}" 2>&1"'
    )

    result = subprocess.run(
        [
            "schtasks", "/create",
            "/tn", TASK_NAME,
            "/tr", command,
            "/sc", "daily",
            "/st", time_str,
            "/f",  # overwrite if exists
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print(f"Task '{TASK_NAME}' created — runs daily at {time_str}.")
        print("Each client's schedule (weekly_friday, monthly_1, etc.) controls when they actually get sent.")
        print(f"  Python:  {python_exe}")
        print(f"  Script:  {main_script}")
        print(f"  Log:     {log_file}")
        print("\nTo verify: open Task Scheduler and look for 'FacebookReceiptAutomation'")
    else:
        print(f"Failed to create task:\n{result.stderr or result.stdout}")


def remove_windows_scheduler():
    """Remove the Windows Task Scheduler job."""
    result = subprocess.run(
        ["schtasks", "/delete", "/tn", TASK_NAME, "/f"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"Task '{TASK_NAME}' removed.")
    else:
        print(f"Could not remove task (may not exist):\n{result.stderr or result.stdout}")


def setup_local_server():
    """
    Add a .vbs launcher to the Windows Startup folder so the local HTTP server
    starts silently at every login — no admin required.
    Also starts the server immediately in the background.
    """
    import os

    project_dir   = Path(__file__).resolve().parent
    # Use pythonw.exe (windowless) so no console appears at startup
    pythonw_exe   = Path(sys.executable).parent / "pythonw.exe"
    if not pythonw_exe.exists():
        pythonw_exe = Path(sys.executable)   # fallback to python.exe
    server_script = project_dir / "scheduler_server.py"

    # Windows Startup folder — runs for current user, no admin needed
    startup_folder = Path(os.environ["APPDATA"]) / \
        "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    vbs_path = startup_folder / "FacebookReceiptServer.vbs"

    vbs_content = (
        'Set oShell = CreateObject("WScript.Shell")\n'
        f'oShell.Run "{pythonw_exe} {server_script}", 0, False\n'
    )
    vbs_path.write_text(vbs_content, encoding="utf-8")
    print(f"Startup launcher created: {vbs_path}")
    print("The server will start automatically at every Windows login (no admin needed).")

    # Start immediately — hidden, no console window
    subprocess.Popen(
        [str(pythonw_exe), str(server_script)],
        cwd=str(project_dir),
        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW,
    )
    print("Server started now at http://localhost:5050")
    print("The Sync Scheduler task is ready to use.")


def register_protocol():
    """
    Register the fbreceipt:// custom URL protocol in Windows Registry.
    After this, clicking fbreceipt://sync-scheduler in any browser will run
    sync_scheduler.bat in the project folder.
    """
    import winreg

    project_dir = Path(__file__).resolve().parent
    bat_file = project_dir / "sync_scheduler.bat"

    # Write the batch file
    bat_file.write_text(
        f'@echo off\n'
        f'cd /d "{project_dir}"\n'
        f'call ".venv\\Scripts\\activate.bat"\n'
        f'python main.py --sync-scheduler\n'
        f'pause\n'
    )

    cmd = f'"{bat_file}" "%1"'

    key_path = r"Software\Classes\fbreceipt"
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, key_path) as key:
        winreg.SetValueEx(key, "", 0, winreg.REG_SZ, "URL:FB Receipt Automation")
        winreg.SetValueEx(key, "URL Protocol", 0, winreg.REG_SZ, "")

    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, key_path + r"\shell\open\command") as key:
        winreg.SetValueEx(key, "", 0, winreg.REG_SZ, cmd)

    print(f"Registered fbreceipt:// protocol.")
    print(f"Batch file: {bat_file}")
    print(f"\nThe Sync Scheduler is configured and ready.")
    print("If prompted by the browser, click 'Open' to allow it.")


def _make_client_task_name(account_id: str) -> str:
    return f"FacebookReceipt_{account_id}"


def _schtasks_schedule_args(schedule_str: str) -> list[str]:
    """Convert a schedule string to schtasks /sc and /d arguments."""
    s = schedule_str.strip().lower()
    day_map = {
        "monday": "MON", "tuesday": "TUE", "wednesday": "WED",
        "thursday": "THU", "friday": "FRI", "saturday": "SAT", "sunday": "SUN",
    }
    if s.startswith("weekly_"):
        day = day_map.get(s[len("weekly_"):], "FRI")
        return ["/sc", "WEEKLY", "/d", day]
    if s.startswith("monthly_"):
        day_num = s[len("monthly_"):]
        return ["/sc", "MONTHLY", "/d", day_num]
    return ["/sc", "WEEKLY", "/d", "FRI"]  # default


def sync_windows_scheduler():
    """
    Create/update a Task Scheduler task for each active client based on their
    schedule setting, and delete tasks for clients that are no longer active.
    """
    from src.db_client import DbClient

    project_dir = Path(__file__).resolve().parent
    python_exe  = Path(sys.executable).resolve()
    main_script = project_dir / "main.py"
    log_file    = project_dir / "receipt_automation.log"

    db = DbClient()
    settings = db.get_settings()
    time_str = settings.get("schedule_time") or SCHEDULE_TIME

    clients = db.get_client_mappings()
    active_ids = {c["ad_account_id"] for c in clients}

    # ── Find existing FacebookReceipt_* tasks ──────────────────────────────────
    query = subprocess.run(
        ["schtasks", "/query", "/fo", "csv", "/nh"],
        capture_output=True, text=True,
    )
    existing_tasks = set()
    for line in query.stdout.splitlines():
        parts = line.strip('"').split('","')
        if parts and parts[0].startswith("FacebookReceipt_"):
            existing_tasks.add(parts[0])

    # ── Write a small launcher .bat to stay under schtasks 261-char /tr limit ───
    launcher = project_dir / "run_client.bat"
    launcher.write_text(
        f'@echo off\r\n'
        f'cd /d "{project_dir}"\r\n'
        f'call ".venv\\Scripts\\activate.bat"\r\n'
        f'python main.py --account-id %1 --since-last-send >> "{log_file}" 2>&1\r\n',
        encoding="utf-8",
    )

    created = updated = deleted = 0

    # ── Create / update one task per active client ─────────────────────────────
    for client in clients:
        account_id    = client["ad_account_id"]
        client_name   = client["client_name"]
        client_sched  = client.get("schedule") or "weekly_friday"
        task_name     = _make_client_task_name(account_id)

        command = f'"{launcher}" {account_id}'

        sched_args = _schtasks_schedule_args(client_sched)
        result = subprocess.run(
            ["schtasks", "/create", "/tn", task_name, "/tr", command,
             *sched_args, "/st", time_str, "/f"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            if task_name in existing_tasks:
                updated += 1
                print(f"  Updated: {task_name}  ({client_sched} @ {time_str})")
            else:
                created += 1
                print(f"  Created: {task_name}  ({client_sched} @ {time_str})")
        else:
            print(f"  FAILED:  {task_name} — {result.stderr.strip()}")

    # ── Delete tasks for accounts no longer active ─────────────────────────────
    for task_name in existing_tasks:
        account_id = task_name[len("FacebookReceipt_"):]
        if account_id not in active_ids:
            result = subprocess.run(
                ["schtasks", "/delete", "/tn", task_name, "/f"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                deleted += 1
                print(f"  Deleted: {task_name}  (no longer active)")

    print(f"\nSync complete — {created} created, {updated} updated, {deleted} deleted.")


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
        "--resend",
        action="store_true",
        help="Ignore sent_log.json and resend receipts even if already sent",
    )
    parser.add_argument(
        "--setup-scheduler",
        action="store_true",
        help="Register a single daily Windows Task Scheduler job",
    )
    parser.add_argument(
        "--sync-scheduler",
        action="store_true",
        help="Sync per-client Task Scheduler tasks from DB (create/update/delete)",
    )
    parser.add_argument(
        "--remove-scheduler",
        action="store_true",
        help="Remove the Windows Task Scheduler job",
    )
    parser.add_argument(
        "--setup-server",
        action="store_true",
        help="Register a startup task that runs the local HTTP server",
    )
    parser.add_argument(
        "--register-protocol",
        action="store_true",
        help="Register fbreceipt:// URL protocol for sync-scheduler",
    )
    parser.add_argument(
        "--account-id",
        type=str,
        metavar="ACCOUNT_ID",
        help="Only process this ad account ID (used by per-client scheduled tasks)",
    )
    parser.add_argument(
        "--history",
        nargs="?",
        const=10,
        type=int,
        metavar="N",
        help="Show activity history (last N runs, default 10). Combine with --account-id to filter.",
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
    if args.history is not None:
        show_history(n=args.history, account_id=args.account_id)
    elif args.setup_server:
        setup_local_server()
    elif args.register_protocol:
        register_protocol()
    elif args.list_accounts:
        list_accounts()
    elif args.last_run:
        show_last_run()
    elif args.setup_scheduler:
        setup_windows_scheduler()
    elif args.sync_scheduler:
        sync_windows_scheduler()
    elif args.remove_scheduler:
        remove_windows_scheduler()
    elif args.schedule:
        start_scheduler()
    else:
        run_once(
            dry_run=args.dry_run,
            start_date=start_date,
            end_date=end_date,
            since_last_send=args.since_last_send,
            resend=args.resend,
            account_id=args.account_id,
        )


if __name__ == "__main__":
    main()
