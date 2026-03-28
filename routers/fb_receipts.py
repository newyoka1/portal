"""FB Ad Receipts — run orchestrator, client config, settings, activity log."""
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fb-receipts")
templates = Jinja2Templates(directory="templates")

# Path resolution: routers/ → portal/ → portal/fb_receipts/
PORTAL_DIR     = Path(__file__).parent.parent
FB_DIR         = PORTAL_DIR / "fb_receipts"
ACTIVITY_FILE  = FB_DIR / "activity_log.json"
LAST_RUN_FILE  = FB_DIR / "last_run.json"


def _get_db_client():
    """Build a DbClient for fb_receipts config (MySQL-backed)."""
    sys.path.insert(0, str(FB_DIR))
    from src.db_client import DbClient
    return DbClient()


def _load_clients():
    """Load all clients (active + inactive) from DB."""
    try:
        return _get_db_client().get_all_clients_raw()
    except Exception as e:
        logger.warning("Could not load clients: %s", e)
        return []


def _load_settings():
    """Load global settings from DB."""
    try:
        return _get_db_client().get_settings()
    except Exception as e:
        logger.warning("Could not load settings: %s", e)
        return {}


@router.get("", response_class=HTMLResponse)
def fb_receipts_page(
    request: Request,
    current_user: User = Depends(require_user),
):
    # Activity log
    activity = []
    if ACTIVITY_FILE.exists():
        try:
            activity = json.loads(ACTIVITY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    last_run = None
    if LAST_RUN_FILE.exists():
        try:
            last_run = json.loads(LAST_RUN_FILE.read_text())
        except Exception:
            pass

    default_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    default_end   = datetime.now().strftime("%Y-%m-%d")

    # Load clients and settings from Google Sheets
    clients  = _load_clients()
    settings = _load_settings()

    return templates.TemplateResponse(request, "fb_receipts.html", {
        "current_user":  current_user,
        "activity":      list(reversed(activity)),
        "last_run":      last_run,
        "default_start": default_start,
        "default_end":   default_end,
        "fb_dir_exists": FB_DIR.exists(),
        "clients":       clients,
        "settings":      settings,
    })


@router.post("/run")
async def fb_run_stream(
    start_date: str  = Form(...),
    end_date:   str  = Form(...),
    dry_run:    str  = Form(""),
    resend:     str  = Form(""),
    no_fb_pdfs: str  = Form(""),
    account_id: str  = Form(""),
    current_user: User = Depends(require_user),
):
    """Stream subprocess output for a receipt run."""
    args = [
        sys.executable,
        str(FB_DIR / "main.py"),
        "--start-date", start_date,
        "--end-date",   end_date,
    ]
    if dry_run:    args.append("--dry-run")
    if resend:     args.append("--resend")
    if no_fb_pdfs: args.append("--no-fb-pdfs")
    if account_id: args += ["--account-id", account_id]

    return StreamingResponse(
        _stream(args, str(FB_DIR)),
        media_type="text/plain",
    )


@router.post("/last-run-info")
async def fb_last_run_info(current_user: User = Depends(require_user)):
    args = [sys.executable, str(FB_DIR / "main.py"), "--last-run"]
    return StreamingResponse(_stream(args, str(FB_DIR)), media_type="text/plain")


@router.post("/setup-db")
async def fb_setup_db(current_user: User = Depends(require_user)):
    """Run setup_fb_db.py to create the fb_receipts database and tables."""
    args = [sys.executable, str(FB_DIR / "setup_fb_db.py")]
    return StreamingResponse(_stream(args, str(FB_DIR)), media_type="text/plain")


@router.post("/import-meta")
async def fb_import_meta(current_user: User = Depends(require_user)):
    """Import ad accounts from Meta API into fb_receipts.clients DB table."""
    args = [sys.executable, str(FB_DIR / "populate_db.py")]
    return StreamingResponse(_stream(args, str(FB_DIR)), media_type="text/plain")


@router.get("/api/clients", response_class=JSONResponse)
def api_clients(current_user: User = Depends(require_user)):
    return _load_clients()


@router.get("/api/settings", response_class=JSONResponse)
def api_settings(current_user: User = Depends(require_user)):
    return _load_settings()


@router.post("/api/settings")
def api_save_settings(
    request: Request,
    admin_email:      str = Form(""),
    notify_email:     str = Form(""),
    schedule_time:    str = Form("09:00"),
    default_schedule: str = Form("weekly_friday"),
    current_user: User = Depends(require_user),
):
    try:
        db = _get_db_client()
        db.save_settings({
            "admin_email":      admin_email,
            "notify_email":     notify_email,
            "schedule_time":    schedule_time,
            "default_schedule": default_schedule,
        })
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def _run_scheduled_receipts():
    """Called by APScheduler — checks which clients are due and runs receipts."""
    import subprocess
    from datetime import date

    now = datetime.now()
    weekday_name = now.strftime("%A").lower()   # monday, tuesday, ...
    day_of_month = now.day

    try:
        settings = _load_settings()
        clients  = _load_clients()
    except Exception as e:
        logger.warning("Receipt scheduler: could not load config: %s", e)
        return

    schedule_time = settings.get("schedule_time", "09:00")
    try:
        target_hour = int(schedule_time.split(":")[0])
    except (ValueError, IndexError):
        target_hour = 9

    # Only run at the configured hour
    if now.hour != target_hour:
        return

    # Determine date range: last 7 days for weekly, last 30 for monthly
    for client in clients:
        if client.get("active") != "yes":
            continue

        sched = client.get("schedule", "weekly_friday")
        should_run = False

        if sched.startswith("weekly_"):
            sched_day = sched.replace("weekly_", "")
            if sched_day == weekday_name:
                should_run = True
        elif sched.startswith("monthly_"):
            try:
                sched_dom = int(sched.replace("monthly_", ""))
                if sched_dom == day_of_month:
                    should_run = True
            except ValueError:
                pass

        if not should_run:
            continue

        # Determine period
        if "weekly" in sched:
            start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        end = now.strftime("%Y-%m-%d")

        account_id = client.get("ad_account_id", "")
        client_name = client.get("client_name", account_id)
        logger.info("Receipt scheduler: running for %s (%s → %s)", client_name, start, end)

        try:
            result = subprocess.run(
                [sys.executable, str(FB_DIR / "main.py"),
                 "--start-date", start, "--end-date", end,
                 "--account-id", account_id, "--no-fb-pdfs"],
                cwd=str(FB_DIR),
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0:
                logger.info("Receipt scheduler: %s completed OK", client_name)
            else:
                logger.warning("Receipt scheduler: %s failed (exit %d): %s",
                               client_name, result.returncode, result.stderr[:500])
        except Exception as e:
            logger.error("Receipt scheduler: %s error: %s", client_name, e)


async def _stream(args: list[str], cwd: str):
    import asyncio
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        async for line in proc.stdout:
            yield line.decode("utf-8", errors="replace")
        await proc.wait()
        yield f"\n[Exit code: {proc.returncode}]\n"
    except Exception as exc:
        yield f"\n[Error starting process: {exc}]\n"
