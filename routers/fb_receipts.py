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


def _get_sheets_client():
    """Build a SheetsClient using portal GCP credentials."""
    sys.path.insert(0, str(FB_DIR))
    from src.sheets_client import SheetsClient
    return SheetsClient()


def _load_clients():
    """Load all clients (active + inactive) from Google Sheets."""
    try:
        return _get_sheets_client().get_all_clients_raw()
    except Exception as e:
        logger.warning("Could not load clients from Sheets: %s", e)
        return []


def _load_settings():
    """Load global settings from Google Sheets."""
    try:
        return _get_sheets_client().get_settings()
    except Exception as e:
        logger.warning("Could not load settings from Sheets: %s", e)
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


@router.post("/import-meta")
async def fb_import_meta(current_user: User = Depends(require_user)):
    """Import ad accounts from Meta API into Google Sheet."""
    args = [sys.executable, str(FB_DIR / "populate_sheet.py")]
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
        sc = _get_sheets_client()
        clients = sc.get_all_clients_raw()
        new_settings = {
            "admin_email":      admin_email,
            "notify_email":     notify_email,
            "schedule_time":    schedule_time,
            "default_schedule": default_schedule,
        }
        sc.save_sheet_data(new_settings, clients)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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
