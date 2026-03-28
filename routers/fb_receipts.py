"""FB Ad Receipts — run orchestrator, view sent log & activity log."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User

router = APIRouter(prefix="/fb-receipts")
templates = Jinja2Templates(directory="templates")

# Path resolution: routers/ → portal/ → portal/fb_receipts/
PORTAL_DIR     = Path(__file__).parent.parent
FB_DIR         = PORTAL_DIR / "fb_receipts"
ACTIVITY_FILE  = FB_DIR / "activity_log.json"
LAST_RUN_FILE  = FB_DIR / "last_run.json"


@router.get("", response_class=HTMLResponse)
def fb_receipts_page(
    request: Request,
    current_user: User = Depends(require_user),
):
    # Load activity log for display
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

    return templates.TemplateResponse(request, "fb_receipts.html", {
        "current_user":  current_user,
        "activity":      list(reversed(activity)),
        "last_run":      last_run,
        "default_start": default_start,
        "default_end":   default_end,
        "fb_dir_exists": FB_DIR.exists(),
    })


@router.post("/run")
async def fb_run_stream(
    start_date: str  = Form(...),
    end_date:   str  = Form(...),
    dry_run:    str  = Form(""),
    no_fb_pdfs: str  = Form(""),
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
    if no_fb_pdfs: args.append("--no-fb-pdfs")

    return StreamingResponse(
        _stream(args, str(FB_DIR)),
        media_type="text/plain",
    )


@router.post("/last-run-info")
async def fb_last_run_info(current_user: User = Depends(require_user)):
    args = [sys.executable, str(FB_DIR / "main.py"), "--last-run"]
    return StreamingResponse(_stream(args, str(FB_DIR)), media_type="text/plain")


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
