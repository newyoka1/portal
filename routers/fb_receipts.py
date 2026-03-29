"""FB Ad Receipts — run orchestrator, client config, settings, activity log."""
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
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
    """Load all clients (active + inactive) from DB, sorted active-first then alpha."""
    try:
        clients = _get_db_client().get_all_clients_raw()
        clients.sort(key=lambda c: (c.get("active") != "yes", (c.get("client_name") or "").lower()))
        return clients
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
    default_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    default_end   = datetime.now().strftime("%Y-%m-%d")

    clients  = _load_clients()
    settings = _load_settings()

    # Sent receipts from DB
    sent_receipts = []
    try:
        sent_receipts = _get_db_client().get_sent_receipts(limit=100)
    except Exception as e:
        logger.warning("Could not load sent receipts: %s", e)

    return templates.TemplateResponse(request, "fb_receipts.html", {
        "current_user":  current_user,
        "sent_receipts": sent_receipts,
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
    if account_id: args += ["--account-id", account_id]

    return StreamingResponse(
        _stream(args, str(FB_DIR)),
        media_type="text/plain",
    )


@router.post("/last-run-info")
async def fb_last_run_info(current_user: User = Depends(require_user)):
    args = [sys.executable, str(FB_DIR / "main.py"), "--last-run"]
    return StreamingResponse(_stream(args, str(FB_DIR)), media_type="text/plain")


@router.post("/pull")
async def fb_pull_receipts(current_user: User = Depends(require_user)):
    """On-demand: check Gmail for new Meta receipts and process them."""
    async def _pull():
        import asyncio
        loop = asyncio.get_event_loop()
        yield "Checking Gmail for new Meta receipt emails...\n"
        try:
            from fb_receipts.src.receipt_poller import poll_and_send
            await loop.run_in_executor(None, poll_and_send)
            yield "\nDone.\n"
        except Exception as e:
            yield f"\nError: {e}\n"
    return StreamingResponse(_pull(), media_type="text/plain")


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


@router.get("/download/{path:path}")
def download_pdf(path: str, current_user: User = Depends(require_user)):
    """Download a generated receipt PDF from the INVOICES directory."""
    full_path = FB_DIR / path
    if not full_path.exists() or not full_path.is_file():
        return JSONResponse({"error": "File not found"}, status_code=404)
    # Security: ensure path stays within FB_DIR
    try:
        full_path.resolve().relative_to(FB_DIR.resolve())
    except ValueError:
        return JSONResponse({"error": "Access denied"}, status_code=403)
    return FileResponse(str(full_path), filename=full_path.name, media_type="application/pdf")


@router.post("/delete/{receipt_id}")
def delete_receipt(receipt_id: int, current_user: User = Depends(require_user)):
    """Delete a receipt from the database so it can be re-pulled."""
    try:
        db = _get_db_client()
        db.delete_receipt(receipt_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/resend")
async def fb_resend(
    request: Request,
    current_user: User = Depends(require_user),
):
    """Resend a receipt PDF from DB to a custom email address."""
    try:
        body = await request.json()
        receipt_id = body.get("receipt_id")
        to_email = body.get("to_email", "").strip()

        if not to_email:
            return JSONResponse({"ok": False, "error": "No email address provided"}, status_code=400)
        if not receipt_id:
            return JSONResponse({"ok": False, "error": "No receipt ID"}, status_code=400)

        db = _get_db_client()
        result = db.get_receipt_with_images(int(receipt_id))
        if not result:
            return JSONResponse({"ok": False, "error": "Receipt PDF not found in database"}, status_code=404)

        pdf_data = result["pdf_data"]
        client_name = result.get("receipt_for", "Client")

        # Write PDF to temp file
        import tempfile, json as _json, base64
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", prefix="resend_")
        tmp.write(pdf_data)
        tmp.close()

        receipts = [{"pdf_path": tmp.name, "type": "resend"}]

        # Restore ad images from DB to temp files
        ad_images = []
        if result.get("ad_images_json"):
            try:
                for img in _json.loads(result["ad_images_json"]):
                    img_tmp = tempfile.NamedTemporaryFile(
                        delete=False, suffix=Path(img["filename"]).suffix, prefix="img_")
                    img_tmp.write(base64.b64decode(img["data"]))
                    img_tmp.close()
                    ad_images.append(Path(img_tmp.name))
            except Exception:
                pass

        sys.path.insert(0, str(FB_DIR))
        from src.email_service import EmailService
        svc = EmailService()

        ok = svc.send_receipt(
            to_email=to_email,
            client_name=client_name,
            receipts=receipts,
            ad_images=ad_images,
            subject=f"Facebook Ads Receipt — {client_name} (resent)",
        )

        # Clean up temp file
        Path(tmp.name).unlink(missing_ok=True)

        if ok:
            return JSONResponse({"ok": True})
        else:
            return JSONResponse({"ok": False, "error": "Email send failed"}, status_code=500)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.get("/download-db/{receipt_id}")
def download_db_pdf(receipt_id: int, current_user: User = Depends(require_user)):
    """Download a receipt PDF from the database."""
    db = _get_db_client()
    result = db.get_receipt_pdf(receipt_id)
    if not result:
        return JSONResponse({"error": "Receipt not found"}, status_code=404)
    pdf_data, pdf_filename = result
    from starlette.responses import Response
    return Response(
        content=pdf_data,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{pdf_filename}"'},
    )


@router.get("/api/clients", response_class=JSONResponse)
def api_clients(current_user: User = Depends(require_user)):
    return _load_clients()


@router.post("/api/clients")
async def api_save_clients(
    request: Request,
    current_user: User = Depends(require_user),
):
    """Save the full client list from the editable table."""
    try:
        body = await request.json()
        clients = body.get("clients", [])
        db = _get_db_client()
        db.save_clients(clients)
        return JSONResponse({"ok": True, "count": len(clients)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.get("/api/settings", response_class=JSONResponse)
def api_settings(current_user: User = Depends(require_user)):
    return _load_settings()


@router.post("/api/settings")
def api_save_settings(
    request: Request,
    admin_email:      str = Form(""),
    notify_email:     str = Form(""),
    receipt_inbox:    str = Form("support@politikanyc.com"),
    poll_schedule:    str = Form("hourly"),
    current_user: User = Depends(require_user),
):
    try:
        db = _get_db_client()
        db.save_settings({
            "admin_email":      admin_email,
            "notify_email":     notify_email,
            "receipt_inbox":    receipt_inbox,
            "poll_schedule":    poll_schedule,
        })
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


async def _stream(args: list[str], cwd: str):
    import asyncio
    env = {**__import__("os").environ, "PYTHONUNBUFFERED": "1"}
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # Send keepalive dots if no output for 10s (Railway proxy times out at ~30s)
        while True:
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=10)
                if not line:
                    break
                yield line.decode("utf-8", errors="replace")
            except asyncio.TimeoutError:
                yield ".\n"  # keepalive
        await proc.wait()
        yield f"\n[Exit code: {proc.returncode}]\n"
    except Exception as exc:
        yield f"\n[Error starting process: {exc}]\n"
