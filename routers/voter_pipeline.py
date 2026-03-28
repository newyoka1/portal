"""Voter Pipeline — CRM sync, Aiven sync, export (cloud-safe ops only)."""
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User

router = APIRouter(prefix="/voter-pipeline")
templates = Jinja2Templates(directory="templates")

PORTAL_DIR  = Path(__file__).parent.parent
VOTER_DIR   = PORTAL_DIR / "voter_pipeline"


@router.get("", response_class=HTMLResponse)
def voter_pipeline_page(
    request: Request,
    current_user: User = Depends(require_user),
):
    return templates.TemplateResponse(request, "voter_pipeline.html", {
        "current_user":        current_user,
        "voter_dir_exists":    VOTER_DIR.exists(),
    })


@router.post("/run")
async def voter_run_stream(
    cmd:      str = Form(...),
    extra:    str = Form(""),
    current_user: User = Depends(require_user),
):
    """Stream output for any voter-pipeline main.py subcommand."""
    args = [sys.executable, str(VOTER_DIR / "main.py"), cmd]
    if extra:
        args += extra.split()   # e.g. "--ld 63" or "--full"

    return StreamingResponse(
        _stream(args, str(VOTER_DIR)),
        media_type="text/plain",
    )


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
