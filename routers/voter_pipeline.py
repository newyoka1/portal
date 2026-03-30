"""Voter Pipeline — CRM sync, Aiven sync, export (cloud-safe ops only)."""
import os
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User
import portal_config

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


def _build_env() -> dict:
    """Inject all portal DB settings into the subprocess environment.

    Uses os.environ as the base (so PATH, HOME etc. are inherited) then
    overlays every non-empty portal_settings value on top. This means any
    setting stored in the portal DB — Meta tokens, HubSpot keys, CM keys,
    Mailchimp keys — is automatically available to all pipeline scripts
    without needing to update this function when new settings are added.
    """
    import time
    if time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    env = os.environ.copy()
    for key, val in portal_config._cache.items():
        if val:
            env[key] = val
    return env


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
        _stream(args, str(VOTER_DIR), _build_env()),
        media_type="text/plain",
    )


async def _stream(args: list[str], cwd: str, env: dict | None = None):
    import asyncio
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # Read in chunks (not lines) so \r progress prints flush through nginx
        # without waiting for a \n — avoids proxy_read_timeout on long syncs.
        while True:
            chunk = await proc.stdout.read(4096)
            if not chunk:
                break
            yield chunk.decode("utf-8", errors="replace")
        await proc.wait()
        yield f"\n[Exit code: {proc.returncode}]\n"
    except Exception as exc:
        yield f"\n[Error starting process: {exc}]\n"
