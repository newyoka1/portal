"""
Politika Portal — FastAPI entry point.
Run with: uvicorn main:app --reload
"""
import json as _json
import logging
import os
import sys
from pathlib import Path

from datetime import datetime
from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from apscheduler.schedulers.background import BackgroundScheduler

from auth import get_current_user, require_user
from database import Base, engine, get_db
from gmail_poller import fetch_and_store_emails
from models import Approval, Comment, Email, PortalSetting, User   # noqa: F401 — ensure models are imported before create_all
from routers import auth, clients, comments, emails, fb_ad_approval, fb_receipts, integrations, settings, users, voter_pipeline

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Politika Portal", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def _tojson_parse(value):
    if not value:
        return []
    try:
        result = _json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, (list, dict)) else []
    except Exception:
        return []

templates.env.filters["tojson_parse"] = _tojson_parse
templates.env.filters["from_json"] = _tojson_parse

app.include_router(auth.router)
app.include_router(emails.router)
app.include_router(clients.router)
app.include_router(users.router)
app.include_router(comments.router)
app.include_router(integrations.router)
app.include_router(fb_receipts.router)
app.include_router(voter_pipeline.router)
app.include_router(settings.router)
app.include_router(fb_ad_approval.router)


# ---------------------------------------------------------------------------
# Database init + background scheduler
# ---------------------------------------------------------------------------
@app.on_event("startup")
def startup():
    # Create all tables if they don't exist yet
    Base.metadata.create_all(bind=engine)

    # Seed default portal settings (reads env vars on first run)
    from portal_config import seed_defaults
    seed_defaults()

    # Safe column migrations — check INFORMATION_SCHEMA before altering (MySQL-safe)
    import sqlalchemy as sa

    def _add_column_if_missing(conn, table: str, column: str, definition: str):
        exists = conn.execute(sa.text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = :t AND COLUMN_NAME = :c"
        ), {"t": table, "c": column}).scalar()
        if not exists:
            conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN {column} {definition}"))
            conn.commit()
            logging.info("Migration: added %s.%s", table, column)

    with engine.connect() as conn:
        _add_column_if_missing(conn, "emails",    "sent_for_approval_at", "DATETIME NULL")
        _add_column_if_missing(conn, "approvals", "token", "VARCHAR(100) NULL")

    # Single unified poller — checks Gmail for both email approvals and Meta receipts
    _sched = "hourly"
    try:
        from fb_receipts.src.db_client import DbClient as _RcDb
        _sched = _RcDb().get_settings().get("poll_schedule", "hourly")
    except Exception:
        pass

    def _poll_all():
        """One job: poll email queue + process Meta receipts."""
        fetch_and_store_emails()
        try:
            from fb_receipts.src.receipt_poller import poll_and_send
            poll_and_send()
        except Exception as e:
            logging.warning("Receipt poller error: %s", e)

    scheduler = BackgroundScheduler()

    _DAY_MAP = {"mon": "mon", "tue": "tue", "wed": "wed", "thu": "thu",
                "fri": "fri", "sat": "sat", "sun": "sun"}

    if _sched == "every_15min":
        scheduler.add_job(_poll_all, "interval", minutes=15, id="poller")
    elif _sched == "every_30min":
        scheduler.add_job(_poll_all, "interval", minutes=30, id="poller")
    elif _sched == "every_4hours":
        scheduler.add_job(_poll_all, "interval", hours=4, id="poller")
    elif _sched.startswith("daily_"):
        _hour = int(_sched.replace("daily_", "").replace("am", "").replace("pm", ""))
        if "pm" in _sched and _hour != 12:
            _hour += 12
        scheduler.add_job(_poll_all, "cron", hour=_hour, minute=5, id="poller")
    elif _sched.startswith("weekly_"):
        _day = _sched.replace("weekly_", "")
        scheduler.add_job(_poll_all, "cron", day_of_week=_DAY_MAP.get(_day, "mon"),
                          hour=9, minute=5, id="poller")
    else:  # hourly (default)
        scheduler.add_job(_poll_all, "interval", hours=1, id="poller")

    logging.info("Poller scheduled: %s", _sched)

    # ── Nightly voter CRM sync (optional) ─────────────────────────────────────
    import portal_config as _pc
    if _pc.get_setting("VOTER_NIGHTLY_SYNC", "false").lower() == "true":
        _sync_hour = int(_pc.get_setting("VOTER_SYNC_HOUR", "2") or "2")

        _voter_dir     = Path(__file__).parent / "voter_pipeline"
        _venv_python   = str(_voter_dir / ".venv" / "Scripts" / "python.exe")
        _voter_python  = _venv_python if os.path.exists(_venv_python) else sys.executable
        _voter_main    = str(_voter_dir / "main.py")

        def _run_voter_nightly():
            import subprocess
            from routers.voter_pipeline import _build_env as _venv
            try:
                _env = _venv()
            except Exception as _e:
                logging.warning("Voter nightly: could not build env: %s", _e)
                _env = None
            logging.info("Voter nightly sync: crm-sync starting...")
            r1 = subprocess.run(
                [_voter_python, _voter_main, "crm-sync"],
                cwd=str(_voter_dir), env=_env,
            )
            if r1.returncode == 0:
                logging.info("Voter nightly sync: crm-enrich starting...")
                r2 = subprocess.run(
                    [_voter_python, _voter_main, "crm-enrich"],
                    cwd=str(_voter_dir), env=_env,
                )
                logging.info(
                    "Voter nightly sync: crm-enrich done (exit %d)", r2.returncode
                )
            else:
                logging.error(
                    "Voter nightly sync: crm-sync failed (exit %d)", r1.returncode
                )

        scheduler.add_job(
            _run_voter_nightly, "cron",
            hour=_sync_hour, minute=30,
            id="voter_nightly",
        )
        logging.info(
            "Voter nightly sync scheduled at %02d:30 server time", _sync_hour
        )

    scheduler.start()


# ---------------------------------------------------------------------------
# Dashboard (home page)
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    from sqlalchemy import func
    count_rows = db.query(Email.status, func.count(Email.id)).group_by(Email.status).all()
    count_map  = {row[0]: row[1] for row in count_rows}
    counts = {
        "all":       sum(count_map.values()),
        "pending":   count_map.get("pending",   0),
        "in_review": count_map.get("in_review", 0),
        "approved":  count_map.get("approved",  0),
        "rejected":  count_map.get("rejected",  0),
    }
    return templates.TemplateResponse(request, "dashboard.html", {
        "counts":       counts,
        "current_user": current_user,
    })


# ---------------------------------------------------------------------------
# Email Queue
# ---------------------------------------------------------------------------
@app.get("/emails", response_class=HTMLResponse)
def queue(
    request: Request,
    status: str = "",
    client: int = 0,
    polled: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    query = db.query(Email)
    if status:
        query = query.filter(Email.status == status)
    if client:
        query = query.filter(Email.client_id == client)

    email_list = query.order_by(Email.received_at.desc()).all()

    # Tab counts
    from sqlalchemy import func
    count_rows = db.query(Email.status, func.count(Email.id)).group_by(Email.status).all()
    count_map  = {row[0]: row[1] for row in count_rows}
    counts = {
        "all":       sum(count_map.values()),
        "pending":   count_map.get("pending",   0),
        "in_review": count_map.get("in_review", 0),
        "approved":  count_map.get("approved",  0),
        "rejected":  count_map.get("rejected",  0),
    }

    flash = None
    if polled:
        n = int(polled)
        flash = {
            "type": "success" if n else "info",
            "message": f"Polled Gmail — {n} new email(s) ingested." if n
                       else "Polled Gmail — no new emails.",
        }

    return templates.TemplateResponse(request, "queue.html", {
        "emails":        email_list,
        "status_filter": status,
        "counts":        counts,
        "current_user":  current_user,
        "flash":         flash,
    })


# ---------------------------------------------------------------------------
# Approval Log
# ---------------------------------------------------------------------------
@app.get("/log", response_class=HTMLResponse)
def approval_log(
    request: Request,
    status: str = "approved",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    query = db.query(Email).filter(Email.status.in_(["approved", "rejected"]))
    if status in ("approved", "rejected"):
        query = query.filter(Email.status == status)

    log_emails = query.order_by(Email.received_at.desc()).all()

    return templates.TemplateResponse(request, "log.html", {
        "log_emails":    log_emails,
        "status_filter": status,
        "current_user":  current_user,
    })


# ---------------------------------------------------------------------------
# Public token-based approval (no login required)
# ---------------------------------------------------------------------------
def _recalculate_status(email_id: int, db: Session) -> None:
    approvals = db.query(Approval).filter(Approval.email_id == email_id).all()
    required  = [a for a in approvals if a.required]
    email     = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return
    if any(a.decision == "rejected" for a in required):
        email.status = "rejected"
    elif required and all(a.decision == "approved" for a in required):
        email.status = "approved"
    else:
        email.status = "in_review"


@app.get("/approve/{token}", response_class=HTMLResponse)
def approve_page(token: str, request: Request, db: Session = Depends(get_db)):
    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval:
        return HTMLResponse("<h2>Link not found or already used.</h2>", status_code=404)
    return templates.TemplateResponse(request, "approve_token.html", {
        "approval":     approval,
        "email":        approval.email,
        "current_user": None,
    })


@app.post("/approve/{token}", response_class=HTMLResponse)
def approve_submit(
    token: str,
    request: Request,
    decision: str = Form(...),
    note: str     = Form(""),
    db: Session   = Depends(get_db),
):
    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval or approval.decision != "pending":
        return templates.TemplateResponse(request, "approve_token.html", {
            "approval":     approval,
            "email":        approval.email if approval else None,
            "done":         True,
            "current_user": None,
        })

    if decision in ("approved", "rejected"):
        approval.decision   = decision
        approval.note       = note.strip()
        approval.decided_at = datetime.utcnow()
        approval.token      = None   # invalidate — one-time use

        if note.strip():
            label = "Approved" if decision == "approved" else "Rejected"
            db.add(Comment(
                email_id=approval.email_id,
                user_id=approval.user_id,
                body=f"[{label}] {note.strip()}",
            ))

        _recalculate_status(approval.email_id, db)
        db.commit()

    return templates.TemplateResponse(request, "approve_token.html", {
        "approval":     approval,
        "email":        approval.email,
        "done":         True,
        "current_user": None,
    })
