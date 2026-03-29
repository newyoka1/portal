"""
Politika Portal — FastAPI entry point.
Run with: uvicorn main:app --reload
"""
import logging
import os

from datetime import datetime
from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette.middleware.wsgi import WSGIMiddleware

from apscheduler.schedulers.background import BackgroundScheduler

from auth import get_current_user, require_user
from database import Base, engine, get_db
from gmail_poller import fetch_and_store_emails
from models import Approval, Comment, Email, PortalSetting, User   # noqa: F401 — ensure models are imported before create_all
from routers import auth, clients, comments, emails, fb_receipts, integrations, settings, users, voter_pipeline

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Politika Portal", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.include_router(auth.router)
app.include_router(emails.router)
app.include_router(clients.router)
app.include_router(users.router)
app.include_router(comments.router)
app.include_router(integrations.router)
app.include_router(fb_receipts.router)
app.include_router(voter_pipeline.router)
app.include_router(settings.router)

# ---------------------------------------------------------------------------
# Mount FB Ad Approval Flask app at /fb/
# ---------------------------------------------------------------------------
try:
    from fb_ad_approval.app import app as flask_app
    flask_wsgi = WSGIMiddleware(flask_app)
    app.mount("/fb", flask_wsgi)
    logging.info("FB Ad Approval Flask app mounted at /fb/")
except Exception as exc:
    logging.warning("Could not mount FB Ad Approval: %s — %s", type(exc).__name__, exc)

# Redirect /fb (no trailing slash) to /fb/
@app.get("/fb")
def fb_redirect():
    return RedirectResponse("/fb/", status_code=301)


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

    # Poll Gmail every 5 minutes automatically
    poll_interval = int(os.getenv("POLL_INTERVAL_MINUTES", "5"))
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_store_emails, "interval", minutes=poll_interval)

    # Poll Gmail for new Meta receipt emails and auto-send
    try:
        from fb_receipts.src.receipt_poller import poll_and_send
        from portal_config import get_setting
        _sched = get_setting("RECEIPT_POLL_SCHEDULE", "hourly")

        if _sched == "weekly":
            scheduler.add_job(poll_and_send, "cron", day_of_week="mon", hour=9, minute=5,
                              id="fb_receipt_poller")
            logging.info("FB receipt poller scheduled: weekly (Mon 9:05 AM)")
        elif _sched == "daily":
            scheduler.add_job(poll_and_send, "cron", hour=9, minute=5,
                              id="fb_receipt_poller")
            logging.info("FB receipt poller scheduled: daily (9:05 AM)")
        else:  # hourly (default)
            scheduler.add_job(poll_and_send, "interval", hours=1,
                              id="fb_receipt_poller")
            logging.info("FB receipt poller scheduled: hourly")
    except Exception as exc:
        logging.warning("FB receipt poller not loaded: %s", exc)

    scheduler.start()
    logging.info("Gmail poller scheduled every %d minute(s).", poll_interval)


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
