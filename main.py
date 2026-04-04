"""
Politika Portal — FastAPI entry point.
Run with: uvicorn main:app --reload
"""
import json as _json
import logging
import os
import secrets
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, text
from sqlalchemy.orm import Session, joinedload

from apscheduler.schedulers.background import BackgroundScheduler

from audit import log_action
from auth import get_current_user, purge_expired_sessions, require_admin, require_user
from database import Base, SessionLocal, engine, get_db
from gmail_poller import fetch_and_store_emails, get_poller_health
from models import Approval, AuditLog, Comment, Email, PortalSetting, User   # noqa: F401
from routers import auth, clients, comments, emails, fb_ad_approval, fb_receipts, integrations, settings, users, voter_chat, voter_pipeline
from routers.emails import recalculate_status
from webhook import fire_webhook

logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Lifespan: startup + shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = _startup()
    yield
    scheduler.shutdown(wait=False)
    logging.info("Scheduler shut down.")


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Politika Portal", docs_url=None, redoc_url=None, lifespan=lifespan)

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

_STATUS_LABELS = {
    "pending": "Pending",
    "in_review": "Awaiting Approval",
    "approved": "Approved",
    "rejected": "Rejected",
    "revision_needed": "Needs Revision",
}

def _status_label(value):
    return _STATUS_LABELS.get(value, value.replace("_", " ").title())

templates.env.filters["status_label"] = _status_label

app.include_router(auth.router)
app.include_router(emails.router)
app.include_router(clients.router)
app.include_router(users.router)
app.include_router(comments.router)
app.include_router(integrations.router)
app.include_router(fb_receipts.router)
app.include_router(voter_pipeline.router)
app.include_router(voter_chat.router)
app.include_router(settings.router)
app.include_router(fb_ad_approval.router)


# ---------------------------------------------------------------------------
# Database init + background scheduler
# ---------------------------------------------------------------------------
def _startup() -> BackgroundScheduler:
    """Initialise DB, run migrations, start scheduler. Returns the scheduler."""
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
        _add_column_if_missing(conn, "emails",    "clean_html", "TEXT NULL")
        _add_column_if_missing(conn, "approvals", "token", "VARCHAR(100) NULL")
        _add_column_if_missing(conn, "clients",   "from_name", "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "clients",   "from_email", "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "clients",   "subject_filter", "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "clients",   "email_template", "TEXT NULL")
        _add_column_if_missing(conn, "clients",   "sms_template",   "TEXT NULL")

        # External approver support — name/email on client_approvers and approvals
        _add_column_if_missing(conn, "client_approvers", "approver_name",  "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "client_approvers", "approver_email", "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "client_approvers", "approver_phone", "VARCHAR(30) NULL")
        _add_column_if_missing(conn, "approvals",        "approver_name",  "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "approvals",        "approver_email", "VARCHAR(200) NULL")
        _add_column_if_missing(conn, "approvals",        "approver_phone", "VARCHAR(30) NULL")

        # Make user_id nullable on client_approvers and approvals (for external approvers)
        def _make_nullable(conn, table, column, definition):
            """ALTER a column to be nullable if it isn't already."""
            is_nullable = conn.execute(sa.text(
                "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = :t AND COLUMN_NAME = :c"
            ), {"t": table, "c": column}).scalar()
            if is_nullable == "NO":
                conn.execute(sa.text(f"ALTER TABLE {table} MODIFY COLUMN {column} {definition}"))
                conn.commit()
                logging.info("Migration: made %s.%s nullable", table, column)

        _make_nullable(conn, "client_approvers", "user_id", "INT NULL")
        _make_nullable(conn, "approvals",        "user_id", "INT NULL")
        _make_nullable(conn, "comments",         "user_id", "INT NULL")
        _add_column_if_missing(conn, "comments", "commenter_name", "VARCHAR(200) NULL")

        # Delivered-to alias tracking
        _add_column_if_missing(conn, "emails",    "delivered_to",     "VARCHAR(200) DEFAULT ''")

        # Batch improvements — deadlines, reminders, roles
        _add_column_if_missing(conn, "emails",    "deadline_at",      "DATETIME NULL")
        _add_column_if_missing(conn, "approvals", "last_reminded_at", "DATETIME NULL")
        _add_column_if_missing(conn, "users",     "role",             "VARCHAR(20) DEFAULT 'viewer'")

        # Backfill role from is_admin
        conn.execute(sa.text(
            "UPDATE users SET role = 'admin' WHERE is_admin = 1 AND (role IS NULL OR role = 'viewer')"
        ))
        conn.commit()

    # Backfill clean_html for existing emails that don't have it yet
    try:
        from email_sanitizer import sanitize_email_html as _sanitize
        _bdb = SessionLocal()
        _dirty = _bdb.query(Email).filter(
            Email.html_body != None,
            Email.html_body != "",
            (Email.clean_html == None) | (Email.clean_html == "") | (func.length(Email.clean_html) < 10),
        ).all()
        for _e in _dirty:
            _e.clean_html = _sanitize(_e.html_body)
        if _dirty:
            _bdb.commit()
            logging.info("Backfilled clean_html for %d email(s)", len(_dirty))
        _bdb.close()
    except Exception as exc:
        logging.warning("clean_html backfill failed: %s", exc)

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

    # ── Stale approval reminders (every 4 hours) ────────────────────────────
    def _send_stale_reminders():
        from portal_config import get_setting
        from notifier import send_reminder
        hours = int(get_setting("APPROVAL_REMINDER_HOURS", "24") or "24")
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        db = SessionLocal()
        try:
            stale = db.query(Approval).filter(
                Approval.decision == "pending",
                Approval.token != None,
                (Approval.last_reminded_at == None) | (Approval.last_reminded_at < cutoff),
            ).all()
            if not stale:
                return
            app_url = get_setting("APP_URL", "https://politika.run").rstrip("/")
            for appr in stale:
                try:
                    send_reminder(appr, app_url)
                    appr.last_reminded_at = datetime.now(timezone.utc)
                except Exception as exc:
                    logging.warning("Reminder failed for approval %d: %s", appr.id, exc)
            db.commit()
            logging.info("Sent %d stale approval reminder(s)", len(stale))
        except Exception as exc:
            logging.warning("Stale reminder job error: %s", exc)
        finally:
            db.close()

    scheduler.add_job(_send_stale_reminders, "interval", hours=4, id="stale_reminders")

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

    # Purge expired sessions from the in-memory store every hour
    scheduler.add_job(purge_expired_sessions, "interval", hours=1, id="session_cleanup")

    scheduler.start()

    # Pre-warm enrichment stats cache in background thread
    try:
        from routers.voter_pipeline import refresh_enrich_cache
        refresh_enrich_cache()
        logging.info("Enrichment stats: background computation started")
    except Exception as e:
        logging.warning("Enrichment stats pre-warm failed: %s", e)

    return scheduler


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health/gmail")
def health_gmail():
    health = get_poller_health()
    code = 200 if health["healthy"] else 503
    return JSONResponse(health, status_code=code)


# ---------------------------------------------------------------------------
# Dashboard (home page) with analytics
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    # Status counts
    count_rows = db.query(Email.status, func.count(Email.id)).group_by(Email.status).all()
    status_counts = {row[0]: row[1] for row in count_rows}

    # Avg approval time (emails approved in last 30 days)
    month_ago = now - timedelta(days=30)
    try:
        avg_hours = db.execute(text(
            "SELECT AVG(TIMESTAMPDIFF(HOUR, e.sent_for_approval_at, a.decided_at)) "
            "FROM approvals a JOIN emails e ON a.email_id = e.id "
            "WHERE a.decision = 'approved' AND a.decided_at >= :cutoff "
            "AND e.sent_for_approval_at IS NOT NULL"
        ), {"cutoff": month_ago}).scalar()
    except Exception:
        avg_hours = None

    # Decisions this week
    decisions_week = db.query(func.count(Approval.id)).filter(
        Approval.decided_at >= week_ago,
        Approval.decision != "pending",
    ).scalar() or 0

    # Overdue count
    overdue = db.query(func.count(Email.id)).filter(
        Email.status == "in_review",
        Email.deadline_at != None,
        Email.deadline_at < now,
    ).scalar() or 0

    poller = get_poller_health()

    return templates.TemplateResponse(request, "dashboard.html", {
        "current_user": current_user,
        "status_counts": status_counts,
        "avg_approval_hours": round(avg_hours, 1) if avg_hours else None,
        "decisions_week": decisions_week,
        "overdue_count": overdue,
        "poller_health": poller,
    })


# ---------------------------------------------------------------------------
# Email Approval Guide
# ---------------------------------------------------------------------------
@app.get("/email-guide", response_class=HTMLResponse)
def email_guide(
    request: Request,
    current_user: User = Depends(require_user),
):
    return templates.TemplateResponse(request, "email_guide.html", {
        "current_user": current_user,
    })


# ---------------------------------------------------------------------------
# Email Approval Settings
# ---------------------------------------------------------------------------
@app.get("/email-settings", response_class=HTMLResponse)
def email_settings(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    from portal_config import get_setting
    from models import Client
    subject_filter = get_setting("EMAIL_SUBJECT_FILTER", "")
    clients = db.query(Client).order_by(Client.name).all()
    return templates.TemplateResponse(request, "email_settings.html", {
        "current_user":   current_user,
        "subject_filter": subject_filter,
        "clients":        clients,
    })


# ---------------------------------------------------------------------------
# Email Queue
# ---------------------------------------------------------------------------
PER_PAGE = 50


@app.get("/emails", response_class=HTMLResponse)
def queue(
    request: Request,
    status: str = "",
    client: int = 0,
    page: int = 1,
    polled: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    query = db.query(Email).options(joinedload(Email.client), joinedload(Email.approvals))
    if status:
        query = query.filter(Email.status == status)
    if client:
        query = query.filter(Email.client_id == client)

    total = db.query(func.count(Email.id)).filter(*([Email.status == status] if status else []), *([Email.client_id == client] if client else [])).scalar()
    page = max(1, page)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    email_list = (
        query.order_by(Email.received_at.desc())
        .offset((page - 1) * PER_PAGE)
        .limit(PER_PAGE)
        .all()
    )

    # Tab counts
    count_rows = db.query(Email.status, func.count(Email.id)).group_by(Email.status).all()
    count_map  = {row[0]: row[1] for row in count_rows}
    counts = {
        "all":              sum(count_map.values()),
        "pending":          count_map.get("pending",   0),
        "in_review":        count_map.get("in_review", 0),
        "approved":         count_map.get("approved",  0),
        "rejected":         count_map.get("rejected",  0),
        "revision_needed":  count_map.get("revision_needed", 0),
    }

    flash = None
    if polled:
        n = int(polled)
        flash = {
            "type": "success" if n else "info",
            "message": f"Polled Gmail — {n} new email(s) ingested." if n
                       else "Polled Gmail — no new emails.",
        }

    from models import Client
    all_clients = db.query(Client).order_by(Client.name).all() if current_user.is_admin else []

    return templates.TemplateResponse(request, "queue.html", {
        "emails":        email_list,
        "status_filter": status,
        "counts":        counts,
        "clients":       all_clients,
        "current_user":  current_user,
        "flash":         flash,
        "page":          page,
        "total_pages":   total_pages,
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
    query = db.query(Email).options(
        joinedload(Email.client), joinedload(Email.approvals)
    ).filter(Email.status.in_(["approved", "rejected", "revision_needed"]))
    if status in ("approved", "rejected", "revision_needed"):
        query = query.filter(Email.status == status)

    log_emails = query.order_by(Email.received_at.desc()).limit(200).all()

    return templates.TemplateResponse(request, "log.html", {
        "log_emails":    log_emails,
        "status_filter": status,
        "current_user":  current_user,
    })


# ---------------------------------------------------------------------------
# Public token-based approval (no login required)
# ---------------------------------------------------------------------------
@app.get("/approve/{token}/body", response_class=HTMLResponse)
def approve_email_body(token: str, db: Session = Depends(get_db)):
    """Serve clean email HTML body for iframe — no auth, token-gated."""
    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval:
        return HTMLResponse("Not found", status_code=404)
    email = approval.email
    # Prefer clean_html; fall back to raw if not yet sanitized
    body = email.clean_html or email.html_body or ""
    return HTMLResponse(body)


@app.get("/approve/{token}", response_class=HTMLResponse)
def approve_page(token: str, request: Request, db: Session = Depends(get_db)):
    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval:
        return HTMLResponse("<h2>Link not found or already used.</h2>", status_code=404)
    email = approval.email
    all_approvals = db.query(Approval).filter(Approval.email_id == email.id).all()
    csrf_token = secrets.token_urlsafe(32)
    response = templates.TemplateResponse(request, "approve_token.html", {
        "approval":      approval,
        "email":         email,
        "all_approvals": all_approvals,
        "comments":      email.comments,
        "csrf_token":    csrf_token,
        "current_user":  None,
    })
    response.set_cookie("_csrf", csrf_token, httponly=False, samesite="strict", max_age=3600)
    return response


@app.get("/approve/{token}/status")
def approve_status_api(token: str, db: Session = Depends(get_db)):
    """Real-time polling: return current approval statuses + comments as JSON."""
    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval:
        return JSONResponse({"error": "not found"}, status_code=404)
    email = approval.email
    all_approvals = db.query(Approval).filter(Approval.email_id == email.id).all()
    return JSONResponse({
        "status": email.status,
        "approvals": [
            {"name": a.display_name, "decision": a.decision, "required": a.required}
            for a in all_approvals
        ],
        "comments": [
            {"author": c.commenter_name or (c.user.name if c.user else "Unknown"),
             "body": c.body,
             "created_at": c.created_at.strftime("%b %d, %I:%M %p")}
            for c in email.comments
        ],
    })


@app.post("/approve/{token}", response_class=HTMLResponse)
def approve_submit(
    token: str,
    request: Request,
    decision: str  = Form(...),
    note: str      = Form(""),
    csrf_token: str = Form(""),
    db: Session    = Depends(get_db),
):
    # CSRF check (double-submit cookie)
    csrf_cookie = request.cookies.get("_csrf", "")
    if not csrf_cookie or csrf_cookie != csrf_token:
        return HTMLResponse("Invalid request — please reload the page.", status_code=403)

    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval or approval.decision != "pending":
        return templates.TemplateResponse(request, "approve_token.html", {
            "approval":     approval,
            "email":        approval.email if approval else None,
            "done":         True,
            "current_user": None,
        })

    if decision in ("approved", "rejected", "revision_needed"):
        is_revision = decision == "revision_needed"

        # Record the comment regardless of decision type
        comment_body = note.strip()
        label = {"approved": "Approved", "rejected": "Rejected", "revision_needed": "Needs Revision"}[decision]
        # Always add a comment for revision requests (even without a note)
        if comment_body or is_revision:
            db.add(Comment(
                email_id=approval.email_id,
                user_id=approval.user_id,
                commenter_name=approval.display_name if not approval.user_id else None,
                body=f"[{label}] {comment_body}" if comment_body else f"[{label}] Revision requested",
            ))

        # Audit trail
        log_action(db, email_id=approval.email_id, user_id=approval.user_id,
                   actor_name=approval.display_name, action=decision, detail=comment_body)

        if is_revision:
            # Revision is NOT a final decision — keep token alive, stay on page
            # Temporarily set to revision_needed so recalculate picks it up,
            # then reset to pending so the approver can decide again later.
            approval.decision = "revision_needed"
            recalculate_status(approval.email_id, db)
            approval.decision = "pending"
            approval.note = ""
            approval.decided_at = None
            # Token stays — link keeps working
        else:
            # Final decision — invalidate the token
            approval.decision   = decision
            approval.note       = comment_body
            approval.decided_at = datetime.now(timezone.utc)
            approval.token      = None
            recalculate_status(approval.email_id, db)

        db.commit()

        # Webhook (after commit so final status is available)
        email_obj = approval.email
        fire_webhook({
            "event": "approval_decision",
            "email_id": email_obj.id,
            "email_subject": email_obj.subject,
            "approver": approval.display_name,
            "decision": decision,
            "note": comment_body,
            "final_status": email_obj.status,
        })

        if is_revision:
            # Stay on the live approval page with a confirmation banner
            all_approvals = db.query(Approval).filter(Approval.email_id == approval.email_id).all()
            new_csrf = secrets.token_urlsafe(32)
            response = templates.TemplateResponse(request, "approve_token.html", {
                "approval":      approval,
                "email":         email_obj,
                "all_approvals": all_approvals,
                "comments":      email_obj.comments,
                "csrf_token":    new_csrf,
                "revision_sent": True,
                "current_user":  None,
            })
            response.set_cookie("_csrf", new_csrf, httponly=False, samesite="strict", max_age=3600)
            return response

    return templates.TemplateResponse(request, "approve_token.html", {
        "approval":      approval,
        "email":         approval.email,
        "all_approvals": db.query(Approval).filter(Approval.email_id == approval.email_id).all(),
        "comments":      approval.email.comments,
        "done":          True,
        "current_user":  None,
    })


@app.post("/approve/{token}/comment")
def approve_add_comment(
    token: str,
    body: str   = Form(...),
    csrf_token: str = Form(""),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Add a comment from the public approval page (token-gated)."""
    csrf_cookie = request.cookies.get("_csrf", "") if request else ""
    if not csrf_cookie or csrf_cookie != csrf_token:
        return HTMLResponse("Invalid request — please reload the page.", status_code=403)

    approval = db.query(Approval).filter(Approval.token == token).first()
    if not approval:
        return RedirectResponse(f"/approve/{token}", status_code=302)
    if body.strip():
        db.add(Comment(
            email_id=approval.email_id,
            user_id=approval.user_id,
            commenter_name=approval.display_name if not approval.user_id else None,
            body=body.strip(),
        ))
        log_action(db, email_id=approval.email_id, user_id=approval.user_id,
                   actor_name=approval.display_name, action="comment", detail=body.strip()[:200])
        db.commit()

        # Notify other approvers in background
        _notify_comment_bg(approval, body.strip(), db)

    return RedirectResponse(f"/approve/{token}", status_code=302)


def _notify_comment_bg(approval, comment_body, db):
    """Send comment notification to other approvers (background thread)."""
    try:
        from portal_config import get_setting
        from notifier import send_comment_notification
        all_apprs = db.query(Approval).filter(
            Approval.email_id == approval.email_id,
            Approval.id != approval.id,
        ).all()
        recipients = [(a.display_name, a.display_email) for a in all_apprs if a.display_email]
        if not recipients:
            return
        email_obj = approval.email
        app_url = get_setting("APP_URL", "https://politika.run").rstrip("/")
        threading.Thread(
            target=send_comment_notification,
            args=(email_obj.subject, approval.display_name, comment_body, recipients, app_url),
            daemon=True,
        ).start()
    except Exception as exc:
        logging.warning("Comment notification setup failed: %s", exc)
