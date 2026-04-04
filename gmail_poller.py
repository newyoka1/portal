"""
Gmail poller — uses a Google Workspace Service Account with Domain-Wide
Delegation to impersonate support@politikanyc.com and fetch unread emails
via the Gmail API. No App Password required.
"""
import base64
import email
import logging
import os
import time as _time
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.orm import Session

from database import SessionLocal
from email_parser import detect_origin, extract_html_body, extract_text_body, get_raw_headers
from email_sanitizer import sanitize_email_html
from gcp_credentials import build_credentials
from models import Email

logger = logging.getLogger(__name__)

SCOPES = ["https://mail.google.com/"]

# ── Poller health tracking ────────────────────────────────────────────────
_poller_healthy: bool = True
_last_poll_time: float = 0
_last_poll_error: str = ""
_consecutive_failures: int = 0


def get_poller_health() -> dict:
    """Return a snapshot of poller health for the /health/gmail endpoint."""
    from datetime import datetime, timezone
    last_dt = datetime.fromtimestamp(_last_poll_time, tz=timezone.utc).isoformat() if _last_poll_time else None
    return {
        "healthy": _poller_healthy,
        "last_poll": last_dt,
        "consecutive_failures": _consecutive_failures,
        "last_error": _last_poll_error,
    }

# Cached Gmail API service — avoids re-downloading the discovery document on
# every poll cycle.  Refreshed every 30 minutes to pick up credential changes.
_gmail_svc = None
_gmail_svc_ts: float = 0
_GMAIL_SVC_TTL = 1800  # 30 minutes


def _gmail_service():
    """Return a (cached) authenticated Gmail API service."""
    global _gmail_svc, _gmail_svc_ts
    now = _time.time()
    if _gmail_svc and now - _gmail_svc_ts < _GMAIL_SVC_TTL:
        return _gmail_svc
    from portal_config import get_setting
    impersonate = get_setting("GMAIL_ADDRESS")
    if not impersonate:
        raise RuntimeError("GMAIL_ADDRESS must be set in Settings or .env")
    creds = build_credentials(SCOPES, impersonate)
    _gmail_svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
    _gmail_svc_ts = now
    return _gmail_svc


def fetch_and_store_emails() -> int:
    """
    Fetch UNREAD messages from the inbox, parse and insert new ones into MySQL.
    Returns the number of new emails ingested.
    """
    global _poller_healthy, _last_poll_time, _last_poll_error, _consecutive_failures
    try:
        service = _gmail_service()
    except RuntimeError as exc:
        logger.error("Gmail poller config error: %s", exc)
        _poller_healthy = False
        _last_poll_error = str(exc)
        _consecutive_failures += 1
        _last_poll_time = _time.time()
        return 0

    ingested = 0
    try:
        # List unread messages in INBOX, with optional subject filter
        # Build filter from per-client subject_filter values, fall back to global setting
        from database import SessionLocal as _SL
        from models import Client as _Client
        from portal_config import get_setting

        _db = _SL()
        try:
            client_filters = [
                c.subject_filter.strip()
                for c in _db.query(_Client).all()
                if c.subject_filter and c.subject_filter.strip()
            ]
        finally:
            _db.close()

        # Build subject filter clauses
        if client_filters:
            subject_clause = " OR ".join(f"subject:{f}" for f in client_filters)
        else:
            global_filter = get_setting("EMAIL_SUBJECT_FILTER", "").strip()
            subject_clause = f"subject:{global_filter}" if global_filter else ""

        # Also pull emails addressed to additional aliases (queue + direct)
        alias_clauses = []
        queue_aliases = get_setting("EMAIL_QUEUE_ALIASES", "").strip()
        direct_alias = get_setting("EMAIL_DIRECT_ALIAS", "").strip()
        for addr in (queue_aliases.split(",") if queue_aliases else []):
            addr = addr.strip()
            if addr:
                alias_clauses.append(f"to:{addr}")
        if direct_alias:
            alias_clauses.append(f"to:{direct_alias}")

        # Combine: (subject filters) OR (alias matches)
        parts = []
        if subject_clause:
            parts.append(f"({subject_clause})")
        if alias_clauses:
            parts.append("(" + " OR ".join(alias_clauses) + ")")
        query_str = " OR ".join(parts) if parts else ""

        list_kwargs = {
            "userId":    "me",
            "labelIds":  ["INBOX", "UNREAD"],
            "maxResults": 50,
        }
        if query_str:
            list_kwargs["q"] = query_str
            logger.info("Gmail poller: filtering with query '%s'", query_str)

        result = service.users().messages().list(**list_kwargs).execute()

        messages = result.get("messages", [])
        if not messages:
            return 0

        logger.info("Gmail poller: %d unread message(s) found.", len(messages))

        db: Session = SessionLocal()
        try:
            for msg_ref in messages:
                ingested += _process_message(service, msg_ref["id"], db)
            db.commit()
        except Exception:
            db.rollback()
            logger.exception("Gmail poller: DB error, rolling back.")
        finally:
            db.close()

    except HttpError as exc:
        logger.error("Gmail API error: %s", exc)
        _poller_healthy = False
        _last_poll_error = str(exc)
        _consecutive_failures += 1
        _last_poll_time = _time.time()
        return ingested

    # Success
    _poller_healthy = True
    _last_poll_error = ""
    _consecutive_failures = 0
    _last_poll_time = _time.time()
    return ingested


def _detect_delivered_to(msg) -> str:
    """Extract the alias/address this email was actually delivered to."""
    # Check Delivered-To first (most reliable), then X-Original-To, then To
    for header in ("Delivered-To", "X-Original-To"):
        val = msg.get(header, "").strip()
        if val:
            _, addr = parseaddr(val)
            if addr:
                return addr.lower()
    # Fall back to To header (may have multiple recipients)
    to_header = msg.get("To", "")
    if to_header:
        _, addr = parseaddr(to_header)
        if addr:
            return addr.lower()
    return ""


def _match_client_by_subject(subject: str, db: Session):
    """Find the client whose subject_filter matches the email subject."""
    from models import Client as _Client
    for c in db.query(_Client).all():
        if c.subject_filter and c.subject_filter.strip():
            if c.subject_filter.strip().lower() in subject.lower():
                return c
    return None


def _parse_plus_tag(delivered_to: str) -> str:
    """Extract the +tag from an address like direct+1@domain.com → '1'."""
    local = delivered_to.split("@")[0] if "@" in delivered_to else delivered_to
    if "+" in local:
        return local.split("+", 1)[1].strip()
    return ""


def _match_client_by_tag(tag: str, db: Session):
    """Find the client by numeric ID from the +tag (e.g. +1 → client id=1)."""
    from models import Client as _Client
    if not tag:
        return None
    # Primary: numeric ID
    if tag.isdigit():
        return db.query(_Client).filter(_Client.id == int(tag)).first()
    # Fallback: slug (for backwards compat)
    return db.query(_Client).filter(_Client.slug == tag.lower()).first()


def _auto_send_for_approval(email_obj, client, db: Session) -> None:
    """Auto-assign to client and send for approval without manual intervention."""
    import secrets
    import threading
    from datetime import timedelta
    from models import Approval, ClientApprover
    from portal_config import get_setting
    from audit import log_action

    if not client:
        logger.warning("Auto-send: no client provided — skipping")
        return

    # Assign to client
    email_obj.client_id = client.id
    email_obj.assigned_at = datetime.now(timezone.utc)
    email_obj.status = "in_review"

    # Create approval records for each client approver
    approvers = db.query(ClientApprover).filter(ClientApprover.client_id == client.id).all()
    if not approvers:
        logger.warning("Auto-send: client '%s' has no approvers — skipping send", client.name)
        db.flush()
        return

    for ca in approvers:
        db.add(Approval(
            email_id=email_obj.id,
            user_id=ca.user_id,
            approver_email=ca.approver_email,
            approver_name=ca.approver_name,
            approver_phone=getattr(ca, "approver_phone", None),
            required=ca.required,
            decision="pending",
            token=secrets.token_urlsafe(32),
        ))
    db.flush()

    # Set deadline
    deadline_hours = int(get_setting("APPROVAL_DEADLINE_HOURS", "48") or "48")
    email_obj.deadline_at = datetime.now(timezone.utc) + timedelta(hours=deadline_hours)
    email_obj.sent_for_approval_at = datetime.now(timezone.utc)

    log_action(db, email_id=email_obj.id, actor_name="system",
               action="auto_send", detail=f"Auto-sent via direct@ to {len(approvers)} approver(s)")

    db.flush()

    # Snapshot for background thread
    pending = db.query(Approval).filter(
        Approval.email_id == email_obj.id, Approval.decision == "pending"
    ).all()
    approval_pairs = [
        (a.display_name, a.display_email, a.approver_phone or "", a.token)
        for a in pending
    ]
    app_url = get_setting("APP_URL", "http://localhost:8000").rstrip("/")
    email_snapshot = {
        "id": email_obj.id,
        "subject": email_obj.subject,
        "from_name": email_obj.from_name,
        "from_address": email_obj.from_address,
        "client_name": client.name,
        "client_from_email": client.from_email,
        "client_from_name": client.from_name,
        "client_email_template": client.email_template,
        "client_sms_template": client.sms_template,
    }

    def _bg_send():
        from notifier import send_approval_requests_bg
        send_approval_requests_bg(email_snapshot, approval_pairs, app_url)

    threading.Thread(target=_bg_send, daemon=True).start()
    logger.info("Auto-sent for approval: '%s' → client '%s' (%d approvers)",
                email_obj.subject, client.name, len(approvers))


def _process_message(service, msg_id: str, db: Session) -> int:
    """Fetch, parse, and insert one message. Returns 1 if inserted, 0 if skipped."""

    # Fetch the full raw RFC822 message
    raw = service.users().messages().get(
        userId="me", id=msg_id, format="raw"
    ).execute()

    raw_bytes = base64.urlsafe_b64decode(raw["raw"] + "==")
    msg = email.message_from_bytes(raw_bytes)

    gmail_uid = msg.get("Message-ID", "").strip() or f"gmail-{msg_id}"

    # Skip if already ingested
    if db.query(Email).filter(Email.gmail_message_id == gmail_uid).first():
        _mark_read(service, msg_id)
        return 0

    raw_from = msg.get("From", "")
    from_name, from_address = parseaddr(raw_from)
    subject     = msg.get("Subject", "(No subject)")
    raw_headers = get_raw_headers(msg)
    html_body   = extract_html_body(msg)
    text_body   = extract_text_body(msg)
    origin      = detect_origin(raw_headers, html_body)
    delivered_to = _detect_delivered_to(msg)

    # Reject bare direct@ with no +tag — trash and skip entirely
    from portal_config import get_setting
    direct_alias = get_setting("EMAIL_DIRECT_ALIAS", "").strip().lower()
    if direct_alias and delivered_to:
        da_local = direct_alias.split("@")[0] if "@" in direct_alias else direct_alias
        da_domain = direct_alias.split("@")[1] if "@" in direct_alias else ""
        dt_local = delivered_to.split("@")[0] if "@" in delivered_to else ""
        dt_domain = delivered_to.split("@")[1] if "@" in delivered_to else ""
        dt_base = dt_local.split("+")[0] if "+" in dt_local else dt_local
        is_direct = da_domain == dt_domain and dt_base == da_local
        has_tag = "+" in dt_local
        if is_direct and not has_tag:
            logger.info("Bare direct@ with no +tag — trashing and skipping: %s", subject)
            _mark_read(service, msg_id)
            return 0

    date_header = msg.get("Date", "")
    try:
        received_at = parsedate_to_datetime(date_header).replace(tzinfo=None)
    except Exception:
        received_at = datetime.now(timezone.utc)

    email_obj = Email(
        gmail_message_id = gmail_uid,
        subject          = subject[:500],
        from_address     = from_address[:200],
        from_name        = from_name[:200],
        delivered_to     = delivered_to[:200],
        html_body        = html_body,
        clean_html       = sanitize_email_html(html_body),
        text_body        = text_body,
        origin_system    = origin,
        received_at      = received_at,
        status           = "pending",
    )
    db.add(email_obj)
    db.flush()  # get the ID assigned

    # ── Alias-based client routing ──────────────────────────────────────
    # Both email+{id}@ and direct+{id}@ use +tag to identify the client.
    # email+{id}@ → auto-assign only (manual send for approval)
    # direct+{id}@ → auto-assign AND auto-send for approval

    def _alias_matches(alias_setting: str) -> bool:
        """Check if delivered_to matches the given alias (with or without +tag)."""
        alias = alias_setting.strip().lower()
        if not alias or not delivered_to:
            return False
        alias_local, alias_domain = alias.split("@") if "@" in alias else (alias, "")
        deliv_local = delivered_to.split("@")[0] if "@" in delivered_to else ""
        deliv_domain = delivered_to.split("@")[1] if "@" in delivered_to else ""
        base_local = deliv_local.split("+")[0] if "+" in deliv_local else deliv_local
        return alias_domain == deliv_domain and base_local == alias_local

    plus_tag = _parse_plus_tag(delivered_to)
    tag_client = _match_client_by_tag(plus_tag, db) if plus_tag else None

    # Auto-assign: by +tag first, then fall back to subject match
    assign_client = tag_client or _match_client_by_subject(subject, db)
    if assign_client and not email_obj.client_id:
        email_obj.client_id = assign_client.id
        email_obj.assigned_at = datetime.now(timezone.utc)
        logger.info("Auto-assigned to client '%s' (id=%d)", assign_client.name, assign_client.id)

    # Queue alias (email+{id}@) — assignment only, no auto-send
    queue_aliases = get_setting("EMAIL_QUEUE_ALIASES", "").strip()
    for alias_addr in (queue_aliases.split(",") if queue_aliases else []):
        if _alias_matches(alias_addr):
            if tag_client and not email_obj.client_id:
                email_obj.client_id = tag_client.id
                email_obj.assigned_at = datetime.now(timezone.utc)
            logger.info("Queue alias hit (%s) — assigned to queue", delivered_to)
            break

    # Direct alias (direct+{id}@) — auto-assign AND auto-send for approval
    # Requires a +tag — bare direct@ with no tag is ignored
    direct_alias = get_setting("EMAIL_DIRECT_ALIAS", "").strip()
    if _alias_matches(direct_alias) and plus_tag:
        if tag_client:
            logger.info("Direct alias hit (%s, client=%s) — auto-sending: %s",
                        delivered_to, tag_client.name, subject)
            _auto_send_for_approval(email_obj, tag_client, db)
        else:
            logger.warning("Direct alias hit (%s) but no client matched tag '%s' — queued as pending",
                           delivered_to, plus_tag)

    _mark_read(service, msg_id)
    logger.info("Ingested: %s from %s (delivered-to: %s)", subject, from_address, delivered_to)
    return 1


def _mark_read(service, msg_id: str) -> None:
    """Remove the UNREAD label and trash the message so the inbox stays clean."""
    try:
        service.users().messages().trash(userId="me", id=msg_id).execute()
    except HttpError as exc:
        logger.warning("Could not trash message %s: %s", msg_id, exc)
