"""
Gmail poller — uses a Google Workspace Service Account with Domain-Wide
Delegation to impersonate support@politikanyc.com and fetch unread emails
via the Gmail API. No App Password required.
"""
import base64
import email
import logging
import os
from datetime import datetime
from email.utils import parseaddr, parsedate_to_datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.orm import Session

from database import SessionLocal
from email_parser import detect_origin, extract_html_body, extract_text_body, get_raw_headers
from gcp_credentials import build_credentials
from models import Email

logger = logging.getLogger(__name__)

SCOPES = ["https://mail.google.com/"]


def _gmail_service():
    """Build an authenticated Gmail API service impersonating GMAIL_ADDRESS."""
    impersonate = os.getenv("GMAIL_ADDRESS")
    if not impersonate:
        raise RuntimeError("GMAIL_ADDRESS must be set in .env")
    creds = build_credentials(SCOPES, impersonate)
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def fetch_and_store_emails() -> int:
    """
    Fetch UNREAD messages from the inbox, parse and insert new ones into MySQL.
    Returns the number of new emails ingested.
    """
    try:
        service = _gmail_service()
    except RuntimeError as exc:
        logger.error("Gmail poller config error: %s", exc)
        return 0

    ingested = 0
    try:
        # List unread messages in INBOX, with optional subject filter
        subject_filter = os.getenv("GMAIL_SUBJECT_FILTER", "")
        list_kwargs = {
            "userId":    "me",
            "labelIds":  ["INBOX", "UNREAD"],
            "maxResults": 50,
        }
        if subject_filter:
            list_kwargs["q"] = subject_filter
            logger.info("Gmail poller: filtering with query '%s'", subject_filter)

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

    return ingested


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

    date_header = msg.get("Date", "")
    try:
        received_at = parsedate_to_datetime(date_header).replace(tzinfo=None)
    except Exception:
        received_at = datetime.utcnow()

    db.add(Email(
        gmail_message_id = gmail_uid,
        subject          = subject[:500],
        from_address     = from_address[:200],
        from_name        = from_name[:200],
        html_body        = html_body,
        text_body        = text_body,
        origin_system    = origin,
        received_at      = received_at,
        status           = "pending",
    ))

    _mark_read(service, msg_id)
    logger.info("Ingested: %s from %s", subject, from_address)
    return 1


def _mark_read(service, msg_id: str) -> None:
    """Remove the UNREAD label so we don't re-process this message."""
    try:
        service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={"removeLabelIds": ["UNREAD"]},
        ).execute()
    except HttpError as exc:
        logger.warning("Could not mark message %s as read: %s", msg_id, exc)
