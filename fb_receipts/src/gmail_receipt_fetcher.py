"""
Gmail Receipt Fetcher — extracts Meta billing receipt data from Gmail emails.

Searches support@politikanyc.com for Meta billing receipt emails, parses
all receipt fields from the HTML body, and returns structured data that
the PDF generator can use to create exact replicas.

Extracted fields:
  - receipt_for (client name from Meta)
  - account_id
  - transaction_id
  - amount
  - currency
  - date_range_start, date_range_end
  - billing_reason
  - product_type
  - payment_method
  - reference_number
"""

import base64
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ["https://mail.google.com/"]
RECEIPT_EMAIL = os.getenv("GMAIL_RECEIPT_ADDRESS", "support@politikanyc.com")


def _get_gmail_service():
    """Build Gmail API service impersonating the receipt inbox."""
    from src.config import GOOGLE_SERVICE_ACCOUNT_FILE
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
    ).with_subject(RECEIPT_EMAIL)

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def fetch_meta_receipts(
    start_date: datetime,
    end_date: datetime,
    account_id: str | None = None,
) -> list[dict]:
    """
    Search Gmail for Meta billing receipt emails and extract structured data.

    Returns list of dicts with all receipt fields parsed from email body.
    """
    service = _get_gmail_service()

    after = start_date.strftime("%Y/%m/%d")
    before = (end_date + timedelta(days=1)).strftime("%Y/%m/%d")
    query = (
        f"from:(business-updates.facebook.com OR facebookmail.com OR meta.com OR facebook.com) "
        f"subject:(receipt) after:{after} before:{before}"
    )

    logger.info("Searching Gmail (%s): %s", RECEIPT_EMAIL, query)

    all_receipts = []
    page_token = None

    while True:
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=100, pageToken=page_token,
        ).execute()

        messages = resp.get("messages", [])
        if not messages:
            logger.info("No receipt emails found")
            break

        logger.info("Found %d receipt email(s)", len(messages))

        for msg_ref in messages:
            receipt = _parse_receipt_email(service, msg_ref["id"])
            if receipt:
                # Filter by account_id if specified
                if account_id and receipt.get("account_id") and receipt["account_id"] != account_id:
                    logger.info("  Skipping — account %s != filter %s",
                                receipt["account_id"], account_id)
                    continue
                all_receipts.append(receipt)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info("Extracted %d receipt(s) from Gmail", len(all_receipts))
    return all_receipts


def _parse_receipt_email(service, msg_id: str) -> dict | None:
    """Parse a single Meta receipt email into structured data."""
    msg = service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute()

    headers = {h["name"].lower(): h["value"]
               for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")
    date_str = headers.get("date", "")
    msg_date = _parse_email_date(date_str)

    logger.info("  Email: '%s' (%s)", subject[:80], date_str[:25])

    # Get email body text (HTML → stripped text)
    body = _get_body_text(msg.get("payload", {}))
    if not body:
        logger.warning("  Empty email body — skipping")
        return None

    # ── Parse all receipt fields ──────────────────────────────────────────

    # Account ID — from subject "Your Meta ads receipt (Account ID: 563674964651869)"
    # or from body "(563674964651869)"
    acct_id = ""
    m = re.search(r"Account\s*ID:\s*(\d{9,20})", subject, re.IGNORECASE)
    if m:
        acct_id = m.group(1)
    if not acct_id:
        m = re.search(r"Account\s*ID:\s*(\d{9,20})", body, re.IGNORECASE)
        if m:
            acct_id = m.group(1)
    if not acct_id:
        m = re.search(r"\((\d{9,20})\)", body)
        if m:
            acct_id = m.group(1)

    # Receipt for (client name)
    receipt_for = ""
    m = re.search(r"Receipt\s+for\s+(.+?)(?:\(|Transaction|$)", body, re.IGNORECASE)
    if m:
        receipt_for = m.group(1).strip()

    # Transaction ID — long hyphenated number
    txn_id = ""
    m = re.search(r"Transaction\s*ID\s*[:\s]*(\d{10,25}-\d{10,25})", body, re.IGNORECASE)
    if m:
        txn_id = m.group(1)
    if not txn_id:
        m = re.search(r"(\d{15,25}-\d{15,25})", body)
        if m:
            txn_id = m.group(1)

    # Amount billed
    amount = 0.0
    currency = "USD"
    m = re.search(r"Amount\s+billed\s*\$([0-9,]+\.\d{2})\s*(USD)?", body, re.IGNORECASE)
    if m:
        amount = float(m.group(1).replace(",", ""))
        currency = m.group(2) or "USD"
    if amount == 0:
        m = re.search(r"\$([0-9,]+\.\d{2})\s*USD", body)
        if m:
            amount = float(m.group(1).replace(",", ""))

    # Date range
    date_range_start = ""
    date_range_end = ""
    m = re.search(
        r"Date\s+range\s+([\w\s,]+\d{4}[,\s]*\d{1,2}:\d{2}\s*[AP]M)\s*[-–]\s*([\w\s,]+\d{4}[,\s]*\d{1,2}:\d{2}\s*[AP]M)",
        body, re.IGNORECASE,
    )
    if m:
        date_range_start = m.group(1).strip()
        date_range_end = m.group(2).strip()

    # Billing reason
    billing_reason = ""
    m = re.search(r"Billing\s+reason\s+(.+?)(?:Product|Payment|$)", body, re.IGNORECASE)
    if m:
        billing_reason = m.group(1).strip().rstrip(".")

    # Product type
    product_type = "Meta ads"
    m = re.search(r"Product\s+type\s+(.+?)(?:Payment|Billing|$)", body, re.IGNORECASE)
    if m:
        product_type = m.group(1).strip()

    # Payment method
    payment_method = ""
    m = re.search(r"Payment\s+method\s+(.+?)(?:Reference|$)", body, re.IGNORECASE)
    if m:
        payment_method = m.group(1).strip()

    # Reference number
    reference_number = ""
    m = re.search(r"Reference\s+number\s+[ⓘ]?\s*(\w+)", body, re.IGNORECASE)
    if m:
        reference_number = m.group(1).strip()

    if not acct_id:
        logger.warning("  Could not extract account ID — skipping")
        return None

    logger.info("  Parsed: account=%s, $%.2f, txn=%s", acct_id, amount, txn_id[:25] if txn_id else "?")

    return {
        "account_id": acct_id,
        "receipt_for": receipt_for,
        "transaction_id": txn_id,
        "amount": amount,
        "currency": currency,
        "date_range_start": date_range_start,
        "date_range_end": date_range_end,
        "billing_reason": billing_reason,
        "product_type": product_type,
        "payment_method": payment_method,
        "reference_number": reference_number,
        "email_date": msg_date.isoformat() if msg_date else "",
        "email_subject": subject,
    }


def _get_body_text(payload: dict) -> str:
    """Extract text from Gmail message payload (handles multipart)."""
    texts = []

    def _walk(part):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data", "")

        if mime in ("text/plain", "text/html") and data:
            try:
                decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                if mime == "text/html":
                    decoded = re.sub(r"<[^>]+>", " ", decoded)
                    decoded = re.sub(r"&nbsp;", " ", decoded)
                    decoded = re.sub(r"&amp;", "&", decoded)
                    decoded = re.sub(r"&#\d+;", " ", decoded)
                    decoded = re.sub(r"\s+", " ", decoded)
                texts.append(decoded)
            except Exception:
                pass

        for sub in part.get("parts", []):
            _walk(sub)

    _walk(payload)
    return "\n".join(texts)


def _parse_email_date(date_str: str) -> datetime | None:
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(date_str).replace(tzinfo=None)
    except Exception:
        return None
