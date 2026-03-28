"""
Gmail Receipt Fetcher — extracts real Meta billing PDF attachments from Gmail.

Searches support@politikanyc.com (or configured GMAIL_RECEIPT_ADDRESS) for
emails from Meta/Facebook containing receipt PDF attachments, downloads them,
and matches them to ad accounts by parsing the PDF filename or email subject.

Flow:
  1. Search Gmail for Meta billing emails with PDF attachments
  2. Download each PDF attachment
  3. Parse account ID + transaction ID from the email/PDF
  4. Return list of {account_id, transaction_id, date, amount, pdf_path}
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

# Email address where Meta sends billing receipts
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
    base_dir: Path | None = None,
) -> list[dict]:
    """
    Search Gmail for Meta billing receipt emails and extract PDF attachments.

    Args:
        start_date: Search for emails after this date
        end_date: Search for emails before this date
        account_id: Optional — filter to receipts for this ad account only
        base_dir: Directory to save PDFs (default: INVOICES/)

    Returns:
        List of dicts: {account_id, transaction_id, date, amount, pdf_path, subject}
    """
    service = _get_gmail_service()

    # Build Gmail search query
    after = start_date.strftime("%Y/%m/%d")
    before = (end_date + timedelta(days=1)).strftime("%Y/%m/%d")
    query = f"from:(facebookmail.com OR meta.com) subject:(receipt OR transaction OR payment) has:attachment after:{after} before:{before}"

    logger.info("Searching Gmail (%s) for Meta receipts: %s to %s",
                RECEIPT_EMAIL, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    results = []
    page_token = None

    while True:
        resp = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=100,
            pageToken=page_token,
        ).execute()

        messages = resp.get("messages", [])
        if not messages:
            break

        for msg_ref in messages:
            receipt = _process_message(service, msg_ref["id"], account_id, base_dir)
            if receipt:
                if isinstance(receipt, list):
                    results.extend(receipt)
                else:
                    results.append(receipt)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info("Found %d Meta receipt PDF(s) in Gmail", len(results))
    return results


def _process_message(
    service, msg_id: str, filter_account_id: str | None, base_dir: Path | None
) -> list[dict] | None:
    """Process one Gmail message — extract PDF attachments and metadata."""
    msg = service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute()

    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("subject", "")
    date_str = headers.get("date", "")
    msg_date = _parse_email_date(date_str)

    # Extract account ID from subject/body if possible
    # Meta subjects: "Receipt for [Client Name]" or "Payment receipt for ad account XXXXXXXXX"
    acct_match = re.search(r"(?:account|act[_\s]?)(\d{6,20})", subject, re.IGNORECASE)
    email_account_id = acct_match.group(1) if acct_match else None

    # Filter by account_id if specified
    if filter_account_id and email_account_id and email_account_id != filter_account_id:
        return None

    # Find PDF attachments
    pdfs = _find_pdf_attachments(service, msg_id, msg.get("payload", {}))
    if not pdfs:
        return None

    receipts = []
    for pdf_name, pdf_data in pdfs:
        # Try to extract account ID and transaction ID from the PDF filename
        # Meta format: "2026-03-13T15-52 Transaction #26376155962074409-26322177234138952.pdf"
        file_acct_id = email_account_id
        txn_id = ""
        amount = 0.0

        txn_match = re.search(r"Transaction\s*#?\s*(\d+-\d+)", pdf_name)
        if txn_match:
            txn_id = txn_match.group(1)

        # Try to extract account ID from PDF content (first few bytes)
        try:
            text = pdf_data.decode("latin-1", errors="ignore")
            acct_in_pdf = re.search(r"Account\s*ID:\s*(\d{6,20})", text)
            if acct_in_pdf:
                file_acct_id = acct_in_pdf.group(1)
            # Extract amount
            amt_match = re.search(r"\$([0-9,]+\.\d{2})\s*(?:USD)?", text)
            if amt_match:
                amount = float(amt_match.group(1).replace(",", ""))
        except Exception:
            pass

        # Filter by account_id if specified and we now know it
        if filter_account_id and file_acct_id and file_acct_id != filter_account_id:
            continue

        # Save PDF
        save_dir = base_dir or Path("INVOICES")
        if file_acct_id:
            save_dir = save_dir / file_acct_id
        save_dir.mkdir(parents=True, exist_ok=True)

        # Use original filename from Meta
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', pdf_name)
        pdf_path = save_dir / safe_name
        pdf_path.write_bytes(pdf_data)

        logger.info("Saved Meta receipt: %s (account: %s, $%.2f)",
                     pdf_path.name, file_acct_id or "unknown", amount)

        receipts.append({
            "account_id": file_acct_id or "",
            "transaction_id": txn_id,
            "date": msg_date.strftime("%Y-%m-%d") if msg_date else "",
            "time": msg_date.isoformat() if msg_date else "",
            "amount": amount,
            "subject": subject,
            "pdf_path": str(pdf_path),
            "pdf_name": pdf_name,
        })

    return receipts


def _find_pdf_attachments(service, msg_id: str, payload: dict) -> list[tuple[str, bytes]]:
    """Recursively find all PDF attachments in a Gmail message payload."""
    pdfs = []

    def _walk(part):
        filename = part.get("filename", "")
        mime_type = part.get("mimeType", "")
        body = part.get("body", {})

        if filename.lower().endswith(".pdf") or mime_type == "application/pdf":
            att_id = body.get("attachmentId")
            if att_id:
                try:
                    att = service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=att_id
                    ).execute()
                    data = base64.urlsafe_b64decode(att["data"])
                    pdfs.append((filename or f"receipt_{msg_id}.pdf", data))
                except Exception as e:
                    logger.warning("Could not download attachment %s: %s", filename, e)

        for sub in part.get("parts", []):
            _walk(sub)

    _walk(payload)
    return pdfs


def _parse_email_date(date_str: str) -> datetime | None:
    """Parse an email Date header into a datetime."""
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(date_str).replace(tzinfo=None)
    except Exception:
        return None
