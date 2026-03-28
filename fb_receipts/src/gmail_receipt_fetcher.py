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

    # Build Gmail search query — broad match on Meta/Facebook billing emails with attachments
    after = start_date.strftime("%Y/%m/%d")
    before = (end_date + timedelta(days=1)).strftime("%Y/%m/%d")
    query = f"from:(facebookmail.com OR meta.com OR facebook.com OR business-updates.facebook.com) subject:(receipt) after:{after} before:{before}"

    logger.info("Searching Gmail (%s) with query: %s", RECEIPT_EMAIL, query)

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
            logger.info("No emails found matching query")
            break

        logger.info("Found %d email(s) to check for PDFs", len(messages))

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
    from_addr = headers.get("from", "")
    logger.info("  Email: '%s' from=%s", subject[:80], from_addr[:50])
    date_str = headers.get("date", "")
    msg_date = _parse_email_date(date_str)

    # Extract account ID from subject
    # Format: "Your Meta ads receipt (Account ID: 563674964651869)"
    acct_match = re.search(r"Account\s*ID:\s*(\d{6,20})", subject, re.IGNORECASE)
    if not acct_match:
        acct_match = re.search(r"\((\d{9,20})\)", subject)
    email_account_id = acct_match.group(1) if acct_match else None

    # Also extract from email body
    body_text = _get_body_text(msg.get("payload", {}))
    if not email_account_id and body_text:
        acct_body = re.search(r"\((\d{9,20})\)", body_text)
        if acct_body:
            email_account_id = acct_body.group(1)

    logger.info("    Account ID from email: %s", email_account_id or "not found")

    # Filter by account_id if specified
    if filter_account_id and email_account_id and email_account_id != filter_account_id:
        logger.info("    Skipping — account %s doesn't match filter %s", email_account_id, filter_account_id)
        return None

    # Extract transaction ID and amount from body
    txn_id = ""
    amount = 0.0
    if body_text:
        txn_match = re.search(r"(\d{10,25}-\d{10,25})", body_text)
        if txn_match:
            txn_id = txn_match.group(1)
        amt_match = re.search(r"\$([0-9,]+\.\d{2})\s*(?:USD)?", body_text)
        if amt_match:
            amount = float(amt_match.group(1).replace(",", ""))

    # Find PDF attachments
    pdfs = _find_pdf_attachments(service, msg_id, msg.get("payload", {}))

    receipts = []

    if pdfs:
        # Has PDF attachments — save them
        for pdf_name, pdf_data in pdfs:
            file_acct_id = email_account_id

            # Try to get more info from PDF content
            try:
                pdf_text = pdf_data.decode("latin-1", errors="ignore")
                acct_in_pdf = re.search(r"Account\s*ID:\s*(\d{6,20})", pdf_text)
                if acct_in_pdf:
                    file_acct_id = acct_in_pdf.group(1)
                if not txn_id:
                    txn_m = re.search(r"(\d{10,25}-\d{10,25})", pdf_text)
                    if txn_m:
                        txn_id = txn_m.group(1)
                if amount == 0:
                    amt_m = re.search(r"\$([0-9,]+\.\d{2})", pdf_text)
                    if amt_m:
                        amount = float(amt_m.group(1).replace(",", ""))
            except Exception:
                pass

            if filter_account_id and file_acct_id and file_acct_id != filter_account_id:
                continue

            save_dir = base_dir or Path("INVOICES")
            if file_acct_id:
                save_dir = save_dir / file_acct_id
            save_dir.mkdir(parents=True, exist_ok=True)

            safe_name = re.sub(r'[<>:"/\\|?*]', '_', pdf_name)
            pdf_path = save_dir / safe_name
            pdf_path.write_bytes(pdf_data)

            logger.info("    Saved PDF: %s (account: %s, $%.2f)", safe_name, file_acct_id or "?", amount)

            receipts.append({
                "account_id": file_acct_id or email_account_id or "",
                "transaction_id": txn_id,
                "date": msg_date.strftime("%Y-%m-%d") if msg_date else "",
                "time": msg_date.isoformat() if msg_date else "",
                "amount": amount,
                "subject": subject,
                "pdf_path": str(pdf_path),
                "pdf_name": pdf_name,
            })
    else:
        # No PDF attachment — check for download link in email body
        # Meta receipt emails often have a link to download the receipt
        download_url = None
        if body_text:
            link_match = re.search(
                r'https?://[^\s"<>]*(?:billing_hub|payment_activity|receipt)[^\s"<>]*',
                body_text, re.IGNORECASE,
            )
            if link_match:
                download_url = link_match.group(0)

        if email_account_id and (amount > 0 or txn_id):
            # We have receipt info from the email body even without a PDF
            logger.info("    No PDF attached — receipt data from email body (account: %s, $%.2f, txn: %s)",
                        email_account_id, amount, txn_id[:30] if txn_id else "?")
            receipts.append({
                "account_id": email_account_id,
                "transaction_id": txn_id,
                "date": msg_date.strftime("%Y-%m-%d") if msg_date else "",
                "time": msg_date.isoformat() if msg_date else "",
                "amount": amount,
                "subject": subject,
                "pdf_path": "",  # no PDF file
                "pdf_name": "",
                "download_url": download_url or "",
            })

    return receipts if receipts else None


def _get_body_text(payload: dict) -> str:
    """Extract plain text or HTML body from a Gmail message payload."""
    texts = []

    def _walk(part):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data", "")

        if mime in ("text/plain", "text/html") and data:
            try:
                decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                # Strip HTML tags for plain text extraction
                if mime == "text/html":
                    decoded = re.sub(r"<[^>]+>", " ", decoded)
                    decoded = re.sub(r"\s+", " ", decoded)
                texts.append(decoded)
            except Exception:
                pass

        for sub in part.get("parts", []):
            _walk(sub)

    _walk(payload)
    return "\n".join(texts)


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
