"""Send approval-request notification emails via Gmail API."""
import base64
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.errors import HttpError

from gmail_poller import _gmail_service   # reuse cached service

logger = logging.getLogger(__name__)


def send_approval_requests(email, approval_pairs: list, app_url: str) -> int:
    """
    Send approval-request emails.
    *approval_pairs* is a list of (name, email_addr, token_str) tuples.
    Returns the count of messages successfully sent.
    """
    from portal_config import get_setting
    # Use per-client from_email if set, otherwise fall back to global setting
    client_sender = email.client.from_email if email.client and email.client.from_email else ""
    sender = client_sender or get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    try:
        service = _gmail_service()
    except Exception as exc:
        logger.error("Notifier: could not build Gmail service: %s", exc)
        return 0

    sent        = 0
    client_name = email.client.name if email.client else "Unassigned"

    for approver_name, approver_email, token in approval_pairs:
        approve_url = f"{app_url}/approve/{token}"

        msg = MIMEMultipart("alternative")
        msg["From"]    = sender
        msg["To"]      = approver_email
        msg["Subject"] = f"[Approval Needed] {email.subject}"

        html_body = f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:0 auto;">
  <div style="background:#1a1a2e;padding:16px 24px;border-radius:6px 6px 0 0;">
    <span style="color:#fff;font-size:18px;font-weight:bold;">&#9993; Approval Requested</span>
  </div>
  <div style="border:1px solid #ddd;border-top:none;padding:24px;border-radius:0 0 6px 6px;">
    <p>Hi <strong>{approver_name}</strong>,</p>
    <p>An email has been submitted and requires your approval.</p>
    <table style="width:100%;border-collapse:collapse;margin:16px 0;">
      <tr>
        <td style="padding:8px;background:#f8f9fa;font-weight:bold;width:120px;">Subject</td>
        <td style="padding:8px;border-bottom:1px solid #eee;">{email.subject}</td>
      </tr>
      <tr>
        <td style="padding:8px;background:#f8f9fa;font-weight:bold;">From</td>
        <td style="padding:8px;border-bottom:1px solid #eee;">{email.from_name or email.from_address}</td>
      </tr>
      <tr>
        <td style="padding:8px;background:#f8f9fa;font-weight:bold;">Client</td>
        <td style="padding:8px;">{client_name}</td>
      </tr>
    </table>
    <p style="text-align:center;margin:24px 0;">
      <a href="{approve_url}"
         style="background:#0d6efd;color:#fff;padding:12px 28px;border-radius:6px;
                text-decoration:none;font-weight:bold;display:inline-block;">
        Review &amp; Approve
      </a>
    </p>
    <p style="color:#888;font-size:12px;">
      No account needed — just click the link above.<br><br>
      If the button does not work, copy this link:<br>
      <a href="{approve_url}" style="color:#0d6efd;">{approve_url}</a>
    </p>
  </div>
</body>
</html>"""

        msg.attach(MIMEText(html_body, "html"))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        try:
            service.users().messages().send(
                userId="me", body={"raw": raw}
            ).execute()
            sent += 1
            logger.info("Approval request sent to %s for email %d", approver_email, email.id)
        except HttpError as exc:
            logger.error("Failed to notify %s: %s", approver_email, exc)

    return sent
