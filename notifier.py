"""Send approval-request notifications via Gmail API and Twilio SMS."""
import base64
import logging
import os
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.errors import HttpError

from gmail_poller import _gmail_service   # reuse cached service

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default templates (used when no per-client override is set)
# ---------------------------------------------------------------------------
_DEFAULT_EMAIL_INNER = """\
<p>Hi <strong>{approver_name}</strong>,</p>
<p>An email has been submitted and requires your approval.</p>
<table style="width:100%;border-collapse:collapse;margin:16px 0;">
  <tr>
    <td style="padding:8px;background:#f8f9fa;font-weight:bold;width:120px;">Subject</td>
    <td style="padding:8px;border-bottom:1px solid #eee;">{subject}</td>
  </tr>
  <tr>
    <td style="padding:8px;background:#f8f9fa;font-weight:bold;">From</td>
    <td style="padding:8px;border-bottom:1px solid #eee;">{from}</td>
  </tr>
  <tr>
    <td style="padding:8px;background:#f8f9fa;font-weight:bold;">Client</td>
    <td style="padding:8px;">{client}</td>
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
</p>"""

_DEFAULT_SMS = "Approval needed: {subject}\nReview: {approve_url}"

_EMAIL_WRAPPER = """\
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:0 auto;">
  <div style="background:#1a1a2e;padding:16px 24px;border-radius:6px 6px 0 0;">
    <span style="color:#fff;font-size:18px;font-weight:bold;">&#9993; Approval Requested</span>
  </div>
  <div style="border:1px solid #ddd;border-top:none;padding:24px;border-radius:0 0 6px 6px;">
    {inner_body}
  </div>
</body>
</html>"""


def _safe_substitute(template: str, variables: dict) -> str:
    """Substitute {var} placeholders, leaving unknown/CSS braces untouched."""
    # Use format_map with a defaultdict so unrecognised keys (like CSS
    # width:100% in {curly} style attributes) stay as-is instead of raising.
    safe = defaultdict(str, variables)
    try:
        return template.format_map(safe)
    except Exception:
        # Final fallback: manual replacement
        result = template
        for key, val in variables.items():
            result = result.replace("{" + key + "}", val)
        return result


def _ensure_send_as_name(send_as_email: str, display_name: str) -> None:
    """Update the Gmail Send-As alias display name so recipients see the right name.

    Uses a separate credentials object with the gmail.settings.basic scope
    so that a missing DWD scope doesn't break the main mail-sending service.
    """
    if not display_name or not send_as_email:
        return
    try:
        from gcp_credentials import build_credentials
        from googleapiclient.discovery import build
        from portal_config import get_setting

        impersonate = get_setting("GMAIL_ADDRESS")
        if not impersonate:
            return
        creds = build_credentials(
            ["https://www.googleapis.com/auth/gmail.settings.basic"], impersonate
        )
        svc = build("gmail", "v1", credentials=creds, cache_discovery=False)

        send_as = svc.users().settings().sendAs().get(
            userId="me", sendAsEmail=send_as_email
        ).execute()
        if send_as.get("displayName") != display_name:
            svc.users().settings().sendAs().update(
                userId="me",
                sendAsEmail=send_as_email,
                body={"displayName": display_name, "sendAsEmail": send_as_email},
            ).execute()
            logger.info("Updated Send-As display name for %s to '%s'", send_as_email, display_name)
    except HttpError as exc:
        logger.warning("Could not update Send-As name for %s: %s", send_as_email, exc)
    except Exception as exc:
        logger.warning("Send-As name update skipped: %s", exc)


def send_approval_requests(email, approval_pairs: list, app_url: str) -> dict:
    """
    Send approval-request emails and SMS.
    *approval_pairs* is a list of (name, email_addr, phone, token_str) tuples.
    Returns {"emails": N, "sms": M} counts.
    """
    from email.utils import formataddr
    from portal_config import get_setting
    # Use per-client from_email if set, otherwise fall back to global setting
    client_sender = email.client.from_email if email.client and email.client.from_email else ""
    sender_email = client_sender or get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    # Build proper RFC 2822 "Display Name <email>" format
    sender_name = email.client.from_name if email.client and email.client.from_name else ""
    sender = formataddr((sender_name, sender_email)) if sender_name else sender_email

    # Update Gmail Send-As alias display name if needed
    _ensure_send_as_name(sender_email, sender_name)
    try:
        service = _gmail_service()
    except Exception as exc:
        logger.error("Notifier: could not build Gmail service: %s", exc)
        return {"emails": 0, "sms": 0}

    sent_email = 0
    sent_sms   = 0
    client_name = email.client.name if email.client else "Unassigned"

    # Per-client custom templates (may be None)
    custom_email_tpl = email.client.email_template if email.client else None
    custom_sms_tpl   = email.client.sms_template   if email.client else None

    for approver_name, approver_email, approver_phone, token in approval_pairs:
        approve_url = f"{app_url}/approve/{token}"

        # Template variables available for substitution
        tpl_vars = {
            "approver_name": approver_name,
            "subject":       email.subject,
            "from":          email.from_name or email.from_address,
            "client":        client_name,
            "approve_url":   approve_url,
        }

        # ── Build email body ──────────────────────────────────────────
        inner_html = _safe_substitute(
            custom_email_tpl or _DEFAULT_EMAIL_INNER, tpl_vars
        )
        html_body = _EMAIL_WRAPPER.replace("{inner_body}", inner_html)

        msg = MIMEMultipart("alternative")
        msg["From"]    = sender
        msg["To"]      = approver_email
        msg["Subject"] = f"[Approval Needed] {email.subject}"

        msg.attach(MIMEText(html_body, "html"))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        try:
            service.users().messages().send(
                userId="me", body={"raw": raw}
            ).execute()
            sent_email += 1
            logger.info("Approval email sent to %s for email %d", approver_email, email.id)
        except HttpError as exc:
            logger.error("Failed to email %s: %s", approver_email, exc)

        # ── Send SMS (if phone number and Twilio configured) ──────────
        if approver_phone:
            sms_body = _safe_substitute(
                custom_sms_tpl or _DEFAULT_SMS, tpl_vars
            )
            sms_ok = _send_sms(to=approver_phone, body=sms_body)
            if sms_ok:
                sent_sms += 1

    return {"emails": sent_email, "sms": sent_sms}


def _normalize_phone(number: str) -> str:
    """Ensure phone number is in E.164 format (e.g. +12125551234)."""
    number = number.strip()
    if not number.startswith("+"):
        number = "+1" + number.lstrip("1")  # assume US if no country code
    return number


def _send_sms(to: str, body: str) -> bool:
    """Send an SMS via Twilio. Returns True on success, False on failure or if unconfigured."""
    to = _normalize_phone(to)
    from portal_config import get_setting

    sid   = get_setting("TWILIO_ACCOUNT_SID", "")
    token = get_setting("TWILIO_AUTH_TOKEN", "")
    from_number = get_setting("TWILIO_PHONE_NUMBER", "")

    if not (sid and token and from_number):
        logger.debug("Twilio not configured — skipping SMS to %s", to)
        return False

    try:
        from twilio.rest import Client
        client = Client(sid, token)
        message = client.messages.create(
            body=body,
            from_=from_number,
            to=to,
        )
        logger.info("SMS sent to %s (SID: %s)", to, message.sid)
        return True
    except ImportError:
        logger.warning("twilio package not installed — pip install twilio")
        return False
    except Exception as exc:
        logger.error("Failed to send SMS to %s: %s", to, exc)
        return False
