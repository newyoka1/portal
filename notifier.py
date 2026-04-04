"""Send approval-request notifications via Gmail API and Twilio SMS."""
import base64
import logging
import os
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.errors import HttpError

from gmail_poller import _gmail_service, SCOPES   # reuse cached service

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



class _EmailSnapshot:
    """Lightweight stand-in for an ORM Email, usable across threads."""
    def __init__(self, d: dict):
        self.id = d["id"]
        self.subject = d["subject"]
        self.from_name = d["from_name"]
        self.from_address = d["from_address"]
        # Mimic email.client.* via a simple inner object
        self.client = type("C", (), {
            "name": d["client_name"],
            "from_email": d.get("client_from_email"),
            "from_name": d.get("client_from_name"),
            "email_template": d.get("client_email_template"),
            "sms_template": d.get("client_sms_template"),
        })() if d.get("client_name") else None


def send_approval_requests_bg(email_dict: dict, approval_pairs: list, app_url: str) -> dict:
    """Thread-safe wrapper: accepts a plain dict snapshot instead of ORM object."""
    snap = _EmailSnapshot(email_dict)
    return send_approval_requests(snap, approval_pairs, app_url)


def send_approval_requests(email, approval_pairs: list, app_url: str) -> dict:
    """
    Send approval-request emails and SMS.
    *approval_pairs* is a list of (name, email_addr, phone, token_str) tuples.
    Returns {"emails": N, "sms": M} counts.
    """
    from email.utils import formataddr
    from portal_config import get_setting
    gmail_address = get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    client_sender = email.client.from_email if email.client and email.client.from_email else ""
    sender_name = email.client.from_name if email.client and email.client.from_name else ""
    # DWD can only impersonate users within the Workspace domain.
    # If the client's from_email is an external address (e.g. noreply@hubspot.com),
    # fall back to the default Gmail service.
    workspace_domain = gmail_address.split("@")[-1] if gmail_address else ""
    can_impersonate = (
        client_sender
        and client_sender != gmail_address
        and workspace_domain
        and client_sender.lower().endswith(f"@{workspace_domain}")
    )

    if can_impersonate:
        sender_email = client_sender
    else:
        sender_email = gmail_address

    # From header still shows client name even when we can't impersonate the address
    display_email = client_sender or gmail_address
    sender = formataddr((sender_name, display_email)) if sender_name else display_email

    # Build Gmail service
    try:
        if can_impersonate:
            from gcp_credentials import build_credentials
            from googleapiclient.discovery import build as build_svc
            creds = build_credentials(SCOPES, sender_email)
            service = build_svc("gmail", "v1", credentials=creds, cache_discovery=False)
            logger.info("Sending as %s (impersonating via DWD)", sender_email)
        else:
            service = _gmail_service()
            logger.info("Sending via default service (%s), From header: %s", gmail_address, sender)
    except Exception as exc:
        logger.error("Notifier: could not build Gmail service for %s: %s", sender_email, exc)
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


# ---------------------------------------------------------------------------
# Stale approval reminder
# ---------------------------------------------------------------------------
def send_reminder(approval, app_url: str) -> None:
    """Re-send a reminder email for a single pending approval."""
    from email.mime.multipart import MIMEMultipart as _MM
    from email.mime.text import MIMEText as _MT
    from email.utils import formataddr
    from portal_config import get_setting

    if not approval.token or not approval.display_email:
        return

    gmail_address = get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    approve_url = f"{app_url}/approve/{approval.token}"
    email_obj = approval.email

    subject_line = f"[Reminder] Approval needed: {email_obj.subject}"
    html_body = _EMAIL_WRAPPER.replace("{inner_body}", f"""
    <p>Hi <strong>{approval.display_name}</strong>,</p>
    <p>This is a friendly reminder that the following email is still waiting for your review:</p>
    <p><strong>{email_obj.subject}</strong></p>
    <p style="text-align:center;margin:24px 0;">
      <a href="{approve_url}"
         style="background:#0d6efd;color:#fff;padding:12px 28px;border-radius:6px;
                text-decoration:none;font-weight:bold;display:inline-block;">
        Review &amp; Approve
      </a>
    </p>
    """)

    try:
        service = _gmail_service()
        msg = _MM("alternative")
        msg["From"] = gmail_address
        msg["To"] = approval.display_email
        msg["Subject"] = subject_line
        msg.attach(_MT(html_body, "html"))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.info("Reminder sent to %s for email %d", approval.display_email, email_obj.id)
    except Exception as exc:
        logger.error("Reminder to %s failed: %s", approval.display_email, exc)


# ---------------------------------------------------------------------------
# Comment notification
# ---------------------------------------------------------------------------
def send_comment_notification(email_subject: str, commenter: str, comment_body: str,
                              recipients: list, app_url: str) -> None:
    """Notify approvers when someone posts a comment. Thread-safe."""
    from email.mime.multipart import MIMEMultipart as _MM
    from email.mime.text import MIMEText as _MT
    from portal_config import get_setting

    gmail_address = get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    subject_line = f"[Comment] {email_subject}"
    inner = f"""
    <p><strong>{commenter}</strong> commented:</p>
    <blockquote style="border-left:3px solid #ddd;padding:8px 16px;color:#555;margin:12px 0;">
      {comment_body}
    </blockquote>
    <p style="color:#888;font-size:12px;">
      Regarding: <strong>{email_subject}</strong>
    </p>
    """
    html_body = _EMAIL_WRAPPER.replace("{inner_body}", inner)

    try:
        service = _gmail_service()
        for name, email_addr in recipients:
            if not email_addr:
                continue
            msg = _MM("alternative")
            msg["From"] = gmail_address
            msg["To"] = email_addr
            msg["Subject"] = subject_line
            msg.attach(_MT(html_body, "html"))
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            try:
                service.users().messages().send(userId="me", body={"raw": raw}).execute()
            except Exception as exc:
                logger.warning("Comment notification to %s failed: %s", email_addr, exc)
    except Exception as exc:
        logger.error("Comment notification service error: %s", exc)
