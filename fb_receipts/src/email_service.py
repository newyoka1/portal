"""
Gmail email service for sending receipts to clients.

Uses the Gmail API with service account domain-wide delegation.
Falls back to SMTP if Gmail API credentials are not available.
"""

import base64
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from pathlib import Path

from src.config import GMAIL_SENDER_EMAIL, GMAIL_APP_PASSWORD, NOTIFY_EMAIL

logger = logging.getLogger(__name__)

GMAIL_API_SCOPES = ["https://mail.google.com/"]


def _get_gmail_api_service():
    """Build a Gmail API service using the portal's GCP credentials."""
    try:
        from src.config import GOOGLE_SERVICE_ACCOUNT_FILE
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        # Try portal settings first, then fb_receipts config
        sender = GMAIL_SENDER_EMAIL
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
            from portal_config import get_setting
            sender = get_setting("GMAIL_SENDER_EMAIL", sender)
        except Exception:
            pass
        if not sender:
            return None

        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=GMAIL_API_SCOPES
        ).with_subject(sender)

        return build("gmail", "v1", credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.debug("Gmail API not available: %s", e)
        return None


class EmailService:
    def __init__(
        self,
        sender_email: str = GMAIL_SENDER_EMAIL,
        app_password: str = GMAIL_APP_PASSWORD,
    ):
        self.sender_email = sender_email
        self.app_password = app_password
        self._gmail_service = None

    def _get_service(self):
        """Lazy-load Gmail API service."""
        if self._gmail_service is None:
            self._gmail_service = _get_gmail_api_service()
        return self._gmail_service

    # ── Meta brand colors (match pdf_generator.py) ───────────────────────────
    _META_BLUE   = "#0668E1"
    _DARK        = "#1C2B33"
    _GRAY        = "#65676B"
    _LIGHT_GRAY  = "#DADDE1"

    def _build_html_body(
        self,
        client_name: str,
        pdf_attachments: list[Path],
        receipts: list[dict],
        ad_images: list[Path] | None,
    ) -> str:
        """Build an HTML email body styled to match the Meta receipt PDF."""
        # ── Content rows ────────────────────────────────────────────────────
        if pdf_attachments:
            n = len(pdf_attachments)
            content_html = f"""
            <p style="margin:0 0 12px;">Please find your Facebook Ads receipt(s) attached
            ({n} PDF{'s' if n > 1 else ''}).</p>
            <table width="100%" cellpadding="6" cellspacing="0"
                   style="border:1px solid {self._LIGHT_GRAY}; border-radius:4px; margin-bottom:16px;">
              <tr style="background:{self._LIGHT_GRAY};">
                <th align="left" style="font-size:11px; color:{self._GRAY}; font-weight:600;
                    padding:8px 12px;">Attached Receipts</th>
              </tr>
              {"".join(
                  f'<tr><td style="padding:6px 12px; font-size:13px; color:{self._DARK}; '
                  f'border-top:1px solid {self._LIGHT_GRAY};">&#128206; {p.name}</td></tr>'
                  for p in pdf_attachments
              )}
            </table>"""
        else:
            total = sum(float(r.get("amount", 0) or 0) for r in receipts)
            rows_html = ""
            for r in receipts:
                amount = float(r.get("amount", 0) or 0)
                date = r.get("date", "N/A")
                impr = r.get("impressions", "")
                clicks = r.get("clicks", "")
                extra = f"&nbsp;&nbsp;·&nbsp;&nbsp;{impr:,} impr&nbsp;&nbsp;{clicks:,} clicks" \
                    if impr else ""
                rows_html += (
                    f'<tr>'
                    f'<td style="padding:6px 12px; font-size:13px; color:{self._GRAY}; '
                    f'border-top:1px solid {self._LIGHT_GRAY};">{date}</td>'
                    f'<td align="right" style="padding:6px 12px; font-size:13px; '
                    f'color:{self._DARK}; border-top:1px solid {self._LIGHT_GRAY}; white-space:nowrap;">'
                    f'<strong>${amount:,.2f}</strong>{extra}</td>'
                    f'</tr>'
                )
            content_html = f"""
            <p style="margin:0 0 12px;">Please find your Facebook Ads spend summary below.</p>
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="border:1px solid {self._LIGHT_GRAY}; border-radius:4px; margin-bottom:16px;">
              <tr style="background:{self._LIGHT_GRAY};">
                <th align="left"  style="font-size:11px; color:{self._GRAY}; font-weight:600; padding:8px 12px;">Date</th>
                <th align="right" style="font-size:11px; color:{self._GRAY}; font-weight:600; padding:8px 12px;">Amount</th>
              </tr>
              {rows_html}
              <tr style="background:#f9f9f9;">
                <td style="padding:8px 12px; font-size:13px; font-weight:600; color:{self._DARK};
                    border-top:2px solid {self._LIGHT_GRAY};">Total</td>
                <td align="right" style="padding:8px 12px; font-size:15px; font-weight:700;
                    color:{self._DARK}; border-top:2px solid {self._LIGHT_GRAY};">${total:,.2f} USD</td>
              </tr>
            </table>"""

        ad_note = (
            f'<p style="font-size:12px; color:{self._GRAY}; margin:12px 0 0;">'
            f'&#128248; {len(ad_images)} ad creative(s) attached.</p>'
        ) if ad_images else ""

        return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0; padding:20px; background:#f0f2f5; font-family:Helvetica,Arial,sans-serif;">
<table width="600" cellpadding="0" cellspacing="0" align="center"
       style="background:#ffffff; border-radius:8px; overflow:hidden;
              box-shadow:0 1px 4px rgba(0,0,0,.12); max-width:600px;">

  <!-- Header -->
  <tr>
    <td style="padding:20px 32px; border-bottom:1px solid {self._LIGHT_GRAY};">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="font-size:13px; font-weight:700; color:{self._DARK}; letter-spacing:.02em;">
            POLITIKA NYC
          </td>
          <td align="right"
              style="font-size:20px; font-weight:700; color:{self._META_BLUE};
                     font-family:Helvetica,Arial,sans-serif; letter-spacing:-.01em;">
            &#8734;&nbsp;Meta
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:28px 32px;">
      <p style="margin:0 0 20px; font-size:15px; color:{self._DARK};">Hi <strong>{client_name}</strong>,</p>
      {content_html}
      {ad_note}

      <p style="margin:28px 0 0; font-size:14px; color:{self._DARK};">Best regards,<br>
        <strong>Politika NYC</strong></p>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="padding:16px 32px; background:#f7f8fa; border-top:1px solid {self._LIGHT_GRAY};">
      <p style="margin:0; font-size:11px; color:{self._GRAY}; line-height:1.5;">
        This receipt was generated by Politika NYC using billing data from Meta Platforms, Inc.
        &nbsp;·&nbsp; 1 Meta Way, Menlo Park, CA 94025
      </p>
    </td>
  </tr>

</table>
</body>
</html>"""

    def _build_message(
        self,
        to_email: str,
        client_name: str,
        receipts: list[dict],
        subject: str | None = None,
        ad_images: list[Path] | None = None,
        bcc: str | None = None,
    ) -> MIMEMultipart:
        # Outer container holds alternative body + file attachments
        msg = MIMEMultipart("mixed")
        msg["From"] = f"Politika NYC — Invoice Delivery <{self.sender_email}>"
        msg["To"] = to_email
        if bcc:
            msg["Bcc"] = bcc

        # Collect PDF attachments
        pdf_attachments: list[Path] = []
        for r in receipts:
            pdf_path = r.get("pdf_path")
            if pdf_path and Path(pdf_path).exists():
                pdf_attachments.append(Path(pdf_path))

        # Build subject
        if subject is None:
            date_part = receipts[0].get("date", "recent") if receipts else "recent"
            subject = f"Facebook Ads Receipt — {client_name} ({date_part})"
        msg["Subject"] = subject

        # ── Plain text fallback ────────────────────────────────────────────
        lines = [f"Hi {client_name},\n"]
        if pdf_attachments:
            n = len(pdf_attachments)
            lines.append(f"Please find your Facebook Ads receipt(s) attached ({n} PDF{'s' if n > 1 else ''}).\n")
            for p in pdf_attachments:
                lines.append(f"  - {p.name}")
        else:
            total = 0.0
            for r in receipts:
                amount = float(r.get("amount", 0) or 0)
                total += amount
                lines.append(f"  - {r.get('date', 'N/A')}  ${amount:.2f} USD")
            lines.append(f"\nTotal: ${total:.2f} USD")
        if ad_images:
            lines.append(f"\nAd creatives ({len(ad_images)} image(s) attached).")
        lines.append("\nBest regards,\nPolitika NYC")
        plain_body = "\n".join(lines)

        # ── HTML body ──────────────────────────────────────────────────────
        html_body = self._build_html_body(client_name, pdf_attachments, receipts, ad_images)

        # Attach both text variants — clients pick whichever they support
        alternative = MIMEMultipart("alternative")
        alternative.attach(MIMEText(plain_body, "plain"))
        alternative.attach(MIMEText(html_body, "html"))
        msg.attach(alternative)

        # Attach PDFs
        for p in pdf_attachments:
            with open(p, "rb") as f:
                attachment = MIMEApplication(f.read(), _subtype="pdf")
                attachment.add_header("Content-Disposition", "attachment", filename=p.name)
                msg.attach(attachment)

        # Attach ad images
        logger.info("Ad images to attach: %d", len(ad_images or []))
        for img_path in (ad_images or []):
            img_path = Path(img_path)
            logger.info("  Image: %s exists=%s", img_path, img_path.exists())
            if img_path.exists():
                with open(img_path, "rb") as f:
                    img_data = f.read()
                ext = img_path.suffix.lower()
                subtype = "jpeg" if ext in (".jpg", ".jpeg") else \
                          "png"  if ext in (".png", ".webp") else \
                          "gif"  if ext == ".gif" else "jpeg"
                img_attachment = MIMEImage(img_data, _subtype=subtype)
                img_attachment.add_header(
                    "Content-Disposition", "attachment", filename=img_path.name
                )
                msg.attach(img_attachment)
                logger.info("  Attached %s (%d KB)", img_path.name, len(img_data) // 1024)

        return msg

    def _send_via_gmail_api(self, msg: MIMEMultipart) -> bool:
        """Send email using Gmail API (primary method on VPS)."""
        service = self._get_service()
        if not service:
            return False

        try:
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            service.users().messages().send(
                userId="me", body={"raw": raw}
            ).execute()
            return True
        except Exception as e:
            logger.error("Gmail API send failed: %s", e)
            return False

    def _send_via_smtp(self, msg: MIMEMultipart) -> bool:
        """Send email using SMTP (local dev fallback)."""
        if not self.app_password:
            logger.error("No GMAIL_APP_PASSWORD set — cannot send via SMTP")
            return False
        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
                server.starttls()
                server.login(self.sender_email, self.app_password)
                server.send_message(msg)
            return True
        except Exception as e:
            logger.error("SMTP send failed: %s", e)
            return False

    def send_receipt(
        self,
        to_email: str,
        client_name: str,
        receipts: list[dict],
        subject: str | None = None,
        ad_images: list[Path] | None = None,
        bcc: str | None = None,
    ) -> bool:
        """Send receipt email — tries Gmail API first, falls back to SMTP."""
        if not receipts:
            logger.info("No receipts to send for %s (%s)", client_name, to_email)
            return False

        msg = self._build_message(to_email, client_name, receipts, subject, ad_images, bcc)

        # Try Gmail API first
        logger.info("Sending email to %s via Gmail API...", to_email)
        if self._send_via_gmail_api(msg):
            logger.info("Sent %d receipt(s) to %s <%s> via Gmail API",
                        len(receipts), client_name, to_email)
            return True

        # Fall back to SMTP
        logger.info("Gmail API unavailable, trying SMTP for %s...", to_email)
        if self._send_via_smtp(msg):
            logger.info("Sent %d receipt(s) to %s <%s> via SMTP",
                        len(receipts), client_name, to_email)
            return True

        logger.error("All send methods failed for %s <%s>", client_name, to_email)
        return False

    def send_failure_notification(self, client_name: str, ad_account_id: str, reason: str, notify_email: str | None = None) -> None:
        """Notify the admin that a receipt could not be sent."""
        to = notify_email or NOTIFY_EMAIL
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = to
        msg["Subject"] = f"[Receipt Automation] FAILED — {client_name}"
        body = (
            f"Receipt delivery failed for {client_name} (act_{ad_account_id}).\n\n"
            f"Reason: {reason}\n\n"
            "No email was sent to the client. Please download and send the receipt manually."
        )
        msg.attach(MIMEText(body, "plain"))

        if self._send_via_gmail_api(msg):
            logger.info("Sent failure notification to %s for %s", to, client_name)
        elif self._send_via_smtp(msg):
            logger.info("Sent failure notification to %s for %s (SMTP)", to, client_name)
        else:
            logger.error("Could not send failure notification for %s", client_name)
