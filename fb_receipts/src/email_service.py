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

    def _build_message(
        self,
        to_email: str,
        client_name: str,
        receipts: list[dict],
        subject: str | None = None,
        ad_images: list[Path] | None = None,
        bcc: str | None = None,
    ) -> MIMEMultipart:
        msg = MIMEMultipart()
        msg["From"] = f"George - Politika - Invoice Delivery <{self.sender_email}>"
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
            if pdf_attachments:
                first_name = pdf_attachments[0].name
                date_part = first_name.split("T")[0] if "T" in first_name else \
                    receipts[0].get("date", "recent") if receipts else "recent"
            else:
                date_part = receipts[0].get("date", "recent") if receipts else "recent"
            subject = f"Facebook Ads Receipt — {client_name} ({date_part})"
        msg["Subject"] = subject

        # ── Email body ──
        lines = [f"Hi {client_name},\n"]

        if pdf_attachments:
            lines.append(
                f"Please find your Facebook Ads receipt(s) attached "
                f"({len(pdf_attachments)} PDF{'s' if len(pdf_attachments) > 1 else ''}).\n"
            )
            lines.append("Attached receipts:")
            lines.append("-" * 40)
            for p in pdf_attachments:
                lines.append(f"  - {p.name}")
            lines.append("-" * 40)
        else:
            lines.append("Please find your Facebook Ads spend summary below.\n")
            lines.append("Spend summary:")
            lines.append("-" * 40)
            total = 0.0
            for r in receipts:
                amount = float(r.get("amount", 0) or 0)
                total += amount
                date = r.get("date", "N/A")
                impressions = r.get("impressions", "")
                clicks = r.get("clicks", "")
                extra = f"  |  Impr: {impressions}  Clicks: {clicks}" if impressions else ""
                lines.append(f"  - {date}  |  ${amount:.2f} USD{extra}")
            lines.append("-" * 40)
            lines.append(f"  Total spend: ${total:.2f} USD")

        if ad_images:
            lines.append(f"\nAd creatives ({len(ad_images)} image(s) attached below):")

        lines.append("\nBest regards,\nPolitika NYC")

        body = "\n".join(lines)
        msg.attach(MIMEText(body, "plain"))

        # Attach PDFs
        for p in pdf_attachments:
            with open(p, "rb") as f:
                attachment = MIMEApplication(f.read(), _subtype="pdf")
                attachment.add_header("Content-Disposition", "attachment", filename=p.name)
                msg.attach(attachment)

        # Attach ad images
        for img_path in (ad_images or []):
            if img_path.exists():
                with open(img_path, "rb") as f:
                    img_data = f.read()
                subtype = "jpeg" if img_path.suffix.lower() in (".jpg", ".jpeg") else "png"
                img_attachment = MIMEImage(img_data, _subtype=subtype)
                img_attachment.add_header(
                    "Content-Disposition", "attachment", filename=img_path.name
                )
                msg.attach(img_attachment)

        return msg

    def _send_via_gmail_api(self, msg: MIMEMultipart) -> bool:
        """Send email using Gmail API (works on Railway — no SMTP needed)."""
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

        # Try Gmail API first (works on Railway)
        logger.info("Sending email to %s via Gmail API...", to_email)
        if self._send_via_gmail_api(msg):
            logger.info("Sent %d receipt(s) to %s <%s> via Gmail API",
                        len(receipts), client_name, to_email)
            return True

        # Fall back to SMTP (works locally)
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
