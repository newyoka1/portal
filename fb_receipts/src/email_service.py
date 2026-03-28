"""
Gmail email service for sending receipts to clients.

Supports two auth modes:
1. Gmail App Password (simpler — works with any Gmail/Google Workspace account)
2. Google Service Account with domain-wide delegation (for Google Workspace orgs)
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from pathlib import Path

from src.config import GMAIL_SENDER_EMAIL, GMAIL_APP_PASSWORD, NOTIFY_EMAIL

logger = logging.getLogger(__name__)


class EmailService:
    def __init__(
        self,
        sender_email: str = GMAIL_SENDER_EMAIL,
        app_password: str = GMAIL_APP_PASSWORD,
    ):
        self.sender_email = sender_email
        self.app_password = app_password

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

        # Collect PDF attachments first so we know what we're sending
        pdf_attachments: list[Path] = []
        for r in receipts:
            pdf_path = r.get("pdf_path")
            if pdf_path and Path(pdf_path).exists():
                pdf_attachments.append(Path(pdf_path))

        # Build subject using first PDF date or fallback to spend date
        if subject is None:
            if pdf_attachments:
                # e.g. "2026-03-13T15-52 Transaction #..." -> grab date part
                first_name = pdf_attachments[0].name
                date_part = first_name.split("T")[0] if "T" in first_name else \
                    receipts[0].get("date", "recent") if receipts else "recent"
            else:
                date_part = receipts[0].get("date", "recent") if receipts else "recent"
            subject = f"Facebook Ads Receipt — {client_name} ({date_part})"
        msg["Subject"] = subject

        # ── Email body ────────────────────────────────────────────────────
        lines = [f"Hi {client_name},\n"]

        if pdf_attachments:
            # Real Facebook PDFs attached — list them cleanly
            lines.append(
                f"Please find your Facebook Ads receipt(s) attached "
                f"({len(pdf_attachments)} PDF{'s' if len(pdf_attachments) > 1 else ''}).\n"
            )
            lines.append("Attached receipts:")
            lines.append("-" * 40)
            for p in pdf_attachments:
                lines.append(f"  • {p.name}")
            lines.append("-" * 40)
        else:
            # No real PDFs — show spend summary from Insights API
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
                lines.append(f"  • {date}  |  ${amount:.2f} USD{extra}")
            lines.append("-" * 40)
            lines.append(f"  Total spend: ${total:.2f} USD")
            lines.append(
                "\nNote: Billing receipts were not available for download. "
                "The summary above reflects daily spend data from Meta Ads Manager."
            )

        # Ad images section
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

    def send_receipt(
        self,
        to_email: str,
        client_name: str,
        receipts: list[dict],
        subject: str | None = None,
        ad_images: list[Path] | None = None,
        bcc: str | None = None,
    ) -> bool:
        """Send receipt email with optional PDF attachments and ad images."""
        if not receipts:
            logger.info("No receipts to send for %s (%s)", client_name, to_email)
            return False

        msg = self._build_message(to_email, client_name, receipts, subject, ad_images, bcc)

        try:
            logger.info("Connecting to smtp.gmail.com:587...")
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
                server.starttls()
                logger.info("Logging in as %s...", self.sender_email)
                server.login(self.sender_email, self.app_password)
                logger.info("Sending email to %s...", to_email)
                server.send_message(msg)

            logger.info(
                "Sent %d receipt(s) to %s <%s>",
                len(receipts),
                client_name,
                to_email,
            )
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error("SMTP auth failed for %s: %s — check GMAIL_SENDER_EMAIL and GMAIL_APP_PASSWORD", self.sender_email, e)
            return False
        except Exception as e:
            logger.error("Failed to send email to %s: %s", to_email, e)
            return False

    def send_failure_notification(self, client_name: str, ad_account_id: str, reason: str, notify_email: str | None = None) -> None:
        """Notify the admin that a receipt could not be sent for a client."""
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
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(self.sender_email, self.app_password)
                server.send_message(msg)
            logger.info("Sent failure notification to %s for %s", to, client_name)
        except Exception as e:
            logger.error("Could not send failure notification: %s", e)
