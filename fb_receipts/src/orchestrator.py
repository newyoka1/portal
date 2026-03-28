"""
Orchestrator: ties together Meta API, MySQL DB, and Gmail.

Flow:
1. Read client mappings from DB
2. For each active client, fetch receipts from their ad account(s)
3. Email receipts to the client
4. Log results
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.meta_client import MetaClient
from src.db_client import DbClient
from src.email_service import EmailService
from src.pdf_generator import generate_receipt_pdf
from src.config import get_run_dir, NOTIFY_EMAIL
from src.activity_logger import ActivityRun

logger = logging.getLogger(__name__)

SENT_LOG_FILE = Path("sent_log.json")

WEEKDAYS = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def should_send_today(schedule: str, today: datetime | None = None) -> bool:
    """
    Return True if today matches the client's schedule.

    Formats:
      weekly_friday   — every Friday
      weekly_monday   — every Monday (etc.)
      monthly_1       — 1st of every month
      monthly_15      — 15th of every month
    """
    if today is None:
        today = datetime.now()
    schedule = schedule.strip().lower()

    if schedule.startswith("weekly_"):
        day_name = schedule[len("weekly_"):]
        target = WEEKDAYS.get(day_name)
        if target is None:
            logger.warning("Unknown schedule day '%s' — defaulting to Friday", day_name)
            target = WEEKDAYS["friday"]
        return today.weekday() == target

    if schedule.startswith("monthly_"):
        try:
            target_day = int(schedule[len("monthly_"):])
        except ValueError:
            logger.warning("Invalid monthly schedule '%s' — defaulting to 1st", schedule)
            target_day = 1
        return today.day == target_day

    logger.warning("Unrecognised schedule '%s' — defaulting to weekly Friday", schedule)
    return today.weekday() == WEEKDAYS["friday"]


def _load_sent_log() -> dict:
    if SENT_LOG_FILE.exists():
        try:
            with open(SENT_LOG_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read %s: %s", SENT_LOG_FILE, e)
    return {}


def _save_sent_log(log: dict) -> None:
    try:
        with open(SENT_LOG_FILE, "w") as f:
            json.dump(log, f, indent=2)
    except Exception as e:
        logger.warning("Could not save sent log: %s", e)


class Orchestrator:
    def __init__(self):
        self.meta = MetaClient()
        self.db   = DbClient()
        self.email = EmailService()

    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        dry_run: bool = False,
        resend: bool = False,
        account_id: str | None = None,
        activity: ActivityRun | None = None,
        manual: bool = False,
    ) -> dict:
        """
        Main execution: fetch receipts and email them to clients.

        Args:
            start_date: Beginning of billing period (default: 7 days ago)
            end_date: End of billing period (default: now)
            dry_run: If True, fetch and log but don't send emails

        Returns:
            Summary dict with counts of successes, failures, and skips.
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now()

        # Date-stamped local folder: INVOICES/2026-03-10_2026-03-17/
        run_dir = get_run_dir(start_date, end_date)

        logger.info(
            "Starting receipt run for period %s to %s — saving to %s",
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            run_dir,
        )

        # 1. Load settings + client mappings from DB
        db_settings = self.db.get_settings()
        notify_email = db_settings.get("notify_email") or NOTIFY_EMAIL
        default_schedule = db_settings.get("default_schedule") or "weekly_friday"
        admin_email = db_settings.get("admin_email") or None

        mappings = self.db.get_client_mappings()
        if account_id:
            mappings = [m for m in mappings if m["ad_account_id"] == account_id]
            if not mappings:
                logger.warning("No active client found for account_id %s", account_id)
        if not mappings:
            logger.warning("No active client mappings found in DB")
            return {"sent": 0, "failed": 0, "skipped": 0, "no_receipts": 0}

        results = {"sent": 0, "failed": 0, "skipped": 0, "no_receipts": 0}
        sent_log = _load_sent_log()
        period_end_str = end_date.strftime("%Y-%m-%d")

        # 2. Process each client
        for client in mappings:
            client_name = client["client_name"]
            ad_account_id = client["ad_account_id"]
            emails = client.get("emails") or [client["email"]]

            client_schedule = client.get("schedule") or default_schedule
            logger.info(
                "Processing: %s (act_%s) -> %s  [schedule: %s]",
                client_name, ad_account_id, ", ".join(emails), client_schedule,
            )

            # Skip if today is not this client's send day (ignored for manual/dry runs)
            if not manual and not dry_run and not resend and not should_send_today(client_schedule):
                logger.info(
                    "Skipping %s — not their send day (schedule: %s)",
                    client_name, client_schedule,
                )
                results["skipped"] += 1
                if activity:
                    activity.record_skipped(client_name, ad_account_id, "wrong_day", emails)
                continue

            # Skip if already sent for this period (ignored for manual/resend runs)
            prev = sent_log.get(ad_account_id, {})
            if not manual and not dry_run and not resend and prev.get("period_end") is not None and prev.get("period_end") >= period_end_str:
                logger.info(
                    "Skipping %s — already sent up to %s (use --resend to override)",
                    client_name, prev["period_end"],
                )
                results["skipped"] += 1
                if activity:
                    activity.record_skipped(client_name, ad_account_id, "already_sent", emails)
                continue

            # Fetch receipts
            receipts = self.meta.fetch_receipts_for_account(
                ad_account_id, start_date, end_date
            )

            if not receipts:
                logger.info("No receipts found for %s", client_name)
                results["no_receipts"] += 1
                if activity:
                    activity.record_no_receipts(client_name, ad_account_id, emails)
                continue

            logger.info("Found %d receipt(s) for %s", len(receipts), client_name)

            # 3. Generate Meta-style receipt PDFs
            pdf_paths: list = []
            campaigns = self.meta.get_campaign_spend(ad_account_id, start_date, end_date)
            receipt_pdf = generate_receipt_pdf(
                client_name=client_name,
                ad_account_id=ad_account_id,
                receipts=receipts,
                start_date=start_date,
                end_date=end_date,
                base_dir=run_dir,
                campaigns=campaigns,
            )
            if receipt_pdf:
                pdf_paths.append(receipt_pdf)
                logger.info("Generated receipt PDF: %s", receipt_pdf)

            if not pdf_paths:
                logger.warning("No PDFs generated for %s", client_name)
                results["failed"] += 1
                if not dry_run:
                    self.email.send_failure_notification(
                        client_name, ad_account_id,
                        "No spend data found for this period", notify_email,
                    )
                if activity:
                    activity.record_failed(client_name, ad_account_id, emails, "no_pdfs")
                continue

            # Attach PDF paths to receipts so email_service picks them up
            for i, pdf_path in enumerate(pdf_paths):
                if i < len(receipts):
                    receipts[i]["pdf_path"] = str(pdf_path)
                else:
                    # Add a placeholder receipt entry for any extra PDFs
                    receipts.append({"pdf_path": str(pdf_path), "type": "fb_receipt"})

            # 4. Fetch ad creative images (max 4 to keep it fast)
            ad_images = []
            try:
                ad_images = self.meta.get_ad_images(
                    ad_account_id, start_date, end_date,
                    max_images=4, base_dir=run_dir,
                )
                if ad_images:
                    logger.info(
                        "Fetched %d ad image(s) for %s", len(ad_images), client_name
                    )
            except Exception as e:
                logger.warning("Could not fetch ad images for %s: %s", client_name, e)

            # 5. Send email to all recipients
            if dry_run:
                logger.info(
                    "[DRY RUN] Would send receipt to %s <%s> — PDFs: %d, Images: %d",
                    client_name, ", ".join(emails), len(pdf_paths), len(ad_images),
                )
                results["skipped"] += 1
                if activity:
                    activity.record_skipped(client_name, ad_account_id, "dry_run", emails)
            else:
                any_sent = False
                failed_recipients = []
                for recipient in emails:
                    success = self.email.send_receipt(
                        recipient, client_name, receipts, ad_images=ad_images,
                        bcc=admin_email,
                    )
                    if success:
                        any_sent = True
                    else:
                        failed_recipients.append(recipient)
                        results["failed"] += 1
                if failed_recipients:
                    self.email.send_failure_notification(
                        client_name, ad_account_id,
                        f"Email delivery failed for: {', '.join(failed_recipients)}",
                        notify_email,
                    )
                if any_sent:
                    results["sent"] += 1
                    sent_log[ad_account_id] = {
                        "period_start": start_date.strftime("%Y-%m-%d"),
                        "period_end": period_end_str,
                        "sent_at": datetime.now().isoformat(),
                        "recipients": emails,
                    }
                    _save_sent_log(sent_log)
                if activity:
                    activity.record_sent(
                        client_name, ad_account_id,
                        emails, failed_recipients,
                        pdf_count=len(pdf_paths),
                        receipt_count=len(receipts),
                    )

        logger.info("Run complete: %s", results)
        return results
