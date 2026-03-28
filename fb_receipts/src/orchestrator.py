"""
Orchestrator: ties together Meta API, Google Sheets, and Gmail.

Flow:
1. Read client mappings from Google Sheet
2. For each active client, fetch receipts from their ad account(s)
3. Email receipts to the client
4. Log results
"""

import logging
from datetime import datetime, timedelta

from src.meta_client import MetaClient
from src.sheets_client import SheetsClient
from src.email_service import EmailService
from src.pdf_generator import generate_receipt_pdf
from src.facebook_downloader import download_receipts_for_account
from src.config import get_run_dir

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self):
        self.meta = MetaClient()
        self.sheets = SheetsClient()
        self.email = EmailService()

    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        dry_run: bool = False,
        use_fb_pdfs: bool = True,
    ) -> dict:
        """
        Main execution: fetch receipts and email them to clients.

        Args:
            start_date: Beginning of billing period (default: 35 days ago)
            end_date: End of billing period (default: now)
            dry_run: If True, fetch and log but don't send emails

        Returns:
            Summary dict with counts of successes, failures, and skips.
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=35)
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

        # 1. Load client mappings
        mappings = self.sheets.get_client_mappings()
        if not mappings:
            logger.warning("No active client mappings found in Google Sheet")
            return {"sent": 0, "failed": 0, "skipped": 0, "no_receipts": 0}

        results = {"sent": 0, "failed": 0, "skipped": 0, "no_receipts": 0}

        # 2. Process each client
        for client in mappings:
            client_name = client["client_name"]
            ad_account_id = client["ad_account_id"]
            emails = client.get("emails") or [client["email"]]

            logger.info(
                "Processing: %s (act_%s) -> %s",
                client_name, ad_account_id, ", ".join(emails),
            )

            # Fetch receipts
            receipts = self.meta.fetch_receipts_for_account(
                ad_account_id, start_date, end_date
            )

            if not receipts:
                logger.info("No receipts found for %s", client_name)
                results["no_receipts"] += 1
                continue

            logger.info("Found %d receipt(s) for %s", len(receipts), client_name)

            # 3. Get PDF receipt — try real Facebook PDFs first, fall back to generated
            pdf_paths: list = []

            if use_fb_pdfs:
                try:
                    from src.config import META_BUSINESS_IDS
                    business_id = META_BUSINESS_IDS[0] if META_BUSINESS_IDS else ""
                    pdf_paths = download_receipts_for_account(
                        ad_account_id, business_id, start_date, end_date,
                        base_dir=run_dir,
                    )
                    if pdf_paths:
                        logger.info(
                            "Downloaded %d real Facebook PDF(s) for %s",
                            len(pdf_paths), client_name,
                        )
                except Exception as e:
                    logger.warning(
                        "Facebook PDF download failed for %s (%s) — falling back to generated PDF",
                        client_name, e,
                    )

            if not pdf_paths:
                # Fallback: generate our own PDF from spend data
                generated = generate_receipt_pdf(
                    client_name=client_name,
                    ad_account_id=ad_account_id,
                    receipts=receipts,
                    start_date=start_date,
                    end_date=end_date,
                    base_dir=run_dir,
                )
                if generated:
                    pdf_paths = [generated]
                    logger.info("Generated fallback PDF: %s", generated)
                else:
                    logger.warning("PDF generation failed for %s", client_name)

            # Attach PDF paths to receipts so email_service picks them up
            for i, pdf_path in enumerate(pdf_paths):
                if i < len(receipts):
                    receipts[i]["pdf_path"] = str(pdf_path)
                else:
                    # Add a placeholder receipt entry for any extra PDFs
                    receipts.append({"pdf_path": str(pdf_path), "type": "fb_receipt"})

            # 4. Fetch ad creative images
            ad_images = []
            try:
                ad_images = self.meta.get_ad_images(
                    ad_account_id, start_date, end_date, base_dir=run_dir
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
            else:
                any_sent = False
                for recipient in emails:
                    success = self.email.send_receipt(
                        recipient, client_name, receipts, ad_images=ad_images
                    )
                    if success:
                        any_sent = True
                    else:
                        results["failed"] += 1
                if any_sent:
                    results["sent"] += 1

        logger.info("Run complete: %s", results)
        return results
