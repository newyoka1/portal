"""
Receipt Poller — checks Gmail for new Meta receipt emails and auto-sends.

Called by APScheduler every N minutes. For each new Meta receipt email:
1. Parse receipt data from email
2. Match to a client in fb_receipts.clients by account_id
3. Skip if client not active or no email set
4. Skip if already processed (transaction_id in sent_receipts)
5. Get campaign/adset breakdown from Meta API
6. Fetch ad images
7. Generate PDF
8. Email to client
9. Store receipt + PDF binary in sent_receipts table
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import mkdtemp

logger = logging.getLogger(__name__)

FB_DIR = Path(__file__).resolve().parent.parent


def poll_and_send():
    """Main entry point — called by APScheduler."""
    try:
        _run()
    except Exception as e:
        logger.error("Receipt poller error: %s", e, exc_info=True)


def _run():
    sys.path.insert(0, str(FB_DIR))

    from src.db_client import DbClient
    from src.gmail_receipt_fetcher import fetch_meta_receipts
    from src.pdf_generator import generate_email_receipt_pdf
    from src.email_service import EmailService
    from src.meta_client import MetaClient

    db = DbClient()
    meta = MetaClient()
    email_svc = EmailService()

    # Load active clients
    clients = db.get_client_mappings()
    if not clients:
        return

    # Build account_id → client mapping
    client_map = {c["ad_account_id"]: c for c in clients}
    settings = db.get_settings()
    admin_email = settings.get("admin_email") or None

    # Search Gmail for receipts from the last 7 days
    end = datetime.now()
    start = end - timedelta(days=7)

    logger.info("Receipt poller: checking Gmail for new Meta receipts...")
    gmail_receipts = fetch_meta_receipts(start_date=start, end_date=end)

    if not gmail_receipts:
        logger.info("Receipt poller: no new Meta receipt emails found")
        return

    sent = 0
    skipped = 0

    for receipt in gmail_receipts:
        acct_id = receipt.get("account_id", "")
        txn_id = receipt.get("transaction_id", "")

        # Match to client
        client = client_map.get(acct_id)
        if not client:
            logger.debug("Receipt poller: no active client for account %s — skipping", acct_id)
            skipped += 1
            continue

        # Already processed?
        if txn_id and db.is_receipt_sent(acct_id, txn_id):
            logger.debug("Receipt poller: txn %s already sent — skipping", txn_id[:25])
            skipped += 1
            continue

        client_name = client["client_name"]
        emails = client.get("emails") or [client["email"]]
        logger.info("Receipt poller: new receipt for %s ($%.2f) — sending to %s",
                     client_name, receipt.get("amount", 0), ", ".join(emails))

        # Get campaign + adset detail
        try:
            # Parse date range to get the right period for campaign data
            campaigns = meta.get_campaign_spend(f"act_{acct_id}", start, end)
            adsets = meta.get_adset_spend(f"act_{acct_id}", start, end)
        except Exception:
            campaigns, adsets = [], []

        # Get ad images
        ad_images = []
        tmp_dir = Path(mkdtemp(prefix="receipts_"))
        try:
            ad_images = meta.get_ad_images(
                f"act_{acct_id}", start, end, max_images=4, base_dir=tmp_dir
            )
        except Exception:
            pass

        # Generate PDF
        pdf_path = generate_email_receipt_pdf(
            receipt=receipt,
            campaigns=campaigns,
            adsets=adsets,
            ad_images=ad_images,
            base_dir=tmp_dir,
        )

        if not pdf_path:
            logger.warning("Receipt poller: could not generate PDF for %s", client_name)
            db.save_sent_receipt(receipt, b"", "", ", ".join(emails), "failed", "PDF generation failed")
            continue

        # Read PDF binary
        pdf_data = Path(pdf_path).read_bytes()
        pdf_filename = Path(pdf_path).name

        # Build receipts list for email builder
        email_receipts = [{"pdf_path": str(pdf_path), "type": "receipt"}]

        # Send
        any_sent = False
        for recipient in emails:
            ok = email_svc.send_receipt(
                to_email=recipient,
                client_name=client_name,
                receipts=email_receipts,
                ad_images=ad_images,
                bcc=admin_email,
            )
            if ok:
                any_sent = True
                logger.info("Receipt poller: sent to %s <%s>", client_name, recipient)

        # Store in DB
        db.save_sent_receipt(
            receipt=receipt,
            pdf_data=pdf_data,
            pdf_filename=pdf_filename,
            sent_to=", ".join(emails),
            status="sent" if any_sent else "failed",
            error="" if any_sent else "email send failed",
        )

        if any_sent:
            sent += 1
        else:
            skipped += 1

    logger.info("Receipt poller done: %d sent, %d skipped", sent, skipped)
