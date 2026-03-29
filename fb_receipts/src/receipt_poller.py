"""
Receipt Poller — checks Gmail for new Meta receipt emails and auto-sends.

Logic is simple:
  1. Search Gmail for ALL Meta receipt emails (last 90 days max)
  2. For each email, extract the transaction_id + account_id
  3. Check if transaction_id already exists in sent_receipts table
  4. If not → match to active client → generate PDF → send → store in DB
  5. If yes → skip (already processed)

No lookback window or schedule needed. The sent_receipts table IS the state.
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

    client_map = {c["ad_account_id"]: c for c in clients}
    settings = db.get_settings()
    admin_email = settings.get("admin_email") or None

    # Search Gmail for Meta receipt emails (last 90 days — wide net, DB deduplicates)
    end = datetime.now()
    start = end - timedelta(days=90)

    logger.info("Receipt poller: checking Gmail for new Meta receipts...")
    gmail_receipts = fetch_meta_receipts(start_date=start, end_date=end)

    if not gmail_receipts:
        logger.info("Receipt poller: no Meta receipt emails found")
        return

    sent = 0
    skipped = 0

    for receipt in gmail_receipts:
        acct_id = receipt.get("account_id", "")
        txn_id = receipt.get("transaction_id", "")

        # Must have a transaction_id to deduplicate
        if not txn_id:
            skipped += 1
            continue

        # Already in DB?
        if db.is_receipt_sent(acct_id, txn_id):
            skipped += 1
            continue

        # Match to active client
        client = client_map.get(acct_id)
        if not client:
            logger.debug("Receipt poller: no active client for account %s — skipping", acct_id)
            skipped += 1
            continue

        client_name = client["client_name"]
        emails = client.get("emails") or [client["email"]]
        amount = receipt.get("amount", 0)
        logger.info("Receipt poller: NEW receipt for %s ($%.2f, txn %s) — sending to %s",
                     client_name, amount, txn_id[:25], ", ".join(emails))

        # Get campaign + adset detail from Meta API
        campaigns, adsets = [], []
        try:
            campaigns = meta.get_campaign_spend(f"act_{acct_id}", start, end)
            adsets = meta.get_adset_spend(f"act_{acct_id}", start, end)
        except Exception:
            pass

        # Get ad images → upload to SFTP → store in DB
        import base64, json as _json
        ad_image_bytes = []  # list of (filename, bytes)
        try:
            # Fetch from Meta API into memory via temp dir
            _tmp = Path(mkdtemp(prefix="img_"))
            raw_images = meta.get_ad_images(
                f"act_{acct_id}", start, end, max_images=4, base_dir=_tmp
            )
            for img_path in (raw_images or []):
                if img_path.exists():
                    ad_image_bytes.append((img_path.name, img_path.read_bytes()))

            # Upload to SFTP for persistent storage
            if ad_image_bytes:
                try:
                    sys.path.insert(0, str(FB_DIR.parent))
                    from utils_sftp import sftp_upload
                    for fname, _ in ad_image_bytes:
                        fpath = _tmp / acct_id / "images" / fname
                        if fpath.exists():
                            sftp_upload(str(fpath), remote_dir=f"receipts/{acct_id}")
                except Exception as sftp_err:
                    logger.debug("SFTP image upload skipped: %s", sftp_err)

            # Clean up temp
            import shutil
            shutil.rmtree(_tmp, ignore_errors=True)

            if ad_image_bytes:
                logger.info("Receipt poller: fetched %d ad image(s) for %s", len(ad_image_bytes), client_name)
            else:
                logger.info("Receipt poller: no ad images found for %s", client_name)
        except Exception as e:
            logger.warning("Receipt poller: ad image fetch failed for %s: %s", client_name, e)

        # Serialize images for DB (base64) and email attachment (temp files)
        images_for_db = [{"filename": fn, "data": base64.b64encode(d).decode()} for fn, d in ad_image_bytes]
        ad_images_json = _json.dumps(images_for_db) if images_for_db else ""

        # Write images to temp files for email attachment
        import tempfile
        ad_image_paths = []
        for fname, img_data in ad_image_bytes:
            ext = Path(fname).suffix or ".jpg"
            tmp_img = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="ad_")
            tmp_img.write(img_data)
            tmp_img.close()
            ad_image_paths.append(Path(tmp_img.name))

        # Generate PDF (no images embedded — they go as email attachments)
        _pdf_tmp = Path(mkdtemp(prefix="pdf_"))
        pdf_path = generate_email_receipt_pdf(
            receipt=receipt,
            campaigns=campaigns,
            adsets=adsets,
            base_dir=_pdf_tmp,
        )

        if not pdf_path:
            logger.warning("Receipt poller: could not generate PDF for %s", client_name)
            db.save_sent_receipt(receipt, b"", "", ", ".join(emails), "failed", "PDF generation failed")
            continue

        # Read PDF binary for DB storage
        pdf_data = Path(pdf_path).read_bytes()
        pdf_filename = Path(pdf_path).name

        # Send email with PDF + ad images attached
        email_receipts = [{"pdf_path": str(pdf_path), "type": "receipt"}]
        any_sent = False
        for recipient in emails:
            ok = email_svc.send_receipt(
                to_email=recipient,
                client_name=client_name,
                receipts=email_receipts,
                ad_images=ad_image_paths,
                bcc=admin_email,
            )
            if ok:
                any_sent = True
                logger.info("Receipt poller: sent to %s <%s>", client_name, recipient)

        # Store in DB (PDF + images + metadata — survives redeploys)
        db.save_sent_receipt(
            receipt=receipt,
            pdf_data=pdf_data,
            pdf_filename=pdf_filename,
            sent_to=", ".join(emails),
            status="sent" if any_sent else "failed",
            error="" if any_sent else "email send failed",
            ad_images_json=ad_images_json,
        )

        if any_sent:
            sent += 1

    logger.info("Receipt poller done: %d sent, %d skipped", sent, skipped)
