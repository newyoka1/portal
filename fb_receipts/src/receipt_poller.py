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

        # ── All files go through SFTP ─────────────────────────────────────
        import base64, json as _json, shutil, tempfile
        sys.path.insert(0, str(FB_DIR.parent))
        from utils_sftp import sftp_upload

        sftp_dir = f"receipts/{acct_id}"
        _tmp = Path(mkdtemp(prefix="receipt_"))

        # 1. Fetch ad images from Meta → SFTP
        ad_image_bytes = []  # (filename, bytes) for DB + email
        try:
            raw_images = meta.get_ad_images(
                f"act_{acct_id}", start, end, max_images=4, base_dir=_tmp
            )
            for img_path in (raw_images or []):
                if img_path.exists():
                    img_bytes = img_path.read_bytes()
                    ad_image_bytes.append((img_path.name, img_bytes))
                    sftp_upload(str(img_path), remote_dir=sftp_dir, cleanup=False)
                    logger.info("  Uploaded image to SFTP: %s/%s", sftp_dir, img_path.name)
        except Exception as e:
            logger.warning("Receipt poller: ad image fetch/upload failed: %s", e)

        if ad_image_bytes:
            logger.info("Receipt poller: %d ad image(s) for %s", len(ad_image_bytes), client_name)

        # 2. Generate PDF → SFTP
        pdf_path = generate_email_receipt_pdf(
            receipt=receipt, campaigns=campaigns, adsets=adsets, base_dir=_tmp,
        )

        if not pdf_path:
            logger.warning("Receipt poller: could not generate PDF for %s", client_name)
            db.save_sent_receipt(receipt, b"", "", ", ".join(emails), "failed", "PDF generation failed")
            shutil.rmtree(_tmp, ignore_errors=True)
            continue

        pdf_data = Path(pdf_path).read_bytes()
        pdf_filename = Path(pdf_path).name
        try:
            sftp_upload(str(pdf_path), remote_dir=sftp_dir, cleanup=False)
            print(f"  Uploaded PDF to SFTP: {sftp_dir}/{pdf_filename}")
        except Exception as sftp_err:
            print(f"  SFTP PDF upload skipped: {sftp_err}")

        # 3. Store in DB (base64 images + PDF binary)
        images_for_db = [{"filename": fn, "data": base64.b64encode(d).decode()} for fn, d in ad_image_bytes]
        ad_images_json = _json.dumps(images_for_db) if images_for_db else ""

        # 4. Build email attachments from the files still in _tmp
        email_receipts = [{"pdf_path": str(pdf_path), "type": "receipt"}]
        ad_image_paths = [_tmp / acct_id / "images" / fn for fn, _ in ad_image_bytes]
        # Fallback: check if images are directly in _tmp subdirs
        ad_image_paths = [p for p in ad_image_paths if p.exists()]
        if not ad_image_paths:
            # Images may be in a different structure — find all image files
            ad_image_paths = list(_tmp.rglob("*.jpg")) + list(_tmp.rglob("*.png")) + list(_tmp.rglob("*.webp"))

        # 5. Send email
        print(f"  PDF: {pdf_path}")
        print(f"  Ad images for email: {len(ad_image_paths)}")
        for p in ad_image_paths:
            print(f"    {p} exists={p.exists()} size={p.stat().st_size if p.exists() else 0}")
        print(f"  Ad images in DB: {len(ad_image_bytes)}")
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

        # Clean up temp dir
        shutil.rmtree(_tmp, ignore_errors=True)

        if any_sent:
            sent += 1

    logger.info("Receipt poller done: %d sent, %d skipped", sent, skipped)
