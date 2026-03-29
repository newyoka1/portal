"""
Receipt Poller — checks Gmail for new Meta receipt emails, stores them, and sends.

Flow:
  poll_only()    → fetch Gmail receipts → generate PDFs → store in DB as "pending"
  send_pending() → load all "pending" DB rows → email each client → mark "sent"
  poll_and_send()→ calls both (used by APScheduler)

The sent_receipts table is the deduplication state:
  - is_receipt_sent() returns True for any status (pending, sent, failed)
  - so re-polling never creates duplicates
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import mkdtemp

logger = logging.getLogger(__name__)

FB_DIR = Path(__file__).resolve().parent.parent


# ── Public entry points ────────────────────────────────────────────────────────

def poll_only():
    """Fetch new Gmail receipts and store them in DB as 'pending' (no email sent)."""
    try:
        _run_poll_only()
    except Exception as e:
        logger.error("Receipt poll error: %s", e, exc_info=True)


def send_pending():
    """Email all receipts currently stored as 'pending' in DB."""
    try:
        _run_send_pending()
    except Exception as e:
        logger.error("Receipt send error: %s", e, exc_info=True)


def poll_and_send():
    """Combined entry point — used by APScheduler (poll then send in one call)."""
    try:
        _run_poll_only()
        _run_send_pending()
    except Exception as e:
        logger.error("Receipt poller error: %s", e, exc_info=True)


# ── Internal implementation ────────────────────────────────────────────────────

def _run_poll_only():
    """Fetch Gmail receipts, generate PDFs, and store in DB as status='pending'."""
    sys.path.insert(0, str(FB_DIR))

    from src.db_client import DbClient
    from src.gmail_receipt_fetcher import fetch_meta_receipts
    from src.pdf_generator import generate_email_receipt_pdf
    from src.meta_client import MetaClient

    db = DbClient()
    meta = MetaClient()

    clients = db.get_client_mappings()
    if not clients:
        logger.info("Receipt poller: no active clients configured")
        return

    client_map = {c["ad_account_id"]: c for c in clients}

    # Wide net — DB deduplicates
    end = datetime.now()
    start = end - timedelta(days=90)

    logger.info("Receipt poller: checking Gmail for new Meta receipts...")
    gmail_receipts = fetch_meta_receipts(start_date=start, end_date=end)

    if not gmail_receipts:
        logger.info("Receipt poller: no Meta receipt emails found")
        return

    stored = 0
    skipped = 0

    for receipt in gmail_receipts:
        acct_id = receipt.get("account_id", "")
        txn_id  = receipt.get("transaction_id", "")

        if not txn_id:
            skipped += 1
            continue

        # Already in DB (any status)?
        if db.is_receipt_sent(acct_id, txn_id):
            skipped += 1
            continue

        client = client_map.get(acct_id)
        if not client:
            logger.debug("Receipt poller: no active client for account %s — skipping", acct_id)
            skipped += 1
            continue

        # Apply per-client filter words (if set, at least one must appear in subject or receipt_for)
        filter_words = client.get("filter_words") or []
        if filter_words:
            haystack = " ".join([
                receipt.get("email_subject", ""),
                receipt.get("receipt_for", ""),
            ]).lower()
            if not any(w in haystack for w in filter_words):
                logger.debug(
                    "Receipt poller: receipt for account %s skipped — filter words %s not found in '%s'",
                    acct_id, filter_words, haystack[:80],
                )
                skipped += 1
                continue

        client_name = client["client_name"]
        emails      = client.get("emails") or [client["email"]]
        amount      = receipt.get("amount", 0)
        logger.info("Receipt poller: NEW receipt for %s ($%.2f, txn %s)",
                    client_name, amount, txn_id[:25])

        # Fetch campaign/adset detail from Meta API
        campaigns, adsets = [], []
        try:
            campaigns = meta.get_campaign_spend(f"act_{acct_id}", start, end)
            adsets    = meta.get_adset_spend(f"act_{acct_id}", start, end)
        except Exception:
            pass

        import base64, json as _json, shutil

        _tmp = Path(mkdtemp(prefix="receipt_"))

        # Fetch ad images
        ad_image_bytes = []
        try:
            raw_images = meta.get_ad_images(
                f"act_{acct_id}", start, end, max_images=4, base_dir=_tmp
            )
            for img_path in (raw_images or []):
                if img_path.exists():
                    ad_image_bytes.append((img_path.name, img_path.read_bytes()))
        except Exception as e:
            logger.warning("Receipt poller: ad image fetch failed: %s", e)

        # Generate PDF
        pdf_path = generate_email_receipt_pdf(
            receipt=receipt, campaigns=campaigns, adsets=adsets, base_dir=_tmp,
        )

        if not pdf_path:
            logger.warning("Receipt poller: could not generate PDF for %s", client_name)
            db.save_sent_receipt(receipt, b"", "", ", ".join(emails), "failed", "PDF generation failed")
            shutil.rmtree(_tmp, ignore_errors=True)
            continue

        pdf_data     = Path(pdf_path).read_bytes()
        pdf_filename = Path(pdf_path).name

        images_for_db  = [{"filename": fn, "data": base64.b64encode(d).decode()} for fn, d in ad_image_bytes]
        ad_images_json = _json.dumps(images_for_db) if images_for_db else ""

        # Store as PENDING — email will be sent separately
        db.save_sent_receipt(
            receipt=receipt,
            pdf_data=pdf_data,
            pdf_filename=pdf_filename,
            sent_to=", ".join(emails),
            status="pending",
            error="",
            ad_images_json=ad_images_json,
        )

        shutil.rmtree(_tmp, ignore_errors=True)
        stored += 1
        logger.info("Receipt poller: stored %s ($%.2f) as pending", client_name, amount)

    logger.info("Receipt poll done: %d stored as pending, %d skipped", stored, skipped)


def _run_send_pending():
    """Send emails for all receipts in DB with status='pending'."""
    sys.path.insert(0, str(FB_DIR))

    from src.db_client import DbClient
    from src.email_service import EmailService

    db        = DbClient()
    email_svc = EmailService()

    settings    = db.get_settings()
    admin_email = settings.get("admin_email") or None

    # Load current client emails (may differ from when receipt was stored)
    client_map = {c["ad_account_id"]: c for c in db.get_client_mappings()}

    pending = db.get_pending_receipts()
    if not pending:
        logger.info("Send pending: no pending receipts to send")
        return

    logger.info("Send pending: %d receipt(s) to send", len(pending))

    import tempfile, json as _json, base64

    sent   = 0
    failed = 0

    for row in pending:
        receipt_id  = row["id"]
        acct_id     = row["ad_account_id"]
        client_name = row.get("receipt_for", "Client")
        pdf_data    = row.get("pdf_data", b"")
        pdf_filename = row.get("pdf_filename", "receipt.pdf")
        ad_images_json = row.get("ad_images_json", "")

        # Prefer current client email over stored sent_to
        client = client_map.get(acct_id)
        if client:
            emails = client.get("emails") or [client["email"]]
        else:
            emails = [e.strip() for e in (row.get("sent_to") or "").split(",") if e.strip()]

        if not emails:
            logger.warning("Send pending: no email for receipt %d (%s)", receipt_id, client_name)
            db.update_receipt_status(receipt_id, "failed", "no email address")
            failed += 1
            continue

        if not pdf_data:
            logger.warning("Send pending: no PDF for receipt %d (%s)", receipt_id, client_name)
            db.update_receipt_status(receipt_id, "failed", "no PDF data")
            failed += 1
            continue

        # Write PDF to temp file
        tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", prefix="send_")
        tmp_pdf.write(pdf_data)
        tmp_pdf.close()

        # Decode ad images from DB
        ad_image_paths = []
        if ad_images_json:
            try:
                for img in _json.loads(ad_images_json):
                    img_tmp = tempfile.NamedTemporaryFile(
                        delete=False, suffix=Path(img["filename"]).suffix, prefix="img_"
                    )
                    img_tmp.write(base64.b64decode(img["data"]))
                    img_tmp.close()
                    ad_image_paths.append(Path(img_tmp.name))
            except Exception as e:
                logger.warning("Send pending: could not decode ad images for %s: %s", client_name, e)

        receipts = [{"pdf_path": tmp_pdf.name, "type": "receipt"}]

        any_sent = False
        for recipient in emails:
            ok = email_svc.send_receipt(
                to_email=recipient,
                client_name=client_name,
                receipts=receipts,
                ad_images=ad_image_paths,
                bcc=admin_email,
            )
            if ok:
                any_sent = True
                logger.info("Send pending: sent to %s <%s>", client_name, recipient)

        # Clean up temp files
        Path(tmp_pdf.name).unlink(missing_ok=True)
        for p in ad_image_paths:
            p.unlink(missing_ok=True)

        if any_sent:
            db.update_receipt_status(receipt_id, "sent")
            sent += 1
        else:
            db.update_receipt_status(receipt_id, "failed", "email send failed")
            failed += 1

    logger.info("Send pending done: %d sent, %d failed", sent, failed)
