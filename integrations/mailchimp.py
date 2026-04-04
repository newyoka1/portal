"""
Mailchimp campaign fetcher.
Uses an API key (format: key-us1, where 'us1' is the data center prefix).
Pulls DRAFT campaigns only — acts as a pre-send approval gate.

API ref: https://mailchimp.com/developer/marketing/api/campaigns/
"""
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.orm import Session

from models import Email

logger = logging.getLogger(__name__)


def _data_center(api_key: str) -> str:
    """Extract data center prefix from API key (e.g. 'abc123-us6' → 'us6')."""
    parts = api_key.strip().split("-")
    return parts[-1] if len(parts) > 1 else "us1"


def fetch(api_key: str, db: Session, client_id: int) -> int:
    """
    Pull draft Mailchimp campaigns and insert new ones into the approval queue.
    Only drafts (status='save') are fetched — sent campaigns are ignored.
    Returns the number of new emails ingested.
    """
    dc      = _data_center(api_key)
    base    = f"https://{dc}.api.mailchimp.com/3.0"
    auth    = ("anystring", api_key)   # Mailchimp ignores the username
    ingested = 0
    offset   = 0
    count    = 50

    while True:
        try:
            resp = requests.get(
                f"{base}/campaigns",
                auth=auth,
                params={
                    "status": "save",
                    "count":  count,
                    "offset": offset,
                    "fields": "campaigns.id,campaigns.settings,campaigns.send_time,campaigns.reply_to,total_items",
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Mailchimp API error: %s", exc)
            break

        data        = resp.json()
        campaigns   = data.get("campaigns", [])
        total_items = data.get("total_items", 0)

        for campaign in campaigns:
            ingested += _process_campaign(campaign, base, auth, db, client_id)

        offset += count
        if offset >= total_items:
            break

    return ingested


def _process_campaign(campaign: dict, base: str, auth: tuple, db: Session, client_id: int) -> int:
    mc_id    = campaign.get("id", "")
    dedup_id = f"mailchimp-{mc_id}"

    if db.query(Email).filter(Email.gmail_message_id == dedup_id).first():
        return 0

    settings     = campaign.get("settings", {})
    subject      = settings.get("subject_line") or settings.get("title") or "(No subject)"
    from_name    = settings.get("from_name", "")
    from_address = campaign.get("reply_to") or settings.get("reply_to", "")

    send_time = campaign.get("send_time", "")
    try:
        received_at = datetime.strptime(send_time, "%Y-%m-%dT%H:%M:%S+00:00") if send_time else datetime.now(timezone.utc)
    except ValueError:
        received_at = datetime.now(timezone.utc)

    html_body = _fetch_html(mc_id, base, auth)

    db.add(Email(
        gmail_message_id = dedup_id,
        client_id        = client_id,
        subject          = str(subject)[:500],
        from_address     = str(from_address)[:200],
        from_name        = str(from_name)[:200],
        html_body        = html_body,
        text_body        = "",
        origin_system    = "Mailchimp",
        received_at      = received_at,
        status           = "pending",
    ))
    logger.info("Mailchimp: ingested '%s'", subject)
    return 1


def _fetch_html(campaign_id: str, base: str, auth: tuple) -> str:
    try:
        resp = requests.get(
            f"{base}/campaigns/{campaign_id}/content",
            auth=auth,
            params={"fields": "html"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("html") or "<p><em>(No HTML content available)</em></p>"
    except requests.RequestException as exc:
        logger.warning("Mailchimp: could not fetch HTML for %s: %s", campaign_id, exc)
        return "<p><em>(Could not load email content)</em></p>"
