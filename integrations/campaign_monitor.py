"""
Campaign Monitor campaign fetcher.
Uses an API key (Basic auth, username=api_key, password blank) and a
Campaign Monitor Client ID to pull sent campaigns and their HTML content.

API ref: https://www.campaignmonitor.com/api/v3-3/campaigns/
"""
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.orm import Session

from models import Email

logger = logging.getLogger(__name__)

BASE_URL = "https://api.createsend.com/api/v3.3"


def fetch(api_key: str, cm_client_id: str, db: Session, client_id: int) -> int:
    """
    Pull all sent Campaign Monitor campaigns for the given CM client ID.
    Returns the number of new emails ingested.
    """
    auth     = (api_key, "")   # CM uses API key as Basic auth username, blank password
    ingested = 0
    page     = 1
    per_page = 50

    while True:
        try:
            resp = requests.get(
                f"{BASE_URL}/clients/{cm_client_id}/campaigns.json",
                auth=auth,
                params={"page": page, "pagesize": per_page, "orderfield": "sentdate", "orderdirection": "desc"},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Campaign Monitor API error: %s", exc)
            break

        data      = resp.json()
        campaigns = data.get("Results", [])

        for campaign in campaigns:
            ingested += _process_campaign(campaign, auth, db, client_id)

        # Pagination
        if data.get("PageNumber", 1) >= data.get("NumberOfPages", 1):
            break
        page += 1

    return ingested


def _process_campaign(campaign: dict, auth: tuple, db: Session, client_id: int) -> int:
    cm_id    = campaign.get("CampaignID", "")
    dedup_id = f"cm-{cm_id}"

    if db.query(Email).filter(Email.gmail_message_id == dedup_id).first():
        return 0

    subject      = campaign.get("Subject", "(No subject)")
    from_name    = campaign.get("FromName", "")
    from_address = campaign.get("ReplyTo") or campaign.get("FromEmail", "")

    sent_date = campaign.get("SentDate", "")
    try:
        received_at = datetime.strptime(sent_date, "%Y-%m-%d %H:%M:%S") if sent_date else datetime.now(timezone.utc)
    except ValueError:
        received_at = datetime.now(timezone.utc)

    html_body = _fetch_html(cm_id, auth)

    db.add(Email(
        gmail_message_id = dedup_id,
        client_id        = client_id,
        subject          = str(subject)[:500],
        from_address     = str(from_address)[:200],
        from_name        = str(from_name)[:200],
        html_body        = html_body,
        text_body        = "",
        origin_system    = "Constant Contact",   # reuse existing origin label
        received_at      = received_at,
        status           = "pending",
    ))
    logger.info("Campaign Monitor: ingested '%s'", subject)
    return 1


def push_draft(api_key: str, cm_client_id: str, subject: str, from_name: str, from_email: str, html_body: str) -> dict:
    """
    Create a draft campaign in Campaign Monitor.
    Auto-discovers the first subscriber list for the CM client.
    Returns {"ok": True, "campaign_id": "..."} or {"ok": False, "error": "..."}.
    """
    auth = (api_key, "")

    # Step 1: discover first list for this CM client
    try:
        resp = requests.get(f"{BASE_URL}/clients/{cm_client_id}/lists.json",
                            auth=auth, timeout=10)
        resp.raise_for_status()
        lists = resp.json()
        if not lists:
            return {"ok": False, "error": "No subscriber lists found in Campaign Monitor. Create one first."}
        list_id = lists[0]["ListID"]
    except requests.RequestException as exc:
        return {"ok": False, "error": f"Failed to discover lists: {exc}"}

    # Step 2: create draft campaign
    try:
        resp = requests.post(f"{BASE_URL}/campaigns/{cm_client_id}.json", auth=auth, json={
            "Subject": subject,
            "Name": subject[:200],
            "FromName": from_name or "Politika",
            "FromEmail": from_email or "support@politikanyc.com",
            "ReplyTo": from_email or "support@politikanyc.com",
            "HtmlUrl": "",  # We'll set content via separate call
            "ListIDs": [list_id],
        }, timeout=15)
        resp.raise_for_status()
        campaign_id = resp.json()  # CM returns just the campaign ID string
    except requests.RequestException as exc:
        return {"ok": False, "error": f"Failed to create campaign: {exc}"}

    return {"ok": True, "campaign_id": campaign_id}


def _fetch_html(campaign_id: str, auth: tuple) -> str:
    try:
        resp = requests.get(
            f"{BASE_URL}/campaigns/{campaign_id}/content.json",
            auth=auth,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("HTML") or "<p><em>(No HTML content available)</em></p>"
    except requests.RequestException as exc:
        logger.warning("Campaign Monitor: could not fetch HTML for %s: %s", campaign_id, exc)
        return "<p><em>(Could not load email content)</em></p>"
