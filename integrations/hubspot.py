"""
HubSpot Marketing Email fetcher.
Pulls DRAFT and SCHEDULED emails into the approval queue.
Uses a Private App token (Bearer auth).

API ref: https://developers.hubspot.com/docs/api/marketing/marketing-emails
"""
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.orm import Session

from models import Email

logger = logging.getLogger(__name__)

V3_URL  = "https://api.hubapi.com/marketing/v3/emails"
CRM_URL = "https://api.hubapi.com/crm/v3/objects/marketing_emails"

# Pull drafts + scheduled — these need approval before sending.
FETCH_STATES = ["DRAFT", "SCHEDULED"]


def fetch(api_key: str, db: Session, client_id: int) -> int:
    """
    Pull DRAFT and SCHEDULED HubSpot marketing emails.
    Tries v3 API first; falls back to v1 if scopes are insufficient.
    Returns the number of new emails ingested.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Try v3 first; fall back to CRM objects API if scope is insufficient
    probe = requests.get(V3_URL, headers=headers, params={"limit": 1}, timeout=10)
    if probe.status_code == 403:
        logger.info("HubSpot v3 scope insufficient — falling back to CRM objects API")
        return _fetch_crm(headers, db, client_id)

    ingested = 0
    for state in FETCH_STATES:
        ingested += _fetch_v3_by_state(state, headers, db, client_id)
    return ingested


def _fetch_v3_by_state(state: str, headers: dict, db: Session, client_id: int) -> int:
    ingested = 0
    after    = None

    while True:
        params = {"limit": 50, "state": state}
        if after:
            params["after"] = after

        try:
            resp = requests.get(V3_URL, headers=headers, params=params, timeout=15)
        except requests.RequestException as exc:
            logger.error("HubSpot v3 API error (state=%s): %s", state, exc)
            break

        if not resp.ok:
            logger.error("HubSpot v3 rejected (state=%s): %s", state, resp.text[:300])
            break

        data      = resp.json()
        campaigns = data.get("results", [])
        logger.info("HubSpot v3 [%s]: %d result(s)", state, len(campaigns))

        for campaign in campaigns:
            ingested += _process_campaign(campaign, state, headers, db, client_id)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return ingested


def _fetch_crm(headers: dict, db: Session, client_id: int) -> int:
    """
    CRM Objects API for marketing emails — uses crm.objects.marketing_emails.read
    scope which is available without Marketing Hub.
    Fetches all properties and filters to DRAFT/SCHEDULED client-side.
    """
    ingested = 0
    after    = None

    # Request all useful properties explicitly
    properties = "hs_name,hs_subject,hs_from_name,hs_reply_to_email,hs_email_html,hs_email_status,hs_updated_at,hs_created_at"

    while True:
        params = {"limit": 50, "properties": properties}
        if after:
            params["after"] = after

        try:
            resp = requests.get(CRM_URL, headers=headers, params=params, timeout=15)
        except requests.RequestException as exc:
            logger.error("HubSpot CRM API error: %s", exc)
            break

        logger.info("HubSpot CRM HTTP %s — %s", resp.status_code, resp.text[:500])

        if not resp.ok:
            logger.error("HubSpot CRM rejected: %s", resp.text[:300])
            break

        data    = resp.json()
        results = data.get("results", [])
        logger.info("HubSpot CRM: %d email(s) returned", len(results))

        for item in results:
            props = item.get("properties", {})
            state = str(props.get("hs_email_status", "")).upper()
            if state in ("DRAFT", "SCHEDULED"):
                ingested += _process_crm_campaign(item, state, db, client_id)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return ingested


def _process_crm_campaign(item: dict, state: str, db: Session, client_id: int) -> int:
    hs_id    = str(item.get("id", ""))
    dedup_id = f"hubspot-{hs_id}"
    props    = item.get("properties", {})

    subject      = props.get("hs_name") or props.get("hs_subject") or "(No subject)"
    from_name    = props.get("hs_from_name", "")
    from_address = props.get("hs_reply_to_email", "")
    html_body    = props.get("hs_email_html") or "<p><em>(No content yet)</em></p>"

    ts = props.get("hs_updated_at") or props.get("hs_created_at")
    try:
        received_at = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None) if ts else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        received_at = datetime.now(timezone.utc)

    existing = db.query(Email).filter(Email.gmail_message_id == dedup_id).first()
    if existing:
        existing.subject      = str(subject)[:500]
        existing.html_body    = html_body
        existing.from_name    = str(from_name)[:200]
        existing.from_address = str(from_address)[:200]
        logger.info("HubSpot CRM: updated '%s'", subject)
        return 0

    db.add(Email(
        gmail_message_id = dedup_id,
        client_id        = client_id,
        subject          = str(subject)[:500],
        from_address     = str(from_address)[:200],
        from_name        = str(from_name)[:200],
        html_body        = html_body,
        text_body        = "",
        origin_system    = "HubSpot",
        received_at      = received_at,
        status           = "pending",
    ))
    logger.info("HubSpot CRM: ingested %s draft '%s'", state, subject)
    return 1


def _process_campaign(campaign: dict, state: str, headers: dict, db: Session, client_id: int) -> int:
    hs_id    = str(campaign.get("id", ""))
    dedup_id = f"hubspot-{hs_id}"

    subject      = campaign.get("name", "(No subject)")
    from_name    = (campaign.get("fromName") or "").strip()
    from_address = (campaign.get("replyTo") or
                    campaign.get("from", {}).get("fromEmail", "")).strip()

    html_body = _fetch_html(hs_id, headers)

    # Use updatedAt for drafts so re-syncing reflects edits made in HubSpot
    ts = campaign.get("updatedAt") or campaign.get("createdAt")
    try:
        received_at = datetime.utcfromtimestamp(int(ts) / 1000) if ts else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        received_at = datetime.now(timezone.utc)

    existing = db.query(Email).filter(Email.gmail_message_id == dedup_id).first()

    if existing:
        # Update HTML + subject if the draft was edited in HubSpot since last sync
        existing.subject   = str(subject)[:500]
        existing.html_body = html_body
        existing.from_name    = str(from_name)[:200]
        existing.from_address = str(from_address)[:200]
        logger.info("HubSpot: updated draft '%s'", subject)
        return 0   # not a new ingest
    else:
        db.add(Email(
            gmail_message_id = dedup_id,
            client_id        = client_id,
            subject          = str(subject)[:500],
            from_address     = str(from_address)[:200],
            from_name        = str(from_name)[:200],
            html_body        = html_body,
            text_body        = "",
            origin_system    = "HubSpot",
            received_at      = received_at,
            status           = "pending",
        ))
        logger.info("HubSpot: ingested %s draft '%s'", state, subject)
        return 1


def _fetch_html(campaign_id: str, headers: dict) -> str:
    """
    Fetch the HTML body for a draft email.
    Drafts won't have renderedHtmlBody (tokens not substituted yet) —
    htmlBody is the raw template which still renders correctly for review.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/{campaign_id}",
            headers=headers,
            params={"includeRenderedContent": "true"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return (
            data.get("renderedHtmlBody")   # set only for published/sent
            or data.get("htmlBody")        # raw template — available for drafts
            or data.get("content", {}).get("body", "")
            or "<p><em>(No HTML content — email may still be empty in HubSpot)</em></p>"
        )
    except requests.RequestException as exc:
        logger.warning("HubSpot: could not fetch HTML for %s: %s", campaign_id, exc)
        return "<p><em>(Could not load email content)</em></p>"
