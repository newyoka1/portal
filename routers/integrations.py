"""Per-client CRM integration routes (admin only).

Each client can have HubSpot, Mailchimp, and/or Campaign Monitor API keys.
These keys are used by the Voter Pipeline CRM sync (nightly or manual) to
pull contacts/subscribers into the unified CRM database.
"""
import json
import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import Client, ClientIntegration, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/clients/{client_id}/integrations")
templates = Jinja2Templates(directory="templates")

PLATFORMS = ["hubspot", "mailchimp", "campaign_monitor"]


@router.get("")
def integrations_page(
    client_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        return RedirectResponse("/clients", status_code=302)

    integrations = db.query(ClientIntegration).filter(
        ClientIntegration.client_id == client_id
    ).all()

    # Parse extra_config JSON for template
    for i in integrations:
        i._config = json.loads(i.extra_config or "{}")

    return templates.TemplateResponse(request, "integrations.html", {
        "client":       client,
        "integrations": integrations,
        "platforms":    PLATFORMS,
        "current_user": current_user,
    })


@router.post("/add")
def add_integration(
    client_id: int,
    platform:      str = Form(...),
    api_key:       str = Form(...),
    cm_client_id:  str = Form(""),   # Campaign Monitor only
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    if platform not in PLATFORMS:
        return RedirectResponse(f"/clients/{client_id}/integrations", status_code=302)

    # Build extra_config
    extra: dict = {}
    if platform == "mailchimp":
        parts = api_key.strip().split("-")
        extra["data_center"] = parts[-1] if len(parts) > 1 else "us1"
    elif platform == "campaign_monitor":
        extra["cm_client_id"] = cm_client_id.strip()

    # Replace existing integration for same platform
    existing = db.query(ClientIntegration).filter_by(
        client_id=client_id, platform=platform
    ).first()
    if existing:
        existing.api_key       = api_key.strip()
        existing.extra_config  = json.dumps(extra)
        existing.enabled       = True
    else:
        db.add(ClientIntegration(
            client_id    = client_id,
            platform     = platform,
            api_key      = api_key.strip(),
            extra_config = json.dumps(extra),
            enabled      = True,
        ))

    db.commit()
    return RedirectResponse(f"/clients/{client_id}/integrations", status_code=302)


@router.post("/{integration_id}/delete")
def delete_integration(
    client_id: int,
    integration_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    integration = db.query(ClientIntegration).filter(
        ClientIntegration.id == integration_id,
        ClientIntegration.client_id == client_id,
    ).first()
    if integration:
        db.delete(integration)
        db.commit()
    return RedirectResponse(f"/clients/{client_id}/integrations", status_code=302)


