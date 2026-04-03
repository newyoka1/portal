"""Client and approver-assignment routes (admin only)."""
import re
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import require_admin, get_current_user
from database import get_db
from models import Client, ClientApprover, User

router = APIRouter(prefix="/clients")
templates = Jinja2Templates(directory="templates")


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


@router.get("")
def list_clients(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    clients = db.query(Client).order_by(Client.name).all()
    return templates.TemplateResponse(request, "clients.html", {
        "clients": clients,
        "current_user": current_user,
    })


@router.post("")
def create_client(
    name: str = Form(...),
    from_email: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    slug = _slugify(name)
    existing = db.query(Client).filter(Client.slug == slug).first()
    if existing:
        slug = f"{slug}-2"
    db.add(Client(name=name, slug=slug, from_email=from_email.strip() or None))
    db.commit()
    return RedirectResponse("/clients", status_code=302)


ALLOWED_CLIENT_FIELDS = {"from_email", "subject_filter"}


@router.post("/{client_id}/update-field")
def update_client_field(
    client_id: int,
    field: str = Form(...),
    value: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    if field not in ALLOWED_CLIENT_FIELDS:
        return {"ok": False, "error": "Invalid field"}
    client = db.query(Client).filter(Client.id == client_id).first()
    if client:
        setattr(client, field, value.strip() or None)
        db.commit()
    return {"ok": True}


# Keep old endpoint for backward compat (email_settings page)
@router.post("/{client_id}/from-email")
def update_from_email(
    client_id: int,
    from_email: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    client = db.query(Client).filter(Client.id == client_id).first()
    if client:
        client.from_email = from_email.strip() or None
        db.commit()
    return {"ok": True}


@router.post("/{client_id}/delete")
def delete_client(
    client_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    client = db.query(Client).filter(Client.id == client_id).first()
    if client:
        db.delete(client)
        db.commit()
    return RedirectResponse("/clients", status_code=302)


@router.post("/{client_id}/approvers")
def add_approver(
    client_id: int,
    approver_name: str = Form(...),
    approver_email: str = Form(...),
    approver_phone: str = Form(""),
    required: str = Form("1"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    email_addr = approver_email.strip().lower()
    # Avoid duplicates by email
    exists = db.query(ClientApprover).filter_by(
        client_id=client_id, approver_email=email_addr
    ).first()
    if not exists:
        # Check if this email belongs to a portal user — link if so
        portal_user = db.query(User).filter(User.email == email_addr).first()
        db.add(ClientApprover(
            client_id=client_id,
            user_id=portal_user.id if portal_user else None,
            approver_name=approver_name.strip(),
            approver_email=email_addr,
            approver_phone=approver_phone.strip() or None,
            required=required == "1",
        ))
        db.commit()
    return RedirectResponse("/clients", status_code=302)


@router.post("/{client_id}/approvers/{ca_id}/edit")
def edit_approver(
    client_id: int,
    ca_id: int,
    approver_name: str = Form(...),
    approver_email: str = Form(...),
    approver_phone: str = Form(""),
    required: str = Form("1"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    ca = db.query(ClientApprover).filter(
        ClientApprover.id == ca_id,
        ClientApprover.client_id == client_id,
    ).first()
    if ca:
        email_addr = approver_email.strip().lower()
        portal_user = db.query(User).filter(User.email == email_addr).first()
        ca.approver_name  = approver_name.strip()
        ca.approver_email = email_addr
        ca.approver_phone = approver_phone.strip() or None
        ca.required       = required == "1"
        ca.user_id        = portal_user.id if portal_user else None
        db.commit()
    return RedirectResponse("/clients", status_code=302)


@router.post("/{client_id}/approvers/{ca_id}/delete")
def remove_approver(
    client_id: int,
    ca_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    ca = db.query(ClientApprover).filter(
        ClientApprover.id == ca_id,
        ClientApprover.client_id == client_id,
    ).first()
    if ca:
        db.delete(ca)
        db.commit()
    return RedirectResponse("/clients", status_code=302)
