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
    users   = db.query(User).order_by(User.name).all()
    return templates.TemplateResponse(request, "clients.html", {
        "clients": clients,
        "users": users,
        "current_user": current_user,
    })


@router.post("")
def create_client(
    name: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    slug = _slugify(name)
    # Ensure slug uniqueness
    existing = db.query(Client).filter(Client.slug == slug).first()
    if existing:
        slug = f"{slug}-2"
    db.add(Client(name=name, slug=slug))
    db.commit()
    return RedirectResponse("/clients", status_code=302)


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
    user_id: int = Form(...),
    required: str = Form("1"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    # Avoid duplicates
    exists = db.query(ClientApprover).filter_by(
        client_id=client_id, user_id=user_id
    ).first()
    if not exists:
        db.add(ClientApprover(
            client_id=client_id,
            user_id=user_id,
            required=required == "1",
        ))
        db.commit()
    return RedirectResponse("/clients", status_code=302)


@router.post("/{client_id}/approvers/batch")
async def add_approvers_batch(
    client_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    form     = await request.form()
    user_ids = form.getlist("user_ids")
    for uid in user_ids:
        uid = int(uid)
        exists = db.query(ClientApprover).filter_by(
            client_id=client_id, user_id=uid
        ).first()
        if not exists:
            required = form.get(f"required_{uid}", "1") == "1"
            db.add(ClientApprover(client_id=client_id, user_id=uid, required=required))
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
