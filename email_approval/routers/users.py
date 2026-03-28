"""User management routes (admin only)."""
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import hash_password, require_admin, get_current_user
from database import get_db
from models import User

router = APIRouter(prefix="/users")
templates = Jinja2Templates(directory="templates")


@router.get("")
def list_users(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    users = db.query(User).order_by(User.name).all()
    return templates.TemplateResponse(request, "users.html", {
        "users": users,
        "current_user": current_user,
    })


@router.post("")
def create_user(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    is_admin: str = Form(default=""),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        return RedirectResponse("/users?error=email_taken", status_code=302)
    db.add(User(
        name=name,
        email=email,
        password_hash=hash_password(password),
        is_admin=bool(is_admin),
    ))
    db.commit()
    return RedirectResponse("/users", status_code=302)


@router.post("/{user_id}/delete")
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if user and user.id != current_user.id:
        db.delete(user)
        db.commit()
    return RedirectResponse("/users", status_code=302)
