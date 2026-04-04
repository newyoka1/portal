"""Comment routes."""
from fastapi import APIRouter, Depends, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from audit import log_action
from auth import require_user
from database import get_db
from models import Comment, User

router = APIRouter()


@router.post("/emails/{email_id}/comments")
def add_comment(
    email_id: int,
    body: str         = Form(...),
    parent_id: str    = Form(""),
    db: Session       = Depends(get_db),
    current_user: User = Depends(require_user),
):
    pid = int(parent_id) if parent_id.strip() else None
    db.add(Comment(
        email_id=email_id,
        user_id=current_user.id,
        body=body.strip(),
        parent_id=pid,
    ))
    log_action(db, email_id=email_id, user_id=current_user.id,
               actor_name=current_user.name, action="comment", detail=body.strip()[:200])
    db.commit()
    return RedirectResponse(f"/emails/{email_id}#comments", status_code=302)
