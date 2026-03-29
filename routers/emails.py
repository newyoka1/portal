"""Email queue, detail, assignment, and Gmail poll trigger."""
import os
import secrets
from datetime import datetime
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import require_admin, require_user, get_current_user
from database import get_db
from gmail_poller import fetch_and_store_emails
from models import Approval, Client, ClientApprover, Comment, Email, User
from notifier import send_approval_requests

router = APIRouter(prefix="/emails")
templates = Jinja2Templates(directory="templates")


@router.get("/poll")
def poll_now(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Manually trigger a Gmail poll (admin only)."""
    count = fetch_and_store_emails()
    return RedirectResponse(f"/emails?polled={count}", status_code=302)


@router.get("/{email_id}", response_class=HTMLResponse)
def email_detail(
    email_id: int,
    request: Request,
    notified: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return RedirectResponse("/emails", status_code=302)

    approvals = db.query(Approval).filter(Approval.email_id == email_id).all()
    comments  = email.comments  # ordered by created_at via model
    clients   = db.query(Client).order_by(Client.name).all()

    flash = None
    if notified:
        n = int(notified)
        flash = {
            "type": "success" if n else "warning",
            "message": f"Approval request sent to {n} approver(s)." if n
                       else "No pending approvers to notify.",
        }

    return templates.TemplateResponse(request, "email_detail.html", {
        "email":        email,
        "approvals":    approvals,
        "comments":     comments,
        "clients":      clients,
        "current_user": current_user,
        "flash":        flash,
    })


@router.post("/{email_id}/delete")
def delete_email(
    email_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    email = db.query(Email).filter(Email.id == email_id).first()
    if email:
        db.delete(email)
        db.commit()
    return RedirectResponse("/", status_code=302)


@router.post("/{email_id}/assign")
def assign_email(
    email_id: int,
    client_id: str = Form(""),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return RedirectResponse("/emails", status_code=302)

    # Clear old approvals before reassigning
    db.query(Approval).filter(Approval.email_id == email_id).delete()

    if client_id:
        email.client_id   = int(client_id)
        email.assigned_at = datetime.utcnow()
        email.status      = "in_review"

        # Create an Approval row for each configured approver on this client
        approvers = db.query(ClientApprover).filter(
            ClientApprover.client_id == int(client_id)
        ).all()
        for ca in approvers:
            db.add(Approval(
                email_id=email_id,
                user_id=ca.user_id,
                required=ca.required,
                decision="pending",
            ))
    else:
        email.client_id   = None
        email.assigned_at = None
        email.status      = "pending"

    db.commit()
    return RedirectResponse(f"/emails/{email_id}", status_code=302)


@router.post("/{email_id}/approve")
def vote(
    email_id: int,
    approval_id: int = Form(...),
    decision: str    = Form(...),
    note: str        = Form(""),
    db: Session      = Depends(get_db),
    current_user: User = Depends(require_user),
):
    approval = db.query(Approval).filter(
        Approval.id       == approval_id,
        Approval.email_id == email_id,
        Approval.user_id  == current_user.id,
    ).first()

    if approval and decision in ("approved", "rejected"):
        approval.decision   = decision
        approval.note       = note.strip()
        approval.decided_at = datetime.utcnow()
        db.flush()

        # Mirror the note into the comment thread so the team sees it inline
        if note.strip():
            label = "Approved" if decision == "approved" else "Rejected"
            db.add(Comment(
                email_id=email_id,
                user_id=current_user.id,
                body=f"[{label}] {note.strip()}",
            ))

        # Recalculate overall email status
        _recalculate_status(email_id, db)
        db.commit()

    return RedirectResponse(f"/emails/{email_id}", status_code=302)


@router.post("/{email_id}/send-for-approval")
def send_for_approval(
    email_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Email all pending approvers a notification with a direct link."""
    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return RedirectResponse("/emails", status_code=302)

    pending_approvals = db.query(Approval).filter(
        Approval.email_id == email_id,
        Approval.decision == "pending",
    ).all()

    # Generate a fresh token for each pending approval
    for appr in pending_approvals:
        appr.token = secrets.token_urlsafe(32)
    db.flush()

    from portal_config import get_setting
    app_url        = get_setting("APP_URL", "http://localhost:8000").rstrip("/")
    approval_pairs = [(a.user, a.token) for a in pending_approvals]
    sent           = send_approval_requests(email, approval_pairs, app_url)

    email.sent_for_approval_at = datetime.utcnow()
    db.commit()

    return RedirectResponse(
        f"/emails/{email_id}?notified={sent}", status_code=302
    )


def _recalculate_status(email_id: int, db: Session) -> None:
    """Update email.status based on required approver decisions."""
    approvals = db.query(Approval).filter(Approval.email_id == email_id).all()
    required  = [a for a in approvals if a.required]

    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return

    if any(a.decision == "rejected" for a in required):
        email.status = "rejected"
    elif required and all(a.decision == "approved" for a in required):
        email.status = "approved"
    else:
        email.status = "in_review"
