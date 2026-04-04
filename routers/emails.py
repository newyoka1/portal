"""Email queue, detail, assignment, and Gmail poll trigger."""
import os
import secrets
import threading
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from audit import log_action
from auth import require_admin, require_manager, require_user, get_current_user
from database import get_db
from gmail_poller import fetch_and_store_emails
from models import Approval, Client, ClientApprover, Comment, Email, User
from notifier import send_approval_requests
from webhook import fire_webhook

router = APIRouter(prefix="/emails")
templates = Jinja2Templates(directory="templates")

_STATUS_LABELS = {
    "pending": "Pending",
    "in_review": "Awaiting Approval",
    "approved": "Approved",
    "rejected": "Rejected",
    "revision_needed": "Needs Revision",
}
templates.env.filters["status_label"] = lambda v: _STATUS_LABELS.get(v, v.replace("_", " ").title())


@router.get("/poll")
def poll_now(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Manually trigger a Gmail poll (admin only)."""
    count = fetch_and_store_emails()
    return RedirectResponse(f"/emails?polled={count}", status_code=302)


# ── Bulk actions (must be before /{email_id} routes to avoid path conflict) ──
@router.post("/bulk/assign")
def bulk_assign(
    email_ids: str = Form(""),
    client_id: str = Form(""),
    db: Session    = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    """Assign multiple emails to a client at once."""
    ids = [int(x) for x in email_ids.split(",") if x.strip().isdigit()]
    if not ids or not client_id:
        return RedirectResponse("/emails", status_code=302)
    cid = int(client_id)
    approvers = db.query(ClientApprover).filter(ClientApprover.client_id == cid).all()
    for eid in ids:
        email = db.query(Email).filter(Email.id == eid).first()
        if not email:
            continue
        db.query(Approval).filter(Approval.email_id == eid).delete()
        email.client_id = cid
        email.assigned_at = datetime.now(timezone.utc)
        email.status = "in_review"
        for ca in approvers:
            db.add(Approval(
                email_id=eid, user_id=ca.user_id,
                approver_name=ca.approver_name, approver_email=ca.approver_email,
                approver_phone=ca.approver_phone, required=ca.required, decision="pending",
            ))
        log_action(db, email_id=eid, user_id=current_user.id,
                   actor_name=current_user.name, action="assign",
                   detail=f"Bulk assigned to client {cid}")
    db.commit()
    return RedirectResponse("/emails", status_code=302)


@router.post("/bulk/delete")
def bulk_delete(
    email_ids: str = Form(""),
    db: Session    = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Delete multiple emails at once."""
    ids = [int(x) for x in email_ids.split(",") if x.strip().isdigit()]
    if not ids:
        return RedirectResponse("/emails", status_code=302)
    for eid in ids:
        log_action(db, email_id=eid, user_id=current_user.id,
                   actor_name=current_user.name, action="delete", detail="Bulk delete")
    db.query(Approval).filter(Approval.email_id.in_(ids)).delete(synchronize_session=False)
    db.query(Comment).filter(Comment.email_id.in_(ids)).delete(synchronize_session=False)
    db.query(Email).filter(Email.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return RedirectResponse("/emails", status_code=302)


@router.get("/{email_id}/body", response_class=HTMLResponse)
def email_body(
    email_id: int,
    raw: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    """Serve clean email HTML body for iframe — requires login. ?raw=1 for original."""
    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return HTMLResponse("Not found", status_code=404)
    if raw:
        return HTMLResponse(email.html_body or "")
    return HTMLResponse(email.clean_html or email.html_body or "")


@router.get("/{email_id}", response_class=HTMLResponse)
def email_detail(
    email_id: int,
    request: Request,
    notified: str = "",
    sms: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_user),
):
    email = db.query(Email).options(
        joinedload(Email.client), joinedload(Email.approvals), joinedload(Email.comments)
    ).filter(Email.id == email_id).first()
    if not email:
        return RedirectResponse("/emails", status_code=302)

    approvals = email.approvals
    comments  = email.comments
    clients   = db.query(Client).order_by(Client.name).all()

    flash = None
    if notified:
        n = int(notified)
        s = int(sms) if sms else 0
        parts = []
        if n:
            parts.append(f"{n} email(s)")
        if s:
            parts.append(f"{s} SMS")
        if parts:
            flash = {"type": "success", "message": f"Approval request sent: {', '.join(parts)}."}
        else:
            flash = {"type": "warning", "message": "No pending approvers to notify."}

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
        email.assigned_at = datetime.now(timezone.utc)
        email.status      = "in_review"

        # Create an Approval row for each configured approver on this client
        approvers = db.query(ClientApprover).filter(
            ClientApprover.client_id == int(client_id)
        ).all()
        for ca in approvers:
            db.add(Approval(
                email_id=email_id,
                user_id=ca.user_id,
                approver_name=ca.approver_name,
                approver_email=ca.approver_email,
                approver_phone=ca.approver_phone,
                required=ca.required,
                decision="pending",
            ))
    else:
        email.client_id   = None
        email.assigned_at = None
        email.status      = "pending"

    log_action(db, email_id=email_id, user_id=current_user.id,
               actor_name=current_user.name, action="assign",
               detail=f"Assigned to client {client_id}" if client_id else "Unassigned")
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

    if approval and decision in ("approved", "rejected", "revision_needed"):
        approval.decision   = decision
        approval.note       = note.strip()
        approval.decided_at = datetime.now(timezone.utc)
        db.flush()

        # Mirror the note into the comment thread so the team sees it inline
        if note.strip():
            label = {"approved": "Approved", "rejected": "Rejected", "revision_needed": "Needs Revision"}[decision]
            db.add(Comment(
                email_id=email_id,
                user_id=current_user.id,
                body=f"[{label}] {note.strip()}",
            ))

        # Recalculate overall email status
        recalculate_status(email_id, db)
        log_action(db, email_id=email_id, user_id=current_user.id,
                   actor_name=current_user.name, action=decision, detail=note.strip())
        db.commit()

        # Webhook
        email_obj = db.query(Email).filter(Email.id == email_id).first()
        if email_obj:
            fire_webhook({
                "event": "approval_decision",
                "email_id": email_id,
                "email_subject": email_obj.subject,
                "approver": current_user.name,
                "decision": decision,
                "note": note.strip(),
                "final_status": email_obj.status,
            })

    return RedirectResponse(f"/emails/{email_id}", status_code=302)


@router.post("/{email_id}/send-for-approval")
def send_for_approval(
    email_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    """Email all pending approvers a notification with a direct link."""
    email = db.query(Email).options(joinedload(Email.client)).filter(Email.id == email_id).first()
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
    app_url = get_setting("APP_URL", "http://localhost:8000").rstrip("/")

    # Snapshot data for background thread (ORM objects can't cross threads)
    approval_pairs = [
        (a.display_name, a.display_email, a.approver_phone or "", a.token)
        for a in pending_approvals
    ]
    email_snapshot = {
        "id": email.id,
        "subject": email.subject,
        "from_name": email.from_name,
        "from_address": email.from_address,
        "client_name": email.client.name if email.client else "Unassigned",
        "client_from_email": email.client.from_email if email.client else None,
        "client_from_name": email.client.from_name if email.client else None,
        "client_email_template": email.client.email_template if email.client else None,
        "client_sms_template": email.client.sms_template if email.client else None,
    }

    # Set deadline
    deadline_hours = int(get_setting("APPROVAL_DEADLINE_HOURS", "48") or "48")
    email.deadline_at = datetime.now(timezone.utc) + timedelta(hours=deadline_hours)
    email.sent_for_approval_at = datetime.now(timezone.utc)

    log_action(db, email_id=email_id, user_id=current_user.id,
               actor_name=current_user.name, action="send",
               detail=f"Sent to {len(pending_approvals)} approver(s)")
    db.commit()

    # Send in background so the page returns immediately
    def _bg_send():
        from notifier import send_approval_requests_bg
        send_approval_requests_bg(email_snapshot, approval_pairs, app_url)

    threading.Thread(target=_bg_send, daemon=True).start()

    return RedirectResponse(
        f"/emails/{email_id}?notified={len(approval_pairs)}&sms=0", status_code=302
    )


def recalculate_status(email_id: int, db: Session) -> None:
    """Update email.status based on required approver decisions."""
    approvals = db.query(Approval).filter(Approval.email_id == email_id).all()
    required  = [a for a in approvals if a.required]

    email = db.query(Email).filter(Email.id == email_id).first()
    if not email:
        return

    if any(a.decision == "rejected" for a in required):
        email.status = "rejected"
    elif any(a.decision == "revision_needed" for a in required):
        email.status = "revision_needed"
    elif required and all(a.decision == "approved" for a in required):
        email.status = "approved"
    else:
        email.status = "in_review"
