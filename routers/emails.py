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
    "sent": "Sent",
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
    # Get subjects for audit before deleting
    emails = db.query(Email).filter(Email.id.in_(ids)).all()
    subjects = {e.id: e.subject for e in emails}
    # Delete children first, then emails
    db.query(Approval).filter(Approval.email_id.in_(ids)).delete(synchronize_session=False)
    db.query(Comment).filter(Comment.email_id.in_(ids)).delete(synchronize_session=False)
    db.query(Email).filter(Email.id.in_(ids)).delete(synchronize_session=False)
    # Audit log after delete (no FK reference, just record what was deleted)
    for eid in ids:
        log_action(db, user_id=current_user.id,
                   actor_name=current_user.name, action="delete",
                   detail=f"Deleted email {eid}: {subjects.get(eid, '?')}")
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

    from models import ClientIntegration

    approvals = email.approvals
    comments  = email.comments
    clients   = db.query(Client).order_by(Client.name).all()

    # Fetch active integrations for this client (for "Push to..." buttons)
    integrations = []
    if email.client_id and email.status == "approved":
        all_intgs = db.query(ClientIntegration).filter_by(
            client_id=email.client_id, enabled=True
        ).all()
        for intg in all_intgs:
            if intg.platform == "hubspot":
                from integrations.hubspot import check_push_access
                if check_push_access(intg.api_key):
                    integrations.append(intg)
            else:
                integrations.append(intg)

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
        "integrations": integrations,
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
    # Generate a shared approval link token (one link for all approvers)
    if not email.share_token:
        email.share_token = secrets.token_urlsafe(32)
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


@router.post("/{email_id}/push/{platform}")
def push_to_platform(
    email_id: int,
    platform: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    """Push an approved email as a draft to an integrated platform."""
    from models import ClientIntegration
    import json as _json

    email = db.query(Email).options(joinedload(Email.client)).filter(Email.id == email_id).first()
    if not email or email.status != "approved" or not email.client_id:
        return RedirectResponse(f"/emails/{email_id}", status_code=302)

    # Find the integration for this client + platform
    intg = db.query(ClientIntegration).filter_by(
        client_id=email.client_id, platform=platform, enabled=True
    ).first()
    if not intg:
        return RedirectResponse(f"/emails/{email_id}?push_error=no_integration", status_code=302)

    config  = _json.loads(intg.extra_config or "{}")
    result  = {"ok": False, "error": "Unknown platform"}

    html = email.html_body or email.clean_html or ""
    subj = email.subject
    fname = email.from_name or (email.client.from_name if email.client else "")
    femail = email.from_address or (email.client.from_email if email.client else "")

    if platform == "mailchimp":
        from integrations.mailchimp import push_draft
        result = push_draft(intg.api_key, subj, fname, femail, html)
    elif platform == "hubspot":
        from integrations.hubspot import push_draft
        result = push_draft(intg.api_key, subj, fname, femail, html)
    elif platform == "campaign_monitor":
        from integrations.campaign_monitor import push_draft
        cm_client_id = config.get("cm_client_id", "")
        if not cm_client_id:
            return RedirectResponse(f"/emails/{email_id}?push_error=no_cm_client_id", status_code=302)
        result = push_draft(intg.api_key, cm_client_id, subj, fname, femail, html)

    if result.get("ok"):
        log_action(db, email_id=email_id, user_id=current_user.id,
                   actor_name=current_user.name, action=f"pushed_{platform}",
                   detail=f"Draft created in {platform}: {_json.dumps(result)}")
        db.commit()
        return RedirectResponse(
            f"/emails/{email_id}?pushed={platform}", status_code=302
        )
    else:
        log_action(db, email_id=email_id, user_id=current_user.id,
                   actor_name=current_user.name, action=f"push_failed_{platform}",
                   detail=result.get("error", "Unknown error"))
        db.commit()
        return RedirectResponse(
            f"/emails/{email_id}?push_error={platform}", status_code=302
        )


@router.post("/{email_id}/send")
def send_approved_email(
    email_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_manager),
):
    """Send an approved composed email to recipients via Gmail."""
    email = db.query(Email).options(joinedload(Email.client)).filter(Email.id == email_id).first()
    if not email or email.status != "approved" or not email.delivered_to:
        return RedirectResponse(f"/emails/{email_id}", status_code=302)

    import base64
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import formataddr
    from portal_config import get_setting
    from gmail_poller import _gmail_service, SCOPES

    recipients = [r.strip() for r in email.delivered_to.split(",") if r.strip()]
    if not recipients:
        return RedirectResponse(f"/emails/{email_id}?send_error=no_recipients", status_code=302)

    gmail_address = get_setting("GMAIL_ADDRESS", "support@politikanyc.com")
    sender_email = email.from_address or gmail_address
    sender_name  = email.from_name or ""
    workspace_domain = gmail_address.split("@")[-1] if gmail_address else ""

    # DWD impersonation only for workspace domain addresses
    can_impersonate = (
        sender_email != gmail_address
        and workspace_domain
        and sender_email.lower().endswith(f"@{workspace_domain}")
    )

    try:
        if can_impersonate:
            from gcp_credentials import build_credentials
            from googleapiclient.discovery import build as build_svc
            creds = build_credentials(SCOPES, sender_email)
            service = build_svc("gmail", "v1", credentials=creds, cache_discovery=False)
        else:
            service = _gmail_service()
    except Exception as exc:
        log_action(db, email_id=email_id, user_id=current_user.id,
                   actor_name=current_user.name, action="send_failed",
                   detail=f"Gmail service error: {exc}")
        db.commit()
        return RedirectResponse(f"/emails/{email_id}?send_error=service", status_code=302)

    sent_count = 0
    for recipient in recipients:
        msg = MIMEMultipart("alternative")
        msg["From"]    = formataddr((sender_name, sender_email)) if sender_name else sender_email
        msg["To"]      = recipient
        msg["Subject"] = email.subject
        msg.attach(MIMEText(email.html_body or email.clean_html or "", "html"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        try:
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            sent_count += 1
        except Exception as exc:
            log_action(db, email_id=email_id, user_id=current_user.id,
                       actor_name=current_user.name, action="send_failed",
                       detail=f"Failed to send to {recipient}: {exc}")

    if sent_count:
        email.status = "sent"
        log_action(db, email_id=email_id, user_id=current_user.id,
                   actor_name=current_user.name, action="sent",
                   detail=f"Sent to {sent_count}/{len(recipients)} recipient(s)")
    db.commit()

    return RedirectResponse(
        f"/emails/{email_id}?sent={sent_count}&total={len(recipients)}", status_code=302
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
