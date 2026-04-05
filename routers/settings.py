"""Portal Settings — admin-only page for managing all configurable values."""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import PortalSetting, User

router = APIRouter(prefix="/settings")
templates = Jinja2Templates(directory="templates")

# All categories in display order
CATEGORIES = [
    ("meta",           "Facebook"),
    ("email",          "Email"),
    ("email_approval", "Email Approval"),
    ("twilio",         "Twilio SMS"),
    ("fb_approval",    "Facebook Ad Approval"),
    ("voter",          "Voter"),
    ("ai",             "AI / Claude"),
]

# Dynamic per-platform token rows — FB ad accounts only (CRM keys moved to client integrations)
_DYNAMIC_PREFIXES = ("FB_ACCESS_TOKEN_", "FB_AD_ACCOUNT_ID_")


@router.get("", response_class=HTMLResponse)
def settings_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rows = db.query(PortalSetting).order_by(PortalSetting.category, PortalSetting.key).all()
    grouped = {}
    for r in rows:
        # Skip old global CRM keys (now stored as client integrations)
        if any(r.key.startswith(p) for p in ("HUBSPOT_TOKEN_", "CM_API_KEY_", "MAILCHIMP_KEY_")):
            continue
        grouped.setdefault(r.category or "general", []).append(r)

    return templates.TemplateResponse(request, "settings.html", {
        "current_user":    current_user,
        "categories":      CATEGORIES,
        "grouped":         grouped,
        "page_title":      "Settings",
    })


@router.post("", response_class=JSONResponse)
async def save_settings(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Save settings. Updates existing rows; inserts new rows for dynamic-prefix keys."""
    try:
        body = await request.json()
        updated = 0
        for key, value in body.items():
            row = db.query(PortalSetting).filter(PortalSetting.key == key).first()
            if row:
                row.value = value
                updated += 1
            else:
                # Insert new row for dynamic FB ad account keys
                if key.startswith(("FB_ACCESS_TOKEN_", "FB_AD_ACCOUNT_ID_")) and value:
                    db.add(PortalSetting(
                        key=key,
                        value=value,
                        label="Facebook",
                        category="meta",
                        is_secret=key.startswith("FB_ACCESS_TOKEN_"),
                    ))
                    updated += 1
        db.commit()

        import portal_config
        portal_config._cache_ts = 0

        return {"ok": True, "updated": updated}
    except Exception as e:
        db.rollback()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/test-sms", response_class=JSONResponse)
async def test_sms(
    request: Request,
    current_user: User = Depends(require_admin),
):
    """Send a test SMS to verify Twilio credentials."""
    body = await request.json()
    to = body.get("to", "").strip()
    if not to:
        return JSONResponse({"ok": False, "error": "No phone number provided"}, status_code=400)

    # Auto-prepend +1 for bare US numbers
    if not to.startswith("+"):
        to = "+1" + to.lstrip("1")

    from notifier import _send_sms
    ok = _send_sms(to=to, body="Politika Portal — Twilio test SMS. Your connection is working!")
    if ok:
        return {"ok": True}
    else:
        return JSONResponse(
            {"ok": False, "error": "SMS failed — check your Twilio credentials in the fields above and click Save All first."},
            status_code=400,
        )


@router.delete("/{key}", response_class=JSONResponse)
def delete_setting(
    key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Delete a dynamic setting row (FB ad accounts)."""
    if not any(key.startswith(p) for p in _DYNAMIC_PREFIXES):
        return JSONResponse({"ok": False, "error": "Not deletable"}, status_code=400)
    row = db.query(PortalSetting).filter(PortalSetting.key == key).first()
    if row:
        db.delete(row)
        db.commit()
        import portal_config
        portal_config._cache_ts = 0
    return {"ok": True}
