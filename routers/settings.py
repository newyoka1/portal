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

# Dynamic per-platform token rows (created/deleted on demand)
VOTER_PLATFORMS = [
    {"prefix": "HUBSPOT_TOKEN_", "label": "HubSpot",          "is_secret": True},
    {"prefix": "CM_API_KEY_",    "label": "Campaign Monitor",  "is_secret": True},
    {"prefix": "MAILCHIMP_KEY_", "label": "Mailchimp",         "is_secret": True},
]
_DYNAMIC_PREFIXES = tuple(p["prefix"] for p in VOTER_PLATFORMS)


@router.get("", response_class=HTMLResponse)
def settings_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rows = db.query(PortalSetting).order_by(PortalSetting.category, PortalSetting.key).all()
    grouped = {}
    for r in rows:
        grouped.setdefault(r.category or "general", []).append(r)

    # Build per-platform token groups for the voter section
    voter_platforms = []
    for plat in VOTER_PLATFORMS:
        prefix = plat["prefix"]
        voter_platforms.append({
            **plat,
            "rows": sorted(
                [r for r in rows if r.key.startswith(prefix)],
                key=lambda r: r.key,
            ),
        })

    return templates.TemplateResponse(request, "settings.html", {
        "current_user":    current_user,
        "categories":      CATEGORIES,
        "grouped":         grouped,
        "voter_platforms": voter_platforms,
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
                # Insert new row for dynamic voter-platform keys
                for plat in VOTER_PLATFORMS:
                    if key.startswith(plat["prefix"]) and value:
                        db.add(PortalSetting(
                            key=key,
                            value=value,
                            label=plat["label"],
                            category="voter",
                            is_secret=plat["is_secret"],
                        ))
                        updated += 1
                        break
        db.commit()

        import portal_config
        portal_config._cache_ts = 0

        return {"ok": True, "updated": updated}
    except Exception as e:
        db.rollback()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.delete("/{key}", response_class=JSONResponse)
def delete_setting(
    key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Delete a dynamic voter-platform token row."""
    if not any(key.startswith(p) for p in _DYNAMIC_PREFIXES):
        return JSONResponse({"ok": False, "error": "Not deletable"}, status_code=400)
    row = db.query(PortalSetting).filter(PortalSetting.key == key).first()
    if row:
        db.delete(row)
        db.commit()
        import portal_config
        portal_config._cache_ts = 0
    return {"ok": True}
