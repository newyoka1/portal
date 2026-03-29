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

# Categories in display order
CATEGORIES = [
    ("meta",        "Meta / Facebook"),
    ("email",       "Email"),
    ("sftp",        "SFTP / File Storage"),
    ("polling",     "Polling & Automation"),
    ("fb_approval", "FB Ad Approval"),
]


@router.get("", response_class=HTMLResponse)
def settings_page(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rows = db.query(PortalSetting).order_by(PortalSetting.category, PortalSetting.key).all()
    grouped = {}
    for r in rows:
        cat = r.category or "general"
        grouped.setdefault(cat, []).append(r)

    return templates.TemplateResponse(request, "settings.html", {
        "current_user": current_user,
        "categories":   CATEGORIES,
        "grouped":      grouped,
    })


@router.post("", response_class=JSONResponse)
async def save_settings(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Save all settings from the form (JSON body: {key: value, ...})."""
    try:
        body = await request.json()
        updated = 0
        for key, value in body.items():
            row = db.query(PortalSetting).filter(PortalSetting.key == key).first()
            if row:
                row.value = value
                updated += 1
        db.commit()

        # Bust the cache so changes take effect immediately
        import portal_config
        portal_config._cache_ts = 0

        return {"ok": True, "updated": updated}
    except Exception as e:
        db.rollback()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
