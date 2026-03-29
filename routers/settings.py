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
    ("meta",        "Meta / Facebook"),
    ("email",       "Email"),
    ("sftp",        "SFTP / File Storage"),
    ("fb_approval", "FB Ad Approval"),
    ("voter",       "Voter & Research"),
]

# Which DB categories each section filter shows
_SECTION_CATS = {
    "email":    ["email"],
    "facebook": ["meta", "sftp", "fb_approval"],
    "voter":    ["voter"],
}

_SECTION_TITLES = {
    "email":    "Email Approval Settings",
    "facebook": "Facebook Settings",
    "voter":    "Voter & Research Settings",
}


@router.get("", response_class=HTMLResponse)
def settings_page(
    request: Request,
    cat: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rows = db.query(PortalSetting).order_by(PortalSetting.category, PortalSetting.key).all()
    grouped = {}
    for r in rows:
        grouped.setdefault(r.category or "general", []).append(r)

    # Filter categories to the requested section; show all when no filter
    active_cats = _SECTION_CATS.get(cat, [c for c, _ in CATEGORIES])
    visible_categories = [(k, label) for k, label in CATEGORIES if k in active_cats]
    page_title = _SECTION_TITLES.get(cat, "Portal Settings")

    return templates.TemplateResponse(request, "settings.html", {
        "current_user":       current_user,
        "categories":         visible_categories,
        "grouped":            grouped,
        "page_title":         page_title,
        "active_cat":         cat,
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
