"""
Facebook Ad Approval — FastAPI router (migrated from Flask fb_ad_approval/app.py).
"""
import json, os, secrets, sys, logging
from pathlib import Path
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Form, Query, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import require_user, require_admin, get_current_user
from models import User

logger = logging.getLogger(__name__)

router    = APIRouter(prefix="/fb")
templates = Jinja2Templates(directory="templates")

# ── Import core helpers ────────────────────────────────────────────────────
PORTAL_DIR = Path(__file__).parent.parent
FB_DIR     = PORTAL_DIR / "fb_ad_approval"
sys.path.insert(0, str(PORTAL_DIR))

from fb_ad_approval.core import (
    _db_list, _db_save, _db_delete, _db_delete_where, _db_find_by, _db_update, _db_get_all,
    get_setting, now_iso, gen_id, safe_url, safe_brand_color,
    _read_stored_fb_token, _write_stored_fb_token, _clear_stored_fb_token,
    meta_get, meta_get_all, _init_meta, _cached_sdk, _sdk_cursor_to_list, _meta_cache,
    meta_launch_campaign, meta_launch_adset, meta_launch_ad,
    build_targeting_spec, _fb_error_detail, _send_email,
    AD_PLACEMENTS, CTA_LABELS, CLIENT_COLORS, ACCOUNT_STATUS,
    OBJECTIVE_DEFAULTS, OBJECTIVE_VALID_GOALS, AUTOBID_ONLY_GOALS, NO_COST_CAP_GOALS,
    LEAD_FORM_QUESTION_TYPES, UPLOAD_FOLDER, ALLOWED_MEDIA, ALLOWED_IMAGES, ALLOWED_VIDEOS,
    FB_IMAGE_MAX_SIZE, FB_VIDEO_MAX_SIZE,
    META_GRAPH, GMAIL_SENDER,
)

import requests as http_requests
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign as FBCampaign
from facebook_business.adobjects.adset import AdSet as FBAdSet
from facebook_business.adobjects.ad import Ad as FBAd
from facebook_business.adobjects.user import User as FBUser
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.targetingsearch import TargetingSearch

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
FB_APP_ID         = os.getenv("FB_APP_ID", "")
FB_APP_SECRET     = os.getenv("FB_APP_SECRET", "")
BASE_URL          = os.getenv("BASE_URL", "http://localhost:8000")

# ── Per-user FB session state (in-memory, keyed by portal user ID) ─────────
_fb_state: dict[int, dict] = {}


def _get_fb_token(user_id: int) -> str:
    """Get the active FB token for this user: memory → disk file → env var."""
    token = _fb_state.get(user_id, {}).get("fb_user_token", "")
    if token:
        return token
    stored = _read_stored_fb_token()
    if stored[0]:
        _fb_state.setdefault(user_id, {})["fb_user_token"] = stored[0]
        _fb_state[user_id]["fb_user_name"] = stored[1]
        _fb_state[user_id]["fb_user_id"]   = stored[2]
        return stored[0]
    return META_ACCESS_TOKEN


def _get_active_client_id(user_id: int) -> str | None:
    return _fb_state.get(user_id, {}).get("active_client_id")


def _set_active_client_id(user_id: int, client_id: str):
    _fb_state.setdefault(user_id, {})["active_client_id"] = client_id


def _client_id(user_id: int) -> str:
    """Get the active client's id, auto-selecting if only one exists."""
    cid = _get_active_client_id(user_id)
    if not cid:
        clients = _db_list("clients", {})
        if len(clients) == 1:
            cid = clients[0]["id"]
            _set_active_client_id(user_id, cid)
    return cid or ""


def _get_active_client(user_id: int):
    cid = _client_id(user_id)
    if not cid:
        return None
    return _db_find_by("clients", "id", cid)


def _ad_account(user_id: int) -> str:
    client = _get_active_client(user_id)
    return ((client or {}).get("meta_ad_account_id") or "").replace("act_", "").strip()


# ── Flash queue (per user, cleared on next page render) ───────────────────
_flash_queue: dict[int, list] = {}


def _flash(user_id: int, message: str, category: str = "info"):
    _flash_queue.setdefault(user_id, []).append({"type": category, "message": message})


def _pop_flash(user_id: int) -> dict | None:
    queue = _flash_queue.get(user_id, [])
    if queue:
        _flash_queue[user_id] = queue[1:]
        return queue[0]
    return None


def _fb_ctx(user_id: int) -> dict:
    """Build the context variables that Flask's context_processor injected."""
    all_clients   = _db_list("clients", {})
    cid           = _client_id(user_id)
    active_client = next((c for c in all_clients if c["id"] == cid), None)
    if not active_client and len(all_clients) == 1:
        active_client = all_clients[0]
        _set_active_client_id(user_id, active_client["id"])
    token     = _get_fb_token(user_id)
    fb_state  = _fb_state.get(user_id, {})
    return {
        "active_client":   active_client,
        "all_clients":     all_clients,
        "fb_app_id":       FB_APP_ID,
        "fb_connected_name": fb_state.get("fb_user_name", ""),
        "fb_connected":    bool(token and token != META_ACCESS_TOKEN),
    }


def _render(request: Request, template: str, ctx: dict, user: User, flash=None):
    """Render a template with portal context + flash."""
    f = flash or _pop_flash(user.id)
    return templates.TemplateResponse(request, f"fb/{template}", {
        "current_user": user,
        "flash":        f,
        **_fb_ctx(user.id),
        **ctx,
    })


def _redirect(path: str):
    return RedirectResponse(path, status_code=303)


# ── Email helpers ──────────────────────────────────────────────────────────

def _render_email_template(template_name: str, ctx: dict) -> str:
    """Render a Jinja2 template to a string for email sending."""
    env = templates.env
    tmpl = env.get_template(f"fb/{template_name}")
    return tmpl.render(**ctx)


def send_approval_email(to_email, to_name, ad, token, client=None):
    base_url    = get_setting("BASE_URL") or BASE_URL
    approve_url = f"{base_url}/fb/respond/{token}/approve"
    reject_url  = f"{base_url}/fb/respond/{token}/reject"

    link_url = ad.get("link_url", "")
    try:
        domain = urlparse(link_url).netloc.replace("www.", "") if link_url else ""
    except Exception:
        domain = ""

    cta_code  = ad.get("cta", "LEARN_MORE")
    cta_label = CTA_LABELS.get(cta_code, cta_code.replace("_", " ").title())

    carousel_cards = []
    if ad.get("carousel_cards"):
        try:
            carousel_cards = json.loads(ad["carousel_cards"])
        except (json.JSONDecodeError, TypeError):
            carousel_cards = []

    campaign = None
    adset    = None
    try:
        campaign_id = ad.get("campaign_id", "")
        if campaign_id:
            camps    = _db_list("campaigns", {"id": campaign_id})
            campaign = camps[0] if camps else None
        adset_id = ad.get("adset_id", "")
        if adset_id:
            adsets = _db_list("adsets", {"id": adset_id})
            adset  = adsets[0] if adsets else None
    except Exception:
        pass

    lead_form = None
    lead_form_id = ad.get("lead_form_id", "").strip()

    if not lead_form:
        ad_id = ad.get("id", "")
        try:
            local_forms = _db_list("lead_forms", {"ad_id": ad_id})
            if local_forms:
                lf        = local_forms[0]
                questions = []
                try:
                    questions = json.loads(lf.get("questions") or "[]")
                except Exception:
                    pass
                lead_form = {
                    "id":   lf.get("meta_form_id") or lf.get("id", ""),
                    "name": lf.get("form_name", "Lead Form"),
                    "questions": [{"type": q.get("type", "CUSTOM"), "label": q.get("label", "")} for q in questions],
                    "privacy_policy": {"url": lf.get("privacy_url", "")},
                    "context_card": {
                        "title":   lf.get("intro_headline", ""),
                        "content": [lf.get("intro_description", "")] if lf.get("intro_description") else [],
                    },
                    "thank_you_page": {
                        "title": lf.get("thank_you_title", "Thanks!"),
                        "body":  lf.get("thank_you_body", ""),
                    },
                }
        except Exception as exc:
            logger.warning("Could not load local lead form for ad %s: %s", ad_id, exc)

    if lead_form_id and not lead_form:
        try:
            # We don't have a token here easily; skip Meta API fallback
            pass
        except Exception:
            pass

    ad = dict(ad)
    image_parts = []

    has_public_images = any(
        (ad.get(f) or "").startswith("http")
        for f in ("image_square", "image_landscape", "image_portrait", "image_stories")
    )

    if not has_public_images:
        for img_field in ("image_square", "image_landscape", "image_portrait", "image_stories"):
            img_val = ad.get(img_field, "")
            if not img_val or not img_val.startswith("/static/uploads/"):
                continue
            local_path = os.path.join(str(PORTAL_DIR), *img_val.lstrip("/").split("/"))
            try:
                with open(local_path, "rb") as fh:
                    img_bytes = fh.read()
                ext       = img_val.rsplit(".", 1)[-1].lower()
                mime_type = "image/png" if ext == "png" else "image/jpeg"
                cid       = f"{img_field}_{secrets.token_hex(4)}"
                ad[img_field] = f"cid:{cid}"
                maintype, subtype = mime_type.split("/", 1)
                img_part = MIMEImage(img_bytes, _subtype=subtype)
                img_part.add_header("Content-ID", f"<{cid}>")
                img_part.add_header("Content-Disposition", "inline", filename=f"{img_field}.{subtype}")
                image_parts.append(img_part)
            except FileNotFoundError:
                continue

    html = _render_email_template("email_approval.html", {
        "to_name":       to_name,
        "ad":            ad,
        "client":        client,
        "approve_url":   approve_url,
        "reject_url":    reject_url,
        "domain":        domain,
        "cta_label":     cta_label,
        "carousel_cards": carousel_cards,
        "placements":    AD_PLACEMENTS,
        "lead_form":     lead_form,
        "campaign":      campaign,
        "adset":         adset,
    })

    page_name = (client or {}).get("fb_page_name") or "Politika NYC"

    msg = MIMEMultipart("related")
    msg["Subject"] = f"[Approval Needed] {ad.get('title') or ad.get('ad_name') or 'Facebook Ad'} — {page_name}"
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = to_email

    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(html, "html"))
    msg.attach(alt_part)

    for img_part in image_parts:
        msg.attach(img_part)

    _send_email(msg)


def send_campaign_approval_email(to_email, to_name, campaign, primary_ad, token, client=None):
    ad_copy = dict(primary_ad) if primary_ad else {}
    ad_copy["title"]    = campaign.get("campaign_name", "Untitled Campaign")
    ad_copy["_campaign"] = campaign
    ad_copy["_adsets"]   = campaign.get("_adsets", [])
    ad_copy["_all_ads"]  = campaign.get("_ads", [])
    send_approval_email(to_email, to_name, ad_copy, token, client=client)


def check_ad_approval_status(ad_id):
    """Update approval_status on campaign/ad based on approval responses."""
    ad_approvals = _db_list("approvals", {"ad_id": ad_id})
    campaign     = _db_find_by("campaigns", "id", ad_id)
    if campaign:
        client_id = campaign.get("client_id", "")
        client    = _db_find_by("clients", "id", client_id) if client_id else None
    else:
        ad = _db_find_by("meta_ads", "id", ad_id)
        if not ad:
            return
        client_id = ad.get("client_id", "")
        client    = _db_find_by("clients", "id", client_id) if client_id else None

    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = _db_list("approvers", {"meta_ad_account_id": ad_account_id})
    else:
        approvers = _db_list("approvers", {"client_id": client_id}) if client_id else _db_get_all("approvers")

    required_emails = {a["email"] for a in approvers if a.get("required") in (1, True, "1", "TRUE", "true")}

    latest_by_email: dict = {}
    for apvl in ad_approvals:
        email    = apvl["approver_email"]
        existing = latest_by_email.get(email)
        if not existing or (apvl.get("responded_at") or "") > (existing.get("responded_at") or ""):
            latest_by_email[email] = apvl

    all_required_approved, any_rejected = True, False
    for email in required_emails:
        apvl = latest_by_email.get(email)
        if not apvl or apvl["status"] == "pending":
            all_required_approved = False
        elif apvl["status"] == "rejected":
            any_rejected = True

    new_status = "rejected" if any_rejected else (
        "approved" if all_required_approved and required_emails else None
    )
    if not new_status:
        return

    if campaign:
        _db_update("campaigns", ad_id, {"approval_status": new_status, "updated_at": now_iso()})
        adsets = _db_list("adsets", {"campaign_id": ad_id})
        for s in adsets:
            try:
                _db_update("adsets", s["id"], {"approval_status": new_status, "updated_at": now_iso()})
            except Exception:
                pass
            ads_in_set = _db_list("meta_ads", {"adset_id": s["id"]})
            for a in ads_in_set:
                try:
                    _db_update("meta_ads", a["id"], {"approval_status": new_status, "updated_at": now_iso()})
                except Exception:
                    pass
    else:
        meta_ad = _db_find_by("meta_ads", "id", ad_id)
        if meta_ad:
            _db_update("meta_ads", ad_id, {"approval_status": new_status, "updated_at": now_iso()})
            campaign_id = meta_ad.get("campaign_id", "")
            adset_id    = meta_ad.get("adset_id", "")
            if campaign_id:
                try:
                    _db_update("campaigns", campaign_id, {"approval_status": new_status, "updated_at": now_iso()})
                except Exception:
                    pass
            if adset_id:
                try:
                    _db_update("adsets", adset_id, {"approval_status": new_status, "updated_at": now_iso()})
                except Exception:
                    pass


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("")
@router.get("/")
async def dashboard(current_user: User = Depends(require_user)):
    return _redirect("/fb/campaigns")


# ── Facebook Auth ───────────────────────────────────────────────────────────

@router.post("/auth/fb/token")
async def auth_fb_token(request: Request, current_user: User = Depends(require_user)):
    data  = await request.json()
    token = data.get("token", "").strip()
    if not token:
        return JSONResponse({"error": "No token provided"}, status_code=400)

    ll_success = False
    if FB_APP_ID and FB_APP_SECRET and FB_APP_ID != "YOUR_APP_ID_HERE":
        try:
            debug_resp = http_requests.get(
                "https://graph.facebook.com/debug_token",
                params={"input_token": token, "access_token": f"{FB_APP_ID}|{FB_APP_SECRET}"},
                timeout=10,
            )
            debug_data   = debug_resp.json().get("data", {})
            token_app_id = str(debug_data.get("app_id", ""))
            expires_at   = debug_data.get("expires_at", 0)
            is_valid     = debug_data.get("is_valid", False)

            if is_valid and token_app_id == FB_APP_ID:
                resp = http_requests.get(
                    "https://graph.facebook.com/v21.0/oauth/access_token",
                    params={
                        "grant_type":       "fb_exchange_token",
                        "client_id":        FB_APP_ID,
                        "client_secret":    FB_APP_SECRET,
                        "fb_exchange_token": token,
                    },
                    timeout=10,
                )
                ll = resp.json()
                if "access_token" in ll:
                    token      = ll["access_token"]
                    ll_success = True
            elif is_valid:
                import time as _time
                if expires_at == 0 or expires_at > _time.time() + 86400 * 7:
                    ll_success = True
        except Exception as exc:
            logger.warning("Token exchange error: %s", exc)

    check = meta_get("/me", {"fields": "id,name"}, token=token)
    if "error" in check:
        err = check["error"]
        return JSONResponse({"error": err.get("message", str(err)) if isinstance(err, dict) else str(err)}, status_code=400)

    name = check.get("name", "")
    uid  = check.get("id", "")
    _fb_state.setdefault(current_user.id, {})["fb_user_token"] = token
    _fb_state[current_user.id]["fb_user_name"] = name
    _fb_state[current_user.id]["fb_user_id"]   = uid
    _write_stored_fb_token(token, name, uid)
    return JSONResponse({"ok": True, "name": name, "id": uid, "long_lived": ll_success})


@router.post("/auth/fb/disconnect")
async def auth_fb_disconnect(current_user: User = Depends(require_user)):
    if current_user.id in _fb_state:
        _fb_state[current_user.id].pop("fb_user_token", None)
        _fb_state[current_user.id].pop("fb_user_name", None)
        _fb_state[current_user.id].pop("fb_user_id", None)
    _clear_stored_fb_token()
    return JSONResponse({"ok": True})


@router.get("/auth/fb/status")
async def auth_fb_status(current_user: User = Depends(require_user)):
    token = _get_fb_token(current_user.id)
    if token and token != META_ACCESS_TOKEN:
        fb = _fb_state.get(current_user.id, {})
        return JSONResponse({"connected": True, "name": fb.get("fb_user_name", ""), "id": fb.get("fb_user_id", "")})
    return JSONResponse({"connected": False})


# ── Image upload ────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_image(
    file: UploadFile = File(...),
    current_user: User = Depends(require_user),
):
    if not file or not file.filename:
        return JSONResponse({"error": "No file provided"}, status_code=400)
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ALLOWED_MEDIA:
        return JSONResponse({"error": f"File type .{ext} not allowed."}, status_code=400)
    is_video   = ext in ALLOWED_VIDEOS
    filename   = f"{secrets.token_hex(10)}.{ext}"
    file_bytes = await file.read()
    file_size  = len(file_bytes)
    file_type  = "video" if is_video else "image"

    if is_video and file_size > FB_VIDEO_MAX_SIZE:
        return JSONResponse({"error": f"Video too large ({file_size // (1024*1024)}MB). Facebook max is 4GB."}, status_code=400)
    if not is_video and file_size > FB_IMAGE_MAX_SIZE:
        return JSONResponse({"error": f"Image too large ({file_size // (1024*1024)}MB). Facebook max is 30MB."}, status_code=400)

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    with open(os.path.join(UPLOAD_FOLDER, filename), "wb") as lf:
        lf.write(file_bytes)
    return JSONResponse({"url": f"/static/uploads/{filename}", "type": file_type, "size": file_size, "ext": ext})


# ── Meta API proxy ──────────────────────────────────────────────────────────

@router.post("/api/meta/cache-clear")
async def api_meta_cache_clear(current_user: User = Depends(require_user)):
    _meta_cache.clear()
    return JSONResponse({"ok": True, "msg": "Cache cleared"})


@router.get("/api/meta/status")
async def api_meta_status(current_user: User = Depends(require_user)):
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        me = FBUser('me').api_get(fields=['id', 'name'])
        return JSONResponse({"ok": True, "name": me.get("name"), "id": me.get("id")})
    except FacebookRequestError as e:
        return JSONResponse({"ok": False, "error": e.api_error_message()})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.get("/api/meta/adaccounts")
async def api_meta_adaccounts(current_user: User = Depends(require_user)):
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        def _fetch():
            cursor = FBUser('me').get_ad_accounts(
                fields=['id', 'name', 'account_status', 'currency', 'timezone_name', 'business'],
            )
            return _sdk_cursor_to_list(cursor)
        data     = _cached_sdk("adaccounts", _fetch)
        accounts = []
        for acct in data:
            accounts.append({
                "id":             str(acct.get("id", "")).replace("act_", ""),
                "name":           acct.get("name", ""),
                "account_status": acct.get("account_status", 0),
                "currency":       acct.get("currency", ""),
                "business_id":    (acct.get("business") or {}).get("id", ""),
            })
        return JSONResponse({"accounts": accounts})
    except FacebookRequestError as e:
        return JSONResponse({"error": e.api_error_message()})
    except Exception as e:
        return JSONResponse({"error": str(e)})


@router.get("/api/meta/pages")
async def api_meta_pages(current_user: User = Depends(require_user)):
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        def _fetch():
            cursor = FBUser('me').get_accounts(fields=['id', 'name', 'category'])
            return _sdk_cursor_to_list(cursor)
        data  = _cached_sdk("pages", _fetch)
        pages = sorted([
            {"id": p["id"], "name": p.get("name", ""), "category": p.get("category", "")}
            for p in data if p.get("id")
        ], key=lambda p: p["name"].lower())
        return JSONResponse({"pages": pages, "total": len(pages)})
    except FacebookRequestError as e:
        return JSONResponse({"pages": [], "error": e.api_error_message()})
    except Exception as e:
        return JSONResponse({"pages": [], "error": str(e)})


@router.get("/api/meta/audiences/{ad_account_id:path}")
async def api_meta_audiences(ad_account_id: str, current_user: User = Depends(require_user)):
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        acct    = ad_account_id.replace("act_", "")
        account = AdAccount(f'act_{acct}')

        saved_error = custom_error = None
        saved_list  = []
        custom_list = []

        try:
            saved_data = _cached_sdk(f"saved_aud_{acct}", lambda: _sdk_cursor_to_list(
                account.get_saved_audiences(fields=['id', 'name', 'run_status', 'approximate_count'])
            ))
            saved_list = [{"id": a["id"], "name": a["name"],
                           "subtype": a.get("run_status", "").replace("_", " ").title()}
                          for a in saved_data]
        except FacebookRequestError as e:
            saved_error = e.api_error_message()

        try:
            custom_data = _cached_sdk(f"custom_aud_{acct}", lambda: _sdk_cursor_to_list(
                account.get_custom_audiences(fields=['id', 'name', 'subtype', 'approximate_count_lower_bound'])
            ))
            custom_list = [{"id": a["id"], "name": a["name"],
                            "subtype": a.get("subtype", "").replace("_", " ").title()}
                           for a in custom_data]
        except FacebookRequestError as e:
            custom_error = e.api_error_message()

        return JSONResponse({"saved": saved_list, "custom": custom_list,
                             "saved_error": saved_error, "custom_error": custom_error})
    except Exception as e:
        return JSONResponse({"error": str(e)})


@router.get("/api/meta/pages/debug")
async def api_meta_pages_debug(current_user: User = Depends(require_user)):
    token = _get_fb_token(current_user.id)
    if not token:
        return JSONResponse({"error": "No Facebook token — connect your account on the Clients page."})
    out = {}
    out["/me/accounts"]   = meta_get_all("/me/accounts", {"fields": "id,name,category"}, token=token)
    businesses            = meta_get_all("/me/businesses", {"fields": "id,name"}, token=token)
    out["/me/businesses"] = businesses
    biz_ids = [b["id"] for b in businesses.get("data", []) if b.get("id")]
    for env_bid in os.getenv("META_BUSINESS_IDS", "").split(","):
        env_bid = env_bid.strip()
        if env_bid and env_bid not in biz_ids:
            biz_ids.append(env_bid)
    for bid in biz_ids:
        for ep in (f"/{bid}/owned_pages", f"/{bid}/client_pages"):
            out[ep] = meta_get_all(ep, {"fields": "id,name,category"}, token=token)
    ad_accounts = meta_get_all("/me/adaccounts", {"fields": "id,name,business"}, token=token)
    out["/me/adaccounts"] = {"data": [{"id": a["id"], "name": a["name"], "business": a.get("business")} for a in ad_accounts.get("data", [])]}
    return JSONResponse(out)


@router.get("/api/meta/targeting/search")
async def api_meta_targeting_search(
    q: str = Query(""),
    current_user: User = Depends(require_user),
):
    if not q.strip():
        return JSONResponse({"data": []})
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        results = TargetingSearch.search(params={
            'type': TargetingSearch.TargetingSearchTypes.interest,
            'q': q.strip(),
            'limit': 20,
        })
        items = []
        for item in results:
            d    = dict(item)
            path = d.get("path", [])
            items.append({
                "id":         d["id"],
                "name":       d["name"],
                "type":       "interest",
                "breadcrumb": " > ".join(path[:-1]) if len(path) > 1 else (d.get("topic") or "Interest"),
                "size_low":   d.get("audience_size_lower_bound", 0),
                "size_high":  d.get("audience_size_upper_bound", 0),
            })
        return JSONResponse({"data": items})
    except FacebookRequestError as e:
        return JSONResponse({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return JSONResponse({"error": str(e), "data": []})


@router.get("/api/meta/location/search")
async def api_meta_location_search(
    q: str = Query(""),
    type_filter: str = Query(""),
    current_user: User = Depends(require_user),
):
    if not q.strip():
        return JSONResponse({"data": []})
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        all_types = ["city","region","country","zip","geo_market","electoral_district","neighborhood","subcity","country_group"]
        loc_types = [type_filter] if type_filter else all_types
        results = TargetingSearch.search(params={
            'type': TargetingSearch.TargetingSearchTypes.geolocation,
            'q': q.strip(),
            'location_types': loc_types,
            'limit': 25,
        })
        items = []
        for loc in results:
            d     = dict(loc)
            parts = [d.get("name", "")]
            if d.get("region") and d.get("type") not in ("region",):
                parts.append(d["region"])
            if d.get("country_name") and d.get("type") not in ("country",):
                parts.append(d["country_name"])
            items.append({
                "key":          d.get("key", ""),
                "name":         d.get("name", ""),
                "type":         d.get("type", ""),
                "country_code": d.get("country_code", ""),
                "region":       d.get("region", ""),
                "country_name": d.get("country_name", ""),
                "breadcrumb":   ", ".join(parts[1:]) if len(parts) > 1 else "",
            })
        return JSONResponse({"data": items})
    except FacebookRequestError as e:
        return JSONResponse({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return JSONResponse({"error": str(e), "data": []})


@router.get("/api/meta/instagram-accounts")
async def api_meta_instagram_accounts(
    page_id: str = Query(""),
    current_user: User = Depends(require_user),
):
    if not page_id.strip():
        return JSONResponse({"data": []})
    try:
        token  = _get_fb_token(current_user.id)
        _init_meta(token)
        result = meta_get(f"/{page_id}", {"fields": "instagram_business_account,connected_instagram_accounts"}, token=token)
        accounts = []
        iba = result.get("instagram_business_account")
        if iba:
            detail = meta_get(f"/{iba['id']}", {"fields": "id,username,profile_picture_url,name"}, token=token)
            accounts.append({"id": detail.get("id", iba["id"]), "username": detail.get("username", ""),
                             "name": detail.get("name", ""), "profile_pic": detail.get("profile_picture_url", "")})
        connected = result.get("connected_instagram_accounts", {}).get("data", [])
        seen = {a["id"] for a in accounts}
        for c in connected:
            if c["id"] not in seen:
                accounts.append({"id": c["id"], "username": c.get("username", ""),
                                 "name": c.get("name", ""), "profile_pic": c.get("profile_picture_url", "")})
        return JSONResponse({"data": accounts})
    except Exception as e:
        return JSONResponse({"data": [], "error": str(e)})


@router.get("/api/meta/leadgen-forms")
async def api_meta_leadgen_forms(
    page_id: str = Query(""),
    current_user: User = Depends(require_user),
):
    if not page_id.strip():
        return JSONResponse({"data": []})
    try:
        from fb_ad_approval.core import _get_page_token
        token      = _get_fb_token(current_user.id)
        page_token = _get_page_token(page_id, token)
        result = meta_get_all(f"/{page_id}/leadgen_forms",
                              {"fields": "id,name,status,leads_count"},
                              token=page_token or token)
        if "error" in result:
            err = result["error"]
            return JSONResponse({"data": [], "error": err.get("message", str(err)) if isinstance(err, dict) else str(err)})
        forms = [{"id": f.get("id"), "name": f.get("name", ""),
                  "status": f.get("status", ""), "leads_count": f.get("leads_count", 0)}
                 for f in result.get("data", [])]
        return JSONResponse({"data": forms})
    except Exception as e:
        return JSONResponse({"data": [], "error": str(e)})


@router.get("/api/meta/leadgen-form-detail")
async def api_meta_leadgen_form_detail(
    form_id: str = Query(""),
    current_user: User = Depends(require_user),
):
    if not form_id.strip():
        return JSONResponse({"error": "No form_id"})
    try:
        token = _get_fb_token(current_user.id)
        resp  = http_requests.get(
            f"https://graph.facebook.com/v21.0/{form_id}",
            params={"access_token": token,
                    "fields": "name,questions,privacy_policy_url,legal_content,thank_you_page,context_card"},
            timeout=10,
        )
        data = resp.json()
        if "error" in data:
            return JSONResponse({"error": data["error"].get("message", "Unknown error")})
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)})


@router.post("/api/lead-forms/save")
async def api_lead_forms_save(request: Request, current_user: User = Depends(require_user)):
    body     = await request.json()
    ad_id    = body.pop("ad_id", "")
    link_only = body.pop("_link_only", False)
    body["client_id"] = _client_id(current_user.id)
    body["ad_id"]     = ad_id

    existing = _db_list("lead_forms", {"ad_id": ad_id}) if ad_id else []
    if not existing and body.get("meta_form_id"):
        existing = _db_list("lead_forms", {"meta_form_id": body["meta_form_id"]})

    if existing:
        if link_only:
            _db_save("lead_forms", None, existing[0]["id"], {"ad_id": ad_id})
        else:
            _db_save("lead_forms", None, existing[0]["id"], body)
        return JSONResponse({"ok": True, "id": existing[0]["id"]})
    else:
        new_id = _db_save("lead_forms", None, None, body)
        return JSONResponse({"ok": True, "id": new_id})


@router.post("/api/meta/leadgen-forms/create")
async def api_meta_leadgen_form_create(request: Request, current_user: User = Depends(require_user)):
    body         = await request.json()
    page_id      = body.get("page_id", "").strip()
    form_name    = body.get("name", "").strip()
    questions    = body.get("questions", [])
    privacy_url  = safe_url(body.get("privacy_policy_url", ""))
    follow_up_url = safe_url(body.get("follow_up_action_url", ""))
    thank_you    = body.get("thank_you", {})

    if not page_id:
        return JSONResponse({"error": "page_id is required"}, status_code=400)
    if not form_name:
        return JSONResponse({"error": "Form name is required"}, status_code=400)
    if not questions:
        return JSONResponse({"error": "At least one question is required"}, status_code=400)
    if not privacy_url:
        return JSONResponse({"error": "Privacy policy URL is required"}, status_code=400)

    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        intro     = body.get("intro", {})
        settings  = body.get("settings", {})
        form_type = body.get("form_type", "MORE_VOLUME")

        meta_questions = []
        for q in questions:
            q_type = q.get("type", "").upper()
            q_obj  = {"type": q_type}
            if q_type == "CUSTOM":
                q_obj["key"]   = q.get("key", q.get("label", "custom")).lower().replace(" ", "_")
                q_obj["label"] = q.get("label", "")
            if q.get("field_name"):
                q_obj["field_name"] = q["field_name"]
            meta_questions.append(q_obj)

        privacy_obj  = {"url": privacy_url}
        privacy_text = body.get("privacy_policy_text", "").strip()
        if privacy_text:
            privacy_obj["link_text"] = privacy_text

        params = {
            "name":                form_name,
            "questions":           json.dumps(meta_questions),
            "privacy_policy":      json.dumps(privacy_obj),
            "follow_up_action_url": follow_up_url or privacy_url,
        }

        if form_type == "HIGHER_INTENT":
            params["is_optimized_for_quality"] = "true"
        lang = settings.get("language", "").strip()
        if lang:
            params["locale"] = lang
        sharing = settings.get("sharing", "RESTRICTED")
        if sharing == "OPEN":
            params["allow_organic_lead"] = "true"
        tracking = settings.get("tracking_params", "").strip()
        if tracking:
            params["tracking_parameters"] = json.dumps({"url_tags": tracking})

        if intro.get("greeting") and (intro.get("headline") or intro.get("description")):
            ctx_content = [intro["description"]] if intro.get("description") else []
            params["context_card"] = json.dumps({
                "title":   intro.get("headline", ""),
                "content": ctx_content,
                "style":   "PARAGRAPH_STYLE",
            })

        disclaimers = body.get("disclaimers", [])
        if disclaimers:
            consent_items = []
            for d in disclaimers:
                if d.get("title") or d.get("text"):
                    consent_items.append({
                        "type":        "CUSTOM",
                        "is_required": True,
                        "key":         d.get("title", "disclaimer").lower().replace(" ", "_"),
                        "title":       d.get("title", ""),
                        "content":     d.get("text", ""),
                    })
            if consent_items:
                params["legal_content"] = json.dumps({"custom_disclaimer": consent_items})

        if thank_you.get("headline"):
            action_type = thank_you.get("action_type", "website")
            ty_obj = {
                "title": thank_you.get("headline", "Thanks!"),
                "body":  thank_you.get("description", "We'll be in touch."),
            }
            if action_type == "call":
                ty_obj["button_type"]   = "CALL_BUSINESS"
                ty_obj["business_phone"] = {"number": thank_you.get("button_url", "")}
            elif action_type == "download":
                ty_obj["button_type"] = "VIEW_WEBSITE"
                ty_obj["button_text"] = thank_you.get("button_text", "Download")
                ty_obj["website_url"] = safe_url(thank_you.get("button_url", "")) or follow_up_url or privacy_url
            else:
                ty_obj["button_type"] = "VIEW_WEBSITE"
                ty_obj["button_text"] = thank_you.get("button_text", "View website")
                ty_obj["website_url"] = safe_url(thank_you.get("button_url", "")) or follow_up_url or privacy_url
            params["thank_you_page"] = json.dumps(ty_obj)

        from fb_ad_approval.core import _get_page_token
        page_token = _get_page_token(page_id, token)
        resp = http_requests.post(
            f"{META_GRAPH}/{page_id}/leadgen_forms",
            data={**params, "access_token": page_token or token},
            timeout=30,
        )
        result = resp.json()
        if "error" in result:
            err = result["error"]
            msg = (err.get("error_user_msg") or err.get("message") or str(err)) if isinstance(err, dict) else str(err)
            return JSONResponse({"error": msg}, status_code=400)
        return JSONResponse({"ok": True, "id": result.get("id"), "name": form_name})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/objective-goals")
async def api_objective_goals(current_user: User = Depends(require_user)):
    return JSONResponse(OBJECTIVE_VALID_GOALS)


@router.get("/api/meta/pixels")
async def api_meta_pixels(
    acct: str = Query(""),
    current_user: User = Depends(require_user),
):
    effective_acct = acct.strip() or _ad_account(current_user.id)
    if not effective_acct:
        return JSONResponse({"data": [], "error": "No ad account configured"})
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        account = AdAccount(f'act_{effective_acct}')
        data    = _cached_sdk(f"pixels_{effective_acct}", lambda: _sdk_cursor_to_list(
            account.get_ads_pixels(fields=['id', 'name', 'is_unavailable'])
        ))
        pixels = [{"id": p["id"], "name": p.get("name", p["id"])}
                  for p in data if not p.get("is_unavailable")]
        return JSONResponse({"data": pixels})
    except FacebookRequestError as e:
        return JSONResponse({"data": [], "error": e.api_error_message()})
    except Exception as e:
        return JSONResponse({"data": [], "error": str(e)})


@router.get("/api/meta/targeting/reach")
async def api_meta_targeting_reach(
    account_id: str = Query(""),
    spec: str = Query("{}"),
    current_user: User = Depends(require_user),
):
    acct = account_id.replace("act_", "").strip()
    if not acct:
        return JSONResponse({"error": "Missing account_id."})
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        account = AdAccount(f'act_{acct}')
        result  = account.get_reach_estimate(params={'targeting_spec': spec})
        return JSONResponse({"data": _sdk_cursor_to_list(result)})
    except FacebookRequestError as e:
        return JSONResponse({"error": e.api_error_message()})
    except Exception as e:
        return JSONResponse({"error": str(e)})


@router.get("/api/meta/custom-audiences")
async def api_meta_custom_audiences(current_user: User = Depends(require_user)):
    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"error": "No ad account configured", "data": []})
    try:
        token   = _get_fb_token(current_user.id)
        _init_meta(token)
        account = AdAccount(f'act_{acct}')
        data    = _cached_sdk(f"custom_aud2_{acct}", lambda: _sdk_cursor_to_list(
            account.get_custom_audiences(fields=['id', 'name', 'subtype', 'approximate_count_lower_bound'])
        ))
        audiences = [
            {"id": a.get("id"), "name": a.get("name"),
             "subtype": a.get("subtype", ""), "count": a.get("approximate_count_lower_bound")}
            for a in data
        ]
        return JSONResponse({"data": audiences})
    except FacebookRequestError as e:
        return JSONResponse({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return JSONResponse({"error": str(e), "data": []})


# ── Saved Locations ────────────────────────────────────────────────────────

@router.get("/api/saved-locations")
async def api_list_saved_locations(current_user: User = Depends(require_user)):
    try:
        rows = _db_get_all("saved_locations")
        return JSONResponse({"locations": rows})
    except Exception as e:
        return JSONResponse({"error": str(e), "locations": []})


@router.post("/api/saved-locations")
async def api_create_saved_location(request: Request, current_user: User = Depends(require_user)):
    data          = await request.json()
    name          = data.get("name", "").strip()
    locations     = data.get("locations", [])
    location_type = data.get("location_type", "")
    if not name:
        return JSONResponse({"error": "Name is required"}, status_code=400)
    if not locations:
        return JSONResponse({"error": "Select at least one location first"}, status_code=400)
    try:
        new_id = gen_id()
        _db_save("saved_locations", None, None, {
            "id": new_id, "name": name,
            "locations": json.dumps(locations),
            "location_type": location_type,
            "created_at": now_iso(),
        })
        return JSONResponse({"ok": True, "id": new_id, "name": name})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/api/saved-locations/{loc_id}")
async def api_delete_saved_location(loc_id: str, current_user: User = Depends(require_user)):
    try:
        existing = _db_find_by("saved_locations", "id", loc_id)
        if not existing:
            return JSONResponse({"error": "Not found"}, status_code=404)
        _db_delete("saved_locations", loc_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Clients ────────────────────────────────────────────────────────────────

@router.get("/clients", response_class=HTMLResponse)
async def manage_clients(request: Request, current_user: User = Depends(require_user)):
    clients = _db_get_all("clients")
    return _render(request, "clients.html", {"clients": clients, "color_palette": CLIENT_COLORS}, current_user)


@router.post("/clients/add")
async def add_client(
    request: Request,
    name: str = Form(""),
    meta_business_id: str = Form(""),
    meta_ad_account_id: str = Form(""),
    fb_page_name: str = Form(""),
    fb_page_id: str = Form(""),
    brand_color: str = Form("#1877F2"),
    default_pixel_id: str = Form(""),
    default_url_tags: str = Form(""),
    current_user: User = Depends(require_user),
):
    name = name.strip()
    if not name:
        _flash(current_user.id, "Client name is required.", "error")
        return _redirect("/fb/clients")

    existing = _db_get_all("clients")
    if any(c["name"].lower() == name.lower() for c in existing):
        _flash(current_user.id, "A client with that name already exists.", "error")
        return _redirect("/fb/clients")

    color = safe_brand_color(brand_color.strip())
    if not color:
        color = CLIENT_COLORS[len(existing) % len(CLIENT_COLORS)]

    data = {
        "id":                gen_id(),
        "name":              name,
        "meta_business_id":  meta_business_id.strip(),
        "meta_ad_account_id": meta_ad_account_id.strip(),
        "fb_page_name":      fb_page_name.strip() or name,
        "fb_page_id":        fb_page_id.strip(),
        "brand_color":       color,
        "default_pixel_id":  default_pixel_id.strip(),
        "default_url_tags":  default_url_tags.strip(),
        "created_at":        now_iso(),
    }
    _db_save("clients", None, None, data)

    if not existing:
        _set_active_client_id(current_user.id, data["id"])

    _flash(current_user.id, f"Client '{name}' added.", "success")
    return _redirect("/fb/clients")


@router.post("/clients/{client_id}/delete")
async def delete_client(client_id: str, current_user: User = Depends(require_user)):
    existing = _db_find_by("clients", "id", client_id)
    if existing:
        _db_delete("clients", client_id)
        if _get_active_client_id(current_user.id) == client_id:
            if current_user.id in _fb_state:
                _fb_state[current_user.id].pop("active_client_id", None)
        _flash(current_user.id, "Client removed.", "success")
    else:
        _flash(current_user.id, "Client not found.", "error")
    return _redirect("/fb/clients")


@router.get("/clients/{client_id}/select")
async def select_client(
    client_id: str,
    next: str = Query("/fb/campaigns"),
    current_user: User = Depends(require_user),
):
    _set_active_client_id(current_user.id, client_id)
    return _redirect(next)


@router.post("/clients/{client_id}/edit")
async def edit_client(
    client_id: str,
    name: str = Form(""),
    meta_business_id: str = Form(""),
    meta_ad_account_id: str = Form(""),
    fb_page_name: str = Form(""),
    fb_page_id: str = Form(""),
    brand_color: str = Form("#1877F2"),
    default_pixel_id: str = Form(""),
    default_url_tags: str = Form(""),
    current_user: User = Depends(require_user),
):
    existing = _db_find_by("clients", "id", client_id)
    if not existing:
        _flash(current_user.id, "Client not found.", "error")
        return _redirect("/fb/clients")
    _db_update("clients", client_id, {
        "name":               name.strip(),
        "meta_business_id":   meta_business_id.strip(),
        "meta_ad_account_id": meta_ad_account_id.strip(),
        "fb_page_name":       fb_page_name.strip(),
        "fb_page_id":         fb_page_id.strip(),
        "brand_color":        safe_brand_color(brand_color.strip()),
        "default_pixel_id":   default_pixel_id.strip(),
        "default_url_tags":   default_url_tags.strip(),
    })
    _flash(current_user.id, "Client updated.", "success")
    return _redirect("/fb/clients")


# ── Users ──────────────────────────────────────────────────────────────────

@router.get("/users", response_class=HTMLResponse)
async def manage_users(request: Request, current_user: User = Depends(require_admin)):
    users = _db_list("users", {})
    return _render(request, "users.html", {"users": users}, current_user)


@router.post("/users/add")
async def add_user(
    email: str = Form(""),
    name: str = Form(""),
    password: str = Form(""),
    role: str = Form("manager"),
    current_user: User = Depends(require_admin),
):
    import uuid as _uuid
    from werkzeug.security import generate_password_hash
    email    = email.strip().lower()
    name     = name.strip()
    password = password.strip()
    if not email or not password or not name:
        _flash(current_user.id, "All fields are required.", "danger")
        return _redirect("/fb/users")
    existing = _db_list("users", {"email": email})
    if existing:
        _flash(current_user.id, f"User {email} already exists.", "warning")
        return _redirect("/fb/users")
    if role not in ("admin", "manager", "viewer"):
        role = "manager"
    _db_save("users", None, None, {
        "id":            _uuid.uuid4().hex[:12],
        "email":         email,
        "name":          name,
        "password_hash": generate_password_hash(password),
        "role":          role,
        "is_active":     1,
        "created_at":    now_iso(),
        "updated_at":    now_iso(),
    })
    _flash(current_user.id, f"User {name} ({email}) added as {role}.", "success")
    return _redirect("/fb/users")


@router.post("/users/{uid}/toggle")
async def toggle_user(uid: str, current_user: User = Depends(require_admin)):
    users = _db_list("users", {"id": uid})
    if not users:
        _flash(current_user.id, "User not found.", "danger")
        return _redirect("/fb/users")
    new_active = 0 if users[0].get("is_active") else 1
    _db_save("users", None, uid, {"is_active": new_active})
    _flash(current_user.id, f"User {'enabled' if new_active else 'disabled'}.", "info")
    return _redirect("/fb/users")


@router.post("/users/{uid}/role")
async def change_role(
    uid: str,
    role: str = Form("manager"),
    current_user: User = Depends(require_admin),
):
    if role not in ("admin", "manager", "viewer"):
        role = "manager"
    _db_save("users", None, uid, {"role": role})
    _flash(current_user.id, f"Role updated to {role}.", "info")
    return _redirect("/fb/users")


@router.post("/users/{uid}/delete")
async def delete_user(uid: str, current_user: User = Depends(require_admin)):
    if str(uid) == str(current_user.id):
        _flash(current_user.id, "You can't delete yourself.", "danger")
        return _redirect("/fb/users")
    _db_delete("users", uid)
    _flash(current_user.id, "User deleted.", "info")
    return _redirect("/fb/users")


@router.post("/users/{uid}/reset-password")
async def reset_password(
    uid: str,
    password: str = Form(""),
    current_user: User = Depends(require_admin),
):
    from werkzeug.security import generate_password_hash
    if not password.strip():
        _flash(current_user.id, "Password is required.", "danger")
        return _redirect("/fb/users")
    _db_save("users", None, uid, {"password_hash": generate_password_hash(password.strip())})
    _flash(current_user.id, "Password reset.", "success")
    return _redirect("/fb/users")


# ── Settings ───────────────────────────────────────────────────────────────

@router.get("/settings", response_class=HTMLResponse)
async def app_settings(request: Request, current_user: User = Depends(require_admin)):
    rows  = _db_list("settings", {})
    by_cat: dict = {}
    for r in rows:
        cat = r.get("category", "general")
        by_cat.setdefault(cat, []).append(r)
    return _render(request, "settings.html", {"settings_by_cat": by_cat}, current_user)


@router.post("/settings/save")
async def save_settings(request: Request, current_user: User = Depends(require_admin)):
    from fb_ad_approval.core import _settings_cache_ts
    import fb_ad_approval.core as _core
    form = await request.form()
    from fb_ad_approval.core import get_db as _get_db
    for key in form:
        val = form[key]
        conn = _get_db()
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE settings SET setting_value = %s WHERE setting_key = %s", (val, key))
            conn.commit()
        finally:
            conn.close()
    _core._settings_cache_ts = 0  # bust cache
    _flash(current_user.id, "Settings saved.", "success")
    return _redirect("/fb/settings")


# ── Approvers ──────────────────────────────────────────────────────────────

@router.get("/approvers", response_class=HTMLResponse)
async def manage_approvers(request: Request, current_user: User = Depends(require_user)):
    all_approvers  = _db_get_all("approvers")
    active_client  = _get_active_client(current_user.id)
    ad_account_id  = (active_client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        client_id = _client_id(current_user.id)
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers
    return _render(request, "approvers.html",
                   {"approvers": approvers, "active_client": active_client}, current_user)


@router.post("/approvers/add")
async def add_approver(
    name: str = Form(""),
    email: str = Form(""),
    required: str = Form(""),
    current_user: User = Depends(require_user),
):
    name  = name.strip()
    email = email.strip()
    req   = 1 if required else 0
    cid   = _client_id(current_user.id)
    active_client = _get_active_client(current_user.id)
    ad_account_id = (active_client or {}).get("meta_ad_account_id", "").strip()

    if not name or not email:
        _flash(current_user.id, "Name and email are required.", "error")
        return _redirect("/fb/approvers")

    existing  = _db_get_all("approvers")
    scope_key = "meta_ad_account_id" if ad_account_id else "client_id"
    scope_val = ad_account_id if ad_account_id else cid
    if any(a["email"].lower() == email.lower() and a.get(scope_key) == scope_val for a in existing):
        _flash(current_user.id, "Approver with this email already exists for this ad account.", "error")
        return _redirect("/fb/approvers")

    _db_save("approvers", None, None, {
        "id": gen_id(), "client_id": cid,
        "meta_ad_account_id": ad_account_id,
        "name": name, "email": email, "required": req,
    })
    _flash(current_user.id, f"Added {name} as approver.", "success")
    return _redirect("/fb/approvers")


@router.post("/approvers/{approver_id}/delete")
async def delete_approver(approver_id: str, current_user: User = Depends(require_user)):
    existing = _db_find_by("approvers", "id", approver_id)
    if existing:
        _db_delete("approvers", approver_id)
        _flash(current_user.id, "Approver removed.", "success")
    else:
        _flash(current_user.id, "Approver not found.", "error")
    return _redirect("/fb/approvers")


@router.post("/approvers/{approver_id}/toggle-required")
async def toggle_required(approver_id: str, current_user: User = Depends(require_user)):
    approver = _db_find_by("approvers", "id", approver_id)
    if approver:
        cur        = approver.get("required")
        is_required = cur in (1, True, "1", "TRUE", "true")
        _db_update("approvers", approver_id, {"required": 0 if is_required else 1})
    return _redirect("/fb/approvers")


# ── Campaigns page ─────────────────────────────────────────────────────────

@router.get("/campaigns", response_class=HTMLResponse)
async def campaigns_page(request: Request, current_user: User = Depends(require_user)):
    client = _get_active_client(current_user.id) or {}
    return _render(request, "campaigns.html", {
        "default_pixel_id": client.get("default_pixel_id", ""),
        "default_url_tags": client.get("default_url_tags",
                                       "utm_source=facebook&utm_medium=cpc&utm_campaign={campaign_name}"),
    }, current_user)


@router.get("/drafts", response_class=HTMLResponse)
async def drafts_page(request: Request, current_user: User = Depends(require_user)):
    return _render(request, "campaigns.html", {"start_mode": "manage"}, current_user)


@router.get("/queue", response_class=HTMLResponse)
async def approval_queue(request: Request, current_user: User = Depends(require_user)):
    return _render(request, "queue.html", {}, current_user)


@router.get("/approved", response_class=HTMLResponse)
async def approved_ads(request: Request, current_user: User = Depends(require_user)):
    return _render(request, "approved.html", {}, current_user)


@router.get("/rejected", response_class=HTMLResponse)
async def rejected_ads(request: Request, current_user: User = Depends(require_user)):
    return _render(request, "rejected.html", {}, current_user)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request, current_user: User = Depends(require_user)):
    client = _get_active_client(current_user.id) or {}
    return _render(request, "dashboard.html", {"active_client": client}, current_user)


# ── API: Approval queue data ────────────────────────────────────────────────

@router.get("/api/approval-queue")
async def api_approval_queue(current_user: User = Depends(require_user)):
    cid = _client_id(current_user.id)
    try:
        campaigns = _db_list("campaigns", {"client_id": cid}) if cid else []
        adsets    = _db_get_all("adsets")
        meta_ads  = _db_get_all("meta_ads")
        approvals = _db_get_all("approvals")
    except Exception:
        return JSONResponse({"pending": [], "error": "Database access failed"})

    pending = []
    approved_list = []
    rejected = []

    for camp in campaigns:
        status = (camp.get("approval_status") or "none").lower()
        if status not in ("pending_approval", "approved", "rejected"):
            continue

        camp_adsets = [s for s in adsets if s.get("campaign_id") == camp["id"]]
        camp_ads    = []
        for s in camp_adsets:
            camp_ads.extend([a for a in meta_ads if a.get("adset_id") == s["id"]])
        camp["_adsets"]      = camp_adsets
        camp["_ads"]         = camp_ads
        camp["_primary_ad"]  = camp_ads[0] if camp_ads else {}

        camp_approvals = [ap for ap in approvals if ap.get("ad_id") == camp["id"]]
        camp["_approvals"]       = camp_approvals
        camp["_approval_count"]  = len([ap for ap in camp_approvals if ap.get("status") == "approved"])
        camp["_rejection_count"] = len([ap for ap in camp_approvals if ap.get("status") == "rejected"])
        camp["_total_sent"]      = len(camp_approvals)

        if status in ("pending_approval", "pending"):
            pending.append(camp)
        elif status == "approved":
            approved_list.append(camp)
        elif status == "rejected":
            rejected.append(camp)

    return JSONResponse({"pending": pending, "approved": approved_list, "rejected": rejected})


# ── API: Campaigns CRUD ────────────────────────────────────────────────────

@router.get("/api/campaigns")
async def api_campaigns_list(current_user: User = Depends(require_user)):
    rows = _db_list("campaigns", {"client_id": _client_id(current_user.id)})
    return JSONResponse(rows)


@router.get("/api/manage-tree")
async def api_manage_tree(
    sync: str = Query("0"),
    current_user: User = Depends(require_user),
):
    cid = _client_id(current_user.id)
    campaigns = _db_list("campaigns", {"client_id": cid})
    adsets    = _db_list("adsets",    {"client_id": cid})
    ads       = _db_list("meta_ads",  {"client_id": cid})

    do_sync = sync == "1"
    if do_sync:
        try:
            token = _get_fb_token(current_user.id)
            _init_meta(token)
            _sync_meta_statuses(campaigns, adsets, ads)
        except Exception:
            pass

    adsets_by_camp: dict = {}
    for s in adsets:
        adsets_by_camp.setdefault(s.get("campaign_id", ""), []).append(s)
    ads_by_adset: dict = {}
    for a in ads:
        ads_by_adset.setdefault(a.get("adset_id", ""), []).append(a)

    try:
        approvals        = _db_get_all("approvals")
        approvals_by_ad: dict = {}
        for apvl in approvals:
            approvals_by_ad.setdefault(apvl.get("ad_id", ""), []).append(apvl)
        for a in ads:
            ad_apvls = approvals_by_ad.get(a["id"], [])
            a["_approval_total"]    = len(ad_apvls)
            a["_approval_approved"] = sum(1 for x in ad_apvls if x.get("status") == "approved")
            a["_approval_rejected"] = sum(1 for x in ad_apvls if x.get("status") == "rejected")
            a["_approval_pending"]  = sum(1 for x in ad_apvls if x.get("status") == "pending")
    except Exception:
        pass

    APPROVAL_STATUSES = {"pending_approval", "approved", "rejected"}
    tree = []
    for c in campaigns:
        appr_status = (c.get("approval_status") or "none").lower()
        if appr_status in APPROVAL_STATUSES:
            continue
        camp_adsets = adsets_by_camp.get(c["id"], [])
        for s in camp_adsets:
            s["ads"] = ads_by_adset.get(s["id"], [])
        c["adsets"] = camp_adsets
        tree.append(c)
    return JSONResponse(tree)


def _sync_meta_statuses(campaigns, adsets, ads):
    to_check = []
    for c in campaigns:
        mid = c.get("meta_campaign_id", "").strip()
        if mid:
            to_check.append(("campaign", mid, "campaigns", c))
    for s in adsets:
        mid = s.get("meta_adset_id", "").strip()
        if mid:
            to_check.append(("adset", mid, "adsets", s))
    for a in ads:
        mid = a.get("meta_ad_id", "").strip()
        if mid:
            to_check.append(("ad", mid, "meta_ads", a))

    if not to_check:
        return

    for obj_type, meta_id, db_table, local in to_check:
        try:
            if obj_type == "campaign":
                obj = FBCampaign(meta_id).api_get(fields=['effective_status'])
            elif obj_type == "adset":
                obj = FBAdSet(meta_id).api_get(fields=['effective_status'])
            else:
                obj = FBAd(meta_id).api_get(fields=['effective_status'])
            live_status = obj.get("effective_status", "").upper()
            status_map = {
                "ACTIVE": "ACTIVE", "PAUSED": "PAUSED", "DELETED": "DELETED",
                "ARCHIVED": "ARCHIVED", "CAMPAIGN_PAUSED": "PAUSED", "ADSET_PAUSED": "PAUSED",
                "DISAPPROVED": "DISAPPROVED", "PENDING_REVIEW": "PENDING_REVIEW",
                "WITH_ISSUES": "WITH_ISSUES", "IN_PROCESS": "IN_PROCESS",
            }
            new_status = status_map.get(live_status, live_status or local.get("launch_status", "draft"))
            old_status = local.get("launch_status", "")
            if new_status and new_status != old_status:
                local["launch_status"] = new_status
                try:
                    _db_save(db_table, None, local["id"], {"launch_status": new_status})
                except Exception:
                    pass
        except FacebookRequestError:
            if local.get("launch_status") not in ("error", "draft", "DELETED"):
                local["launch_status"] = "DELETED"
                try:
                    _db_save(db_table, None, local["id"], {"launch_status": "DELETED"})
                except Exception:
                    pass
        except Exception:
            pass


@router.post("/api/campaigns/save")
async def api_campaigns_save(request: Request, current_user: User = Depends(require_user)):
    body    = await request.json()
    item_id = body.pop("id", "")
    body["client_id"] = _client_id(current_user.id)
    body.setdefault("launch_status", "draft")
    new_id = _db_save("campaigns", None, item_id, body)
    return JSONResponse({"ok": True, "id": new_id})


@router.post("/api/campaigns/{cid}/launch")
async def api_campaigns_launch(cid: str, current_user: User = Depends(require_user)):
    rows = _db_list("campaigns")
    camp = next((r for r in rows if r["id"] == cid), None)
    if not camp:
        return JSONResponse({"error": "Not found"}, status_code=404)
    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"error": "No ad account configured"}, status_code=400)
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        mid = meta_launch_campaign(camp, acct)
        _db_save("campaigns", None, cid,
                 {"meta_campaign_id": mid, "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})
        return JSONResponse({"ok": True, "meta_campaign_id": mid})
    except Exception as e:
        _db_save("campaigns", None, cid, {"launch_status": "error", "error_msg": str(e)})
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/launch-all")
async def api_launch_all(request: Request, current_user: User = Depends(require_user)):
    body   = await request.json()
    cid    = body.get("campaign_id", "")
    sid    = body.get("adset_id", "")
    aid    = body.get("ad_id", "")
    status = body.get("status", "PAUSED")

    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"ok": False, "error": "No ad account configured"}, status_code=400)

    camp_rows  = _db_list("campaigns")
    adset_rows = _db_list("adsets")
    ad_rows    = _db_list("meta_ads")

    camp  = next((r for r in camp_rows  if r["id"] == cid), None)
    adset = next((r for r in adset_rows if r["id"] == sid), None)
    ad    = next((r for r in ad_rows    if r["id"] == aid), None)

    if not camp:  return JSONResponse({"ok": False, "error": "Campaign not found"}, status_code=404)
    if not adset: return JSONResponse({"ok": False, "error": "Ad Set not found"},   status_code=404)
    if not ad:    return JSONResponse({"ok": False, "error": "Ad not found"},        status_code=404)

    all_clients   = _db_get_all("clients")
    client        = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    try:
        all_approvers = _db_get_all("approvers")
        if ad_account_id:
            acct_approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
        else:
            acct_approvers = [a for a in all_approvers if a.get("client_id") == ad.get("client_id", "")]
        if acct_approvers and ad.get("approval_status") != "approved":
            return JSONResponse({"ok": False, "error": "Ad must be approved before publishing. Send for approval first."}, status_code=400)
    except Exception:
        pass

    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        meta_campaign_id = camp.get("meta_campaign_id", "")
        if not meta_campaign_id:
            meta_campaign_id = meta_launch_campaign(camp, acct)
            _db_save("campaigns", None, cid,
                     {"meta_campaign_id": meta_campaign_id, "launch_status": "launched",
                      "launched_at": now_iso(), "error_msg": ""})

        meta_adset_id = adset.get("meta_adset_id", "")
        if not meta_adset_id:
            camp_obj = camp.get("objective", "")
            if not adset.get("page_id") and ad.get("page_id"):
                adset["page_id"] = ad["page_id"]
            meta_adset_id = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=camp_obj)
            _db_save("adsets", None, sid,
                     {"meta_adset_id": meta_adset_id, "launch_status": "launched",
                      "launched_at": now_iso(), "error_msg": ""})

        ad["launch_status"] = status
        ad["_special_ad_categories"] = camp.get("special_ad_categories", "[]")
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid,
                 {"meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
                  "launch_status": status, "launched_at": now_iso(), "error_msg": ""})

        return JSONResponse({"ok": True, "meta_campaign_id": meta_campaign_id,
                             "meta_adset_id": meta_adset_id, "meta_ad_id": meta_ad_id})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.delete("/api/campaigns/{cid}/delete")
async def api_campaigns_delete(cid: str, current_user: User = Depends(require_user)):
    _db_delete("campaigns", cid)
    return JSONResponse({"ok": True})


# ── API: Ad Sets CRUD ──────────────────────────────────────────────────────

@router.get("/api/adsets")
async def api_adsets_list(
    campaign_id: str = Query(""),
    current_user: User = Depends(require_user),
):
    cid  = _client_id(current_user.id)
    rows = (_db_list("adsets", {"client_id": cid, "campaign_id": campaign_id})
            if campaign_id else _db_list("adsets", {"client_id": cid}))
    return JSONResponse(rows)


@router.post("/api/adsets/save")
async def api_adsets_save(request: Request, current_user: User = Depends(require_user)):
    body    = await request.json()
    item_id = body.pop("id", "")
    body["client_id"] = _client_id(current_user.id)
    body.setdefault("launch_status", "draft")
    new_id = _db_save("adsets", None, item_id, body)
    return JSONResponse({"ok": True, "id": new_id})


@router.post("/api/adsets/{sid}/launch")
async def api_adsets_launch(sid: str, current_user: User = Depends(require_user)):
    rows  = _db_list("adsets")
    adset = next((r for r in rows if r["id"] == sid), None)
    if not adset:
        return JSONResponse({"error": "Not found"}, status_code=404)
    camp_rows        = _db_list("campaigns")
    camp             = next((r for r in camp_rows if r["id"] == adset.get("campaign_id", "")), None)
    meta_campaign_id = (camp or {}).get("meta_campaign_id", "")
    if not meta_campaign_id:
        return JSONResponse({"error": "Parent campaign must be launched to Meta first"}, status_code=400)
    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"error": "No ad account configured"}, status_code=400)
    try:
        token    = _get_fb_token(current_user.id)
        _init_meta(token)
        camp_obj = (camp or {}).get("objective", "")
        mid = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=camp_obj)
        _db_save("adsets", None, sid,
                 {"meta_adset_id": mid, "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})
        return JSONResponse({"ok": True, "meta_adset_id": mid})
    except Exception as e:
        _db_save("adsets", None, sid, {"launch_status": "error", "error_msg": str(e)})
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/api/adsets/{sid}/delete")
async def api_adsets_delete(sid: str, current_user: User = Depends(require_user)):
    _db_delete("adsets", sid)
    return JSONResponse({"ok": True})


# ── API: Meta Ads CRUD ─────────────────────────────────────────────────────

@router.get("/api/meta-ads")
async def api_meta_ads_list(
    adset_id: str = Query(""),
    current_user: User = Depends(require_user),
):
    cid = _client_id(current_user.id)
    if adset_id:
        rows = _db_list("meta_ads", {"adset_id": adset_id})
        if cid:
            rows = [r for r in rows if r.get("client_id") in (cid, "", None)]
    else:
        rows = _db_list("meta_ads", {"client_id": cid} if cid else {})
    return JSONResponse(rows)


@router.post("/api/meta-ads/save")
async def api_meta_ads_save(request: Request, current_user: User = Depends(require_user)):
    body    = await request.json()
    item_id = body.pop("id", "")
    body["client_id"] = _client_id(current_user.id)
    body.setdefault("launch_status", "draft")
    if "link_url" in body:
        body["link_url"] = safe_url(body["link_url"])
    new_id = _db_save("meta_ads", None, item_id, body)
    return JSONResponse({"ok": True, "id": new_id})


@router.post("/api/meta-ads/{aid}/launch")
async def api_meta_ads_launch(
    aid: str,
    request: Request,
    current_user: User = Depends(require_user),
):
    rows = _db_list("meta_ads")
    ad   = next((r for r in rows if r["id"] == aid), None)
    if not ad:
        return JSONResponse({"error": "Not found"}, status_code=404)
    adset_rows    = _db_list("adsets")
    adset         = next((r for r in adset_rows if r["id"] == ad.get("adset_id", "")), None)
    meta_adset_id = (adset or {}).get("meta_adset_id", "")
    if not meta_adset_id:
        return JSONResponse({"error": "Parent ad set must be launched to Meta first"}, status_code=400)
    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"error": "No ad account configured"}, status_code=400)
    camp_rows = _db_list("campaigns")
    camp      = next((r for r in camp_rows if r["id"] == ad.get("campaign_id", "")), None)
    if not camp and adset:
        camp = next((r for r in camp_rows if r["id"] == adset.get("campaign_id", "")), None)
    ad["_special_ad_categories"] = (camp or {}).get("special_ad_categories", "[]")
    req_data            = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    ad["launch_status"] = req_data.get("status", "PAUSED") if req_data else "PAUSED"
    try:
        token = _get_fb_token(current_user.id)
        _init_meta(token)
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid, {
            "meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
            "launch_status": ad["launch_status"], "launched_at": now_iso(), "error_msg": "",
        })
        return JSONResponse({"ok": True, "meta_creative_id": creative_id, "meta_ad_id": meta_ad_id})
    except Exception as e:
        _db_save("meta_ads", None, aid, {"launch_status": "error", "error_msg": str(e)})
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/meta-ads/{aid}/publish")
async def api_meta_ads_publish(aid: str, request: Request, current_user: User = Depends(require_user)):
    acct = _ad_account(current_user.id)
    if not acct:
        return JSONResponse({"error": "No ad account configured"}, status_code=400)

    ads = _db_list("meta_ads")
    ad  = next((a for a in ads if a["id"] == aid), None)
    if not ad:
        return JSONResponse({"error": "Ad not found"}, status_code=404)

    req_data = {}
    try:
        req_data = await request.json()
    except Exception:
        pass
    status = req_data.get("status", "PAUSED")

    camp_rows = _db_list("campaigns")
    camp      = next((r for r in camp_rows if r["id"] == ad.get("campaign_id", "")), None)
    if not camp:
        return JSONResponse({"error": "Parent campaign not found in sheet"}, status_code=400)

    token = _get_fb_token(current_user.id)
    _init_meta(token)

    meta_campaign_id = camp.get("meta_campaign_id", "")
    if not meta_campaign_id:
        try:
            meta_campaign_id = meta_launch_campaign(camp, acct)
            _db_save("campaigns", None, camp["id"],
                     {"meta_campaign_id": meta_campaign_id, "launch_status": "launched",
                      "launched_at": now_iso(), "error_msg": ""})
        except Exception as e:
            return JSONResponse({"error": f"[campaigns] {e}"}, status_code=500)

    adset_rows = _db_list("adsets")
    adset      = next((r for r in adset_rows if r["id"] == ad.get("adset_id", "")), None)
    if not adset:
        return JSONResponse({"error": "Parent ad set not found in sheet"}, status_code=400)

    meta_adset_id = adset.get("meta_adset_id", "")
    if not meta_adset_id:
        try:
            campaign_obj  = camp.get("objective", "OUTCOME_AWARENESS")
            meta_adset_id = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=campaign_obj)
            _db_save("adsets", None, adset["id"],
                     {"meta_adset_id": meta_adset_id, "launch_status": "launched",
                      "launched_at": now_iso(), "error_msg": ""})
        except Exception as e:
            return JSONResponse({"error": f"[adsets] {e}"}, status_code=500)

    ad["_special_ad_categories"] = camp.get("special_ad_categories", "[]")
    ad["launch_status"] = status
    try:
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid,
                 {"meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
                  "launch_status": status, "launched_at": now_iso(), "error_msg": ""})
        return JSONResponse({"ok": True, "meta_ad_id": meta_ad_id})
    except Exception as e:
        _db_save("meta_ads", None, aid, {"launch_status": "error", "error_msg": str(e)})
        return JSONResponse({"error": f"[ad] {e}"}, status_code=500)


@router.post("/api/campaigns/{cid}/remove-from-queue")
async def api_campaign_remove_from_queue(cid: str, current_user: User = Depends(require_user)):
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return JSONResponse({"error": "Campaign not found.", "ok": False}, status_code=404)

    _db_update("campaigns", cid, {"approval_status": "none", "updated_at": now_iso()})
    adsets = _db_list("adsets", {"campaign_id": cid})
    for s in adsets:
        try:
            _db_update("adsets", s["id"], {"approval_status": "none", "updated_at": now_iso()})
        except Exception:
            pass
        for a in _db_list("meta_ads", {"adset_id": s["id"]}):
            try:
                _db_update("meta_ads", a["id"], {"approval_status": "none", "updated_at": now_iso()})
            except Exception:
                pass

    approvals = _db_list("approvals", {"ad_id": cid})
    for ap in approvals:
        try:
            _db_delete("approvals", ap["id"])
        except Exception:
            pass

    return JSONResponse({"ok": True})


@router.post("/api/campaigns/{cid}/unapprove")
async def api_campaign_unapprove(cid: str, current_user: User = Depends(require_user)):
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return JSONResponse({"error": "Campaign not found."}, status_code=404)

    _db_update("campaigns", cid, {"approval_status": "pending_approval", "updated_at": now_iso()})
    adsets = _db_list("adsets", {"campaign_id": cid})
    for s in adsets:
        try:
            _db_update("adsets", s["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
        except Exception:
            pass
        for a in _db_list("meta_ads", {"adset_id": s["id"]}):
            try:
                _db_update("meta_ads", a["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
            except Exception:
                pass

    approvals = _db_list("approvals", {"ad_id": cid})
    for ap in approvals:
        try:
            _db_update("approvals", ap["id"], {"status": "pending", "responded_at": None, "comments": ""})
        except Exception:
            pass

    return JSONResponse({"ok": True})


@router.post("/api/campaigns/{cid}/send-for-approval")
async def api_campaign_send_approval(cid: str, current_user: User = Depends(require_user)):
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return JSONResponse({"error": "Campaign not found."}, status_code=404)

    adsets   = _db_list("adsets", {"campaign_id": cid})
    all_ads  = []
    for s in adsets:
        all_ads.extend(_db_list("meta_ads", {"adset_id": s["id"]}))

    client_id    = campaign.get("client_id", "")
    client       = _db_find_by("clients", "id", client_id) if client_id else None
    all_approvers = _db_get_all("approvers")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        return JSONResponse({"error": "No approvers configured. Add approvers in the Approvers tab first."}, status_code=400)

    campaign["_adsets"] = adsets
    campaign["_ads"]    = all_ads
    primary_ad          = all_ads[0] if all_ads else {}

    existing_approvals = _db_list("approvals", {"ad_id": cid})
    existing_emails: dict = {}
    for ea in existing_approvals:
        existing_emails[ea["approver_email"]] = ea["status"]

    errors, sent_count = [], 0
    for apvr in approvers:
        email = apvr["email"]
        if existing_emails.get(email) == "approved":
            continue
        for ea in existing_approvals:
            if ea["approver_email"] == email and ea["status"] == "pending":
                try:
                    _db_delete("approvals", ea["id"])
                except Exception:
                    pass

        token = secrets.token_urlsafe(32)
        _db_save("approvals", None, None, {
            "id": gen_id(), "ad_id": cid,
            "approver_email": email, "approver_name": apvr["name"],
            "token": token, "status": "pending", "comments": "", "sent_at": now_iso(),
        })
        try:
            send_campaign_approval_email(apvr["email"], apvr["name"], campaign, primary_ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    _db_update("campaigns", cid, {"approval_status": "pending_approval", "updated_at": now_iso()})
    for s in adsets:
        _db_update("adsets", s["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
    for a in all_ads:
        _db_update("meta_ads", a["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})

    return JSONResponse({"ok": True, "sent": sent_count, "total_approvers": len(approvers), "errors": errors})


@router.post("/api/meta-ads/{aid}/send-for-approval")
async def api_meta_ads_send_approval(aid: str, current_user: User = Depends(require_user)):
    ads = _db_list("meta_ads")
    ad  = next((a for a in ads if a["id"] == aid), None)
    if not ad:
        return JSONResponse({"error": "Ad not found."}, status_code=404)

    all_clients   = _db_get_all("clients")
    client_id     = ad.get("client_id", "")
    client        = next((c for c in all_clients if c["id"] == client_id), None)
    all_approvers = _db_get_all("approvers")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        return JSONResponse({"error": "No approvers configured for this ad account. Add approvers in the Approvers tab first."}, status_code=400)

    ad.setdefault("title", ad.get("ad_name", "Untitled Ad"))

    existing_approvals = _db_list("approvals", {"ad_id": aid})
    existing_emails: dict = {}
    for ea in existing_approvals:
        existing_emails[ea["approver_email"]] = ea["status"]

    errors, sent_count = [], 0
    for apvr in approvers:
        email = apvr["email"]
        if existing_emails.get(email) == "approved":
            continue
        for ea in existing_approvals:
            if ea["approver_email"] == email and ea["status"] == "pending":
                try:
                    _db_delete("approvals", ea["id"])
                except Exception:
                    pass

        token = secrets.token_urlsafe(32)
        _db_save("approvals", None, None, {
            "id": gen_id(), "ad_id": aid,
            "approver_email": email, "approver_name": apvr["name"],
            "token": token, "status": "pending", "comments": "", "sent_at": now_iso(),
        })
        try:
            send_approval_email(apvr["email"], apvr["name"], ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    _db_save("meta_ads", None, aid, {"approval_status": "pending_approval", "updated_at": now_iso()})
    return JSONResponse({"ok": True, "sent": sent_count, "total_approvers": len(approvers), "errors": errors})


@router.get("/api/meta-ads/{aid}/approval-status")
async def api_meta_ads_approval_status(aid: str, current_user: User = Depends(require_user)):
    approvals    = _db_get_all("approvals")
    ad_approvals = [a for a in approvals if a.get("ad_id") == aid]
    total        = len(ad_approvals)
    approved     = sum(1 for a in ad_approvals if a.get("status") == "approved")
    rejected_n   = sum(1 for a in ad_approvals if a.get("status") == "rejected")
    pending      = sum(1 for a in ad_approvals if a.get("status") == "pending")
    return JSONResponse({
        "total": total, "approved": approved, "rejected": rejected_n, "pending": pending,
        "approvers": [{"name": a.get("approver_name"), "email": a.get("approver_email"),
                       "status": a.get("status"), "comments": a.get("comments"),
                       "responded_at": a.get("responded_at")} for a in ad_approvals]
    })


@router.delete("/api/meta-ads/{aid}/delete")
async def api_meta_ads_delete(aid: str, current_user: User = Depends(require_user)):
    ad = _db_find_by("meta_ads", "id", aid)
    if not ad:
        return JSONResponse({"ok": False, "error": "Ad not found"}, status_code=404)
    if ad.get("client_id") and ad["client_id"] != _client_id(current_user.id):
        return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
    _db_delete("meta_ads", aid)
    try:
        _db_delete_where("approvals", "ad_id", aid)
    except Exception:
        pass
    return JSONResponse({"ok": True})


@router.post("/api/meta-ads/{aid}/remove-from-queue")
async def api_meta_ads_remove_from_queue(aid: str, current_user: User = Depends(require_user)):
    try:
        ad = _db_find_by("meta_ads", "id", aid)
        if not ad:
            return JSONResponse({"ok": False, "error": "Ad not found"}, status_code=404)
        if ad.get("client_id") and ad["client_id"] != _client_id(current_user.id):
            return JSONResponse({"ok": False, "error": "Access denied"}, status_code=403)
        _db_save("meta_ads", None, aid, {"approval_status": "none"})
        _db_delete_where("approvals", "ad_id", aid)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Approval response routes (public — no auth required) ──────────────────

@router.get("/respond/{token}/{action}", response_class=HTMLResponse)
async def respond_to_approval_get(
    token: str,
    action: str,
    request: Request,
    confirmed: str = Query(""),
    comments: str = Query(""),
):
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="Invalid action")

    approval = _db_find_by("approvals", "token", token)
    if not approval:
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  "error",
            "message": "Invalid or expired approval link.",
        })

    if approval.get("status") != "pending":
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  "already",
            "message": f"You already {approval['status']} this ad.",
        })

    if confirmed == "1":
        new_status = "approved" if action == "approve" else "rejected"
        _db_update("approvals", approval["id"], {
            "status":       new_status,
            "comments":     comments,
            "responded_at": now_iso(),
        })
        check_ad_approval_status(approval["ad_id"])
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  new_status,
            "message": f"Thank you! You have {new_status} this ad.",
        })

    return templates.TemplateResponse(request, "fb/response_confirm.html", {
        "current_user": None,
        "token":    token,
        "action":   action,
        "approval": approval,
    })


@router.post("/respond/{token}/{action}", response_class=HTMLResponse)
async def respond_to_approval_post(
    token: str,
    action: str,
    request: Request,
    confirmed: str = Form(""),
    comments: str = Form(""),
):
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="Invalid action")

    approval = _db_find_by("approvals", "token", token)
    if not approval:
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  "error",
            "message": "Invalid or expired approval link.",
        })

    if approval.get("status") != "pending":
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  "already",
            "message": f"You already {approval['status']} this ad.",
        })

    if confirmed == "1":
        new_status = "approved" if action == "approve" else "rejected"
        _db_update("approvals", approval["id"], {
            "status":       new_status,
            "comments":     comments,
            "responded_at": now_iso(),
        })
        check_ad_approval_status(approval["ad_id"])
        return templates.TemplateResponse(request, "fb/response_page.html", {
            "current_user": None,
            "status":  new_status,
            "message": f"Thank you! You have {new_status} this ad.",
        })

    return templates.TemplateResponse(request, "fb/response_confirm.html", {
        "current_user": None,
        "token":    token,
        "action":   action,
        "approval": approval,
    })


# ── Ad detail / send for approval (legacy ad model) ────────────────────────

@router.get("/ad/new", response_class=HTMLResponse)
async def new_ad(request: Request, current_user: User = Depends(require_user)):
    active = _get_active_client(current_user.id)
    if not active:
        _flash(current_user.id, "Select or create a client first.", "error")
        return _redirect("/fb/clients")
    return _render(request, "ad_form.html", {"ad": None}, current_user)


@router.get("/ad/{ad_id}", response_class=HTMLResponse)
async def view_ad(ad_id: str, request: Request, current_user: User = Depends(require_user)):
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        _flash(current_user.id, "Ad not found.", "error")
        return _redirect("/fb/campaigns")

    approvals     = _db_get_all("approvals")
    ad_approvals  = [a for a in approvals if a.get("ad_id") == ad_id]
    all_clients   = _db_get_all("clients")
    client        = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    all_approvers = _db_get_all("approvers")
    client_id     = ad.get("client_id", "")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        scoped_approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        scoped_approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    return _render(request, "ad_detail.html",
                   {"ad": ad, "approvals": ad_approvals, "approvers": scoped_approvers}, current_user)


@router.post("/ad/{ad_id}/send")
async def send_for_approval_route(
    ad_id: str,
    request: Request,
    current_user: User = Depends(require_user),
):
    form = await request.form()
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        _flash(current_user.id, "Ad not found.", "error")
        return _redirect("/fb/campaigns")

    all_clients   = _db_get_all("clients")
    client        = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    all_approvers = _db_get_all("approvers")
    client_id     = ad.get("client_id", "")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        _flash(current_user.id, "No approvers configured for this ad account.", "error")
        return _redirect(f"/fb/ad/{ad_id}")

    selected_ids       = form.getlist("approver_ids")
    if not selected_ids:
        _flash(current_user.id, "Select at least one approver.", "error")
        return _redirect(f"/fb/ad/{ad_id}")

    selected_approvers = [a for a in approvers if a["id"] in selected_ids]
    errors, sent_count = [], 0
    for apvr in selected_approvers:
        token = secrets.token_urlsafe(32)
        _db_save("approvals", None, None, {
            "id": gen_id(), "ad_id": ad_id,
            "approver_email": apvr["email"], "approver_name": apvr["name"],
            "token": token, "status": "pending",
            "comments": "", "sent_at": now_iso(), "responded_at": "",
        })
        try:
            send_approval_email(apvr["email"], apvr["name"], ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    existing_ad = _db_find_by("ads", "id", ad_id)
    if existing_ad:
        _db_update("ads", ad_id, {"status": "pending_approval", "updated_at": now_iso()})

    if sent_count:
        _flash(current_user.id, f"Sent approval request to {sent_count} approver(s).", "success")
    for err in errors:
        _flash(current_user.id, err, "error")

    return _redirect(f"/fb/ad/{ad_id}")
