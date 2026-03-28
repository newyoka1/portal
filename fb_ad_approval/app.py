"""
Facebook Ad Approval Workflow
─────────────────────────────
Clients → Draft ads → Send for approval → Track responses → Push to Facebook
"""

import os, json, uuid, smtplib, secrets, time
from datetime import timedelta
from werkzeug.utils import secure_filename
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from functools import wraps
from urllib.parse import urlparse

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, abort, session
)
from dotenv import load_dotenv
import requests as http_requests

# ── Meta Business SDK ────────────────────────────────────────────────────
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign as FBCampaign
from facebook_business.adobjects.adset import AdSet as FBAdSet
from facebook_business.adobjects.ad import Ad as FBAd
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User as FBUser
from facebook_business.adobjects.targetingsearch import TargetingSearch
from facebook_business.exceptions import FacebookRequestError

import re as _re
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# Custom Jinja filter to parse JSON strings into Python objects
@app.template_filter('tojson_parse')
def tojson_parse(value):
    """Parse a JSON string into a Python list/dict for use in templates."""
    if not value:
        return []
    try:
        result = json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, (list, dict)) else []
    except (json.JSONDecodeError, TypeError):
        return []

# ── CSRF Protection ──────────────────────────────────────────────────────
from flask_wtf.csrf import CSRFProtect, generate_csrf
csrf = CSRFProtect(app)

# Make CSRF token available to templates
@app.context_processor
def _inject_csrf():
    return dict(csrf_token=generate_csrf)


# ── Authentication ────────────────────────────────────────────────────────
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message_category = "info"

class AppUser(UserMixin):
    def __init__(self, row):
        self.id = row["id"]
        self.email = row["email"]
        self.name = row["name"]
        self.role = row["role"]
        self.is_active_flag = row.get("is_active", 1)

    def get_id(self):
        return self.id

    @property
    def is_active(self):
        return bool(self.is_active_flag)

    @property
    def is_admin(self):
        return self.role == "admin"

@login_manager.user_loader
def load_user(user_id):
    try:
        users = _db_list("users", {"id": user_id})
        if users:
            return AppUser(users[0])
    except Exception:
        pass
    return None

def admin_required(f):
    """Decorator: require admin role."""
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not current_user.is_admin:
            flash("Admin access required.", "danger")
            return redirect(url_for("campaigns_page"))
        return f(*args, **kwargs)
    return decorated


UPLOAD_FOLDER   = os.path.join(os.path.dirname(__file__), "static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_IMAGES  = {"jpg", "jpeg", "png", "gif", "webp"}
ALLOWED_VIDEOS  = {"mp4", "mov", "avi", "mkv", "webm"}
ALLOWED_MEDIA   = ALLOWED_IMAGES | ALLOWED_VIDEOS

# Facebook media limits
FB_IMAGE_MAX_SIZE = 30 * 1024 * 1024      # 30 MB for images
FB_VIDEO_MAX_SIZE = 4 * 1024 * 1024 * 1024  # 4 GB for videos (API limit)
UPLOAD_MAX_SIZE   = 500 * 1024 * 1024     # 500 MB practical upload limit

app.config["MAX_CONTENT_LENGTH"] = UPLOAD_MAX_SIZE
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=365)

@app.template_filter("from_json")
def from_json_filter(value):
    try:
        return json.loads(value) if value else []
    except (json.JSONDecodeError, TypeError):
        return []

# ── Config ────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
FB_APP_ID         = os.getenv("FB_APP_ID", "")
FB_APP_SECRET     = os.getenv("FB_APP_SECRET", "")
GMAIL_SENDER      = os.getenv("GMAIL_SENDER_EMAIL", "")
GMAIL_APP_PASS    = os.getenv("GMAIL_APP_PASSWORD", "")
BASE_URL          = os.getenv("BASE_URL", "http://localhost:5000")

# ── Facebook Ad Placements ─────────────────────────────────────────────────
AD_PLACEMENTS = [
    ("image_square",    "Feed — Square",    "1080×1080", "1:1"),
    ("image_landscape", "Feed — Landscape", "1200×628",  "1.91:1"),
    ("image_portrait",  "Feed — Portrait",  "1080×1350", "4:5"),
    ("image_stories",   "Stories & Reels",  "1080×1920", "9:16"),
]

CTA_LABELS = {
    "LEARN_MORE": "Learn More",  "SHOP_NOW":    "Shop Now",
    "SIGN_UP":    "Sign Up",     "BOOK_NOW":    "Book Now",
    "CONTACT_US": "Contact Us",  "DONATE_NOW":  "Donate Now",
    "GET_OFFER":  "Get Offer",   "APPLY_NOW":   "Apply Now",
    "SUBSCRIBE":  "Subscribe",   "DOWNLOAD":    "Download",
    "WATCH_MORE": "Watch More",  "GET_QUOTE":   "Get Quote",
    "NO_BUTTON":  "",
}

CLIENT_COLORS = [
    "#1877F2","#E74C3C","#2ECC71","#F39C12",
    "#9B59B6","#1ABC9C","#E67E22","#3498DB",
    "#E91E63","#00BCD4","#FF5722","#607D8B",
]

# ── Meta API ──────────────────────────────────────────────────────────────
META_GRAPH = "https://graph.facebook.com/v21.0"

ACCOUNT_STATUS = {
    "1": "Active", "2": "Disabled", "3": "Unsettled",
    "7": "Pending Review", "9": "In Grace Period",
    "100": "Pending Closure", "101": "Closed",
}

FB_TOKEN_FILE = os.path.join(os.path.dirname(__file__), "credentials", "fb_token.json")

def _read_stored_fb_token():
    """Read the persisted personal FB token from disk, if any."""
    try:
        with open(FB_TOKEN_FILE) as f:
            data = json.load(f)
            return data.get("token", ""), data.get("name", ""), data.get("id", "")
    except Exception:
        return "", "", ""

def _write_stored_fb_token(token, name, uid):
    os.makedirs(os.path.dirname(FB_TOKEN_FILE), exist_ok=True)
    with open(FB_TOKEN_FILE, "w") as f:
        json.dump({"token": token, "name": name, "id": uid}, f)

def _clear_stored_fb_token():
    try:
        os.remove(FB_TOKEN_FILE)
    except FileNotFoundError:
        pass

def get_active_token():
    """Return the best available Meta access token: session > persisted file > .env."""
    from flask import session as _s
    t = _s.get("fb_user_token")
    if t:
        return t
    # Auto-restore from persisted file if session is empty
    stored_token, stored_name, stored_id = _read_stored_fb_token()
    if stored_token:
        _s["fb_user_token"] = stored_token
        _s["fb_user_name"]  = stored_name
        _s["fb_user_id"]    = stored_id
        return stored_token
    return META_ACCESS_TOKEN

def _get_page_token(page_id, user_token=None):
    """Get a page access token for the given page ID.
    Lead gen forms require a page token, not a user token."""
    token = user_token or get_active_token()
    if not token or not page_id:
        return None
    try:
        result = meta_get(f"/{page_id}", {"fields": "access_token"}, token=token)
        return result.get("access_token")
    except Exception:
        return None

# ── Simple in-memory cache for Meta API responses ─────────────────────────
_meta_cache = {}
_META_CACHE_TTL = 300  # 5 minutes

def _cache_key(endpoint, extra_params):
    """Build a hashable cache key from endpoint + params."""
    p = tuple(sorted((extra_params or {}).items()))
    return (endpoint, p)

def _init_meta(token=None):
    """Initialize the Meta SDK with the current active token. Call per-request."""
    t = token or get_active_token()
    if not t:
        raise Exception("No Meta access token available — connect your Facebook account.")
    # Don't pass app_secret — it forces appsecret_proof which fails if the token
    # was generated from a different app or the Graph API Explorer.
    FacebookAdsApi.init(
        app_id=FB_APP_ID or 'placeholder',
        app_secret='',
        access_token=t,
        api_version='v21.0',
    )
    return t

def _sdk_to_dict(obj):
    """Convert an SDK object (AdAccount, Campaign, etc.) to a plain dict."""
    return dict(obj)

def _sdk_cursor_to_list(cursor):
    """Exhaust a Cursor and return a list of plain dicts."""
    return [dict(item) for item in cursor]

def _cached_sdk(cache_key_str, fetcher):
    """Cache wrapper for SDK calls. Returns cached data or calls fetcher()."""
    hit = _meta_cache.get(cache_key_str)
    if hit and (time.time() - hit["t"]) < _META_CACHE_TTL:
        return hit["data"]
    data = fetcher()
    _meta_cache[cache_key_str] = {"data": data, "t": time.time()}
    return data

# Keep raw meta_get for token exchange & debug endpoint only
def meta_get(endpoint, extra_params=None, token=None, cache=True):
    """GET from Meta Graph API (raw HTTP fallback for non-SDK calls)."""
    if cache:
        key = _cache_key(endpoint, extra_params)
        hit = _meta_cache.get(key)
        if hit and (time.time() - hit["t"]) < _META_CACHE_TTL:
            return hit["data"]
    params = {"access_token": token or get_active_token(), "limit": 200}
    if extra_params:
        params.update(extra_params)
    resp = http_requests.get(f"{META_GRAPH}{endpoint}", params=params, timeout=15)
    data = resp.json()
    if cache and "error" not in data:
        _meta_cache[_cache_key(endpoint, extra_params)] = {"data": data, "t": time.time()}
    return data

def meta_get_all(endpoint, extra_params=None, token=None):
    """GET from Meta Graph API with pagination (raw HTTP fallback for debug)."""
    result = meta_get(endpoint, extra_params, token=token)
    if "error" in result:
        return result
    items = list(result.get("data", []))
    paging = result.get("paging", {})
    while paging.get("next"):
        resp = http_requests.get(paging["next"], timeout=15)
        page = resp.json()
        if "error" in page:
            break
        items.extend(page.get("data", []))
        paging = page.get("paging", {})
    return {"data": items}

# ── Routes: Facebook personal login (session token) ───────────────────────
from flask import jsonify

@app.route("/auth/fb/token", methods=["POST"])
@csrf.exempt
def auth_fb_token():
    """Store the user's personal FB access token.
    If FB_APP_ID + FB_APP_SECRET are configured, auto-exchange for a
    long-lived token (~60 days) before saving."""
    data  = request.get_json(silent=True) or {}
    token = data.get("token", "").strip()
    if not token:
        return jsonify({"error": "No token provided"}), 400

    # ── Attempt long-lived token exchange (~60 days) ────────────
    # Only exchange if the token belongs to our app — tokens from Graph Explorer
    # or other apps will fail the exchange but are still valid for API calls.
    ll_success = False
    if FB_APP_ID and FB_APP_SECRET and FB_APP_ID != "YOUR_APP_ID_HERE":
        try:
            # Check which app the token belongs to
            debug_resp = http_requests.get(
                "https://graph.facebook.com/debug_token",
                params={"input_token": token, "access_token": f"{FB_APP_ID}|{FB_APP_SECRET}"},
                timeout=10,
            )
            debug_data = debug_resp.json().get("data", {})
            token_app_id = str(debug_data.get("app_id", ""))
            expires_at = debug_data.get("expires_at", 0)
            is_valid = debug_data.get("is_valid", False)

            if not is_valid:
                print(f"[WARN] Token is invalid according to debug_token")
            elif token_app_id == FB_APP_ID:
                # Token belongs to our app — exchange for long-lived
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
                    token = ll["access_token"]
                    ll_success = True
                    print(f"[OK] Exchanged for long-lived token (expires_in={ll.get('expires_in', '?')}s)")
                else:
                    print(f"[WARN] Long-lived exchange failed: {ll.get('error', {}).get('message', 'unknown')}")
            else:
                # Token from a different app (e.g. Graph Explorer) — check its expiry
                import time as _time
                if expires_at == 0:
                    print(f"[OK] Token from app {token_app_id} has no expiry (long-lived/system token)")
                    ll_success = True
                elif expires_at > _time.time() + 86400 * 7:
                    days_left = int((expires_at - _time.time()) / 86400)
                    print(f"[OK] Token from app {token_app_id} expires in {days_left} days — already long-lived")
                    ll_success = True
                else:
                    days_left = max(0, int((expires_at - _time.time()) / 86400))
                    hours_left = max(0, int((expires_at - _time.time()) / 3600))
                    print(f"[WARN] Token from app {token_app_id} expires in {hours_left}h — short-lived, can't exchange (different app)")
                    print(f"[HINT] For a 60-day token: use Graph Explorer with app {FB_APP_ID}, or generate from your app's settings")
        except Exception as exc:
            print(f"[WARN] Token exchange error: {exc}")

    check = meta_get("/me", {"fields": "id,name"}, token=token)
    if "error" in check:
        err = check["error"]
        return jsonify({"error": err.get("message", str(err)) if isinstance(err, dict) else str(err)}), 400
    name = check.get("name", "")
    uid  = check.get("id", "")
    session["fb_user_token"] = token
    session["fb_user_name"]  = name
    session["fb_user_id"]    = uid
    session.permanent        = True
    _write_stored_fb_token(token, name, uid)
    return jsonify({"ok": True, "name": name, "id": uid, "long_lived": ll_success})

@app.route("/auth/fb/disconnect", methods=["POST"])
@csrf.exempt
def auth_fb_disconnect():
    session.pop("fb_user_token", None)
    session.pop("fb_user_name",  None)
    session.pop("fb_user_id",    None)
    _clear_stored_fb_token()
    return jsonify({"ok": True})

@app.route("/auth/fb/status")
def auth_fb_status():
    # get_active_token() auto-restores from file if session is empty
    token = get_active_token()
    if token and token != META_ACCESS_TOKEN:
        return jsonify({"connected": True, "name": session.get("fb_user_name", ""), "id": session.get("fb_user_id", "")})
    return jsonify({"connected": False})

# ── Route: Image upload ────────────────────────────────────────────────────
# Images are uploaded via SFTP to WP Engine for public hosting,
# with a local fallback to static/uploads/ if SFTP is not configured.

# SFTP config — read from DB settings at runtime via get_setting()

def _sftp_upload(file_bytes, filename):
    """Upload file bytes to WP Engine via SFTP. Returns public URL."""
    import paramiko
    host = get_setting("SFTP_HOST")
    port = int(get_setting("SFTP_PORT") or "2222")
    user = get_setting("SFTP_USER")
    pw   = get_setting("SFTP_PASS")
    sdir = get_setting("SFTP_DIR") or "ad-images"
    base = get_setting("SFTP_BASE_URL") or "https://politikanyc.com/ad-images"
    if not host or not user or not pw:
        raise Exception("SFTP not configured — check Settings")
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=pw)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        sftp.mkdir(sdir)
    except IOError:
        pass
    with sftp.open(f"{sdir}/{filename}", "wb") as remote_file:
        remote_file.write(file_bytes)
    sftp.close()
    transport.close()
    return f"{base}/{filename}"

@app.route("/upload", methods=["POST"])
@csrf.exempt
def upload_image():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided"}), 400
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_MEDIA:
        return jsonify({"error": f"File type .{ext} not allowed. Use JPG, PNG, WebP, GIF, MP4, or MOV."}), 400
    is_video = ext in ALLOWED_VIDEOS
    filename = f"{secrets.token_hex(10)}.{ext}"
    file_bytes = f.read()
    file_size = len(file_bytes)
    file_type = "video" if is_video else "image"

    # Validate file size per Facebook limits
    if is_video and file_size > FB_VIDEO_MAX_SIZE:
        return jsonify({"error": f"Video too large ({file_size // (1024*1024)}MB). Facebook max is 4GB."}), 400
    if not is_video and file_size > FB_IMAGE_MAX_SIZE:
        return jsonify({"error": f"Image too large ({file_size // (1024*1024)}MB). Facebook max is 30MB."}), 400

    # Try SFTP first (production), fall back to local (dev)
    sftp_host = get_setting("SFTP_HOST")
    sftp_user = get_setting("SFTP_USER")
    sftp_pass = get_setting("SFTP_PASS")
    if sftp_host and sftp_user and sftp_pass:
        try:
            url = _sftp_upload(file_bytes, filename)
            # Also save locally for dev/preview
            os.makedirs(UPLOAD_FOLDER, exist_ok=True)
            with open(os.path.join(UPLOAD_FOLDER, filename), "wb") as lf:
                lf.write(file_bytes)
            return jsonify({"url": url, "type": file_type, "size": file_size, "ext": ext})
        except Exception as e:
            print(f"[WARN] SFTP upload failed: {e}, falling back to local")

    # Local fallback
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    with open(os.path.join(UPLOAD_FOLDER, filename), "wb") as lf:
        lf.write(file_bytes)
    return jsonify({"url": f"/static/uploads/{filename}", "type": file_type, "size": file_size, "ext": ext})

# ── Routes: Meta API proxy (called by browser JS) ─────────────────────────

@app.route("/api/meta/cache-clear", methods=["POST"])
@csrf.exempt
def api_meta_cache_clear():
    """Clear the Meta API cache."""
    _meta_cache.clear()
    return jsonify({"ok": True, "msg": "Cache cleared"})

@app.route("/api/meta/status")
def api_meta_status():
    """Check token validity and return the user/business identity."""
    try:
        _init_meta()
        me = FBUser('me').api_get(fields=['id', 'name'])
        return jsonify({"ok": True, "name": me.get("name"), "id": me.get("id")})
    except FacebookRequestError as e:
        return jsonify({"ok": False, "error": e.api_error_message()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/meta/adaccounts")
def api_meta_adaccounts():
    """All ad accounts the token has access to."""
    try:
        _init_meta()
        def _fetch():
            cursor = FBUser('me').get_ad_accounts(
                fields=['id', 'name', 'account_status', 'currency', 'timezone_name', 'business'],
            )
            return _sdk_cursor_to_list(cursor)
        data = _cached_sdk("adaccounts", _fetch)
        accounts = []
        for acct in data:
            accounts.append({
                "id":             str(acct.get("id", "")).replace("act_", ""),
                "name":           acct.get("name", ""),
                "account_status": acct.get("account_status", 0),
                "currency":       acct.get("currency", ""),
                "business_id":    (acct.get("business") or {}).get("id", ""),
            })
        return jsonify({"accounts": accounts})
    except FacebookRequestError as e:
        return jsonify({"error": e.api_error_message()})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/meta/pages")
def api_meta_pages():
    """Facebook Pages via personal token — single /me/accounts call, cached."""
    try:
        _init_meta()
        def _fetch():
            cursor = FBUser('me').get_accounts(fields=['id', 'name', 'category'])
            return _sdk_cursor_to_list(cursor)
        data = _cached_sdk("pages", _fetch)
        pages = sorted([
            {"id": p["id"], "name": p.get("name", ""), "category": p.get("category", "")}
            for p in data if p.get("id")
        ], key=lambda p: p["name"].lower())
        return jsonify({"pages": pages, "total": len(pages)})
    except FacebookRequestError as e:
        return jsonify({"pages": [], "error": e.api_error_message()})
    except Exception as e:
        return jsonify({"pages": [], "error": str(e)})

@app.route("/api/meta/audiences/<path:ad_account_id>")
def api_meta_audiences(ad_account_id):
    """Saved + Custom audiences for a given ad account."""
    try:
        _init_meta()
        acct = ad_account_id.replace("act_", "")
        account = AdAccount(f'act_{acct}')

        saved_error = custom_error = None
        saved_list = []
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

        return jsonify({"saved": saved_list, "custom": custom_list,
                        "saved_error": saved_error, "custom_error": custom_error})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/meta/pages/debug")
def api_meta_pages_debug():
    """Raw page data from every source — use this to diagnose missing pages."""
    if not get_active_token():
        return jsonify({"error": "No Facebook token — connect your account on the Clients page."})
    out = {}
    out["/me/accounts"] = meta_get_all("/me/accounts", {"fields": "id,name,category"})

    businesses = meta_get_all("/me/businesses", {"fields": "id,name"})
    out["/me/businesses"] = businesses

    biz_ids = [b["id"] for b in businesses.get("data", []) if b.get("id")]
    for env_bid in os.getenv("META_BUSINESS_IDS", "").split(","):
        env_bid = env_bid.strip()
        if env_bid and env_bid not in biz_ids:
            biz_ids.append(env_bid)

    for bid in biz_ids:
        for ep in (f"/{bid}/owned_pages", f"/{bid}/client_pages"):
            out[ep] = meta_get_all(ep, {"fields": "id,name,category"})

    ad_accounts = meta_get_all("/me/adaccounts", {"fields": "id,name,business"})
    out["/me/adaccounts"] = {"data": [{"id": a["id"], "name": a["name"], "business": a.get("business")} for a in ad_accounts.get("data", [])]}

    return jsonify(out)

@app.route("/api/meta/targeting/search")
def api_meta_targeting_search():
    """Search interests, behaviors, and demographics."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"data": []})
    try:
        _init_meta()
        results = TargetingSearch.search(params={
            'type': TargetingSearch.TargetingSearchTypes.interest,
            'q': q,
            'limit': 20,
        })
        items = []
        for item in results:
            d = dict(item)
            path = d.get("path", [])
            items.append({
                "id":         d["id"],
                "name":       d["name"],
                "type":       "interest",
                "breadcrumb": " > ".join(path[:-1]) if len(path) > 1 else (d.get("topic") or "Interest"),
                "size_low":   d.get("audience_size_lower_bound", 0),
                "size_high":  d.get("audience_size_upper_bound", 0),
            })
        return jsonify({"data": items})
    except FacebookRequestError as e:
        return jsonify({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return jsonify({"error": str(e), "data": []})

@app.route("/api/meta/location/search")
def api_meta_location_search():
    """Search locations: cities, regions, countries, zip codes, DMAs, electoral districts."""
    q           = request.args.get("q", "").strip()
    type_filter = request.args.get("type_filter", "").strip()
    if not q:
        return jsonify({"data": []})
    try:
        _init_meta()
        all_types = ["city","region","country","zip","geo_market","electoral_district","neighborhood","subcity","country_group"]
        loc_types = [type_filter] if type_filter else all_types
        results = TargetingSearch.search(params={
            'type': TargetingSearch.TargetingSearchTypes.geolocation,
            'q': q,
            'location_types': loc_types,
            'limit': 25,
        })
        items = []
        for loc in results:
            d = dict(loc)
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
        return jsonify({"data": items})
    except FacebookRequestError as e:
        return jsonify({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return jsonify({"error": str(e), "data": []})

@app.route("/api/meta/instagram-accounts")
def api_meta_instagram_accounts():
    """Instagram accounts connected to a Facebook Page."""
    page_id = request.args.get("page_id", "").strip()
    if not page_id:
        return jsonify({"data": []})
    try:
        _init_meta()
        # Get Instagram Business Account linked to this Page
        result = meta_get(f"/{page_id}", {"fields": "instagram_business_account,connected_instagram_accounts"})
        accounts = []
        # Primary Instagram Business Account
        iba = result.get("instagram_business_account")
        if iba:
            detail = meta_get(f"/{iba['id']}", {"fields": "id,username,profile_picture_url,name"})
            accounts.append({
                "id": detail.get("id", iba["id"]),
                "username": detail.get("username", ""),
                "name": detail.get("name", ""),
                "profile_pic": detail.get("profile_picture_url", ""),
            })
        # Connected Instagram accounts (may include additional ones)
        connected = result.get("connected_instagram_accounts", {}).get("data", [])
        seen = {a["id"] for a in accounts}
        for c in connected:
            if c["id"] not in seen:
                accounts.append({
                    "id": c["id"],
                    "username": c.get("username", ""),
                    "name": c.get("name", ""),
                    "profile_pic": c.get("profile_picture_url", ""),
                })
        return jsonify({"data": accounts})
    except Exception as e:
        return jsonify({"data": [], "error": str(e)})


# ── Lead Gen Forms ────────────────────────────────────────────────────────

LEAD_FORM_QUESTION_TYPES = [
    ("FIRST_NAME",    "First Name"),
    ("LAST_NAME",     "Last Name"),
    ("FULL_NAME",     "Full Name"),
    ("EMAIL",         "Email"),
    ("PHONE_NUMBER",  "Phone Number"),
    ("STREET_ADDRESS","Street Address"),
    ("CITY",          "City"),
    ("STATE",         "State"),
    ("ZIP_CODE",      "Zip Code"),
    ("COUNTRY",       "Country"),
    ("DATE_OF_BIRTH", "Date of Birth"),
    ("GENDER",        "Gender"),
    ("MARITAL_STATUS","Marital Status"),
    ("JOB_TITLE",     "Job Title"),
    ("COMPANY_NAME",  "Company Name"),
    ("WORK_EMAIL",    "Work Email"),
    ("WORK_PHONE_NUMBER","Work Phone"),
    ("MILITARY_STATUS","Military Status"),
]

@app.route("/api/meta/leadgen-forms")
def api_meta_leadgen_forms():
    """List existing lead forms for a Facebook Page."""
    page_id = request.args.get("page_id", "").strip()
    if not page_id:
        return jsonify({"data": []})
    try:
        token = get_active_token()
        # Lead gen forms require a page access token
        page_token = _get_page_token(page_id, token)
        result = meta_get_all(f"/{page_id}/leadgen_forms",
                              {"fields": "id,name,status,leads_count"},
                              token=page_token or token)
        if "error" in result:
            err = result["error"]
            return jsonify({"data": [], "error": err.get("message", str(err)) if isinstance(err, dict) else str(err)})
        forms = [{"id": f.get("id"), "name": f.get("name", ""),
                  "status": f.get("status", ""), "leads_count": f.get("leads_count", 0)}
                 for f in result.get("data", [])]
        return jsonify({"data": forms})
    except Exception as e:
        return jsonify({"data": [], "error": str(e)})

@app.route("/api/meta/leadgen-form-detail")
def api_meta_leadgen_form_detail():
    """Fetch full details of a single lead form (for preview)."""
    form_id = request.args.get("form_id", "").strip()
    if not form_id:
        return jsonify({"error": "No form_id"})
    try:
        token = get_active_token()
        resp = http_requests.get(
            f"https://graph.facebook.com/v21.0/{form_id}",
            params={"access_token": token,
                    "fields": "name,questions,privacy_policy_url,legal_content,thank_you_page,context_card"},
            timeout=10,
        )
        data = resp.json()
        if "error" in data:
            return jsonify({"error": data["error"].get("message", "Unknown error")})
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/lead-forms/save", methods=["POST"])
@csrf.exempt
def api_lead_forms_save():
    """Save lead form data locally for approval email previews."""
    body = request.get_json(silent=True) or {}
    ad_id = body.pop("ad_id", "")
    link_only = body.pop("_link_only", False)
    body["client_id"] = _client_id()
    body["ad_id"] = ad_id

    # Check if a form already exists for this ad OR this meta_form_id
    existing = _db_list("lead_forms", {"ad_id": ad_id}) if ad_id else []
    if not existing and body.get("meta_form_id"):
        existing = _db_list("lead_forms", {"meta_form_id": body["meta_form_id"]})

    if existing:
        if link_only:
            # Only update the ad_id link — don't overwrite any other fields
            _db_save("lead_forms", None, existing[0]["id"], {"ad_id": ad_id})
        else:
            _db_save("lead_forms", None, existing[0]["id"], body)
        return jsonify({"ok": True, "id": existing[0]["id"]})
    else:
        new_id = _db_save("lead_forms", None, None, body)
        return jsonify({"ok": True, "id": new_id})

@app.route("/api/meta/leadgen-forms/create", methods=["POST"])
@csrf.exempt
def api_meta_leadgen_form_create():
    """Create a new lead gen form on a Facebook Page."""
    body = request.get_json(silent=True) or {}
    page_id = body.get("page_id", "").strip()
    form_name = body.get("name", "").strip()
    questions = body.get("questions", [])
    privacy_url = safe_url(body.get("privacy_policy_url", ""))
    follow_up_url = safe_url(body.get("follow_up_action_url", ""))
    thank_you = body.get("thank_you", {})

    if not page_id:
        return jsonify({"error": "page_id is required"}), 400
    if not form_name:
        return jsonify({"error": "Form name is required"}), 400
    if not questions:
        return jsonify({"error": "At least one question is required"}), 400
    if not privacy_url:
        return jsonify({"error": "Privacy policy URL is required"}), 400

    try:
        _init_meta()
        intro     = body.get("intro", {})
        settings  = body.get("settings", {})
        form_type = body.get("form_type", "MORE_VOLUME")

        # Build questions array for Meta API
        meta_questions = []
        for q in questions:
            q_type = q.get("type", "").upper()
            q_obj = {"type": q_type}
            if q_type == "CUSTOM":
                q_obj["key"]   = q.get("key", q.get("label", "custom")).lower().replace(" ", "_")
                q_obj["label"] = q.get("label", "")
            if q.get("field_name"):
                q_obj["field_name"] = q["field_name"]
            meta_questions.append(q_obj)

        # Privacy policy
        privacy_obj = {"url": privacy_url}
        privacy_text = body.get("privacy_policy_text", "").strip()
        if privacy_text:
            privacy_obj["link_text"] = privacy_text

        params = {
            "name": form_name,
            "questions": json.dumps(meta_questions),
            "privacy_policy": json.dumps(privacy_obj),
            "follow_up_action_url": follow_up_url or privacy_url,
        }

        # Form type: MORE_VOLUME or HIGHER_INTENT
        if form_type == "HIGHER_INTENT":
            params["is_optimized_for_quality"] = "true"

        # Language
        lang = settings.get("language", "").strip()
        if lang:
            params["locale"] = lang

        # Sharing
        sharing = settings.get("sharing", "RESTRICTED")
        if sharing == "OPEN":
            params["allow_organic_lead"] = "true"

        # Tracking parameters
        tracking = settings.get("tracking_params", "").strip()
        if tracking:
            params["tracking_parameters"] = json.dumps({"url_tags": tracking})

        # Question description
        q_desc = body.get("question_description", "").strip()

        # Context card / Intro greeting
        if intro.get("greeting") and (intro.get("headline") or intro.get("description")):
            ctx_content = []
            if intro.get("description"):
                ctx_content = [intro["description"]]
            params["context_card"] = json.dumps({
                "title": intro.get("headline", ""),
                "content": ctx_content,
                "style": "PARAGRAPH_STYLE",
            })

        # Custom disclaimers
        disclaimers = body.get("disclaimers", [])
        if disclaimers:
            consent_items = []
            for d in disclaimers:
                if d.get("title") or d.get("text"):
                    consent_items.append({
                        "type": "CUSTOM",
                        "is_required": True,
                        "key": d.get("title", "disclaimer").lower().replace(" ", "_"),
                        "title": d.get("title", ""),
                        "content": d.get("text", ""),
                    })
            if consent_items:
                params["legal_content"] = json.dumps({"custom_disclaimer": consent_items})

        # Thank you / Ending screen
        if thank_you.get("headline"):
            action_type = thank_you.get("action_type", "website")
            ty_obj = {
                "title": thank_you.get("headline", "Thanks!"),
                "body": thank_you.get("description", "We'll be in touch."),
            }
            if action_type == "call":
                ty_obj["button_type"] = "CALL_BUSINESS"
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

        # POST to /{page_id}/leadgen_forms — requires page access token
        token = get_active_token()
        page_token = _get_page_token(page_id, token)
        resp = http_requests.post(
            f"{META_GRAPH}/{page_id}/leadgen_forms",
            data={**params, "access_token": page_token or token},
            timeout=30,
        )
        result = resp.json()
        if "error" in result:
            err = result["error"]
            if isinstance(err, dict):
                msg = err.get("error_user_msg") or err.get("message") or str(err)
            else:
                msg = str(err)
            return jsonify({"error": msg}), 400

        return jsonify({"ok": True, "id": result.get("id"), "name": form_name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/objective-goals")
def api_objective_goals():
    """Return valid optimization goals for each campaign objective."""
    return jsonify(OBJECTIVE_VALID_GOALS)

@app.route("/api/meta/pixels")
def api_meta_pixels():
    """List all pixels for the active ad account (or ?acct= override)."""
    acct = request.args.get("acct", "").strip() or _ad_account()
    if not acct:
        return jsonify({"data": [], "error": "No ad account configured"})
    try:
        _init_meta()
        account = AdAccount(f'act_{acct}')
        data = _cached_sdk(f"pixels_{acct}", lambda: _sdk_cursor_to_list(
            account.get_ads_pixels(fields=['id', 'name', 'is_unavailable'])
        ))
        pixels = [{"id": p["id"], "name": p.get("name", p["id"])}
                  for p in data if not p.get("is_unavailable")]
        return jsonify({"data": pixels})
    except FacebookRequestError as e:
        return jsonify({"data": [], "error": e.api_error_message()})
    except Exception as e:
        return jsonify({"data": [], "error": str(e)})

@app.route("/api/meta/targeting/reach")
def api_meta_targeting_reach():
    """Estimated reach for a targeting spec."""
    account_id = request.args.get("account_id", "").replace("act_", "").strip()
    spec_json  = request.args.get("spec", "{}")
    if not account_id:
        return jsonify({"error": "Missing account_id."})
    try:
        _init_meta()
        account = AdAccount(f'act_{account_id}')
        result = account.get_reach_estimate(params={'targeting_spec': spec_json})
        return jsonify({"data": _sdk_cursor_to_list(result)})
    except FacebookRequestError as e:
        return jsonify({"error": e.api_error_message()})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/meta/custom-audiences")
def api_meta_custom_audiences():
    """Custom and saved audiences for the active ad account."""
    acct = _ad_account()
    if not acct:
        return jsonify({"error": "No ad account configured", "data": []})
    try:
        _init_meta()
        account = AdAccount(f'act_{acct}')
        data = _cached_sdk(f"custom_aud2_{acct}", lambda: _sdk_cursor_to_list(
            account.get_custom_audiences(fields=['id', 'name', 'subtype', 'approximate_count_lower_bound'])
        ))
        audiences = [
            {"id": a.get("id"), "name": a.get("name"),
             "subtype": a.get("subtype", ""), "count": a.get("approximate_count_lower_bound")}
            for a in data
        ]
        return jsonify({"data": audiences})
    except FacebookRequestError as e:
        return jsonify({"error": e.api_error_message(), "data": []})
    except Exception as e:
        return jsonify({"error": str(e), "data": []})

# ── Saved Locations ───────────────────────────────────────────────────────

@app.route("/api/saved-locations")
def api_list_saved_locations():
    try:
        rows = _db_get_all("saved_locations")
        return jsonify({"locations": rows})
    except Exception as e:
        return jsonify({"error": str(e), "locations": []})

@app.route("/api/saved-locations", methods=["POST"])
@csrf.exempt
def api_create_saved_location():
    data          = request.get_json(silent=True) or {}
    name          = data.get("name", "").strip()
    locations     = data.get("locations", [])
    location_type = data.get("location_type", "")
    if not name:
        return jsonify({"error": "Name is required"}), 400
    if not locations:
        return jsonify({"error": "Select at least one location first"}), 400
    try:
        new_id = gen_id()
        _db_save("saved_locations", None, None, {
            "id": new_id, "name": name,
            "locations": json.dumps(locations),
            "location_type": location_type,
            "created_at": now_iso(),
        })
        return jsonify({"ok": True, "id": new_id, "name": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/saved-locations/<loc_id>", methods=["DELETE"])
@csrf.exempt
def api_delete_saved_location(loc_id):
    try:
        existing = _db_find_by("saved_locations", "id", loc_id)
        if not existing:
            return jsonify({"error": "Not found"}), 404
        _db_delete("saved_locations", loc_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Campaigns: Meta launch helpers ────────────────────────────────────────

CAMPAIGNS_HEADERS = [
    "id", "client_id",
    "campaign_name", "objective", "special_ad_categories", "buying_type",
    "budget_strategy", "daily_budget", "lifetime_budget",
    "meta_campaign_id", "launch_status", "launched_at", "error_msg",
    "created_at", "updated_at",
]

ADSETS_HEADERS = [
    "id", "client_id", "campaign_id", "campaign_name",
    "adset_name", "budget_type", "daily_budget", "lifetime_budget",
    "start_time", "end_time",
    "optimization_goal", "billing_event", "bid_strategy", "bid_amount",
    "targeting_locations", "targeting_location_type",
    "targeting_age_min", "targeting_age_max", "targeting_genders",
    "targeting_interests", "targeting_exclusions", "targeting_custom_audiences",
    "targeting_excl_custom_audiences",
    "meta_adset_id", "launch_status", "launched_at", "error_msg",
    "created_at", "updated_at",
]

META_ADS_HEADERS = [
    "id", "client_id", "campaign_id", "adset_id", "adset_name",
    "ad_name",
    "page_id", "page_name", "instagram_actor_id",
    "primary_text", "headline", "description", "link_url", "cta",
    "lead_form_id",
    "image_square", "image_landscape", "image_portrait", "image_stories",
    "tracking_pixel_id", "tracking_events", "url_tags",
    "meta_creative_id", "meta_ad_id",
    "launch_status", "launched_at", "error_msg",
    "approval_status",
    "created_at", "updated_at",
]

# Objective → sensible default optimization goal
# ══════════════════════════════════════════════════════════════════════════
# Meta API combo validation — TESTED LIVE March 24 2026 on act_832577245383285
# ══════════════════════════════════════════════════════════════════════════
#
# KEY FINDING #1: bid_strategy is REQUIRED — omitting it always fails.
#   LOWEST_COST_WITHOUT_CAP works universally (no bid_amount needed).
#   LOWEST_COST_WITH_BID_CAP needs bid_amount (works for most goals).
#   COST_CAP needs bid_amount (fails for IMPRESSIONS and CONVERSATIONS goals).
#   AD_RECALL_LIFT only supports LOWEST_COST_WITHOUT_CAP (autobid only).
#
# KEY FINDING #2: billing_event is always IMPRESSIONS for all goals.
#   LINK_CLICKS billing is deprecated/broken for new campaigns.
#
# KEY FINDING #3: destination_type varies:
#   OUTCOME_LEADS:      ON_AD, WEBSITE, MESSENGER (no WHATSAPP/IG/POST/VIDEO/EVENT)
#   OUTCOME_TRAFFIC:    (all except WHATSAPP without promoted_object)
#   OUTCOME_ENGAGEMENT: POST_ENGAGEMENT only works with (none), ON_POST, ON_EVENT
#
OBJECTIVE_DEFAULTS = {
    "OUTCOME_AWARENESS":      ("REACH",              "IMPRESSIONS"),
    "OUTCOME_TRAFFIC":        ("LANDING_PAGE_VIEWS", "IMPRESSIONS"),
    "OUTCOME_ENGAGEMENT":     ("POST_ENGAGEMENT",    "IMPRESSIONS"),
    "OUTCOME_LEADS":          ("LEAD_GENERATION",    "IMPRESSIONS"),
    "OUTCOME_APP_PROMOTION":  ("APP_INSTALLS",       "IMPRESSIONS"),
    "OUTCOME_SALES":          ("OFFSITE_CONVERSIONS","IMPRESSIONS"),
}

# Valid optimization goals per objective
OBJECTIVE_VALID_GOALS = {
    "OUTCOME_AWARENESS":  ["REACH", "IMPRESSIONS", "AD_RECALL_LIFT"],
    "OUTCOME_TRAFFIC":    ["LANDING_PAGE_VIEWS", "LINK_CLICKS", "IMPRESSIONS", "REACH", "CONVERSATIONS"],
    "OUTCOME_ENGAGEMENT": ["POST_ENGAGEMENT", "IMPRESSIONS", "REACH", "LINK_CLICKS", "LANDING_PAGE_VIEWS", "CONVERSATIONS"],
    "OUTCOME_LEADS":      ["LEAD_GENERATION", "QUALITY_LEAD"],
    "OUTCOME_APP_PROMOTION": ["APP_INSTALLS"],
    "OUTCOME_SALES":      ["OFFSITE_CONVERSIONS"],
}

# Goals that ONLY support autobid (LOWEST_COST_WITHOUT_CAP)
AUTOBID_ONLY_GOALS = {"AD_RECALL_LIFT"}

# Goals incompatible with COST_CAP
NO_COST_CAP_GOALS = {"IMPRESSIONS", "CONVERSATIONS"}

def build_targeting_spec(data):
    """Convert our internal format to Meta's targeting_spec dict."""
    spec = {}

    # Geo locations
    locs = []
    try:
        locs = json.loads(data.get("targeting_locations") or "[]")
    except Exception:
        pass

    if locs:
        geo = {}
        type_map = {
            "city":               "cities",
            "region":             "regions",
            "zip":                "zips",
            "geo_market":         "geo_markets",
            "electoral_district": "electoral_districts",
            "neighborhood":       "neighborhoods",
            "subcity":            "subcities",
        }
        for loc in locs:
            t = loc.get("type", "")
            k = loc.get("key", "")
            if t == "country":
                geo.setdefault("countries", []).append(k)
            elif t == "country_group":
                geo.setdefault("country_groups", []).append(k)
            elif t in type_map:
                geo.setdefault(type_map[t], []).append({"key": k})
        loc_type = data.get("targeting_location_type", "")
        if loc_type:
            geo["location_types"] = [loc_type]
        spec["geo_locations"] = geo

    # Age
    try:
        spec["age_min"] = int(data.get("targeting_age_min") or 18)
    except ValueError:
        spec["age_min"] = 18
    try:
        spec["age_max"] = int(data.get("targeting_age_max") or 65)
    except ValueError:
        spec["age_max"] = 65

    # Gender (Meta: 1=male, 2=female, omit for all)
    gender = data.get("targeting_genders", "all")
    if gender == "male":
        spec["genders"] = [1]
    elif gender == "female":
        spec["genders"] = [2]

    # Interests
    interests = []
    try:
        interests = json.loads(data.get("targeting_interests") or "[]")
    except Exception:
        pass
    if interests:
        spec["flexible_spec"] = [{"interests": [{"id": i["id"], "name": i["name"]} for i in interests]}]

    # Exclusions
    exclusions = []
    try:
        exclusions = json.loads(data.get("targeting_exclusions") or "[]")
    except Exception:
        pass
    if exclusions:
        spec["exclusions"] = {"interests": [{"id": e["id"], "name": e["name"]} for e in exclusions]}

    # Custom audiences (include)
    custom = []
    try:
        custom = json.loads(data.get("targeting_custom_audiences") or "[]")
    except Exception:
        pass
    if custom:
        spec["custom_audiences"] = [{"id": c["id"]} for c in custom]

    # Custom audiences (exclude)
    excl_custom = []
    try:
        excl_custom = json.loads(data.get("targeting_excl_custom_audiences") or
                                 data.get("targeting_excl_custom") or "[]")
    except Exception:
        pass
    if excl_custom:
        spec["excluded_custom_audiences"] = [{"id": c["id"]} for c in excl_custom]

    return spec


def _fb_error_detail(step, e, params_sent=None):
    """Build a detailed error message from a FacebookRequestError."""
    parts = [f"[{step}]"]
    # User-facing message (most useful)
    user_msg = e.api_error_message() or ""
    if user_msg:
        parts.append(user_msg)
    # Error subcode + code for debugging
    body = e.body() or {}
    err = body.get("error", {}) if isinstance(body, dict) else {}
    if not isinstance(err, dict):
        err = {"message": str(err)}
    code = err.get("code", "")
    subcode = err.get("error_subcode", "")
    if code:
        parts.append(f"(code {code}{f', subcode {subcode}' if subcode else ''})")
    # Blame info — which field caused the error
    err_data = err.get("error_data", {})
    if not isinstance(err_data, dict):
        err_data = {}
    blame = err_data.get("blame_field_specs") or []
    if blame:
        fields = ", ".join(str(b) for b in blame)
        parts.append(f"Field(s): {fields}")
    # Also check error_user_title
    title = err.get("error_user_title", "")
    if title and title not in user_msg:
        parts.insert(1, title + ":")
    # Log the params we sent for debugging
    if params_sent:
        # Redact access_token if present
        safe = {k: (v if 'token' not in str(k).lower() else '***') for k, v in params_sent.items()}
        parts.append(f"| Params sent: {json.dumps(safe, default=str)[:1500]}")
    return " ".join(parts)


def meta_launch_campaign(camp, ad_account_id):
    """Step 1: Create Meta Campaign via SDK. Returns meta_campaign_id."""
    _init_meta()
    account = AdAccount(f'act_{ad_account_id}')

    # Parse special_ad_categories — must be a non-empty JSON array; default to ["NONE"]
    cats_raw = camp.get("special_ad_categories") or "[]"
    try:
        cats_list = json.loads(cats_raw) if isinstance(cats_raw, str) else cats_raw
    except Exception:
        cats_list = []
    if not cats_list:
        cats_list = ["NONE"]

    params = {
        FBCampaign.Field.name:                  camp["campaign_name"],
        FBCampaign.Field.objective:             camp.get("objective") or "OUTCOME_AWARENESS",
        FBCampaign.Field.status:                FBCampaign.Status.paused,
        FBCampaign.Field.special_ad_categories: cats_list,
        FBCampaign.Field.buying_type:           camp.get("buying_type") or "AUCTION",
    }
    # Campaign Budget Optimization (CBO / Advantage+ campaign budget)
    # NOTE: bid_strategy CANNOT be set at campaign level for this account
    # (always returns "Invalid parameter"). Meta auto-assigns bid strategy.
    budget_strategy = camp.get("budget_strategy", "ADSET")
    if budget_strategy == "CAMPAIGN":
        daily = camp.get("daily_budget", "").strip()
        lifetime = camp.get("lifetime_budget", "").strip()
        if daily:
            params[FBCampaign.Field.daily_budget] = int(float(daily) * 100)
        elif lifetime:
            params[FBCampaign.Field.lifetime_budget] = int(float(lifetime) * 100)

    try:
        result = account.create_campaign(params=params)
        return result["id"]
    except FacebookRequestError as e:
        raise Exception(_fb_error_detail("campaigns", e, params))


def meta_launch_adset(adset, meta_campaign_id, ad_account_id, campaign_objective=None):
    """Step 2: Create Meta Ad Set via SDK. Returns meta_adset_id."""
    _init_meta()
    account = AdAccount(f'act_{ad_account_id}')
    targeting = build_targeting_spec(adset)

    # geo_locations is required by Meta — default to US if none specified
    if "geo_locations" not in targeting:
        targeting["geo_locations"] = {"countries": ["US"]}

    # Meta now requires explicitly opting in or out of Advantage audience
    targeting.setdefault("targeting_automation", {"advantage_audience": 0})

    obj  = campaign_objective or adset.get("objective") or "OUTCOME_AWARENESS"
    opt, bill = OBJECTIVE_DEFAULTS.get(obj, ("REACH", "IMPRESSIONS"))
    user_opt = adset.get("optimization_goal", "").strip()
    # Validate the user's optimization goal is valid for this objective
    valid_goals = OBJECTIVE_VALID_GOALS.get(obj, [])
    if user_opt and user_opt in valid_goals:
        opt = user_opt
    elif user_opt and valid_goals and user_opt not in valid_goals:
        opt = valid_goals[0]

    # ── Budget logic (tested live March 25 2026) ─────────────────────
    # Check if campaign uses CBO (has a budget set at campaign level).
    # If CBO: ad set must NOT have budget (inherits from campaign).
    # If no CBO: ad set MUST have its own budget.
    campaign_has_cbo = False
    campaign_has_lifetime = False
    try:
        _init_meta()
        camp_obj = FBCampaign(meta_campaign_id)
        camp_data = camp_obj.api_get(fields=['daily_budget', 'lifetime_budget', 'bid_strategy'])
        if camp_data.get('daily_budget') or camp_data.get('lifetime_budget'):
            campaign_has_cbo = True
        if camp_data.get('lifetime_budget'):
            campaign_has_lifetime = True
    except Exception:
        pass

    params = {
        FBAdSet.Field.name:              adset["adset_name"],
        FBAdSet.Field.campaign_id:       meta_campaign_id,
        FBAdSet.Field.optimization_goal: opt,
        FBAdSet.Field.billing_event:     "IMPRESSIONS",
        FBAdSet.Field.targeting:         targeting,
        FBAdSet.Field.status:            FBAdSet.Status.paused,
    }

    # Budget: only set on ad set if campaign does NOT have CBO
    if not campaign_has_cbo:
        bt  = adset.get("budget_type") or "daily"
        key = "daily_budget" if bt == "daily" else "lifetime_budget"
        budget_cents = int(float(adset.get(key) or "10") * 100)
        params[key] = budget_cents

    if adset.get("start_time"):  params[FBAdSet.Field.start_time]  = adset["start_time"]
    if adset.get("end_time"):
        params[FBAdSet.Field.end_time] = adset["end_time"]
    elif campaign_has_lifetime:
        # Lifetime budget campaigns REQUIRE end_time on ad sets.
        # Default to 30 days from start_time (or from now).
        from datetime import datetime, timedelta
        start = adset.get("start_time", "")
        try:
            start_dt = datetime.fromisoformat(start) if start else datetime.utcnow()
        except Exception:
            start_dt = datetime.utcnow()
        params[FBAdSet.Field.end_time] = (start_dt + timedelta(days=30)).isoformat()

    # ── Bid amount (tested live March 25 2026) ───────────────────────
    # This ad account requires bid_amount on ALL ad sets.
    # Meta auto-assigns LOWEST_COST_WITH_BID_CAP, so bid_amount is mandatory.
    # Do NOT send bid_strategy at the ad set level — it always fails.
    # Just send bid_amount and let Meta handle strategy via the campaign.
    bid_amount_raw = adset.get("bid_amount", "").strip()
    bid_cents = int(float(bid_amount_raw) * 100) if bid_amount_raw else 500  # default $5
    params["bid_amount"] = bid_cents

    # ── Destination type + promoted_object ────────────────────────────
    dest = adset.get("destination_type", "").strip()
    page_id = adset.get("page_id", "").strip()

    if obj == "OUTCOME_LEADS":
        params["destination_type"] = dest if dest in ("ON_AD", "WEBSITE", "MESSENGER") else "ON_AD"
        if page_id:
            params["promoted_object"] = {"page_id": page_id}
    elif dest:
        params["destination_type"] = dest
        # WHATSAPP needs page_id in promoted_object
        if dest == "WHATSAPP" and page_id:
            params["promoted_object"] = {"page_id": page_id}

    # OUTCOME_SALES needs pixel in promoted_object
    if obj == "OUTCOME_SALES":
        pixel_id = adset.get("pixel_id", "").strip()
        if pixel_id:
            params["promoted_object"] = {"pixel_id": pixel_id, "custom_event_type": "LEAD"}

    try:
        result = account.create_ad_set(params=params)
        return result["id"]
    except FacebookRequestError as e:
        raise Exception(_fb_error_detail("adsets", e, params))


def meta_launch_ad(ad, meta_adset_id, ad_account_id):
    """Step 3: Upload image/video → Create Creative → Create Ad via SDK. Returns (meta_creative_id, meta_ad_id)."""
    _init_meta()
    account = AdAccount(f'act_{ad_account_id}')

    # Determine media URL — prefer square, fall back to others
    media_url = (ad.get("image_square") or ad.get("image_landscape") or
                 ad.get("image_portrait") or ad.get("image_stories") or "")

    # Detect if it's a video based on file extension
    VIDEO_EXTS = {"mp4", "mov", "avi", "mkv", "webm"}
    media_ext = media_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in media_url else ""
    is_video = media_ext in VIDEO_EXTS

    image_hash = None
    video_id = None

    if media_url:
        try:
            # Resolve to local path
            if media_url.startswith("/static/uploads/"):
                local_path = os.path.join(os.path.dirname(__file__),
                                          *media_url.lstrip("/").split("/"))
            else:
                # Download remote file to temp
                resp = http_requests.get(media_url, timeout=60)
                import tempfile
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{media_ext or 'bin'}")
                tmp.write(resp.content)
                tmp.close()
                local_path = tmp.name

            if is_video:
                # Upload video to Meta
                from facebook_business.adobjects.advideo import AdVideo as FBAdVideo
                vid = FBAdVideo(parent_id=f'act_{ad_account_id}')
                vid[FBAdVideo.Field.filepath] = local_path
                vid.remote_create()
                video_id = vid.get_id()
                print(f"[META] Video uploaded: {video_id}")
            else:
                # Upload image to Meta
                img = AdImage(parent_id=f'act_{ad_account_id}')
                img[AdImage.Field.filename] = local_path
                img.remote_create()
                image_hash = img[AdImage.Field.hash]
        except Exception as e:
            print(f"[META] Media upload failed: {e}")

    link_url = ad.get("link_url") or ""
    lead_form_id = ad.get("lead_form_id", "").strip()
    cta = ad.get("cta") or ("SUBSCRIBE" if lead_form_id else "LEARN_MORE")
    page_id = ad.get("page_id", "")

    if is_video and video_id:
        # ── Video ad creative ──────────────────────────────────────────
        # For video ads, use video_data instead of link_data
        video_data = {
            "message":     ad.get("primary_text") or "",
            "title":       ad.get("headline") or "",
            "link_description": ad.get("description") or "",
            "video_id":    video_id,
            "call_to_action": {"type": cta, "value": {"link": link_url or "https://fb.me/"}},
        }
        if lead_form_id:
            video_data["call_to_action"]["value"]["lead_gen_form_id"] = lead_form_id
            video_data["call_to_action"]["value"]["link"] = "https://fb.me/"
        # Use thumbnail from uploaded image if available
        if image_hash:
            video_data["image_hash"] = image_hash
        object_story = {"page_id": page_id, "video_data": video_data}
    elif lead_form_id:
        # ── Lead gen image creative ────────────────────────────────────
        link_data = {
            "message":     ad.get("primary_text") or "",
            "name":        ad.get("headline") or "",
            "description": ad.get("description") or "",
            "link":        "https://fb.me/",
            "call_to_action": {"type": cta or "SUBSCRIBE", "value": {"lead_gen_form_id": lead_form_id}},
        }
        if image_hash:
            link_data["image_hash"] = image_hash
        elif media_url:
            link_data["picture"] = media_url
        object_story = {"page_id": page_id, "link_data": link_data}
    else:
        # ── Regular image ad creative ──────────────────────────────────
        link_data = {
            "message": ad.get("primary_text") or "",
            "name":    ad.get("headline") or "",
            "description": ad.get("description") or "",
            "link": link_url,
        }
        if cta and cta != "NO_BUTTON":
            link_data["call_to_action"] = {"type": cta, "value": {"link": link_url}}
        if image_hash:
            link_data["image_hash"] = image_hash
        elif media_url:
            link_data["picture"] = media_url
        object_story = {"page_id": page_id, "link_data": link_data}
    # Instagram account — if set, ads will show from this IG account
    insta_id = ad.get("instagram_actor_id", "").strip()
    if insta_id:
        object_story["instagram_actor_id"] = insta_id

    creative_params = {
        AdCreative.Field.name: f"{ad.get('ad_name','Ad')} -- Creative",
        AdCreative.Field.object_story_spec: object_story,
        AdCreative.Field.authorization_category: "POLITICAL",
    }

    try:
        creative = account.create_ad_creative(params=creative_params)
        creative_id = creative["id"]
    except FacebookRequestError as e:
        raise Exception(_fb_error_detail("adcreatives", e, creative_params))

    status = ad.get("launch_status") or "PAUSED"
    if status not in ("ACTIVE", "PAUSED"):
        status = "PAUSED"

    ad_params = {
        FBAd.Field.name:     ad.get("ad_name") or "Ad",
        FBAd.Field.adset_id: meta_adset_id,
        FBAd.Field.creative: {"creative_id": creative_id},
        FBAd.Field.status:   status,
    }

    # Tracking pixel → tracking_specs
    pixel_id = ad.get("tracking_pixel_id", "").strip()
    if pixel_id:
        ad_params[FBAd.Field.tracking_specs] = [
            {"action.type": ["offsite_conversion"], "fb_pixel": [pixel_id]}
        ]

    # URL parameters → url_tags
    url_tags = ad.get("url_tags", "").strip()
    if url_tags:
        ad_params["url_tags"] = url_tags

    try:
        result = account.create_ad(params=ad_params)
        return creative_id, result["id"]
    except FacebookRequestError as e:
        raise Exception(_fb_error_detail("ads", e, ad_params))


# ── MySQL Database ────────────────────────────────────────────────────────
import mysql.connector
from mysql.connector import pooling

_db_pool = None

def _get_db_pool():
    global _db_pool
    if _db_pool is None:
        ssl_ca = os.getenv("MYSQL_SSL_CA", "")
        # Also check for bundled cert relative to app directory
        if not ssl_ca or not os.path.exists(ssl_ca):
            bundled = os.path.join(os.path.dirname(__file__), "certs", "ca.pem")
            if os.path.exists(bundled):
                ssl_ca = bundled
        ssl_args = {"ssl_ca": ssl_ca} if ssl_ca and os.path.exists(ssl_ca) else {}
        _db_pool = pooling.MySQLConnectionPool(
            pool_name="fb_ad_pool",
            pool_size=5,
            pool_reset_session=True,
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB"),
            **ssl_args,
        )
    return _db_pool

def get_db():
    return _get_db_pool().get_connection()

# ── SQL injection prevention: allowlisted table/column names ──────────────
import re as _re
_ALLOWED_TABLES = frozenset({
    "clients", "campaigns", "adsets", "meta_ads", "approvers",
    "approvals", "lead_forms", "saved_locations", "users", "settings",
})

# ── Settings helper — DB-backed config with env var fallback ─────────────
_settings_cache = {}
_settings_cache_ts = 0

def get_setting(key, default=""):
    """Get a setting from DB, with env var fallback and 60s cache."""
    global _settings_cache, _settings_cache_ts
    import time as _time
    now = _time.time()
    if now - _settings_cache_ts > 60:
        try:
            rows = _db_list("settings", {})
            _settings_cache = {r["setting_key"]: r["setting_value"] or "" for r in rows}
            _settings_cache_ts = now
        except Exception:
            pass
    val = _settings_cache.get(key, "")
    if val:
        return val
    return os.getenv(key, default)
_IDENT_RE = _re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

def _check_table(table):
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table}")

def _check_column(col):
    if not _IDENT_RE.match(col):
        raise ValueError(f"Invalid column name: {col}")


def _db_list(table, filters=None):
    """List rows from a MySQL table. Returns list of dicts, newest first."""
    _check_table(table)
    conn = get_db()
    try:
        cursor = conn.cursor(dictionary=True)
        sql = f"SELECT * FROM `{table}`"
        params = []
        if filters:
            clauses = []
            for k, v in filters.items():
                _check_column(k)
                clauses.append(f"`{k}` = %s")
                params.append(v)
            sql += " WHERE " + " AND ".join(clauses)
        # Not all tables have created_at (e.g., settings uses setting_key as PK)
        if table != "settings":
            sql += " ORDER BY created_at DESC"
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        # Convert all values to strings for compatibility with existing code
        result = []
        for row in rows:
            d = {}
            for k, v in row.items():
                if v is None:
                    d[k] = ""
                elif isinstance(v, (datetime,)):
                    d[k] = v.strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    d[k] = str(v)
            result.append(d)
        return result
    except Exception as e:
        print(f"[DB] _db_list({table}) error: {e}")
        return []
    finally:
        conn.close()

def _db_save(table, headers_ignored, item_id, data):
    """Insert or update a row. Returns the row id."""
    _check_table(table)
    for k in data.keys():
        _check_column(k)
    # Convert values for MySQL type compatibility
    DATETIME_COLS = {"created_at", "updated_at", "launched_at", "responded_at", "sent_at"}
    DECIMAL_COLS = {"daily_budget", "lifetime_budget", "bid_amount"}
    BOOL_COLS = {"required"}
    for k, v in list(data.items()):
        if v == "" and k in DATETIME_COLS:
            data[k] = None
        elif v == "" and k in DECIMAL_COLS:
            data[k] = None
        elif k in BOOL_COLS:
            if isinstance(v, str):
                data[k] = 1 if v.upper() in ("TRUE", "YES", "1", "ON") else 0
            elif isinstance(v, bool):
                data[k] = 1 if v else 0

    conn = get_db()
    try:
        cursor = conn.cursor()
        data["updated_at"] = now_iso()
        if item_id:
            # Update existing row
            sets = ", ".join(f"`{k}` = %s" for k in data.keys())
            vals = list(data.values()) + [item_id]
            cursor.execute(f"UPDATE `{table}` SET {sets} WHERE id = %s", vals)
            conn.commit()
            return item_id
        else:
            # Insert new row
            data["id"] = gen_id()
            data["created_at"] = now_iso()
            cols = ", ".join(f"`{k}`" for k in data.keys())
            phs = ", ".join(["%s"] * len(data))
            cursor.execute(f"INSERT INTO `{table}` ({cols}) VALUES ({phs})", list(data.values()))
            conn.commit()
            return data["id"]
    except Exception as e:
        print(f"[DB] _db_save({table}) error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def _db_delete(table, item_id):
    """Delete a row by id."""
    _check_table(table)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM `{table}` WHERE id = %s", [item_id])
        conn.commit()
    except Exception as e:
        print(f"[DB] _db_delete({table}) error: {e}")
    finally:
        conn.close()

def _db_delete_where(table, field, value):
    """Delete all rows matching a condition."""
    _check_table(table)
    _check_column(field)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM `{table}` WHERE `{field}` = %s", [value])
        conn.commit()
    except Exception as e:
        print(f"[DB] _db_delete_where({table}) error: {e}")
    finally:
        conn.close()

def _db_get_all(table):
    """Get all rows from a table as list of dicts."""
    return _db_list(table)

def _db_find_by(table, field, value):
    """Find a single row by field value. Returns dict or None."""
    _check_table(table)
    _check_column(field)
    conn = get_db()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM `{table}` WHERE `{field}` = %s LIMIT 1", [value])
        row = cursor.fetchone()
        if not row:
            return None
        return {k: ("" if v is None else str(v) if not isinstance(v, (datetime,)) else v.strftime("%Y-%m-%d %H:%M:%S")) for k, v in row.items()}
    except Exception:
        return None
    finally:
        conn.close()

def _db_update(table, item_id, data):
    """Update specific fields on a row."""
    if not data:
        return
    DATETIME_COLS = {"created_at", "updated_at", "launched_at", "responded_at", "sent_at"}
    DECIMAL_COLS = {"daily_budget", "lifetime_budget", "bid_amount"}
    for k, v in list(data.items()):
        if v == "" and k in DATETIME_COLS:
            data[k] = None
        elif v == "" and k in DECIMAL_COLS:
            data[k] = None
    data["updated_at"] = now_iso()
    conn = get_db()
    try:
        cursor = conn.cursor()
        sets = ", ".join(f"`{k}` = %s" for k in data.keys())
        cursor.execute(f"UPDATE `{table}` SET {sets} WHERE id = %s", list(data.values()) + [item_id])
        conn.commit()
    except Exception as e:
        print(f"[DB] _db_update({table}) error: {e}")
    finally:
        conn.close()

ADS_HEADERS = [
    "id", "client_id", "title", "ad_type",
    "page_id", "page_name",
    "primary_text", "headline",
    "description", "cta",
    "image_square", "image_landscape", "image_portrait", "image_stories",
    "carousel_cards", "video_url", "link_url",
    "targeting_saved_audience_id", "targeting_saved_audience_name",
    "targeting_custom_audiences",
    "targeting_locations", "targeting_location_type",
    "targeting_age_min", "targeting_age_max",
    "targeting_genders", "targeting_interests", "targeting_exclusions",
    "targeting_notes", "budget", "status", "created_at", "updated_at"
]

# ── Helpers ───────────────────────────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def gen_id():
    return uuid.uuid4().hex[:12]

# ── Security helpers ──────────────────────────────────────────────────────
def safe_url(url):
    """Validate URL scheme — only allow http/https. Block javascript:, data:, etc."""
    if not url:
        return ""
    url = url.strip()
    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme.lower() not in ("http", "https", ""):
        return ""  # block dangerous schemes
    # If no scheme, assume https
    if url and not url.startswith(("http://", "https://", "/")):
        url = "https://" + url
    return url

def safe_brand_color(color):
    """Validate hex color — prevent CSS injection."""
    if color and _re.match(r'^#[0-9A-Fa-f]{3,6}$', color):
        return color
    return "#1877F2"  # fallback to Meta blue

def get_all_clients():
    return _db_get_all("clients")

def get_active_client():
    client_id = session.get("active_client_id")
    if not client_id:
        return None
    clients = get_all_clients()
    return next((c for c in clients if c["id"] == client_id), None)

# ── Auth guard — protect all routes except whitelist ──────────────────────
@app.before_request
def require_login():
    # Whitelist: login page, static files, approval response pages
    allowed_prefixes = ("/login", "/static/", "/respond/")
    if any(request.path.startswith(p) for p in allowed_prefixes):
        return
    if not current_user.is_authenticated:
        return redirect(url_for("login", next=request.path))

# ── Template context — inject client info into every page ─────────────────
@app.context_processor
def inject_client_context():
    # Auto-restore personal FB token from disk into session if needed
    get_active_token()
    all_clients = get_all_clients()
    active_client = None
    client_id = session.get("active_client_id")
    if client_id:
        active_client = next((c for c in all_clients if c["id"] == client_id), None)
    # Auto-select the only client if there's just one
    if not active_client and len(all_clients) == 1:
        active_client = all_clients[0]
        session["active_client_id"] = active_client["id"]
    return dict(
        active_client=active_client,
        all_clients=all_clients,
        fb_app_id=FB_APP_ID,
        fb_connected_name=session.get("fb_user_name", ""),
        fb_connected=bool(session.get("fb_user_token")),
        user=current_user if current_user.is_authenticated else None,
    )

# ── Email sending via Gmail API (HTTPS) or SMTP fallback ─────────────────
GMAIL_SA_FILE = os.path.join(os.path.dirname(__file__), "credentials", "gmail-sa.json")

def _get_gmail_service():
    """Build Gmail API service using service account with domain-wide delegation."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/gmail.send"]

    # Try env var first (Railway), then file (local dev)
    sa_key = os.getenv("GMAIL_SA_KEY", "")
    if sa_key:
        info = json.loads(sa_key)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(GMAIL_SA_FILE):
        creds = service_account.Credentials.from_service_account_file(GMAIL_SA_FILE, scopes=scopes)
    else:
        return None

    # Delegate to the actual Gmail user
    send_as = get_setting("GMAIL_SEND_AS") or "support@politikanyc.com"
    creds = creds.with_subject(send_as)
    return build("gmail", "v1", credentials=creds)

def _send_email(msg):
    """Send an email.MIMEMultipart message via Gmail API or SMTP fallback."""
    import base64

    # Try Gmail API first (works on Railway — uses HTTPS)
    try:
        sa_key = os.getenv("GMAIL_SA_KEY", "")
        sa_file_exists = os.path.exists(GMAIL_SA_FILE)
        print(f"[EMAIL] Gmail API check: GMAIL_SA_KEY={'set' if sa_key else 'NOT SET'}, SA file={'exists' if sa_file_exists else 'missing'}")
        service = _get_gmail_service()
        if service:
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
            service.users().messages().send(
                userId="me",
                body={"raw": raw}
            ).execute()
            print("[EMAIL] Sent via Gmail API")
            return
    except Exception as e:
        print(f"[EMAIL] Gmail API failed: {e}")

    # SMTP fallback (local dev)
    if GMAIL_SENDER and GMAIL_APP_PASS:
        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as smtp:
                smtp.starttls()
                smtp.login(GMAIL_SENDER, GMAIL_APP_PASS)
                smtp.send_message(msg)
                print("[EMAIL] Sent via SMTP 587")
                return
        except Exception:
            pass
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
                smtp.login(GMAIL_SENDER, GMAIL_APP_PASS)
                smtp.send_message(msg)
                print("[EMAIL] Sent via SMTP 465")
                return
        except Exception as e:
            raise Exception(f"All email methods failed: {e}")
    else:
        raise Exception("No email credentials configured")


# ── Email ─────────────────────────────────────────────────────────────────
def send_campaign_approval_email(to_email, to_name, campaign, primary_ad, token, client=None):
    """Send campaign-level approval email — reuses the same ad approval email template.
    Passes campaign context so the template can show campaign name, budget, etc."""
    # Build an ad dict that includes campaign context
    ad_copy = dict(primary_ad) if primary_ad else {}
    ad_copy["title"] = campaign.get("campaign_name", "Untitled Campaign")
    ad_copy["_campaign"] = campaign
    ad_copy["_adsets"] = campaign.get("_adsets", [])
    ad_copy["_all_ads"] = campaign.get("_ads", [])
    # Use the existing per-ad email function — it handles images, forms, everything
    send_approval_email(to_email, to_name, ad_copy, token, client=client)


def send_approval_email(to_email, to_name, ad, token, client=None):
    base_url = get_setting("BASE_URL") or BASE_URL
    approve_url = f"{base_url}/respond/{token}/approve"
    reject_url  = f"{base_url}/respond/{token}/reject"

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

    # Resolve campaign and adset data for email details section
    campaign = None
    adset = None
    try:
        campaign_id = ad.get("campaign_id", "")
        if campaign_id:
            camps = _db_list("campaigns", {"id": campaign_id})
            campaign = camps[0] if camps else None
        adset_id = ad.get("adset_id", "")
        if adset_id:
            adsets = _db_list("adsets", {"id": adset_id})
            adset = adsets[0] if adsets else None
    except Exception:
        pass

    # Resolve lead form data if this is a lead gen ad
    lead_form = None
    lead_form_id = ad.get("lead_form_id", "").strip()

    # 1. Try local DB first (forms created in our builder)
    if not lead_form:
        ad_id = ad.get("id", "")
        try:
            local_forms = _db_list("lead_forms", {"ad_id": ad_id})
            if local_forms:
                lf = local_forms[0]
                # Build a structure matching Meta's format for the email template
                questions = []
                try:
                    questions = json.loads(lf.get("questions") or "[]")
                except Exception:
                    pass
                lead_form = {
                    "id": lf.get("meta_form_id") or lf.get("id", ""),
                    "name": lf.get("form_name", "Lead Form"),
                    "questions": [{"type": q.get("type","CUSTOM"), "label": q.get("label","")} for q in questions],
                    "privacy_policy": {"url": lf.get("privacy_url", "")},
                    "context_card": {
                        "title": lf.get("intro_headline", ""),
                        "content": [lf.get("intro_description", "")] if lf.get("intro_description") else [],
                    },
                    "thank_you_page": {
                        "title": lf.get("thank_you_title", "Thanks!"),
                        "body": lf.get("thank_you_body", ""),
                    },
                }
        except Exception as exc:
            print(f"[WARN] Could not load local lead form for ad {ad_id}: {exc}")

    # 2. Fallback to Meta API only if local DB has no form data
    # (form details are now saved locally when user selects an existing form)
    if lead_form_id and not lead_form:
        try:
            fb_token = get_active_token()
            if fb_token:
                form_resp = http_requests.get(
                    f"https://graph.facebook.com/v21.0/{lead_form_id}",
                    params={"access_token": fb_token,
                            "fields": "name,questions,privacy_policy_url,legal_content,thank_you_page,context_card"},
                    timeout=10,
                )
                form_data = form_resp.json()
                if "id" in form_data:
                    # Normalize field names for the email template
                    pp_url = form_data.get("privacy_policy_url", "")
                    if not pp_url:
                        lc = form_data.get("legal_content") or {}
                        pp = lc.get("privacy_policy") or {}
                        pp_url = pp.get("url", "") if isinstance(pp, dict) else ""
                    form_data["privacy_policy"] = {"url": pp_url}
                    lead_form = form_data
        except Exception as exc:
            print(f"[WARN] Could not fetch lead form {lead_form_id}: {exc}")

    print(f"[EMAIL] Lead form: {'FOUND' if lead_form else 'NONE'}, form_id='{lead_form_id}', ad_id='{ad.get('id','')}'")
    if lead_form:
        print(f"  Form name: {lead_form.get('name','?')}, questions: {len(lead_form.get('questions',[]))}")

    # ── Image handling for emails ──────────────────────────────────
    # Images hosted on public URLs (https://politikanyc.com/ad-images/...)
    # are used directly in email <img src="...">, no CID embedding needed.
    # Only fall back to CID for local /static/uploads/ paths (dev mode).
    from email.mime.image import MIMEImage
    ad = dict(ad)  # shallow copy — never mutate the original
    image_parts = []

    has_public_images = any(
        (ad.get(f) or "").startswith("http")
        for f in ("image_square", "image_landscape", "image_portrait", "image_stories")
    )

    if not has_public_images:
        # Dev mode: local files need CID embedding
        for img_field in ("image_square", "image_landscape", "image_portrait", "image_stories"):
            img_val = ad.get(img_field, "")
            if not img_val or not img_val.startswith("/static/uploads/"):
                continue
            local_path = os.path.join(os.path.dirname(__file__),
                                      *img_val.lstrip("/").split("/"))
            try:
                with open(local_path, "rb") as fh:
                    img_bytes = fh.read()
                ext = img_val.rsplit(".", 1)[-1].lower()
                mime_type = "image/png" if ext == "png" else "image/jpeg"
                cid = f"{img_field}_{secrets.token_hex(4)}"
                ad[img_field] = f"cid:{cid}"
                maintype, subtype = mime_type.split("/", 1)
                img_part = MIMEImage(img_bytes, _subtype=subtype)
                img_part.add_header("Content-ID", f"<{cid}>")
                img_part.add_header("Content-Disposition", "inline", filename=f"{img_field}.{subtype}")
                image_parts.append(img_part)
            except FileNotFoundError:
                continue

    print(f"[EMAIL] Images: {'public URLs' if has_public_images else f'{len(image_parts)} CID attachments'}")

    print(f"[EMAIL] Ad data: name='{ad.get('ad_name','')}', primary='{(ad.get('primary_text','') or '')[:30]}', headline='{ad.get('headline','')}', img_sq='{(ad.get('image_square','') or '')[:40]}'")
    print(f"[EMAIL] Campaign: {(campaign or {}).get('campaign_name','?')}, AdSet: {(adset or {}).get('adset_name','?')}")

    html = render_template(
        "email_approval.html",
        to_name=to_name,
        ad=ad,
        client=client,
        approve_url=approve_url,
        reject_url=reject_url,
        domain=domain,
        cta_label=cta_label,
        carousel_cards=carousel_cards,
        placements=AD_PLACEMENTS,
        lead_form=lead_form,
        campaign=campaign,
        adset=adset,
    )

    page_name = (client or {}).get("fb_page_name") or "Politika NYC"

    # Build email: "related" type so inline images render in the HTML
    msg = MIMEMultipart("related")
    msg["Subject"] = f"[Approval Needed] {ad.get('title') or ad.get('ad_name') or 'Facebook Ad'} — {page_name}"
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = to_email

    # HTML body goes inside an "alternative" sub-part
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(html, "html"))
    msg.attach(alt_part)

    # Attach inline images
    for img_part in image_parts:
        msg.attach(img_part)

    # Send via Gmail API (HTTPS, works on Railway) or SMTP fallback (local dev)
    _send_email(msg)

# ── Routes: Client Management ─────────────────────────────────────────────
@app.route("/clients")
def manage_clients():
    clients = get_all_clients()
    return render_template("clients.html", clients=clients, color_palette=CLIENT_COLORS)

@app.route("/clients/add", methods=["POST"])
def add_client():
    name              = request.form.get("name", "").strip()
    meta_business_id  = request.form.get("meta_business_id", "").strip()
    meta_ad_account_id= request.form.get("meta_ad_account_id", "").strip()
    fb_page_name      = request.form.get("fb_page_name", "").strip()
    fb_page_id        = request.form.get("fb_page_id", "").strip()
    brand_color       = safe_brand_color(request.form.get("brand_color", "").strip())

    if not name:
        flash("Client name is required.", "error")
        return redirect(url_for("manage_clients"))

    existing = _db_get_all("clients")
    if any(c["name"].lower() == name.lower() for c in existing):
        flash("A client with that name already exists.", "error")
        return redirect(url_for("manage_clients"))

    # Auto-assign a color if none chosen
    if not brand_color:
        brand_color = CLIENT_COLORS[len(existing) % len(CLIENT_COLORS)]

    data = {
        "id":                gen_id(),
        "name":              name,
        "meta_business_id":  meta_business_id,
        "meta_ad_account_id":meta_ad_account_id,
        "fb_page_name":      fb_page_name or name,
        "fb_page_id":        fb_page_id,
        "brand_color":       brand_color,
        "default_pixel_id":  request.form.get("default_pixel_id", "").strip(),
        "default_url_tags":  request.form.get("default_url_tags", "").strip(),
        "created_at":        now_iso(),
    }
    _db_save("clients", None, None, data)

    # Auto-select this client if it's the first one
    if not existing:
        session["active_client_id"] = data["id"]

    flash(f"Client '{name}' added.", "success")
    return redirect(url_for("manage_clients"))

@app.route("/clients/<client_id>/delete", methods=["POST"])
def delete_client(client_id):
    existing = _db_find_by("clients", "id", client_id)
    if existing:
        _db_delete("clients", client_id)
        if session.get("active_client_id") == client_id:
            session.pop("active_client_id", None)
        flash("Client removed.", "success")
    else:
        flash("Client not found.", "error")
    return redirect(url_for("manage_clients"))

@app.route("/clients/<client_id>/select")
def select_client(client_id):
    session["active_client_id"] = client_id
    next_url = request.args.get("next", url_for("dashboard"))
    return redirect(next_url)

@app.route("/clients/<client_id>/edit", methods=["POST"])
def edit_client(client_id):
    existing = _db_find_by("clients", "id", client_id)
    if not existing:
        flash("Client not found.", "error")
        return redirect(url_for("manage_clients"))
    _db_update("clients", client_id, {
        "name":               request.form.get("name", "").strip(),
        "meta_business_id":   request.form.get("meta_business_id", "").strip(),
        "meta_ad_account_id": request.form.get("meta_ad_account_id", "").strip(),
        "fb_page_name":       request.form.get("fb_page_name", "").strip(),
        "fb_page_id":         request.form.get("fb_page_id", "").strip(),
        "brand_color":        safe_brand_color(request.form.get("brand_color", "").strip()),
        "default_pixel_id":   request.form.get("default_pixel_id", "").strip(),
        "default_url_tags":   request.form.get("default_url_tags", "").strip(),
    })
    flash("Client updated.", "success")
    return redirect(url_for("manage_clients"))

# ── Routes: Authentication ─────────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("campaigns_page"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        users = _db_list("users", {"email": email})
        if users and check_password_hash(users[0]["password_hash"], password):
            user = AppUser(users[0])
            if not user.is_active:
                flash("Account disabled. Contact an admin.", "danger")
                return render_template("login.html")
            login_user(user, remember=True)
            _db_save("users", None, user.id, {"last_login": now_iso()})
            next_url = request.args.get("next") or url_for("campaigns_page")
            return redirect(next_url)
        flash("Invalid email or password.", "danger")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("login"))

@app.route("/users")
@admin_required
def manage_users():
    users = _db_list("users", {})
    return render_template("users.html", users=users)

@app.route("/users/add", methods=["POST"])
@admin_required
def add_user():
    email = request.form.get("email", "").strip().lower()
    name = request.form.get("name", "").strip()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "manager")
    if not email or not password or not name:
        flash("All fields are required.", "danger")
        return redirect(url_for("manage_users"))
    existing = _db_list("users", {"email": email})
    if existing:
        flash(f"User {email} already exists.", "warning")
        return redirect(url_for("manage_users"))
    if role not in ("admin", "manager", "viewer"):
        role = "manager"
    _db_save("users", None, None, {
        "id": uuid.uuid4().hex[:12],
        "email": email,
        "name": name,
        "password_hash": generate_password_hash(password),
        "role": role,
        "is_active": 1,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    })
    flash(f"User {name} ({email}) added as {role}.", "success")
    return redirect(url_for("manage_users"))

@app.route("/users/<uid>/toggle", methods=["POST"])
@admin_required
def toggle_user(uid):
    users = _db_list("users", {"id": uid})
    if not users:
        flash("User not found.", "danger")
        return redirect(url_for("manage_users"))
    new_active = 0 if users[0].get("is_active") else 1
    _db_save("users", None, uid, {"is_active": new_active})
    flash(f"User {'enabled' if new_active else 'disabled'}.", "info")
    return redirect(url_for("manage_users"))

@app.route("/users/<uid>/role", methods=["POST"])
@admin_required
def change_role(uid):
    role = request.form.get("role", "manager")
    if role not in ("admin", "manager", "viewer"):
        role = "manager"
    _db_save("users", None, uid, {"role": role})
    flash(f"Role updated to {role}.", "info")
    return redirect(url_for("manage_users"))

@app.route("/users/<uid>/delete", methods=["POST"])
@admin_required
def delete_user(uid):
    if uid == current_user.id:
        flash("You can't delete yourself.", "danger")
        return redirect(url_for("manage_users"))
    _db_delete("users", uid)
    flash("User deleted.", "info")
    return redirect(url_for("manage_users"))

@app.route("/users/<uid>/reset-password", methods=["POST"])
@admin_required
def reset_password(uid):
    password = request.form.get("password", "").strip()
    if not password:
        flash("Password is required.", "danger")
        return redirect(url_for("manage_users"))
    _db_save("users", None, uid, {"password_hash": generate_password_hash(password)})
    flash("Password reset.", "success")
    return redirect(url_for("manage_users"))


# ── Routes: Settings (admin only) ──────────────────────────────────────────
@app.route("/settings")
@admin_required
def app_settings():
    rows = _db_list("settings", {})
    # Group by category
    by_cat = {}
    for r in rows:
        cat = r.get("category", "general")
        by_cat.setdefault(cat, []).append(r)
    return render_template("settings.html", settings_by_cat=by_cat)

@app.route("/settings/save", methods=["POST"])
@admin_required
def save_settings():
    global _settings_cache_ts
    form = request.form
    for key in form:
        if key == "csrf_token":
            continue
        val = form[key]
        if not val and val != "":
            continue
        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE settings SET setting_value = %s WHERE setting_key = %s",
                (val, key)
            )
            conn.commit()
        finally:
            conn.close()
    _settings_cache_ts = 0  # bust cache
    flash("Settings saved.", "success")
    return redirect(url_for("app_settings"))


# ── Routes: Dashboard (redirects to campaigns) ────────────────────────────
@app.route("/")
@login_required
def dashboard():
    return redirect(url_for("campaigns_page"))

# ── Routes: Create / Edit Ad ──────────────────────────────────────────────
@app.route("/ad/new")
def new_ad():
    active = get_active_client()
    if not active:
        flash("Select or create a client first.", "error")
        return redirect(url_for("manage_clients"))
    return render_template("ad_form.html", ad=None)

@app.route("/ad/<ad_id>/edit")
def edit_ad(ad_id):
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        flash("Ad not found.", "error")
        return redirect(url_for("dashboard"))
    return render_template("ad_form.html", ad=ad)

@app.route("/ad/save", methods=["POST"])
def save_ad():
    ad_id   = request.form.get("ad_id", "").strip()

    data = {
        "client_id":         session.get("active_client_id", ""),
        "title":             request.form.get("title", "").strip(),
        "ad_type":           request.form.get("ad_type", "image"),
        "page_id":           request.form.get("page_id", "").strip(),
        "page_name":         request.form.get("page_name", "").strip(),
        "primary_text":      request.form.get("primary_text", "").strip(),
        "headline":          request.form.get("headline", "").strip(),
        "description":       request.form.get("description", "").strip(),
        "cta":               request.form.get("cta", "LEARN_MORE"),
        "image_square":      request.form.get("image_square", "").strip(),
        "image_landscape":   request.form.get("image_landscape", "").strip(),
        "image_portrait":    request.form.get("image_portrait", "").strip(),
        "image_stories":     request.form.get("image_stories", "").strip(),
        "carousel_cards":    request.form.get("carousel_cards", ""),
        "video_url":         safe_url(request.form.get("video_url", "")),
        "link_url":          safe_url(request.form.get("link_url", "")),
        "targeting_saved_audience_id":   request.form.get("targeting_saved_audience_id", "").strip(),
        "targeting_saved_audience_name": request.form.get("targeting_saved_audience_name", "").strip(),
        "targeting_custom_audiences":    request.form.get("targeting_custom_audiences", ""),
        "targeting_locations":           request.form.get("targeting_locations", ""),
        "targeting_location_type":       request.form.get("targeting_location_type", "home"),
        "targeting_age_min":             request.form.get("targeting_age_min", "18").strip(),
        "targeting_age_max":             request.form.get("targeting_age_max", "65").strip(),
        "targeting_genders":             request.form.get("targeting_genders", "all").strip(),
        "targeting_interests":           request.form.get("targeting_interests", ""),
        "targeting_exclusions":          request.form.get("targeting_exclusions", ""),
        "targeting_notes":               request.form.get("targeting_notes", "").strip(),
        "budget":                        request.form.get("budget", "").strip(),
        "updated_at":        now_iso(),
    }

    if ad_id:
        existing = _db_find_by("ads", "id", ad_id)
        if existing:
            _db_update("ads", ad_id, data)
            flash("Ad updated.", "success")
        else:
            flash("Ad not found.", "error")
    else:
        ad_id = gen_id()
        data["id"]         = ad_id
        data["status"]     = "draft"
        data["created_at"] = now_iso()
        _db_save("ads", None, None, data)
        flash("Ad draft created.", "success")

    return redirect(url_for("view_ad", ad_id=ad_id))

# ── Routes: View Ad Detail ────────────────────────────────────────────────
@app.route("/ad/<ad_id>")
def view_ad(ad_id):
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        flash("Ad not found.", "error")
        return redirect(url_for("dashboard"))

    approvals     = _db_get_all("approvals")
    ad_approvals  = [a for a in approvals if a.get("ad_id") == ad_id]

    # Approvers scoped to this ad account (fall back to client_id)
    all_clients      = get_all_clients()
    client           = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    all_approvers    = _db_get_all("approvers")
    client_id        = ad.get("client_id", "")
    ad_account_id    = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        scoped_approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        scoped_approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    return render_template("ad_detail.html", ad=ad,
                           approvals=ad_approvals, approvers=scoped_approvers)

# ── Routes: Send for Approval ─────────────────────────────────────────────
@app.route("/ad/<ad_id>/send", methods=["POST"])
def send_for_approval(ad_id):
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        flash("Ad not found.", "error")
        return redirect(url_for("dashboard"))

    # Resolve client for email context
    all_clients = get_all_clients()
    client = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)

    all_approvers    = _db_get_all("approvers")
    client_id        = ad.get("client_id", "")
    ad_account_id    = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        flash("No approvers configured for this ad account. Add approvers first.", "error")
        return redirect(url_for("view_ad", ad_id=ad_id))

    selected_ids       = request.form.getlist("approver_ids")
    if not selected_ids:
        flash("Select at least one approver.", "error")
        return redirect(url_for("view_ad", ad_id=ad_id))

    selected_approvers = [a for a in approvers if a["id"] in selected_ids]

    errors, sent_count = [], 0
    for apvr in selected_approvers:
        token = secrets.token_urlsafe(32)
        approval_data = {
            "id":             gen_id(),
            "ad_id":          ad_id,
            "approver_email": apvr["email"],
            "approver_name":  apvr["name"],
            "token":          token,
            "status":         "pending",
            "comments":       "",
            "sent_at":        now_iso(),
            "responded_at":   "",
        }
        _db_save("approvals", None, None, approval_data)
        try:
            send_approval_email(apvr["email"], apvr["name"], ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    existing_ad = _db_find_by("ads", "id", ad_id)
    if existing_ad:
        _db_update("ads", ad_id, {
            "status": "pending_approval", "updated_at": now_iso()
        })

    if sent_count:
        flash(f"Sent approval request to {sent_count} approver(s).", "success")
    for err in errors:
        flash(err, "error")

    return redirect(url_for("view_ad", ad_id=ad_id))

# ── Routes: Approver Response ─────────────────────────────────────────────
@app.route("/respond/<token>/<action>", methods=["GET", "POST"])
@csrf.exempt  # approval links come from emails — no session/cookie
def respond_to_approval(token, action):
    if action not in ("approve", "reject"):
        abort(400)

    approval = _db_find_by("approvals", "token", token)
    if not approval:
        return render_template("response_page.html",
            status="error", message="Invalid or expired approval link.")

    if approval.get("status") != "pending":
        return render_template("response_page.html",
            status="already",
            message=f"You already {approval['status']} this ad.")

    # GET = show confirmation page, POST = process the response
    confirmed = request.form.get("confirmed") or request.args.get("confirmed")
    if confirmed != "1":
        return render_template("response_confirm.html",
            token=token, action=action, approval=approval)

    comments = request.form.get("comments") or request.args.get("comments", "")
    new_status = "approved" if action == "approve" else "rejected"
    _db_update("approvals", approval["id"], {
        "status":       new_status,
        "comments":     comments,
        "responded_at": now_iso(),
    })

    check_ad_approval_status(approval["ad_id"])

    return render_template("response_page.html",
        status=new_status,
        message=f"Thank you! You have {new_status} this ad.")

def check_ad_approval_status(ad_id):
    # ad_id could be a campaign ID (new flow) or an ad ID (legacy)
    ad_approvals = _db_list("approvals", {"ad_id": ad_id})

    # Try campaign first (new flow), then meta_ads (legacy)
    campaign = _db_find_by("campaigns", "id", ad_id)
    if campaign:
        # Campaign-level approval
        client_id = campaign.get("client_id", "")
        client = _db_find_by("clients", "id", client_id) if client_id else None
    else:
        ad = _db_find_by("meta_ads", "id", ad_id)
        if not ad:
            return
        client_id = ad.get("client_id", "")
        client = _db_find_by("clients", "id", client_id) if client_id else None
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()

    if ad_account_id:
        approvers = _db_list("approvers", {"meta_ad_account_id": ad_account_id})
    else:
        approvers = _db_list("approvers", {"client_id": client_id}) if client_id else _db_get_all("approvers")

    required_emails = {a["email"] for a in approvers if a.get("required") in (1, True, "1", "TRUE", "true")}

    # Only consider the LATEST approval per email (ignore older duplicates)
    latest_by_email = {}
    for apvl in ad_approvals:
        email = apvl["approver_email"]
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

    # Try ads table first, then meta_ads
    new_status = "rejected" if any_rejected else ("approved" if all_required_approved and required_emails else None)
    if not new_status:
        return

    if campaign:
        # Campaign-level: update campaign + cascade to all children
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
        # Legacy ad-level approval
        meta_ad = _db_find_by("meta_ads", "id", ad_id)
        if meta_ad:
            _db_update("meta_ads", ad_id, {"approval_status": new_status, "updated_at": now_iso()})
            campaign_id = meta_ad.get("campaign_id", "")
            adset_id = meta_ad.get("adset_id", "")
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

# ── Routes: Approver Management ───────────────────────────────────────────
@app.route("/approvers")
def manage_approvers():
    all_approvers  = _db_get_all("approvers")
    active_client  = get_active_client()
    ad_account_id  = (active_client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        # Fall back to client_id scope when no ad account configured yet
        client_id = session.get("active_client_id", "")
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers
    return render_template("approvers.html", approvers=approvers, active_client=active_client)

@app.route("/approvers/add", methods=["POST"])
def add_approver():
    name          = request.form.get("name", "").strip()
    email         = request.form.get("email", "").strip()
    required      = 1 if request.form.get("required") else 0
    client_id     = session.get("active_client_id", "")
    active_client = get_active_client()
    ad_account_id = (active_client or {}).get("meta_ad_account_id", "").strip()

    if not name or not email:
        flash("Name and email are required.", "error")
        return redirect(url_for("manage_approvers"))

    existing = _db_get_all("approvers")
    scope_key  = "meta_ad_account_id" if ad_account_id else "client_id"
    scope_val  = ad_account_id if ad_account_id else client_id
    if any(a["email"].lower() == email.lower() and a.get(scope_key) == scope_val
           for a in existing):
        flash("Approver with this email already exists for this ad account.", "error")
        return redirect(url_for("manage_approvers"))

    data = {"id": gen_id(), "client_id": client_id,
            "meta_ad_account_id": ad_account_id,
            "name": name, "email": email, "required": required}
    _db_save("approvers", None, None, data)
    flash(f"Added {name} as approver.", "success")
    return redirect(url_for("manage_approvers"))

@app.route("/approvers/<approver_id>/delete", methods=["POST"])
def delete_approver(approver_id):
    existing = _db_find_by("approvers", "id", approver_id)
    if existing:
        _db_delete("approvers", approver_id)
        flash("Approver removed.", "success")
    else:
        flash("Approver not found.", "error")
    return redirect(url_for("manage_approvers"))

@app.route("/approvers/<approver_id>/toggle-required", methods=["POST"])
def toggle_required(approver_id):
    approver = _db_find_by("approvers", "id", approver_id)
    if approver:
        cur = approver.get("required")
        # MySQL TINYINT returns 0/1, old Sheets data might be "TRUE"/"FALSE"
        is_required = cur in (1, True, "1", "TRUE", "true")
        _db_update("approvers", approver_id, {"required": 0 if is_required else 1})
    return redirect(url_for("manage_approvers"))

# ── Routes: Push to Facebook ──────────────────────────────────────────────
@app.route("/ad/<ad_id>/push-to-facebook", methods=["POST"])
def push_to_facebook(ad_id):
    ads = _db_get_all("ads")
    ad  = next((a for a in ads if a["id"] == ad_id), None)
    if not ad:
        flash("Ad not found.", "error")
        return redirect(url_for("dashboard"))

    if ad.get("status") != "approved":
        flash("Only approved ads can be pushed to Facebook.", "error")
        return redirect(url_for("view_ad", ad_id=ad_id))

    # Use client-specific ad account if available, fall back to env var
    all_clients = get_all_clients()
    client      = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    ad_account  = (client or {}).get("meta_ad_account_id", "").strip()
    if not ad_account:
        ad_account = os.getenv("META_BUSINESS_IDS", "").split(",")[0].strip()

    if not ad_account:
        flash("No Meta Ad Account ID configured for this client.", "error")
        return redirect(url_for("view_ad", ad_id=ad_id))

    if not get_active_token():
        flash("Meta access token not configured.", "error")
        return redirect(url_for("view_ad", ad_id=ad_id))

    try:
        campaign_url = f"https://graph.facebook.com/v21.0/act_{ad_account}/campaigns"
        resp = http_requests.post(campaign_url, data={
            "name":                  ad.get("title", "Untitled"),
            "objective":             "OUTCOME_TRAFFIC",
            "status":                "PAUSED",
            "special_ad_categories": "[]",
            "access_token":          get_active_token(),
        })
        data = resp.json()

        if "error" in data:
            flash(f"Facebook API error: {data['error'].get('message', 'Unknown')}", "error")
            return redirect(url_for("view_ad", ad_id=ad_id))

        campaign_id = data.get("id")
        existing_ad = _db_find_by("ads", "id", ad_id)
        if existing_ad:
            _db_update("ads", ad_id, {
                "status": f"pushed_fb_{campaign_id}", "updated_at": now_iso()
            })

        flash(f"Campaign created on Facebook (PAUSED). ID: {campaign_id}. "
              f"Complete setup in Ads Manager.", "success")

    except Exception as e:
        flash(f"Error pushing to Facebook: {e}", "error")

    return redirect(url_for("view_ad", ad_id=ad_id))

# ── Routes: Campaigns page + JSON APIs ────────────────────────────────────

@app.route("/campaigns")
def campaigns_page():
    client = get_active_client() or {}
    return render_template("campaigns.html",
                           default_pixel_id=client.get("default_pixel_id", ""),
                           default_url_tags=client.get("default_url_tags",
                               "utm_source=facebook&utm_medium=cpc&utm_campaign={campaign_name}"))


@app.route("/drafts")
def drafts_page():
    return render_template("campaigns.html", start_mode="manage")

@app.route("/queue")
def approval_queue():
    return render_template("queue.html")

@app.route("/approved")
def approved_ads():
    return render_template("approved.html")

@app.route("/rejected")
def rejected_ads():
    return render_template("rejected.html")

@app.route("/api/approval-queue")
def api_approval_queue():
    """Return campaigns pending approval for the active client."""
    cid = session.get("active_client_id", "")
    try:
        campaigns = _db_list("campaigns", {"client_id": cid}) if cid else []
        adsets = _db_get_all("adsets")
        meta_ads = _db_get_all("meta_ads")
        approvals = _db_get_all("approvals")
    except Exception:
        return jsonify({"pending": [], "error": "Database access failed"})

    pending = []
    approved_list = []
    rejected = []

    for camp in campaigns:
        status = (camp.get("approval_status") or "none").lower()
        if status not in ("pending_approval", "approved", "rejected"):
            continue

        # Attach ad sets and ads
        camp_adsets = [s for s in adsets if s.get("campaign_id") == camp["id"]]
        camp_ads = []
        for s in camp_adsets:
            s_ads = [a for a in meta_ads if a.get("adset_id") == s["id"]]
            camp_ads.extend(s_ads)
        camp["_adsets"] = camp_adsets
        camp["_ads"] = camp_ads

        # Primary ad for preview
        camp["_primary_ad"] = camp_ads[0] if camp_ads else {}

        # Approval info
        camp_approvals = [ap for ap in approvals if ap.get("ad_id") == camp["id"]]
        camp["_approvals"] = camp_approvals
        camp["_approval_count"] = len([ap for ap in camp_approvals if ap.get("status") == "approved"])
        camp["_rejection_count"] = len([ap for ap in camp_approvals if ap.get("status") == "rejected"])
        camp["_total_sent"] = len(camp_approvals)

        if status in ("pending_approval", "pending"):
            pending.append(camp)
        elif status == "approved":
            approved_list.append(camp)
        elif status == "rejected":
            rejected.append(camp)

    return jsonify({"pending": pending, "approved": approved_list, "rejected": rejected})


def _client_id():
    return session.get("active_client_id", "")

def _ad_account():
    c = get_active_client()
    return ((c or {}).get("meta_ad_account_id") or "").replace("act_", "").strip()

# ── Campaigns CRUD ────────────────────────────────────────────────
@app.route("/api/campaigns")
def api_campaigns_list():
    rows = _db_list("campaigns", {"client_id": _client_id()})
    return jsonify(rows)

@app.route("/api/manage-tree")
def api_manage_tree():
    """Full campaign → adset → ad tree for the Manage tab.
    If ?sync=1, also pulls live status from Meta for all items with meta IDs."""
    cid = _client_id()
    campaigns = _db_list("campaigns", {"client_id": cid})
    adsets    = _db_list("adsets",    {"client_id": cid})
    ads       = _db_list("meta_ads",   {"client_id": cid})

    # Sync live status from Meta if requested
    do_sync = request.args.get("sync", "0") == "1"
    if do_sync:
        try:
            _init_meta()
            _sync_meta_statuses(campaigns, adsets, ads)
        except Exception:
            pass  # fail silently — show stale data rather than nothing

    # Group adsets by campaign_id, ads by adset_id
    adsets_by_camp = {}
    for s in adsets:
        adsets_by_camp.setdefault(s.get("campaign_id",""), []).append(s)
    ads_by_adset = {}
    for a in ads:
        ads_by_adset.setdefault(a.get("adset_id",""), []).append(a)
    # Enrich ads with approval progress
    try:
        approvals = _db_get_all("approvals")
        approvals_by_ad = {}
        for apvl in approvals:
            approvals_by_ad.setdefault(apvl.get("ad_id",""), []).append(apvl)
        for a in ads:
            ad_apvls = approvals_by_ad.get(a["id"], [])
            a["_approval_total"]    = len(ad_apvls)
            a["_approval_approved"] = sum(1 for x in ad_apvls if x.get("status") == "approved")
            a["_approval_rejected"] = sum(1 for x in ad_apvls if x.get("status") == "rejected")
            a["_approval_pending"]  = sum(1 for x in ad_apvls if x.get("status") == "pending")
    except Exception:
        pass

    # Filter: only show true drafts — campaigns NOT in the approval flow
    # Uses campaign.approval_status (email-based), NOT launch_status (Meta status)
    APPROVAL_STATUSES = {"pending_approval", "approved", "rejected"}

    tree = []
    for c in campaigns:
        appr_status = (c.get("approval_status") or "none").lower()
        if appr_status in APPROVAL_STATUSES:
            continue  # shown in Queue/Approved/Rejected tabs instead
        camp_adsets = adsets_by_camp.get(c["id"], [])
        for s in camp_adsets:
            s["ads"] = ads_by_adset.get(s["id"], [])
        c["adsets"] = camp_adsets
        tree.append(c)
    return jsonify(tree)


def _sync_meta_statuses(campaigns, adsets, ads):
    """Pull effective_status from Meta for all launched items and update local sheet + data."""
    # Map: meta_id → (sheet_name, local_id, headers, dict_ref)
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

    # Batch-read statuses from Meta (SDK handles rate limits)
    for obj_type, meta_id, db_table, local in to_check:
        try:
            if obj_type == "campaign":
                obj = FBCampaign(meta_id).api_get(fields=['effective_status'])
            elif obj_type == "adset":
                obj = FBAdSet(meta_id).api_get(fields=['effective_status'])
            else:
                obj = FBAd(meta_id).api_get(fields=['effective_status'])

            live_status = obj.get("effective_status", "").upper()
            # Map Meta's effective_status to our display status
            status_map = {
                "ACTIVE":           "ACTIVE",
                "PAUSED":           "PAUSED",
                "DELETED":          "DELETED",
                "ARCHIVED":         "ARCHIVED",
                "CAMPAIGN_PAUSED":  "PAUSED",
                "ADSET_PAUSED":     "PAUSED",
                "DISAPPROVED":      "DISAPPROVED",
                "PENDING_REVIEW":   "PENDING_REVIEW",
                "WITH_ISSUES":      "WITH_ISSUES",
                "IN_PROCESS":       "IN_PROCESS",
            }
            new_status = status_map.get(live_status, live_status or local.get("launch_status", "draft"))

            # Update local dict (for this response)
            old_status = local.get("launch_status", "")
            if new_status and new_status != old_status:
                local["launch_status"] = new_status
                # Persist to sheet
                try:
                    _db_save(db_table, None, local["id"], {"launch_status": new_status})
                except Exception:
                    pass  # sheet write failed, but local dict is updated for this response

        except FacebookRequestError:
            # Object might be deleted on Meta — mark as DELETED
            if local.get("launch_status") not in ("error", "draft", "DELETED"):
                local["launch_status"] = "DELETED"
                try:
                    _db_save(db_table, None, local["id"], {"launch_status": "DELETED"})
                except Exception:
                    pass
        except Exception:
            pass  # skip this item

@app.route("/api/campaigns/save", methods=["POST"])
@csrf.exempt
def api_campaigns_save():
    body = request.get_json(silent=True) or {}
    item_id = body.pop("id", "")
    body["client_id"] = _client_id()
    body.setdefault("launch_status", "draft")
    new_id = _db_save("campaigns", None, item_id, body)
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/campaigns/<cid>/launch", methods=["POST"])
@csrf.exempt
def api_campaigns_launch(cid):
    rows = _db_list("campaigns")
    camp = next((r for r in rows if r["id"] == cid), None)
    if not camp: return jsonify({"error": "Not found"}), 404
    acct = _ad_account()
    if not acct: return jsonify({"error": "No ad account configured"}), 400
    try:
        mid = meta_launch_campaign(camp, acct)
        _db_save("campaigns", None, cid,
                    {"meta_campaign_id": mid, "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})
        return jsonify({"ok": True, "meta_campaign_id": mid})
    except Exception as e:
        _db_save("campaigns", None, cid, {"launch_status": "error", "error_msg": str(e)})
        return jsonify({"error": str(e)}), 500

@app.route("/api/launch-all", methods=["POST"])
@csrf.exempt
def api_launch_all():
    """Launch campaign → adset → ad to Meta in one server-side chain."""
    body  = request.get_json(silent=True) or {}
    cid   = body.get("campaign_id", "")
    sid   = body.get("adset_id", "")
    aid   = body.get("ad_id", "")
    status = body.get("status", "PAUSED")

    acct = _ad_account()
    if not acct:
        return jsonify({"ok": False, "error": "No ad account configured"}), 400

    camp_rows  = _db_list("campaigns")
    adset_rows = _db_list("adsets")
    ad_rows    = _db_list("meta_ads")

    camp  = next((r for r in camp_rows  if r["id"] == cid), None)
    adset = next((r for r in adset_rows if r["id"] == sid), None)
    ad    = next((r for r in ad_rows    if r["id"] == aid), None)

    if not camp:  return jsonify({"ok": False, "error": "Campaign not found"}), 404
    if not adset: return jsonify({"ok": False, "error": "Ad Set not found"}),  404
    if not ad:    return jsonify({"ok": False, "error": "Ad not found"}),       404

    # ── Approval gate: if approvers exist for this account, require approval ─
    all_clients = get_all_clients()
    client = next((c for c in all_clients if c["id"] == ad.get("client_id", "")), None)
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    try:
        all_approvers = _db_get_all("approvers")
        if ad_account_id:
            acct_approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
        else:
            acct_approvers = [a for a in all_approvers if a.get("client_id") == ad.get("client_id", "")]
        if acct_approvers and ad.get("approval_status") != "approved":
            return jsonify({"ok": False, "error": "Ad must be approved before publishing. Send for approval first."}), 400
    except Exception:
        pass  # If approvers sheet doesn't exist, allow launch

    try:
        # ── Step 1: Campaign ──────────────────────────────────────────
        meta_campaign_id = camp.get("meta_campaign_id", "")
        if not meta_campaign_id:
            meta_campaign_id = meta_launch_campaign(camp, acct)
            _db_save("campaigns", None, cid,
                        {"meta_campaign_id": meta_campaign_id,
                         "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})

        # ── Step 2: Ad Set ────────────────────────────────────────────
        meta_adset_id = adset.get("meta_adset_id", "")
        if not meta_adset_id:
            camp_obj = camp.get("objective", "")
            # Lead gen ad sets need page_id for promoted_object — get from ad
            if not adset.get("page_id") and ad.get("page_id"):
                adset["page_id"] = ad["page_id"]
            meta_adset_id = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=camp_obj)
            _db_save("adsets", None, sid,
                        {"meta_adset_id": meta_adset_id,
                         "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})

        # ── Step 3: Ad ────────────────────────────────────────────────
        ad["launch_status"] = status
        ad["_special_ad_categories"] = camp.get("special_ad_categories", "[]")
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid,
                    {"meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
                     "launch_status": status, "launched_at": now_iso(), "error_msg": ""})

        return jsonify({"ok": True, "meta_campaign_id": meta_campaign_id,
                        "meta_adset_id": meta_adset_id, "meta_ad_id": meta_ad_id})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/campaigns/<cid>/delete", methods=["DELETE"])
@csrf.exempt
def api_campaigns_delete(cid):
    _db_delete("campaigns", cid)
    return jsonify({"ok": True})

# ── Ad Sets CRUD ──────────────────────────────────────────────────
@app.route("/api/adsets")
def api_adsets_list():
    cid  = request.args.get("campaign_id", "")
    rows = _db_list("adsets", {"client_id": _client_id(), "campaign_id": cid} if cid else {"client_id": _client_id()})
    return jsonify(rows)

@app.route("/api/adsets/save", methods=["POST"])
@csrf.exempt
def api_adsets_save():
    body = request.get_json(silent=True) or {}
    item_id = body.pop("id", "")
    body["client_id"] = _client_id()
    body.setdefault("launch_status", "draft")
    new_id = _db_save("adsets", None, item_id, body)
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/adsets/<sid>/launch", methods=["POST"])
@csrf.exempt
def api_adsets_launch(sid):
    rows  = _db_list("adsets")
    adset = next((r for r in rows if r["id"] == sid), None)
    if not adset: return jsonify({"error": "Not found"}), 404
    # Get parent campaign's meta_campaign_id
    camp_rows = _db_list("campaigns")
    camp = next((r for r in camp_rows if r["id"] == adset.get("campaign_id", "")), None)
    meta_campaign_id = (camp or {}).get("meta_campaign_id", "")
    if not meta_campaign_id:
        return jsonify({"error": "Parent campaign must be launched to Meta first"}), 400
    acct = _ad_account()
    if not acct: return jsonify({"error": "No ad account configured"}), 400
    try:
        camp_obj = (camp or {}).get("objective", "")
        mid = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=camp_obj)
        _db_save("adsets", None, sid,
                    {"meta_adset_id": mid, "launch_status": "launched", "launched_at": now_iso(), "error_msg": ""})
        return jsonify({"ok": True, "meta_adset_id": mid})
    except Exception as e:
        _db_save("adsets", None, sid, {"launch_status": "error", "error_msg": str(e)})
        return jsonify({"error": str(e)}), 500

@app.route("/api/adsets/<sid>/delete", methods=["DELETE"])
@csrf.exempt
def api_adsets_delete(sid):
    _db_delete("adsets", sid)
    return jsonify({"ok": True})

# ── Meta Ads CRUD ─────────────────────────────────────────────────
@app.route("/api/meta-ads")
def api_meta_ads_list():
    sid  = request.args.get("adset_id", "")
    cid  = _client_id()
    if sid:
        # Get ads by adset_id — match client_id OR empty client_id (legacy data)
        rows = _db_list("meta_ads", {"adset_id": sid})
        if cid:
            rows = [r for r in rows if r.get("client_id") in (cid, "", None)]
    else:
        rows = _db_list("meta_ads", {"client_id": cid} if cid else {})
    return jsonify(rows)

@app.route("/api/meta-ads/save", methods=["POST"])
@csrf.exempt
def api_meta_ads_save():
    body = request.get_json(silent=True) or {}
    item_id = body.pop("id", "")
    body["client_id"] = _client_id()
    body.setdefault("launch_status", "draft")
    # Sanitize URLs to prevent javascript:/data: injection
    if "link_url" in body:
        body["link_url"] = safe_url(body["link_url"])
    new_id = _db_save("meta_ads", None, item_id, body)
    return jsonify({"ok": True, "id": new_id})

@app.route("/api/meta-ads/<aid>/launch", methods=["POST"])
@csrf.exempt
def api_meta_ads_launch(aid):
    rows = _db_list("meta_ads")
    ad   = next((r for r in rows if r["id"] == aid), None)
    if not ad: return jsonify({"error": "Not found"}), 404
    adset_rows = _db_list("adsets")
    adset = next((r for r in adset_rows if r["id"] == ad.get("adset_id", "")), None)
    meta_adset_id = (adset or {}).get("meta_adset_id", "")
    if not meta_adset_id:
        return jsonify({"error": "Parent ad set must be launched to Meta first"}), 400
    acct = _ad_account()
    if not acct: return jsonify({"error": "No ad account configured"}), 400
    # Look up campaign to check special_ad_categories for political auth
    camp_rows = _db_list("campaigns")
    camp = next((r for r in camp_rows if r["id"] == ad.get("campaign_id", "")), None)
    if not camp and adset:
        camp = next((r for r in camp_rows if r["id"] == adset.get("campaign_id", "")), None)
    ad["_special_ad_categories"] = (camp or {}).get("special_ad_categories", "[]")
    req_data = request.get_json(silent=True) or {}
    ad["launch_status"] = req_data.get("status", "PAUSED")
    try:
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid, {
            "meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
            "launch_status": ad["launch_status"], "launched_at": now_iso(), "error_msg": "",
        })
        return jsonify({"ok": True, "meta_creative_id": creative_id, "meta_ad_id": meta_ad_id})
    except Exception as e:
        _db_save("meta_ads", None, aid, {"launch_status": "error", "error_msg": str(e)})
        return jsonify({"error": str(e)}), 500

@app.route("/api/meta-ads/<aid>/publish", methods=["POST"])
@csrf.exempt
def api_meta_ads_publish(aid):
    """Full-chain publish: campaign → ad set → ad on Meta.
    Auto-launches any parent that hasn't been pushed yet."""
    acct = _ad_account()
    if not acct:
        return jsonify({"error": "No ad account configured"}), 400

    ads = _db_list("meta_ads")
    ad = next((a for a in ads if a["id"] == aid), None)
    if not ad:
        return jsonify({"error": "Ad not found"}), 404

    req_data = request.get_json(silent=True) or {}
    status = req_data.get("status", "PAUSED")

    # 1. Ensure campaign is on Meta
    camp_rows = _db_list("campaigns")
    camp = next((r for r in camp_rows if r["id"] == ad.get("campaign_id", "")), None)
    if not camp:
        return jsonify({"error": "Parent campaign not found in sheet"}), 400

    meta_campaign_id = camp.get("meta_campaign_id", "")
    if not meta_campaign_id:
        try:
            meta_campaign_id = meta_launch_campaign(camp, acct)
            _db_save("campaigns", None, camp["id"], {
                "meta_campaign_id": meta_campaign_id, "launch_status": "launched",
                "launched_at": now_iso(), "error_msg": ""
            })
        except Exception as e:
            return jsonify({"error": f"[campaigns] {e}"}), 500

    # 2. Ensure ad set is on Meta
    adset_rows = _db_list("adsets")
    adset = next((r for r in adset_rows if r["id"] == ad.get("adset_id", "")), None)
    if not adset:
        return jsonify({"error": "Parent ad set not found in sheet"}), 400

    meta_adset_id = adset.get("meta_adset_id", "")
    if not meta_adset_id:
        try:
            campaign_obj = camp.get("objective", "OUTCOME_AWARENESS")
            meta_adset_id = meta_launch_adset(adset, meta_campaign_id, acct, campaign_objective=campaign_obj)
            _db_save("adsets", None, adset["id"], {
                "meta_adset_id": meta_adset_id, "launch_status": "launched",
                "launched_at": now_iso(), "error_msg": ""
            })
        except Exception as e:
            return jsonify({"error": f"[adsets] {e}"}), 500

    # 3. Launch the ad
    ad["_special_ad_categories"] = camp.get("special_ad_categories", "[]")
    ad["launch_status"] = status
    try:
        creative_id, meta_ad_id = meta_launch_ad(ad, meta_adset_id, acct)
        _db_save("meta_ads", None, aid, {
            "meta_creative_id": creative_id, "meta_ad_id": meta_ad_id,
            "launch_status": status, "launched_at": now_iso(), "error_msg": "",
        })
        return jsonify({"ok": True, "meta_ad_id": meta_ad_id})
    except Exception as e:
        _db_save("meta_ads", None, aid, {"launch_status": "error", "error_msg": str(e)})
        return jsonify({"error": f"[ad] {e}"}), 500


@app.route("/api/campaigns/<cid>/remove-from-queue", methods=["POST"])
@csrf.exempt
def api_campaign_remove_from_queue(cid):
    """Remove campaign from approval queue — resets approval_status to 'none' so it goes back to Drafts."""
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return jsonify({"error": "Campaign not found.", "ok": False}), 404

    # Reset campaign approval status
    _db_update("campaigns", cid, {"approval_status": "none", "updated_at": now_iso()})

    # Reset all children
    adsets = _db_list("adsets", {"campaign_id": cid})
    for s in adsets:
        try:
            _db_update("adsets", s["id"], {"approval_status": "none", "updated_at": now_iso()})
        except Exception:
            pass
        ads = _db_list("meta_ads", {"adset_id": s["id"]})
        for a in ads:
            try:
                _db_update("meta_ads", a["id"], {"approval_status": "none", "updated_at": now_iso()})
            except Exception:
                pass

    # Delete all approval records for this campaign
    approvals = _db_list("approvals", {"ad_id": cid})
    for ap in approvals:
        try:
            _db_delete("approvals", ap["id"])
        except Exception:
            pass

    return jsonify({"ok": True})


@app.route("/api/campaigns/<cid>/unapprove", methods=["POST"])
@csrf.exempt
def api_campaign_unapprove(cid):
    """Revert a campaign from approved/rejected back to pending_approval."""
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return jsonify({"error": "Campaign not found."}), 404

    # Reset campaign approval status
    _db_update("campaigns", cid, {"approval_status": "pending_approval", "updated_at": now_iso()})

    # Reset all ad sets
    adsets = _db_list("adsets", {"campaign_id": cid})
    for s in adsets:
        try:
            _db_update("adsets", s["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
        except Exception:
            pass
        # Reset all ads
        ads = _db_list("meta_ads", {"adset_id": s["id"]})
        for a in ads:
            try:
                _db_update("meta_ads", a["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
            except Exception:
                pass

    # Reset all approval responses back to pending
    approvals = _db_list("approvals", {"ad_id": cid})
    for ap in approvals:
        try:
            _db_update("approvals", ap["id"], {"status": "pending", "responded_at": None, "comments": ""})
        except Exception:
            pass

    return jsonify({"ok": True})


@app.route("/api/campaigns/<cid>/send-for-approval", methods=["POST"])
@csrf.exempt
def api_campaign_send_approval(cid):
    """Send the entire campaign (with all ad sets + ads) for approval."""
    campaign = _db_find_by("campaigns", "id", cid)
    if not campaign:
        return jsonify({"error": "Campaign not found."}), 404

    # Get all ad sets and ads under this campaign
    adsets = _db_list("adsets", {"campaign_id": cid})
    all_ads = []
    for s in adsets:
        ads = _db_list("meta_ads", {"adset_id": s["id"]})
        all_ads.extend(ads)

    # Resolve client
    client_id = campaign.get("client_id", "")
    client = _db_find_by("clients", "id", client_id) if client_id else None

    # Scope approvers by ad account
    all_approvers = _db_get_all("approvers")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        return jsonify({"error": "No approvers configured. Add approvers in the Approvers tab first."}), 400

    # Build campaign context for the email
    campaign["_adsets"] = adsets
    campaign["_ads"] = all_ads

    # Use the first ad's images/creative for the email preview
    primary_ad = all_ads[0] if all_ads else {}

    # Check for existing pending/approved approvals — don't create duplicates
    existing_approvals = _db_list("approvals", {"ad_id": cid})
    existing_emails = {}
    for ea in existing_approvals:
        existing_emails[ea["approver_email"]] = ea["status"]

    errors, sent_count = [], 0
    for apvr in approvers:
        email = apvr["email"]
        # Skip if already approved
        if existing_emails.get(email) == "approved":
            continue
        # Delete old pending entry for this email (replace with fresh one)
        for ea in existing_approvals:
            if ea["approver_email"] == email and ea["status"] == "pending":
                try:
                    _db_delete("approvals", ea["id"])
                except Exception:
                    pass

        token = secrets.token_urlsafe(32)
        approval_data = {
            "id":             gen_id(),
            "ad_id":          cid,  # campaign ID, not ad ID
            "approver_email": email,
            "approver_name":  apvr["name"],
            "token":          token,
            "status":         "pending",
            "comments":       "",
            "sent_at":        now_iso(),
        }
        _db_save("approvals", None, None, approval_data)
        try:
            send_campaign_approval_email(apvr["email"], apvr["name"], campaign, primary_ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    # Mark campaign + all ad sets + all ads as pending_approval
    _db_update("campaigns", cid, {"approval_status": "pending_approval", "updated_at": now_iso()})
    for s in adsets:
        _db_update("adsets", s["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})
    for a in all_ads:
        _db_update("meta_ads", a["id"], {"approval_status": "pending_approval", "updated_at": now_iso()})

    return jsonify({"ok": True, "sent": sent_count, "total_approvers": len(approvers), "errors": errors})


@app.route("/api/meta-ads/<aid>/send-for-approval", methods=["POST"])
@csrf.exempt
def api_meta_ads_send_approval(aid):
    """Send approval emails to all approvers scoped to this ad's account."""
    ads = _db_list("meta_ads")
    ad = next((a for a in ads if a["id"] == aid), None)
    if not ad:
        return jsonify({"error": "Ad not found."}), 404

    # Resolve client for email context and approver scoping
    all_clients = get_all_clients()
    client_id = ad.get("client_id", "")
    client = next((c for c in all_clients if c["id"] == client_id), None)

    # Scope approvers by ad account (same logic as old send_for_approval)
    all_approvers = _db_get_all("approvers")
    ad_account_id = (client or {}).get("meta_ad_account_id", "").strip()
    if ad_account_id:
        approvers = [a for a in all_approvers if a.get("meta_ad_account_id") == ad_account_id]
    else:
        approvers = [a for a in all_approvers if a.get("client_id") == client_id] or all_approvers

    if not approvers:
        return jsonify({"error": "No approvers configured for this ad account. Add approvers in the Approvers tab first."}), 400

    # Make sure ad has a title field for the email template
    ad.setdefault("title", ad.get("ad_name", "Untitled Ad"))

    # Prevent duplicates — skip already approved, replace pending
    existing_approvals = _db_list("approvals", {"ad_id": aid})
    existing_emails = {}
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
        approval_data = {
            "id":             gen_id(),
            "ad_id":          aid,
            "approver_email": email,
            "approver_name":  apvr["name"],
            "token":          token,
            "status":         "pending",
            "comments":       "",
            "sent_at":        now_iso(),
        }
        _db_save("approvals", None, None, approval_data)
        try:
            send_approval_email(apvr["email"], apvr["name"], ad, token, client=client)
            sent_count += 1
        except Exception as e:
            errors.append(f"Failed to email {apvr['email']}: {e}")

    # Update ad's approval_status
    _db_save("meta_ads", None, aid, {
        "approval_status": "pending_approval", "updated_at": now_iso()
    })

    return jsonify({"ok": True, "sent": sent_count, "total_approvers": len(approvers), "errors": errors})


@app.route("/api/meta-ads/<aid>/approval-status")
def api_meta_ads_approval_status(aid):
    """Return approval progress for an ad."""
    approvals = _db_get_all("approvals")
    ad_approvals = [a for a in approvals if a.get("ad_id") == aid]
    total = len(ad_approvals)
    approved = sum(1 for a in ad_approvals if a.get("status") == "approved")
    rejected = sum(1 for a in ad_approvals if a.get("status") == "rejected")
    pending = sum(1 for a in ad_approvals if a.get("status") == "pending")
    return jsonify({
        "total": total, "approved": approved, "rejected": rejected, "pending": pending,
        "approvers": [{"name": a.get("approver_name"), "email": a.get("approver_email"),
                        "status": a.get("status"), "comments": a.get("comments"),
                        "responded_at": a.get("responded_at")} for a in ad_approvals]
    })


@app.route("/api/meta-ads/<aid>/delete", methods=["DELETE"])
@csrf.exempt
def api_meta_ads_delete(aid):
    # Auth check: verify ad belongs to active client
    ad = _db_find_by("meta_ads", "id", aid)
    if not ad:
        return jsonify({"ok": False, "error": "Ad not found"}), 404
    if ad.get("client_id") and ad["client_id"] != _client_id():
        return jsonify({"ok": False, "error": "Access denied"}), 403
    _db_delete("meta_ads", aid)
    # Also clean up approval records for this ad
    try:
        _db_delete_where("approvals", "ad_id", aid)
    except Exception:
        pass  # Approvals cleanup is best-effort
    return jsonify({"ok": True})

@app.route("/api/meta-ads/<aid>/remove-from-queue", methods=["POST"])
@csrf.exempt
def api_meta_ads_remove_from_queue(aid):
    """Remove ad from approval queue without deleting the ad itself.
    Clears approval status and deletes approval records only."""
    try:
        # Auth check: verify ad belongs to active client
        ad = _db_find_by("meta_ads", "id", aid)
        if not ad:
            return jsonify({"ok": False, "error": "Ad not found"}), 404
        if ad.get("client_id") and ad["client_id"] != _client_id():
            return jsonify({"ok": False, "error": "Access denied"}), 403
        _db_save("meta_ads", None, aid, {"approval_status": "none"})
        _db_delete_where("approvals", "ad_id", aid)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Init ──────────────────────────────────────────────────────────────────
with app.app_context():
    try:
        conn = get_db()
        conn.ping(reconnect=True)
        conn.close()
        print("[OK] MySQL connection verified")
    except Exception as e:
        print(f"[WARN] Could not connect to MySQL: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
