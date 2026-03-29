"""
Facebook Ad Approval — pure-Python core helpers (no Flask).
Imported by routers/fb_ad_approval.py.
"""

import os, json, uuid, time, logging, smtplib, secrets
import re as _re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# ── Meta Business SDK ────────────────────────────────────────────────────
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign as FBCampaign
from facebook_business.adobjects.adset import AdSet as FBAdSet
from facebook_business.adobjects.ad import Ad as FBAd
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adimage import AdImage
from facebook_business.exceptions import FacebookRequestError

import requests as http_requests

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────

META_GRAPH = "https://graph.facebook.com/v21.0"

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

ACCOUNT_STATUS = {
    "1": "Active", "2": "Disabled", "3": "Unsettled",
    "7": "Pending Review", "9": "In Grace Period",
    "100": "Pending Closure", "101": "Closed",
}

OBJECTIVE_DEFAULTS = {
    "OUTCOME_AWARENESS":      ("REACH",              "IMPRESSIONS"),
    "OUTCOME_TRAFFIC":        ("LANDING_PAGE_VIEWS", "IMPRESSIONS"),
    "OUTCOME_ENGAGEMENT":     ("POST_ENGAGEMENT",    "IMPRESSIONS"),
    "OUTCOME_LEADS":          ("LEAD_GENERATION",    "IMPRESSIONS"),
    "OUTCOME_APP_PROMOTION":  ("APP_INSTALLS",       "IMPRESSIONS"),
    "OUTCOME_SALES":          ("OFFSITE_CONVERSIONS","IMPRESSIONS"),
}

OBJECTIVE_VALID_GOALS = {
    "OUTCOME_AWARENESS":  ["REACH", "IMPRESSIONS", "AD_RECALL_LIFT"],
    "OUTCOME_TRAFFIC":    ["LANDING_PAGE_VIEWS", "LINK_CLICKS", "IMPRESSIONS", "REACH", "CONVERSATIONS"],
    "OUTCOME_ENGAGEMENT": ["POST_ENGAGEMENT", "IMPRESSIONS", "REACH", "LINK_CLICKS", "LANDING_PAGE_VIEWS", "CONVERSATIONS"],
    "OUTCOME_LEADS":      ["LEAD_GENERATION", "QUALITY_LEAD"],
    "OUTCOME_APP_PROMOTION": ["APP_INSTALLS"],
    "OUTCOME_SALES":      ["OFFSITE_CONVERSIONS"],
}

AUTOBID_ONLY_GOALS = {"AD_RECALL_LIFT"}
NO_COST_CAP_GOALS  = {"IMPRESSIONS", "CONVERSATIONS"}

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

FB_TOKEN_FILE  = os.path.join(os.path.dirname(__file__), "credentials", "fb_token.json")
UPLOAD_FOLDER  = os.path.join(os.path.dirname(__file__), "..", "static", "uploads")
ALLOWED_IMAGES = {"jpg", "jpeg", "png", "gif", "webp"}
ALLOWED_VIDEOS = {"mp4", "mov", "avi", "mkv", "webm"}
ALLOWED_MEDIA  = ALLOWED_IMAGES | ALLOWED_VIDEOS

FB_IMAGE_MAX_SIZE = 30 * 1024 * 1024
FB_VIDEO_MAX_SIZE = 4 * 1024 * 1024 * 1024
UPLOAD_MAX_SIZE   = 500 * 1024 * 1024

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

# ── Helpers ───────────────────────────────────────────────────────────────

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def gen_id():
    return uuid.uuid4().hex[:12]

def safe_url(url):
    """Validate URL scheme — only allow http/https."""
    if not url:
        return ""
    url = url.strip()
    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme.lower() not in ("http", "https", ""):
        return ""
    if url and not url.startswith(("http://", "https://", "/")):
        url = "https://" + url
    return url

def safe_brand_color(color):
    """Validate hex color — prevent CSS injection."""
    if color and _re.match(r'^#[0-9A-Fa-f]{3,6}$', color):
        return color
    return "#1877F2"

# ── FB token persistence ──────────────────────────────────────────────────

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

# ── MySQL Database ────────────────────────────────────────────────────────
import mysql.connector
from mysql.connector import pooling

_db_pool = None

def _get_db_pool():
    global _db_pool
    if _db_pool is None:
        ssl_ca = os.getenv("MYSQL_SSL_CA", "")
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
_ALLOWED_TABLES = frozenset({
    "clients", "campaigns", "adsets", "meta_ads", "approvers",
    "approvals", "lead_forms", "saved_locations", "users", "settings",
})
_IDENT_RE = _re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

def _check_table(table):
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table}")

def _check_column(col):
    if not _IDENT_RE.match(col):
        raise ValueError(f"Invalid column name: {col}")

# ── Settings helper ────────────────────────────────────────────────────────
_settings_cache = {}
_settings_cache_ts = 0

def get_setting(key, default=""):
    """Get a setting from DB, with env var fallback and 60s cache."""
    global _settings_cache, _settings_cache_ts
    now = time.time()
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

# ── DB helpers ─────────────────────────────────────────────────────────────

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
        if table != "settings":
            sql += " ORDER BY created_at DESC"
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = {}
            for k, v in row.items():
                if v is None:
                    d[k] = ""
                elif isinstance(v, datetime):
                    d[k] = v.strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    d[k] = str(v)
            result.append(d)
        return result
    except Exception as e:
        logger.error("_db_list(%s) error: %s", table, e)
        return []
    finally:
        conn.close()

def _db_save(table, headers_ignored, item_id, data):
    """Insert or update a row. Returns the row id."""
    _check_table(table)
    for k in data.keys():
        _check_column(k)
    DATETIME_COLS = {"created_at", "updated_at", "launched_at", "responded_at", "sent_at"}
    DECIMAL_COLS  = {"daily_budget", "lifetime_budget", "bid_amount"}
    BOOL_COLS     = {"required"}
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
            sets = ", ".join(f"`{k}` = %s" for k in data.keys())
            vals = list(data.values()) + [item_id]
            cursor.execute(f"UPDATE `{table}` SET {sets} WHERE id = %s", vals)
            conn.commit()
            return item_id
        else:
            data["id"] = gen_id()
            data["created_at"] = now_iso()
            cols = ", ".join(f"`{k}`" for k in data.keys())
            phs  = ", ".join(["%s"] * len(data))
            cursor.execute(f"INSERT INTO `{table}` ({cols}) VALUES ({phs})", list(data.values()))
            conn.commit()
            return data["id"]
    except Exception as e:
        logger.error("_db_save(%s) error: %s", table, e)
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
        logger.error("_db_delete(%s) error: %s", table, e)
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
        logger.error("_db_delete_where(%s) error: %s", table, e)
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
        return {
            k: ("" if v is None else str(v) if not isinstance(v, datetime) else v.strftime("%Y-%m-%d %H:%M:%S"))
            for k, v in row.items()
        }
    except Exception:
        return None
    finally:
        conn.close()

def _db_update(table, item_id, data):
    """Update specific fields on a row."""
    if not data:
        return
    DATETIME_COLS = {"created_at", "updated_at", "launched_at", "responded_at", "sent_at"}
    DECIMAL_COLS  = {"daily_budget", "lifetime_budget", "bid_amount"}
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
        logger.error("_db_update(%s) error: %s", table, e)
    finally:
        conn.close()

# ── Meta API cache ─────────────────────────────────────────────────────────
_meta_cache   = {}
_META_CACHE_TTL = 300  # 5 minutes

def _cache_key(endpoint, extra_params):
    p = tuple(sorted((extra_params or {}).items()))
    return (endpoint, p)

def _init_meta(token=None):
    """Initialize the Meta SDK with the given token."""
    t = token
    if not t:
        raise Exception("No Meta access token available — connect your Facebook account.")
    FacebookAdsApi.init(
        app_id=os.getenv("FB_APP_ID", "") or 'placeholder',
        app_secret='',
        access_token=t,
        api_version='v21.0',
    )
    return t

def _sdk_to_dict(obj):
    return dict(obj)

def _sdk_cursor_to_list(cursor):
    return [dict(item) for item in cursor]

def _cached_sdk(cache_key_str, fetcher):
    hit = _meta_cache.get(cache_key_str)
    if hit and (time.time() - hit["t"]) < _META_CACHE_TTL:
        return hit["data"]
    data = fetcher()
    _meta_cache[cache_key_str] = {"data": data, "t": time.time()}
    return data

def meta_get(endpoint, extra_params=None, token=None, cache=True):
    """GET from Meta Graph API (raw HTTP)."""
    if cache:
        key = _cache_key(endpoint, extra_params)
        hit = _meta_cache.get(key)
        if hit and (time.time() - hit["t"]) < _META_CACHE_TTL:
            return hit["data"]
    params = {"access_token": token or "", "limit": 200}
    if extra_params:
        params.update(extra_params)
    resp = http_requests.get(f"{META_GRAPH}{endpoint}", params=params, timeout=15)
    data = resp.json()
    if cache and "error" not in data:
        _meta_cache[_cache_key(endpoint, extra_params)] = {"data": data, "t": time.time()}
    return data

def meta_get_all(endpoint, extra_params=None, token=None):
    """GET from Meta Graph API with pagination."""
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

def _get_page_token(page_id, user_token=None):
    """Get a page access token for the given page ID."""
    token = user_token or ""
    if not token or not page_id:
        return None
    try:
        result = meta_get(f"/{page_id}", {"fields": "access_token"}, token=token)
        return result.get("access_token")
    except Exception:
        return None

# ── Launch helpers ─────────────────────────────────────────────────────────

def build_targeting_spec(data):
    """Convert our internal format to Meta's targeting_spec dict."""
    spec = {}
    locs = []
    try:
        locs = json.loads(data.get("targeting_locations") or "[]")
    except Exception:
        pass
    if locs:
        geo = {}
        type_map = {
            "city": "cities", "region": "regions", "zip": "zips",
            "geo_market": "geo_markets", "electoral_district": "electoral_districts",
            "neighborhood": "neighborhoods", "subcity": "subcities",
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
    try:
        spec["age_min"] = int(data.get("targeting_age_min") or 18)
    except ValueError:
        spec["age_min"] = 18
    try:
        spec["age_max"] = int(data.get("targeting_age_max") or 65)
    except ValueError:
        spec["age_max"] = 65
    gender = data.get("targeting_genders", "all")
    if gender == "male":
        spec["genders"] = [1]
    elif gender == "female":
        spec["genders"] = [2]
    interests = []
    try:
        interests = json.loads(data.get("targeting_interests") or "[]")
    except Exception:
        pass
    if interests:
        spec["flexible_spec"] = [{"interests": [{"id": i["id"], "name": i["name"]} for i in interests]}]
    exclusions = []
    try:
        exclusions = json.loads(data.get("targeting_exclusions") or "[]")
    except Exception:
        pass
    if exclusions:
        spec["exclusions"] = {"interests": [{"id": e["id"], "name": e["name"]} for e in exclusions]}
    custom = []
    try:
        custom = json.loads(data.get("targeting_custom_audiences") or "[]")
    except Exception:
        pass
    if custom:
        spec["custom_audiences"] = [{"id": c["id"]} for c in custom]
    excl_custom = []
    try:
        excl_custom = json.loads(
            data.get("targeting_excl_custom_audiences") or data.get("targeting_excl_custom") or "[]"
        )
    except Exception:
        pass
    if excl_custom:
        spec["excluded_custom_audiences"] = [{"id": c["id"]} for c in excl_custom]
    return spec


def _fb_error_detail(step, e, params_sent=None):
    """Build a detailed error message from a FacebookRequestError."""
    parts = [f"[{step}]"]
    user_msg = e.api_error_message() or ""
    if user_msg:
        parts.append(user_msg)
    body = e.body() or {}
    err  = body.get("error", {}) if isinstance(body, dict) else {}
    if not isinstance(err, dict):
        err = {"message": str(err)}
    code    = err.get("code", "")
    subcode = err.get("error_subcode", "")
    if code:
        parts.append(f"(code {code}{f', subcode {subcode}' if subcode else ''})")
    err_data = err.get("error_data", {})
    if not isinstance(err_data, dict):
        err_data = {}
    blame = err_data.get("blame_field_specs") or []
    if blame:
        parts.append(f"Field(s): {', '.join(str(b) for b in blame)}")
    title = err.get("error_user_title", "")
    if title and title not in user_msg:
        parts.insert(1, title + ":")
    if params_sent:
        safe = {k: (v if 'token' not in str(k).lower() else '***') for k, v in params_sent.items()}
        parts.append(f"| Params sent: {json.dumps(safe, default=str)[:1500]}")
    return " ".join(parts)


def meta_launch_campaign(camp, ad_account_id):
    """Step 1: Create Meta Campaign via SDK. Returns meta_campaign_id."""
    account = AdAccount(f'act_{ad_account_id}')
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
    budget_strategy = camp.get("budget_strategy", "ADSET")
    if budget_strategy == "CAMPAIGN":
        daily    = camp.get("daily_budget", "").strip()
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
    account   = AdAccount(f'act_{ad_account_id}')
    targeting = build_targeting_spec(adset)
    if "geo_locations" not in targeting:
        targeting["geo_locations"] = {"countries": ["US"]}
    targeting.setdefault("targeting_automation", {"advantage_audience": 0})

    obj = campaign_objective or adset.get("objective") or "OUTCOME_AWARENESS"
    opt, bill = OBJECTIVE_DEFAULTS.get(obj, ("REACH", "IMPRESSIONS"))
    user_opt    = adset.get("optimization_goal", "").strip()
    valid_goals = OBJECTIVE_VALID_GOALS.get(obj, [])
    if user_opt and user_opt in valid_goals:
        opt = user_opt
    elif user_opt and valid_goals and user_opt not in valid_goals:
        opt = valid_goals[0]

    campaign_has_cbo      = False
    campaign_has_lifetime = False
    try:
        camp_obj  = FBCampaign(meta_campaign_id)
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

    if not campaign_has_cbo:
        bt  = adset.get("budget_type") or "daily"
        key = "daily_budget" if bt == "daily" else "lifetime_budget"
        budget_cents = int(float(adset.get(key) or "10") * 100)
        params[key] = budget_cents

    if adset.get("start_time"):
        params[FBAdSet.Field.start_time] = adset["start_time"]
    if adset.get("end_time"):
        params[FBAdSet.Field.end_time] = adset["end_time"]
    elif campaign_has_lifetime:
        start = adset.get("start_time", "")
        try:
            start_dt = datetime.fromisoformat(start) if start else datetime.utcnow()
        except Exception:
            start_dt = datetime.utcnow()
        params[FBAdSet.Field.end_time] = (start_dt + timedelta(days=30)).isoformat()

    bid_amount_raw = adset.get("bid_amount", "").strip()
    bid_cents = int(float(bid_amount_raw) * 100) if bid_amount_raw else 500
    params["bid_amount"] = bid_cents

    dest    = adset.get("destination_type", "").strip()
    page_id = adset.get("page_id", "").strip()
    if obj == "OUTCOME_LEADS":
        params["destination_type"] = dest if dest in ("ON_AD", "WEBSITE", "MESSENGER") else "ON_AD"
        if page_id:
            params["promoted_object"] = {"page_id": page_id}
    elif dest:
        params["destination_type"] = dest
        if dest == "WHATSAPP" and page_id:
            params["promoted_object"] = {"page_id": page_id}
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
    """Step 3: Upload media → Create Creative → Create Ad. Returns (meta_creative_id, meta_ad_id)."""
    account   = AdAccount(f'act_{ad_account_id}')
    media_url = (ad.get("image_square") or ad.get("image_landscape") or
                 ad.get("image_portrait") or ad.get("image_stories") or "")
    VIDEO_EXTS = {"mp4", "mov", "avi", "mkv", "webm"}
    media_ext  = media_url.rsplit(".", 1)[-1].lower().split("?")[0] if "." in media_url else ""
    is_video   = media_ext in VIDEO_EXTS

    image_hash = None
    video_id   = None

    if media_url:
        try:
            if media_url.startswith("/static/uploads/"):
                local_path = os.path.join(
                    os.path.dirname(__file__), "..",
                    *media_url.lstrip("/").split("/")
                )
            else:
                resp = http_requests.get(media_url, timeout=60)
                import tempfile
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{media_ext or 'bin'}")
                tmp.write(resp.content)
                tmp.close()
                local_path = tmp.name

            if is_video:
                from facebook_business.adobjects.advideo import AdVideo as FBAdVideo
                vid = FBAdVideo(parent_id=f'act_{ad_account_id}')
                vid[FBAdVideo.Field.filepath] = local_path
                vid.remote_create()
                video_id = vid.get_id()
            else:
                img = AdImage(parent_id=f'act_{ad_account_id}')
                img[AdImage.Field.filename] = local_path
                img.remote_create()
                image_hash = img[AdImage.Field.hash]
        except Exception as e:
            logger.warning("Media upload failed: %s", e)

    link_url     = ad.get("link_url") or ""
    lead_form_id = ad.get("lead_form_id", "").strip()
    cta          = ad.get("cta") or ("SUBSCRIBE" if lead_form_id else "LEARN_MORE")
    page_id      = ad.get("page_id", "")

    if is_video and video_id:
        video_data = {
            "message": ad.get("primary_text") or "",
            "title":   ad.get("headline") or "",
            "link_description": ad.get("description") or "",
            "video_id": video_id,
            "call_to_action": {"type": cta, "value": {"link": link_url or "https://fb.me/"}},
        }
        if lead_form_id:
            video_data["call_to_action"]["value"]["lead_gen_form_id"] = lead_form_id
            video_data["call_to_action"]["value"]["link"] = "https://fb.me/"
        if image_hash:
            video_data["image_hash"] = image_hash
        object_story = {"page_id": page_id, "video_data": video_data}
    elif lead_form_id:
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

    insta_id = ad.get("instagram_actor_id", "").strip()
    if insta_id:
        object_story["instagram_actor_id"] = insta_id

    creative_params = {
        AdCreative.Field.name: f"{ad.get('ad_name', 'Ad')} -- Creative",
        AdCreative.Field.object_story_spec: object_story,
        AdCreative.Field.authorization_category: "POLITICAL",
    }
    try:
        creative    = account.create_ad_creative(params=creative_params)
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
    pixel_id = ad.get("tracking_pixel_id", "").strip()
    if pixel_id:
        ad_params[FBAd.Field.tracking_specs] = [
            {"action.type": ["offsite_conversion"], "fb_pixel": [pixel_id]}
        ]
    url_tags = ad.get("url_tags", "").strip()
    if url_tags:
        ad_params["url_tags"] = url_tags

    try:
        result = account.create_ad(params=ad_params)
        return creative_id, result["id"]
    except FacebookRequestError as e:
        raise Exception(_fb_error_detail("ads", e, ad_params))


# ── Gmail / Email sending ─────────────────────────────────────────────────

GMAIL_SA_FILE  = os.path.join(os.path.dirname(__file__), "credentials", "gmail-sa.json")
GMAIL_SENDER   = os.getenv("GMAIL_SENDER_EMAIL", "")
GMAIL_APP_PASS = os.getenv("GMAIL_APP_PASSWORD", "")


def _get_gmail_service():
    """Build Gmail API service using service account with domain-wide delegation."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/gmail.send"]
    sa_key = os.getenv("GMAIL_SA_KEY", "")
    if sa_key:
        info  = json.loads(sa_key)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(GMAIL_SA_FILE):
        creds = service_account.Credentials.from_service_account_file(GMAIL_SA_FILE, scopes=scopes)
    else:
        return None

    send_as = get_setting("GMAIL_SEND_AS") or "support@politikanyc.com"
    creds   = creds.with_subject(send_as)
    return build("gmail", "v1", credentials=creds)


def _send_email(msg):
    """Send an email.MIMEMultipart message via Gmail API or SMTP fallback."""
    import base64

    try:
        sa_key         = os.getenv("GMAIL_SA_KEY", "")
        sa_file_exists = os.path.exists(GMAIL_SA_FILE)
        logger.info("Gmail API check: GMAIL_SA_KEY=%s, SA file=%s",
                    'set' if sa_key else 'NOT SET', 'exists' if sa_file_exists else 'missing')
        service = _get_gmail_service()
        if service:
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            logger.info("Email sent via Gmail API")
            return
    except Exception as e:
        logger.warning("Gmail API failed: %s", e)

    if GMAIL_SENDER and GMAIL_APP_PASS:
        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as smtp:
                smtp.starttls()
                smtp.login(GMAIL_SENDER, GMAIL_APP_PASS)
                smtp.send_message(msg)
                logger.info("Email sent via SMTP 587")
                return
        except Exception:
            pass
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
                smtp.login(GMAIL_SENDER, GMAIL_APP_PASS)
                smtp.send_message(msg)
                logger.info("Email sent via SMTP 465")
                return
        except Exception as e:
            raise Exception(f"All email methods failed: {e}")
    else:
        raise Exception("No email credentials configured")
