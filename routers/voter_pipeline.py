"""Voter Pipeline — CRM sync, Aiven sync, export (cloud-safe ops only)."""
import asyncio
import csv
import io
import os
import shlex
import subprocess
import sys
import tempfile
import zipfile
import time as _time

import uuid
from datetime import date, datetime
from pathlib import Path

import pymysql
from fastapi import APIRouter, Depends, Form, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User


def _has_voter_access(user: User) -> bool:
    """True if user can access any part of the voter pipeline."""
    return bool(user.is_admin or user.voter_role in ("full", "export_viewer"))

# -- Enrichment stats cache (avoids 80s+ query on every click) --
_enrich_cache = {"data": None, "ts": 0}
_ENRICH_CACHE_TTL = 600  # 10 minutes


def _is_export_viewer(user: User) -> bool:
    """True if user is restricted to export/status/issues tabs only."""
    return not user.is_admin and user.voter_role == "export_viewer"


def require_voter_access(user: User = Depends(require_user)) -> User:
    """Allow full and export_viewer roles (plus admins)."""
    if not _has_voter_access(user):
        raise HTTPException(status_code=403, detail="Voter pipeline access required.")
    return user


def require_voter_full(user: User = Depends(require_user)) -> User:
    """Allow only full-access users and admins (not export_viewer)."""
    if not (user.is_admin or user.voter_role == "full"):
        raise HTTPException(status_code=403, detail="Full voter pipeline access required.")
    return user
import portal_config

router = APIRouter(prefix="/voter-pipeline")
templates = Jinja2Templates(directory="templates")

PORTAL_DIR  = Path(__file__).parent.parent
VOTER_DIR   = PORTAL_DIR / "voter_pipeline"

# ── Background task registry ────────────────────────────────────────────────
# Tasks run as subprocesses with stdout → file. Survives nginx timeouts and
# browser refreshes. Frontend polls /task/<id>/output for new content.
_TASK_DIR = PORTAL_DIR / ".tasks"
_TASK_DIR.mkdir(exist_ok=True)
_tasks: dict[str, dict] = {}  # id → {proc, cmd, started, logfile}


def _crm_connect(env: dict, **overrides) -> pymysql.Connection:
    """Open a PyMySQL connection to crm_unified (or another db via overrides)."""
    params = dict(
        host=env.get("MYSQL_HOST", env.get("DB_HOST", "127.0.0.1")),
        port=int(env.get("MYSQL_PORT", env.get("DB_PORT", "3306"))),
        user=env.get("MYSQL_USER", env.get("DB_USER", "root")),
        password=env.get("MYSQL_PASSWORD", env.get("DB_PASSWORD", "")),
        database="crm_unified",
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=30,
        autocommit=True,
    )
    params.update(overrides)
    return pymysql.connect(**params)


@router.get("", response_class=HTMLResponse)
def voter_pipeline_page(
    request: Request,
    current_user: User = Depends(require_user),
):
    if not _has_voter_access(current_user):
        from fastapi.responses import RedirectResponse
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(request, "voter_pipeline.html", {
        "current_user":        current_user,
        "voter_dir_exists":    VOTER_DIR.exists(),
        "export_viewer_only":  _is_export_viewer(current_user),
    })


def _build_env() -> dict:
    """Inject all portal DB settings into the subprocess environment.

    Uses os.environ as the base (so PATH, HOME etc. are inherited) then
    overlays every non-empty portal_settings value on top. This means any
    setting stored in the portal DB — Meta tokens, HubSpot keys, CM keys,
    Mailchimp keys — is automatically available to all pipeline scripts
    without needing to update this function when new settings are added.
    """
    import time
    if _time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    env = os.environ.copy()
    for key, val in portal_config._cache.items():
        if val:
            env[key] = val
    return env


@router.get("/stats")
def voter_stats(current_user: User = Depends(require_voter_access)):
    """Return CRM pipeline health statistics as JSON."""
    env = _build_env()
    try:
        conn = _crm_connect(env)
    except Exception as exc:
        return JSONResponse({"error": f"DB connect failed: {exc}"}, status_code=500)

    try:
        cur = conn.cursor()

        # NY classification:
        #   is_ny  = state IN ('NY','NEW YORK') OR (no state but NY zip 10000-14999)
        #   non_ny = has a state that isn't NY
        #   unk    = no state and no NY-looking zip
        # COALESCE prevents NULL propagation: NULL IN (...) = NULL not FALSE
        NY_STATE  = "COALESCE(UPPER(TRIM(state)),'') IN ('NY','NEW YORK')"
        NY_ZIP    = "COALESCE(zip5,'') BETWEEN '10000' AND '14999'"
        IS_NY     = f"({NY_STATE} OR ((state IS NULL OR state='') AND {NY_ZIP}))"
        IS_NON_NY = f"(state IS NOT NULL AND state != '' AND NOT ({NY_STATE}))"

        cur.execute(f"""
            SELECT
                COUNT(*)                                               AS total,
                SUM(vf_state_voter_id IS NOT NULL)                     AS matched,
                SUM({IS_NY})                                           AS ny_total,
                SUM({IS_NY} AND vf_state_voter_id IS NOT NULL)         AS ny_matched,
                SUM({IS_NON_NY})                                       AS non_ny_total,
                SUM({IS_NON_NY} AND vf_state_voter_id IS NOT NULL)     AS non_ny_matched,
                SUM(NOT {IS_NY} AND NOT {IS_NON_NY})                   AS unk_total,
                SUM(NOT {IS_NY} AND NOT {IS_NON_NY}
                    AND vf_state_voter_id IS NOT NULL)                  AS unk_matched
            FROM contacts
        """)
        row = cur.fetchone()
        total        = int(row[0] or 0)
        matched      = int(row[1] or 0)
        ny_total     = int(row[2] or 0)
        ny_matched   = int(row[3] or 0)
        non_ny_total = int(row[4] or 0)
        non_ny_matched = int(row[5] or 0)
        unk_total    = int(row[6] or 0)
        unk_matched  = int(row[7] or 0)

        # Unknown sub-breakdown: has a non-NY zip vs truly no zip
        cur.execute(f"""
            SELECT
                SUM(zip5 IS NOT NULL AND zip5 != '' AND NOT ({IS_NY})) AS has_zip,
                SUM(zip5 IS NULL OR zip5 = '') AS no_zip
            FROM contacts
            WHERE NOT {IS_NY} AND NOT {IS_NON_NY}
        """)
        row2 = cur.fetchone()
        unk_has_zip = int(row2[0] or 0)
        unk_no_zip  = int(row2[1] or 0)

        # Unmatched breakdown (NY only — these are the actionable ones)
        cur.execute(f"""
            SELECT
                SUM(vf_state_voter_id IS NULL
                    AND zip5 IS NOT NULL AND zip5 != ''
                    AND clean_last IS NOT NULL AND clean_last != ''),
                SUM(vf_state_voter_id IS NULL
                    AND (zip5 IS NULL OR zip5 = '')),
                SUM(vf_state_voter_id IS NULL
                    AND mobile IS NOT NULL AND mobile != '')
            FROM contacts
            WHERE {IS_NY}
        """)
        row = cur.fetchone()
        unmatched_name_zip = int(row[0] or 0)
        no_zip             = int(row[1] or 0)
        has_mobile         = int(row[2] or 0)

        # Party breakdown of matched contacts
        cur.execute("""
            SELECT vf_party, COUNT(*) AS cnt
            FROM contacts
            WHERE vf_party IS NOT NULL AND vf_party != ''
            GROUP BY vf_party ORDER BY cnt DESC
            LIMIT 8
        """)
        party_data = [{"party": r[0], "count": int(r[1])} for r in cur.fetchall()]

        # Source breakdown (primary source = first token in comma-separated sources)
        cur.execute("""
            SELECT
                SUBSTRING_INDEX(sources, ',', 1)        AS src,
                COUNT(*)                                 AS total,
                SUM(vf_state_voter_id IS NOT NULL)       AS matched_cnt
            FROM contacts
            WHERE sources IS NOT NULL AND sources != ''
            GROUP BY src
            ORDER BY total DESC
            LIMIT 15
        """)
        source_data = []
        for src, tot, mat in cur.fetchall():
            tot = int(tot);  mat = int(mat or 0)
            source_data.append({
                "source":  src or "unknown",
                "total":   tot,
                "matched": mat,
                "pct":     round(mat / tot * 100, 1) if tot else 0,
            })

        # Match method breakdown (vf_match_method column — added by extended_match.py)
        match_methods = []
        try:
            cur.execute("""
                SELECT COALESCE(vf_match_method, 'unlabelled') AS method,
                       COUNT(*) AS cnt
                FROM contacts
                WHERE vf_state_voter_id IS NOT NULL
                GROUP BY method ORDER BY cnt DESC
            """)
            match_methods = [
                {"method": r[0], "count": int(r[1])} for r in cur.fetchall()
            ]
        except Exception:
            pass  # column may not exist yet — silently omit

        # Last contact update
        cur.execute("SELECT MAX(updated_at) FROM contacts")
        last_sync = cur.fetchone()[0]
        conn.close()

        return JSONResponse({
            "total":               total,
            "matched":             matched,
            "pct":                 round(matched / total * 100, 1) if total else 0,
            "ny": {
                "total":   ny_total,
                "matched": ny_matched,
                "pct":     round(ny_matched / ny_total * 100, 1) if ny_total else 0,
            },
            "non_ny": {
                "total":   non_ny_total,
                "matched": non_ny_matched,
            },
            "unknown_state": {
                "total":    unk_total,
                "matched":  unk_matched,
                "has_zip":  unk_has_zip,
                "no_zip":   unk_no_zip,
            },
            "unmatched_name_zip":  unmatched_name_zip,
            "no_zip":              no_zip,
            "has_mobile":          has_mobile,
            "party":               party_data,
            "sources":             source_data,
            "match_methods":       match_methods,
            "last_sync":           last_sync.isoformat() if last_sync else None,
        })
    except Exception as exc:
        conn.close()
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/fb-accounts")
def fb_accounts(current_user: User = Depends(require_voter_full)):
    """Return list of configured FB ad account names from portal settings."""
    if _time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    cache = portal_config._cache

    accounts = []
    # Default account (no suffix)
    token   = (cache.get("META_ACCESS_TOKEN") or cache.get("FB_ACCESS_TOKEN") or "").strip()
    acct_id = (cache.get("FB_AD_ACCOUNT_ID") or "").strip()
    if token and acct_id:
        accounts.append({"name": "", "label": f"Default (act_{acct_id})", "account_id": acct_id})

    # Named accounts: FB_ACCESS_TOKEN_<name> + FB_AD_ACCOUNT_ID_<name>
    for key, val in cache.items():
        if key.startswith("FB_ACCESS_TOKEN_") and val.strip():
            suffix = key[len("FB_ACCESS_TOKEN_"):]
            aid_key = f"FB_AD_ACCOUNT_ID_{suffix}"
            aid = (cache.get(aid_key) or "").strip()
            if aid:
                accounts.append({
                    "name": suffix,
                    "label": f"{suffix}  (act_{aid})",
                    "account_id": aid,
                })

    return JSONResponse({"accounts": accounts})




@router.get("/fb-meta-accounts")
def fb_meta_accounts(current_user: User = Depends(require_voter_full)):
    """Fetch real ad accounts from Meta API using configured token.

    Strategy (each step only runs if the previous returned nothing):
      1. GET /me/adaccounts            — works for user tokens + some system users
      2. GET /me/businesses → per-biz  — auto-discovers businesses the token can see
      3. Configured META_BUSINESS_IDS  — manual fallback if auto-discovery fails
    """
    import requests as _req
    if _time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    cache = portal_config._cache

    token = (cache.get("META_ACCESS_TOKEN") or cache.get("FB_ACCESS_TOKEN") or "").strip()
    if not token:
        return JSONResponse({"accounts": [],
                             "error": "META_ACCESS_TOKEN not configured in Settings"})

    api_ver = cache.get("META_API_VERSION") or "v21.0"
    base    = f"https://graph.facebook.com/{api_ver}"
    seen    = set()
    accounts: list = []
    last_error: str | None = None

    def _fetch_accounts(endpoint: str) -> list:
        """Fetch ad account list from one endpoint; return [] on any failure."""
        nonlocal last_error
        try:
            d = _req.get(f"{base}/{endpoint}",
                         params={"fields": "id,name,account_id,account_status", "limit": 200,
                                 "access_token": token}, timeout=15).json()
            if "error" in d:
                last_error = d["error"].get("message", str(d["error"]))
                return []
            last_error = None
            return d.get("data", [])
        except Exception as exc:
            last_error = str(exc)
            return []

    def _add(rows: list) -> None:
        for a in rows:
            # account_status 1 = ACTIVE; skip disabled/closed/unsettled accounts
            if a.get("account_status") not in (None, 1):
                continue
            aid = a.get("account_id") or a["id"].replace("act_", "")
            if aid not in seen:
                seen.add(aid)
                accounts.append({"id": aid, "name": a.get("name", a["id"]),
                                 "label": f"{a.get('name', a['id'])}  (act_{aid})"})

    # Step 1: /me/adaccounts
    _add(_fetch_accounts("me/adaccounts"))

    # Step 2: auto-discover businesses via /me/businesses
    # (runs regardless of step 1 — adds any additional accounts not on the token directly)
    try:
        biz_resp = _req.get(f"{base}/me/businesses",
                            params={"fields": "id,name", "limit": 50,
                                    "access_token": token}, timeout=15).json()
        biz_list = biz_resp.get("data", [])
    except Exception:
        biz_list = []
    for biz in biz_list:
        bid = biz["id"]
        _add(_fetch_accounts(f"{bid}/owned_ad_accounts"))
        _add(_fetch_accounts(f"{bid}/client_ad_accounts"))

    # Step 3: configured META_BUSINESS_IDS — always query, not just as fallback
    # This ensures manually-configured business IDs are always included alongside
    # any accounts discovered via the token in steps 1 & 2.
    for bid in [b.strip() for b in
                (cache.get("META_BUSINESS_IDS") or "").split(",") if b.strip()]:
        _add(_fetch_accounts(f"{bid}/owned_ad_accounts"))
        _add(_fetch_accounts(f"{bid}/client_ad_accounts"))

    if accounts:
        last_error = None

    return JSONResponse({"accounts": accounts, "error": last_error})


@router.get("/fb-donor-audiences")
def fb_donor_audiences(dist_type: str = "", dist_val: str = "", current_user: User = Depends(require_voter_full)):
    """Return list of voter/segment audiences from voter_audience_bridge."""
    env = _build_env()
    try:
        conn = _crm_connect(env, database="nys_voter_tagging",
                            connect_timeout=10, read_timeout=30)
    except Exception as exc:
        return JSONResponse({"error": f"DB connect failed: {exc}"}, status_code=500)
    # Whitelist dist_type -> actual column name
    GEO_COL = {"ld": "LDName", "sd": "SDName", "cd": "CDName", "county": "CountyName"}
    geo_col = GEO_COL.get(dist_type.lower().strip()) if dist_type else None
    geo_label = f"{dist_type.upper()} {dist_val}" if geo_col and dist_val else "Statewide"
    # Map geo columns to their cache tables
    _CACHE_TABLE = {
        "SDName": "counts_sd_audience",
        "LDName": "counts_ld_audience",
        "CDName": "counts_cd_audience",
    }
    try:
        with conn.cursor() as cur:
            cache_table = _CACHE_TABLE.get(geo_col) if geo_col else None
            if cache_table and dist_val:
                # Try the pre-computed cache table first (instant)
                cur.execute(
                    f"SELECT audience, voters AS cnt "
                    f"FROM nys_voter_tagging.`{cache_table}` "
                    f"WHERE `{geo_col}` = %s "
                    f"ORDER BY audience",
                    [dist_val],
                )
                rows = cur.fetchall()
                if not rows:
                    # Cache empty — fall back to live GROUP BY
                    cur.execute(
                        f"SELECT audience, COUNT(*) AS cnt "
                        f"FROM nys_voter_tagging.voter_audience_bridge "
                        f"WHERE `{geo_col}` = %s "
                        f"GROUP BY audience ORDER BY audience",
                        [dist_val],
                    )
                    rows = cur.fetchall()
            elif not geo_col or not dist_val:
                # Statewide — try cache first
                cur.execute(
                    "SELECT audience, voters AS cnt "
                    "FROM nys_voter_tagging.counts_state_audience "
                    "ORDER BY audience"
                )
                rows = cur.fetchall()
                if not rows:
                    cur.execute(
                        "SELECT audience, COUNT(*) AS cnt "
                        "FROM nys_voter_tagging.voter_audience_bridge "
                        "GROUP BY audience ORDER BY audience"
                    )
                    rows = cur.fetchall()
            else:
                # County or other non-cached geo — live query
                cur.execute(
                    f"SELECT audience, COUNT(*) AS cnt "
                    f"FROM nys_voter_tagging.voter_audience_bridge "
                    f"WHERE `{geo_col}` = %s "
                    f"GROUP BY audience ORDER BY audience",
                    [dist_val],
                )
                rows = cur.fetchall()
        audiences = [{"name": r[0], "count": int(r[1])} for r in rows]
        return JSONResponse({"audiences": audiences, "geo_label": geo_label})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()



@router.get("/enrichment-stats")
def enrichment_stats(current_user: User = Depends(require_voter_full)):
    """Return voter file enrichment coverage statistics."""
    # Serve from cache if fresh
    if _enrich_cache["data"] and (_time.time() - _enrich_cache["ts"]) < _ENRICH_CACHE_TTL:
        return JSONResponse(_enrich_cache["data"])
    env = _build_env()
    try:
        conn = _crm_connect(env, database="nys_voter_tagging",
                            read_timeout=120, connect_timeout=15)
    except Exception as exc:
        return JSONResponse({"error": f"DB connect failed: {exc}"}, status_code=500)

    try:
        cur = conn.cursor()

        # Detect optional columns
        cur.execute("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'nys_voter_tagging'
              AND TABLE_NAME   = 'voter_file'
              AND COLUMN_NAME IN (
                  'cfb_total_count','cfb_total_amt',
                  'is_national_donor','national_total_amount',
                  'national_democratic_amount','national_republican_amount',
                  'boe_total_count','boe_total_amt',
                  'is_new_registrant','is_party_switcher','registration_months',
                  'crm_email','ModeledEthnicity','GeneralRegularity','Latitude'
              )
        """)
        present = {r[0] for r in cur.fetchall()}

        def col(name, expr, fallback="0"):
            return expr if name in present else fallback

        # Core single-scan query
        query = """
            SELECT
                COUNT(*)                                                AS total,
                SUM(Gender = 'M')                                       AS male,
                SUM(Gender = 'F')                                       AS female,
                SUM(PrimaryPhone IS NOT NULL AND PrimaryPhone != '')    AS has_phone,
                SUM(Mobile IS NOT NULL AND Mobile != '')                AS has_mobile,
                SUM(Landline IS NOT NULL AND Landline != '')            AS has_landline,
                SUM(OfficialParty = 'Democrat')                         AS dem,
                SUM(OfficialParty = 'Republican')                       AS rep,
                SUM(OfficialParty = 'Conservative')                     AS conservative,
                SUM(OfficialParty NOT IN ('Democrat','Republican','Conservative')
                    AND OfficialParty IS NOT NULL AND OfficialParty != '') AS other_party,
                SUM(OfficialParty IS NULL OR OfficialParty = '')        AS no_party,
                {eth_col}                                               AS has_ethnicity,
                {boe_cnt}                                               AS boe_donors,
                {boe_amt}                                               AS boe_total_amt,
                {nat_cnt}                                               AS national_donors,
                {nat_amt}                                               AS national_total_amt,
                {nat_dem}                                               AS national_dem_amt,
                {nat_rep}                                               AS national_rep_amt,
                {cfb_cnt}                                               AS cfb_donors,
                {cfb_amt}                                               AS cfb_total_amt,
                {new_reg}                                               AS new_registrants,
                {switcher}                                              AS party_switchers,
                {reg_months}                                            AS avg_reg_months,
                {turnout_avg}                                           AS avg_turnout,
                {turnout_hi}                                            AS high_turnout,
                {turnout_lo}                                            AS low_turnout,
                {email_col}                                             AS has_email,
                COUNT(DISTINCT COALESCE(LDName,''))                     AS ld_count,
                COUNT(DISTINCT COALESCE(SDName,''))                     AS sd_count,
                COUNT(DISTINCT COALESCE(CDName,''))                     AS cd_count,
                COUNT(DISTINCT COALESCE(CountyName,''))                 AS county_count,
                {geocode}                                               AS has_geocode
            FROM voter_file
        """.format(
            eth_col    = col('ModeledEthnicity',
                             "SUM(ModeledEthnicity IS NOT NULL AND ModeledEthnicity NOT IN ('','Unknown'))"),
            boe_cnt    = col('boe_total_count', 'SUM(boe_total_count > 0)'),
            boe_amt    = col('boe_total_amt',   'COALESCE(SUM(boe_total_amt),0)'),
            nat_cnt    = col('is_national_donor','SUM(is_national_donor = 1)'),
            nat_amt    = col('national_total_amount','COALESCE(SUM(national_total_amount),0)'),
            nat_dem    = col('national_democratic_amount','COALESCE(SUM(national_democratic_amount),0)'),
            nat_rep    = col('national_republican_amount','COALESCE(SUM(national_republican_amount),0)'),
            cfb_cnt    = col('cfb_total_count','SUM(cfb_total_count > 0)'),
            cfb_amt    = col('cfb_total_amt','COALESCE(SUM(cfb_total_amt),0)'),
            new_reg    = col('is_new_registrant','SUM(is_new_registrant = 1)'),
            switcher   = col('is_party_switcher','SUM(is_party_switcher = 1)'),
            reg_months = col('registration_months','AVG(registration_months)','NULL'),
            turnout_avg= col('GeneralRegularity',
                             "AVG(CASE WHEN GeneralRegularity > '' THEN CAST(GeneralRegularity AS DECIMAL(5,4)) END)",
                             'NULL'),
            turnout_hi = col('GeneralRegularity',
                             "SUM(CASE WHEN GeneralRegularity > '' AND CAST(GeneralRegularity AS DECIMAL(5,4)) >= 0.6 THEN 1 END)"),
            turnout_lo = col('GeneralRegularity',
                             "SUM(CASE WHEN GeneralRegularity > '' AND CAST(GeneralRegularity AS DECIMAL(5,4)) <= 0.25 THEN 1 END)"),
            email_col  = col('crm_email','SUM(crm_email IS NOT NULL AND crm_email != "")'),
            geocode    = col('Latitude','SUM(Latitude IS NOT NULL AND Latitude != 0)'),
        )
        cur.execute(query)
        r = cur.fetchone()
        keys = [
            'total','male','female','has_phone','has_mobile','has_landline',
            'dem','rep','conservative','other_party','no_party',
            'has_ethnicity',
            'boe_donors','boe_total_amt',
            'national_donors','national_total_amt','national_dem_amt','national_rep_amt',
            'cfb_donors','cfb_total_amt',
            'new_registrants','party_switchers','avg_reg_months',
            'avg_turnout','high_turnout','low_turnout',
            'has_email',
            'ld_count','sd_count','cd_count','county_count','has_geocode',
        ]
        d = {k: (float(v) if v is not None else None) for k, v in zip(keys, r)}
        total = int(d['total'] or 1)

        # Ethnicity breakdown
        eth_broad   = {}
        eth_derived = {}
        BROAD = {'White / Caucasian','Hispanic / Latino','Black / African American',
                 'Asian / Pacific Islander','Other / Multi-Racial'}
        if 'ModeledEthnicity' in present:
            cur.execute("""
                SELECT ModeledEthnicity, COUNT(*) AS cnt
                FROM voter_file
                WHERE ModeledEthnicity IS NOT NULL
                  AND ModeledEthnicity NOT IN ('','Unknown')
                GROUP BY ModeledEthnicity ORDER BY cnt DESC
            """)
            for eth, cnt in cur.fetchall():
                if eth in BROAD:
                    eth_broad[eth] = int(cnt)
                else:
                    eth_derived[eth] = int(cnt)

        # Age range breakdown
        cur.execute("""
            SELECT AgeRange, COUNT(*) AS cnt
            FROM voter_file
            WHERE AgeRange IS NOT NULL AND AgeRange != ''
            GROUP BY AgeRange ORDER BY AgeRange
        """)
        age_ranges = [{"range": r2[0], "count": int(r2[1])} for r2 in cur.fetchall()]

        # Audience tags
        aud_tags   = 0
        aud_tagged = 0
        try:
            cur.execute("SELECT COUNT(DISTINCT audience), COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge")
            row2 = cur.fetchone()
            aud_tags   = int(row2[0] or 0)
            aud_tagged = int(row2[1] or 0)
        except Exception:
            pass

        conn.close()

        def pct(n):
            return round((n or 0) / total * 100, 1)

        payload = {
            "total":       total,
            "gender":      {"male": int(d['male'] or 0), "female": int(d['female'] or 0),
                            "unknown": total - int(d['male'] or 0) - int(d['female'] or 0)},
            "contact": {
                "phone":    int(d['has_phone'] or 0),   "phone_pct":    pct(d['has_phone']),
                "mobile":   int(d['has_mobile'] or 0),  "mobile_pct":   pct(d['has_mobile']),
                "landline": int(d['has_landline'] or 0),"landline_pct": pct(d['has_landline']),
                "email":    int(d['has_email'] or 0),   "email_pct":    pct(d['has_email']),
            },
            "party": {
                "dem":         int(d['dem'] or 0),          "dem_pct":         pct(d['dem']),
                "rep":         int(d['rep'] or 0),          "rep_pct":         pct(d['rep']),
                "conservative":int(d['conservative'] or 0), "conservative_pct":pct(d['conservative']),
                "other":       int(d['other_party'] or 0),  "other_pct":       pct(d['other_party']),
                "none":        int(d['no_party'] or 0),     "none_pct":        pct(d['no_party']),
            },
            "ethnicity": {
                "modeled":     int(d['has_ethnicity'] or 0), "modeled_pct": pct(d['has_ethnicity']),
                "broad":       eth_broad,
                "derived":     eth_derived,
            },
            "donors": {
                "boe":      {"count": int(d['boe_donors'] or 0),     "pct": pct(d['boe_donors']),
                             "total_amt": float(d['boe_total_amt'] or 0)},
                "national": {"count": int(d['national_donors'] or 0),"pct": pct(d['national_donors']),
                             "total_amt": float(d['national_total_amt'] or 0),
                             "dem_amt":   float(d['national_dem_amt'] or 0),
                             "rep_amt":   float(d['national_rep_amt'] or 0)},
                "cfb":      {"count": int(d['cfb_donors'] or 0),     "pct": pct(d['cfb_donors']),
                             "total_amt": float(d['cfb_total_amt'] or 0),
                             "available": 'cfb_total_count' in present},
            },
            "activity": {
                "avg_turnout":     round(d['avg_turnout'] * 100, 1) if d['avg_turnout'] is not None else None,
                "high_turnout":    int(d['high_turnout'] or 0), "high_turnout_pct": pct(d['high_turnout']),
                "low_turnout":     int(d['low_turnout'] or 0),  "low_turnout_pct":  pct(d['low_turnout']),
                "new_registrants": int(d['new_registrants'] or 0), "new_reg_pct": pct(d['new_registrants']),
                "party_switchers": int(d['party_switchers'] or 0), "switcher_pct": pct(d['party_switchers']),
                "avg_reg_years":   round(d['avg_reg_months'] / 12, 1) if d['avg_reg_months'] else None,
            },
            "geography": {
                "ld_count":     int(d['ld_count'] or 0),
                "sd_count":     int(d['sd_count'] or 0),
                "cd_count":     int(d['cd_count'] or 0),
                "county_count": int(d['county_count'] or 0),
                "geocoded":     int(d['has_geocode'] or 0), "geocoded_pct": pct(d['has_geocode']),
            },
            "audiences":  {"tags": aud_tags, "tagged_voters": aud_tagged, "tagged_pct": pct(aud_tagged)},
            "age_ranges": age_ranges,
        }
        _enrich_cache["data"] = payload
        _enrich_cache["ts"] = _time.time()
        return JSONResponse(payload)
    except Exception as exc:
        try: conn.close()
        except Exception: pass
        return JSONResponse({"error": str(exc)}, status_code=500)



@router.get("/export-unmatched")
def export_unmatched(current_user: User = Depends(require_voter_full)):
    """Download unmatched CRM contacts as a CSV file."""
    env = _build_env()

    def _generate():
        try:
            conn = _crm_connect(env)
            cur = conn.cursor()
            cur.execute("""
                SELECT id, email_1, first_name, last_name,
                       mobile, phone_1, address, city, state, zip5,
                       sources, clean_first, clean_last, created_at
                FROM contacts
                WHERE vf_state_voter_id IS NULL
                ORDER BY last_name, first_name
            """)
            headers = [
                "id", "email", "first_name", "last_name",
                "mobile", "phone", "address", "city", "state", "zip5",
                "sources", "clean_first", "clean_last", "created_at",
            ]
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(headers)
            yield buf.getvalue()

            for row in cur:
                buf = io.StringIO()
                csv.writer(buf).writerow(
                    ["" if v is None else str(v) for v in row]
                )
                yield buf.getvalue()
            conn.close()
        except Exception as exc:
            yield f"ERROR,{exc}\n"

    filename = f"unmatched_contacts_{date.today().isoformat()}.csv"
    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/data-status")
def voter_data_status(current_user: User = Depends(require_voter_access)):
    """Return structured file status for all donor source data files."""
    now = _time.time()

    def _file_info(p: Path) -> dict:
        if not p.exists():
            return {"name": p.name, "exists": False, "size_mb": None,
                    "age_seconds": None, "age_str": "missing", "mtime": None}
        if p.is_dir():
            files   = list(p.rglob("*"))
            total   = sum(f.stat().st_size for f in files if f.is_file())
            mtime   = max((f.stat().st_mtime for f in files if f.is_file()), default=p.stat().st_mtime)
            size_mb = round(total / 1_048_576, 1)
        else:
            stat    = p.stat()
            mtime   = stat.st_mtime
            size_mb = round(stat.st_size / 1_048_576, 1)
        age = now - mtime
        if age < 3600:      age_str = f"{int(age/60)}m ago"
        elif age < 86400:   age_str = f"{age/3600:.1f}h ago"
        elif age < 86400*7: age_str = f"{age/86400:.0f}d ago"
        else:
            age_str = datetime.fromtimestamp(mtime).strftime("%-d %b %Y")
        return {
            "name":        p.name,
            "exists":      True,
            "size_mb":     size_mb,
            "age_seconds": int(age),
            "age_str":     age_str,
            "mtime":       mtime,
        }

    _cur_year  = datetime.now().year
    _cur_cycle = _cur_year if _cur_year % 2 == 0 else _cur_year + 1
    fec_cycles = [_cur_cycle - (i * 2) for i in range(6)]

    boe_dir = VOTER_DIR / "data" / "boe_donors"
    fec_dir = VOTER_DIR / "data" / "fec_downloads"
    cfb_dir = VOTER_DIR / "data" / "cfb"

    groups = [
        {
            "label": "BOE State Campaign Finance (extracted)",
            "key":   "boe",
            "files": [_file_info(boe_dir / "extracted" / f) for f in [
                "STATE_CANDIDATE.csv",
                "COUNTY_CANDIDATE.csv",
                "STATE_COMMITTEE.csv",
                "COUNTY_COMMITTEE.csv",
                "COMMCAND.CSV",
            ]],
        },
        {
            "label": "National Donors — Federal Contributions (extracted)",
            "key":   "fec",
            "files": [_file_info(fec_dir / "extracted" / f"indiv{str(c)[-2:]}")
                      for c in fec_cycles],
        },
        {
            "label": "NYC Campaign Finance Board (CFB)",
            "key":   "cfb",
            "files": [_file_info(cfb_dir / f) for f in [
                "2017_Contributions.csv",
                "2021_Contributions.csv",
                "2023_Contributions.csv",
                "2025_Contributions.csv",
            ]],
        },
    ]

    # Audience CSV files
    audience_files = []
    if AUDIENCE_DIR.exists():
        for f in sorted(AUDIENCE_DIR.glob("*.csv")):
            if f.name.lower() == "fullnyvoter.csv":
                continue
            audience_files.append(_file_info(f))

    bridge_rows = bridge_voters = None
    try:
        env  = _build_env()
        conn = _crm_connect(env, database="nys_voter_tagging", connect_timeout=5, read_timeout=10)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge")
            row = cur.fetchone()
            if row:
                bridge_rows, bridge_voters = int(row[0]), int(row[1])
        conn.close()
    except Exception:
        pass

    return JSONResponse({
        "groups": groups,
        "audiences": audience_files,
        "bridge_rows": bridge_rows,
        "bridge_voters": bridge_voters,
    })


# ── Voter file chunked upload ──────────────────────────────────────────────────

ZIPPED_DIR = VOTER_DIR / "data" / "zipped"

@router.post("/voter-file-chunk")
async def voter_file_chunk(
    chunk:    UploadFile = File(...),
    offset:   int        = Form(...),
    filename: str        = Form(...),
    current_user: User   = Depends(require_user),
):
    """Receive one chunk of a voter file ZIP and write it at the correct offset."""
    ZIPPED_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = Path(filename).name          # strip any path traversal
    tmp       = ZIPPED_DIR / (safe_name + ".uploading")
    data      = await chunk.read()
    mode      = "r+b" if tmp.exists() and offset > 0 else "wb"
    with open(tmp, mode) as f:
        f.seek(offset)
        f.write(data)
    return JSONResponse({"ok": True, "offset": offset, "written": len(data)})


@router.post("/voter-file-finalize")
async def voter_file_finalize(
    request:      Request,
    current_user: User = Depends(require_voter_full),
):
    """Rename the completed .uploading temp file to its final name."""
    body     = await request.json()
    filename = Path(body["filename"]).name
    tmp      = ZIPPED_DIR / (filename + ".uploading")
    dst      = ZIPPED_DIR / filename
    if not tmp.exists():
        return JSONResponse({"ok": False, "error": "temp file not found"}, status_code=400)
    tmp.rename(dst)
    size_mb = round(dst.stat().st_size / 1_048_576, 1)
    return JSONResponse({"ok": True, "path": str(dst), "size_mb": size_mb})


@router.get("/voter-file-status")
def voter_file_status(current_user: User = Depends(require_voter_access)):
    """Return current voter ZIP files and voter_file row count."""
    zipped = ZIPPED_DIR if ZIPPED_DIR.exists() else None
    files  = []
    if zipped:
        for f in sorted(zipped.glob("*.zip")):
            stat = f.stat()
            age  = _time.time() - stat.st_mtime
            if age < 3600:      age_str = f"{int(age/60)}m ago"
            elif age < 86400:   age_str = f"{age/3600:.1f}h ago"
            elif age < 86400*7: age_str = f"{age/86400:.0f}d ago"
            else:
                age_str = datetime.fromtimestamp(stat.st_mtime).strftime("%-d %b %Y")
            files.append({"name": f.name, "size_mb": round(stat.st_size/1_048_576, 1), "age_str": age_str})
        # also report any in-progress uploads
        for f in sorted(zipped.glob("*.uploading")):
            stat = f.stat()
            files.append({"name": f.name + " (uploading…)", "size_mb": round(stat.st_size/1_048_576, 1), "age_str": "in progress"})

    row_count = None
    try:
        env = _build_env()
        conn = _crm_connect(env, database="nys_voter_tagging", connect_timeout=5, read_timeout=10)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM voter_file")
            row_count = cur.fetchone()[0]
        conn.close()
    except Exception:
        pass

    return JSONResponse({"files": files, "row_count": row_count})


AUDIENCE_DIR = VOTER_DIR / "data"


# ── Export helpers ───────────────────────────────────────────────────────────
VOTER_OUTPUT_DIR = VOTER_DIR / "output"

@router.get("/export-preview")
async def export_preview(
    type: str = "ld",
    value: str = "63",
    current_user: User = Depends(require_voter_access),
):
    """Return pre-export stats for a district (or statewide)."""
    col_map = {"ld": "LDName", "sd": "SDName", "cd": "CDName", "county": "CountyName"}
    col  = col_map.get(type.lower())
    where  = f"{col} = %s" if col else "1=1"
    params = (value,) if col else ()

    env  = _build_env()
    conn = _crm_connect(env, database="nys_voter_tagging", connect_timeout=10, read_timeout=60)
    try:
        with conn.cursor() as cur:
            # Check which optional columns exist
            cur.execute("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'nys_voter_tagging'
                  AND TABLE_NAME   = 'voter_file'
                  AND COLUMN_NAME IN ('crm_email', 'origin', 'ModeledEthnicity')
            """)
            existing_cols = {row[0] for row in cur.fetchall()}
            email_expr    = "SUM(crm_email IS NOT NULL)"   if "crm_email" in existing_cols else "0"
            audience_expr = "SUM(origin IS NOT NULL AND TRIM(origin) != '')" if "origin" in existing_cols else "0"

            # Total + party + phones + emails + audiences in one pass
            cur.execute(f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(OfficialParty = 'Democrat')     AS dem,
                    SUM(OfficialParty = 'Republican')   AS rep,
                    SUM(OfficialParty = 'Conservative') AS con,
                    SUM(OfficialParty = 'Working Families') AS wf,
                    SUM(OfficialParty NOT IN ('Democrat','Republican','Conservative','Working Families')
                        AND OfficialParty IS NOT NULL AND OfficialParty != '') AS other,
                    SUM(OfficialParty IS NULL OR OfficialParty = '') AS blank,
                    SUM(Mobile IS NOT NULL AND Mobile != '')                            AS mobile,
                    SUM(PrimaryPhone IS NOT NULL AND PrimaryPhone != '')                AS landline,
                    {email_expr}    AS emails,
                    {audience_expr} AS audiences
                FROM voter_file WHERE {where}
            """, params)
            r = cur.fetchone()
            total, dem, rep, con, wf, other, blank, mobile, landline, emails, audiences = [int(x or 0) for x in r]

            # Ethnicity breakdown (optional column)
            ethnicity = None
            if "ModeledEthnicity" in existing_cols:
                cur.execute(f"""
                    SELECT
                        SUM(ModeledEthnicity = 'White / Caucasian')         AS white,
                        SUM(ModeledEthnicity = 'Hispanic / Latino')         AS hispanic,
                        SUM(ModeledEthnicity = 'Black / African American')  AS black,
                        SUM(ModeledEthnicity = 'Asian / Pacific Islander')  AS asian,
                        SUM(ModeledEthnicity = 'Other / Multi-Racial')      AS multi,
                        SUM(ModeledEthnicity = 'Irish')                     AS irish,
                        SUM(ModeledEthnicity = 'Jewish')                    AS jewish,
                        SUM(ModeledEthnicity = 'Italian')                   AS italian,
                        SUM(ModeledEthnicity = 'South Asian')               AS s_asian,
                        SUM(ModeledEthnicity = 'Eastern European')          AS e_euro,
                        SUM(ModeledEthnicity = 'Middle Eastern')            AS m_east
                    FROM voter_file WHERE {where}
                """, params)
                er = cur.fetchone()
                ethnicity = {
                    "broad": {
                        "White":    int(er[0] or 0),
                        "Hispanic": int(er[1] or 0),
                        "Black":    int(er[2] or 0),
                        "Asian":    int(er[3] or 0),
                        "Other":    int(er[4] or 0),
                    },
                    "derived": {
                        "Irish":           int(er[5] or 0),
                        "Jewish":          int(er[6] or 0),
                        "Italian":         int(er[7] or 0),
                        "South Asian":     int(er[8] or 0),
                        "E. European":     int(er[9] or 0),
                        "Middle Eastern":  int(er[10] or 0),
                    },
                }

            # Issue audience breakdown
            audience_detail = None
            if "origin" in existing_cols:
                try:
                    cur.execute(f"""
                        SELECT origin, COUNT(*) as cnt
                        FROM voter_file
                        WHERE origin IS NOT NULL AND TRIM(origin) != ''
                          AND {where}
                        GROUP BY origin
                        ORDER BY cnt DESC
                        LIMIT 20
                    """, params)
                    rows = cur.fetchall()
                    audience_detail = [{"name": r[0], "count": int(r[1])} for r in rows]
                except Exception:
                    audience_detail = None

            # Donor breakdowns (separate — need column existence checks)
            donors = {}
            for key, col_chk, amt_col, flag_col in [
                ("boe",      "boe_total_amt",          "boe_total_amt",        None),
                ("national", "national_total_amount",  "national_total_amount", "is_national_donor"),
                ("cfb",      "cfb_total_amt",          "cfb_total_amt",        None),
            ]:
                try:
                    if flag_col:
                        cur.execute(f"SELECT COUNT(*), COALESCE(SUM({amt_col}),0) FROM voter_file WHERE {where} AND {flag_col}=1", params)
                    else:
                        cur.execute(f"SELECT COUNT(*), COALESCE(SUM({amt_col}),0) FROM voter_file WHERE {where} AND {col_chk}>0", params)
                    cnt, tot = cur.fetchone()
                    donors[key] = {"count": int(cnt or 0), "total": float(tot or 0)}
                except Exception:
                    donors[key] = None

        return JSONResponse({
            "total":            total,
            "parties":          {"Democrat": dem, "Republican": rep, "Conservative": con,
                                 "Working Families": wf, "Other": other, "Blank/Unknown": blank},
            "donors":           donors,
            "mobile":           mobile,
            "landline":         landline,
            "emails":           emails,
            "audiences":        audiences,
            "ethnicity":        ethnicity,
            "audience_detail":  audience_detail,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()



@router.get("/district-list")
async def district_list(
    type: str = "ld",
    current_user: User = Depends(require_voter_access),
):
    """Return sorted district values from the district_values lookup table.

    This table is rebuilt by rebuild_district_values.py after every voter file
    load, so queries are always against a tiny (< 300 row) table — instant,
    no caching needed.
    """
    key = type.lower()
    valid = {"ld", "sd", "cd", "county"}
    if key not in valid:
        return JSONResponse({"values": []})
    order = "CAST(value AS UNSIGNED)" if key != "county" else "value"
    env = _build_env()
    conn = _crm_connect(env, database="nys_voter_tagging", connect_timeout=5, read_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM district_values WHERE type = %s ORDER BY " + order,
                (key,)
            )
            values = [str(r[0]) for r in cur.fetchall()]
        conn.close()
    except Exception as exc:
        values = []
    return JSONResponse({"values": values})

@router.get("/export-reports")
async def export_reports(current_user: User = Depends(require_voter_access)):
    """List generated Excel reports in the output directory."""
    reports = []
    if VOTER_OUTPUT_DIR.exists():
        for f in VOTER_OUTPUT_DIR.rglob("*.xlsx"):
            stat = f.stat()
            reports.append({
                "name":     f.name,
                "rel_path": f.relative_to(VOTER_OUTPUT_DIR).as_posix(),
                "size_mb":  round(stat.st_size / 1_048_576, 1),
                "created":  datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "district": f.parent.name,
            })
    return JSONResponse(sorted(reports, key=lambda x: x["created"], reverse=True))


@router.get("/export-reports/download")
async def download_report(path: str, current_user: User = Depends(require_voter_access)):
    """Serve a generated Excel report as a file download."""
    base = VOTER_OUTPUT_DIR.resolve()
    target = (base / path).resolve()
    if not str(target).startswith(str(base)):
        raise HTTPException(403, "Access denied")
    if not target.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(
        str(target),
        filename=target.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.delete("/export-reports")
async def delete_report(path: str, current_user: User = Depends(require_voter_access)):
    """Delete a generated Excel report."""
    base = VOTER_OUTPUT_DIR.resolve()
    target = (base / path).resolve()
    if not str(target).startswith(str(base)):
        raise HTTPException(403, "Access denied")
    if not target.exists():
        raise HTTPException(404, "File not found")
    target.unlink()
    return {"ok": True}


ALLOWED_CMDS = frozenset({
    "status", "pipeline", "export", "donors", "hubspot-sync", "cm-sync",
    "crm-sync", "crm-enrich", "crm-phone", "crm-extended-match",
    "ethnicity", "enrich-derived", "district-scores", "party-snapshot",
    "fb-audiences", "fb-push", "reset", "sync",
    "voter-file-load", "voter-contact",
    # BOE individual steps
    "boe-download", "boe-load", "boe-enrich", "boe-enrich-only",
    # National (FEC) individual steps
    "fec-download", "fec-load", "fec-enrich", "national-enrich",
    # CFB individual steps
    "cfb-download", "cfb-load", "cfb-enrich",
})


@router.get("/audience-status")
def audience_status(current_user: User = Depends(require_voter_access)):
    """List audience CSV files on disk and bridge row/voter counts from DB."""
    files = []
    if AUDIENCE_DIR.exists():
        for f in sorted(AUDIENCE_DIR.glob("*.csv")):
            if f.name.lower() == "fullnyvoter.csv":
                continue
            stat = f.stat()
            age  = _time.time() - stat.st_mtime
            if age < 3600:      age_str = f"{int(age/60)}m ago"
            elif age < 86400:   age_str = f"{age/3600:.1f}h ago"
            elif age < 86400*7: age_str = f"{age/86400:.0f}d ago"
            else:               age_str = datetime.fromtimestamp(stat.st_mtime).strftime("%-d %b %Y")
            files.append({
                "name":    f.name,
                "size_mb": round(stat.st_size / 1_048_576, 2),
                "age_str": age_str,
            })

    bridge_rows = None
    bridge_voters = None
    try:
        env  = _build_env()
        conn = _crm_connect(env, database="nys_voter_tagging", connect_timeout=5, read_timeout=10)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT StateVoterId) FROM voter_audience_bridge")
            row = cur.fetchone()
            if row:
                bridge_rows, bridge_voters = int(row[0]), int(row[1])
        conn.close()
    except Exception:
        pass

    return JSONResponse({"files": files, "bridge_rows": bridge_rows, "bridge_voters": bridge_voters})


@router.post("/audience-upload")
async def audience_upload(
    file: UploadFile = File(...),
    current_user: User = Depends(require_user),
):
    """Upload an audience CSV (or ZIP of CSVs) to the data/ directory.

    Every CSV must contain a StateVoterId (SBOE ID) column — files using
    name/phone matching only cannot be loaded into voter_audience_bridge and
    are rejected.  ZIPs are extracted in memory; only the CSVs inside are
    saved; the ZIP itself is never written to disk.
    """
    name = Path(file.filename).name
    is_zip = name.lower().endswith(".zip")
    is_csv = name.lower().endswith(".csv")

    if not is_csv and not is_zip:
        return JSONResponse(
            {"ok": False, "error": "Only .csv or .zip files are allowed"},
            status_code=400,
        )
    if name.lower() == "fullnyvoter.csv":
        return JSONResponse({"ok": False, "error": "Cannot overwrite the voter file"}, status_code=400)

    # read the entire upload into memory (needed for zip extraction)
    raw = await file.read()

    def _validate_and_save(csv_name: str, csv_bytes: bytes) -> dict:
        safe_name = Path(csv_name).name
        if safe_name.lower() == "fullnyvoter.csv":
            return {"name": safe_name, "ok": False, "error": "Cannot overwrite the voter file"}
        try:
            header_line = csv_bytes[:4096].decode("utf-8-sig", errors="replace").splitlines()[0]
            columns = [c.strip() for c in next(csv.reader(io.StringIO(header_line)))]
        except Exception:
            columns = []
        if "StateVoterId" not in columns:
            return {
                "name": safe_name,
                "ok": False,
                "error": (
                    f"{safe_name}: Missing StateVoterId (SBOE ID) column. "
                    "Audience CSVs must contain a StateVoterId column."
                ),
            }
        AUDIENCE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = AUDIENCE_DIR / (safe_name + ".uploading")
        dst = AUDIENCE_DIR / safe_name
        try:
            tmp.write_bytes(csv_bytes)
            tmp.rename(dst)
        except Exception as e:
            tmp.unlink(missing_ok=True)
            return {"name": safe_name, "ok": False, "error": str(e)}
        return {"name": safe_name, "ok": True, "size_mb": round(dst.stat().st_size / 1_048_576, 2)}

    if is_zip:
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile:
            return JSONResponse({"ok": False, "error": "Invalid or corrupt ZIP file"}, status_code=400)
        csv_members = [m for m in zf.namelist() if m.lower().endswith(".csv") and not m.startswith("__MACOSX")]
        if not csv_members:
            return JSONResponse({"ok": False, "error": "ZIP contains no .csv files"}, status_code=400)
        results = []
        for member in csv_members:
            csv_bytes = zf.read(member)
            csv_name = Path(member).name
            results.append(_validate_and_save(csv_name, csv_bytes))
        zf.close()
        failed = [r for r in results if not r["ok"]]
        if failed:
            for r in results:
                if r["ok"]:
                    (AUDIENCE_DIR / r["name"]).unlink(missing_ok=True)
            return JSONResponse(
                {"ok": False, "error": failed[0]["error"], "results": results},
                status_code=400,
            )
        total_mb = round(sum(r["size_mb"] for r in results), 2)
        return JSONResponse({
            "ok": True,
            "zip": True,
            "files": results,
            "total_files": len(results),
            "total_mb": total_mb,
        })

    result = _validate_and_save(name, raw)
    if not result["ok"]:
        return JSONResponse({"ok": False, "error": result["error"]}, status_code=400)
    return JSONResponse({"ok": True, "name": result["name"], "size_mb": result["size_mb"]})



@router.delete("/audience/{filename}")
def audience_delete(
    filename: str,
    current_user: User = Depends(require_user),
):
    """Delete an audience CSV from the data/ directory."""
    # Prevent path traversal
    safe = Path(filename).name
    if not safe.lower().endswith(".csv") or safe.lower() == "fullnyvoter.csv":
        return JSONResponse({"ok": False, "error": "Invalid filename"}, status_code=400)
    target = AUDIENCE_DIR / safe
    if not target.exists():
        return JSONResponse({"ok": False, "error": "File not found"}, status_code=404)
    target.unlink()
    return JSONResponse({"ok": True})


@router.post("/run")
async def voter_run_task(
    cmd:      str = Form(...),
    extra:    str = Form(""),
    current_user: User = Depends(require_voter_access),
):
    """Start a pipeline command as a background task. Returns task ID."""
    # export_viewers may only run export commands
    EXPORT_VIEWER_CMDS = frozenset({"export", "status"})
    if _is_export_viewer(current_user) and cmd not in EXPORT_VIEWER_CMDS:
        return JSONResponse({"error": "Access denied for your role."}, status_code=403)
    if cmd not in ALLOWED_CMDS:
        return JSONResponse({"error": f"invalid command: {cmd}"}, status_code=400)

    # Only one task at a time
    for tid, t in list(_tasks.items()):
        if t["proc"].poll() is None:
            return JSONResponse({
                "error": f"A task is already running: {t['cmd']}",
                "task_id": tid,
            }, status_code=409)

    args = [sys.executable, "-u", str(VOTER_DIR / "main.py"), cmd]
    if extra:
        args += shlex.split(extra)

    task_id = uuid.uuid4().hex[:12]
    logfile = _TASK_DIR / f"{task_id}.log"

    fh = open(logfile, "w", buffering=1)
    proc = subprocess.Popen(
        args,
        cwd=str(VOTER_DIR),
        env=_build_env(),
        stdout=fh,
        stderr=subprocess.STDOUT,
    )
    _tasks[task_id] = {
        "proc": proc, "cmd": cmd, "extra": extra,
        "started": _time.time(), "logfile": logfile, "fh": fh,
    }

    return JSONResponse({"task_id": task_id, "cmd": cmd})


@router.post("/task/{task_id}/kill")
async def task_kill(
    task_id: str,
    current_user: User = Depends(require_voter_access),
):
    """Terminate a running background task."""
    task = _tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "unknown task"}, status_code=404)
    proc = task["proc"]
    if proc.poll() is not None:
        return JSONResponse({"ok": False, "msg": "Task already finished"})
    proc.terminate()
    # Give it 3s to exit cleanly, then force-kill
    for _ in range(6):
        await asyncio.sleep(0.5)
        if proc.poll() is not None:
            break
    else:
        proc.kill()
    task["fh"].close()
    return JSONResponse({"ok": True, "msg": "Task terminated"})


@router.get("/task/{task_id}/output")
async def task_output(
    task_id: str,
    offset:  int = 0,
    current_user: User = Depends(require_voter_access),
):
    """Poll for new output from a running task. Returns text from offset."""
    task = _tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "unknown task"}, status_code=404)

    logfile = task["logfile"]
    if not logfile.exists():
        return JSONResponse({"text": "", "offset": 0, "running": True})

    with open(logfile, "r", errors="replace") as f:
        f.seek(offset)
        text = f.read()
        new_offset = f.tell()

    proc = task["proc"]
    rc = proc.poll()
    running = rc is None

    result = {"text": text, "offset": new_offset, "running": running}
    if not running:
        result["exit_code"] = rc
        # Clean up file handle
        task["fh"].close()
    return JSONResponse(result)
