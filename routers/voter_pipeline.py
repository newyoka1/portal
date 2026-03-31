"""Voter Pipeline — CRM sync, Aiven sync, export (cloud-safe ops only)."""
import csv
import io
import os
import sys
import time as _time
from datetime import date, datetime
from pathlib import Path

import pymysql
from fastapi import APIRouter, Depends, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User
import portal_config

router = APIRouter(prefix="/voter-pipeline")
templates = Jinja2Templates(directory="templates")

PORTAL_DIR  = Path(__file__).parent.parent
VOTER_DIR   = PORTAL_DIR / "voter_pipeline"


def _crm_connect(env: dict, **overrides) -> pymysql.Connection:
    """Open a PyMySQL connection to crm_unified (or another db via overrides)."""
    return pymysql.connect(
        host=env.get("MYSQL_HOST", env.get("DB_HOST", "127.0.0.1")),
        port=int(env.get("MYSQL_PORT", env.get("DB_PORT", "3306"))),
        user=env.get("MYSQL_USER", env.get("DB_USER", "root")),
        password=env.get("MYSQL_PASSWORD", env.get("DB_PASSWORD", "")),
        database="crm_unified",
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=30,
        autocommit=True,
        **overrides,
    )


@router.get("", response_class=HTMLResponse)
def voter_pipeline_page(
    request: Request,
    current_user: User = Depends(require_user),
):
    return templates.TemplateResponse(request, "voter_pipeline.html", {
        "current_user":        current_user,
        "voter_dir_exists":    VOTER_DIR.exists(),
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
    if time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    env = os.environ.copy()
    for key, val in portal_config._cache.items():
        if val:
            env[key] = val
    return env


@router.get("/stats")
def voter_stats(current_user: User = Depends(require_user)):
    """Return CRM pipeline health statistics as JSON."""
    env = _build_env()
    try:
        conn = _crm_connect(env)
    except Exception as exc:
        return JSONResponse({"error": f"DB connect failed: {exc}"}, status_code=500)

    try:
        cur = conn.cursor()

        # Total + matched
        cur.execute(
            "SELECT COUNT(*), SUM(vf_state_voter_id IS NOT NULL) FROM contacts"
        )
        total, matched = cur.fetchone()
        total   = int(total   or 0)
        matched = int(matched or 0)

        # Unmatched breakdown
        cur.execute("""
            SELECT
                SUM(vf_state_voter_id IS NULL
                    AND zip5     IS NOT NULL AND zip5     != ''
                    AND clean_last IS NOT NULL AND clean_last != ''),
                SUM(vf_state_voter_id IS NULL
                    AND (zip5 IS NULL OR zip5 = '')),
                SUM(vf_state_voter_id IS NULL
                    AND mobile IS NOT NULL AND mobile != '')
            FROM contacts
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


@router.get("/export-unmatched")
def export_unmatched(current_user: User = Depends(require_user)):
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
def voter_data_status(current_user: User = Depends(require_user)):
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

    return JSONResponse({"groups": groups})


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
    current_user: User = Depends(require_user),
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
def voter_file_status(current_user: User = Depends(require_user)):
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


ALLOWED_CMDS = frozenset({
    "status", "pipeline", "export", "donors", "hubspot-sync", "cm-sync",
    "crm-sync", "crm-enrich", "crm-phone", "ethnicity", "fb-audiences",
    "fb-push", "reset", "sync",
})


@router.post("/run")
async def voter_run_stream(
    cmd:      str = Form(...),
    extra:    str = Form(""),
    current_user: User = Depends(require_user),
):
    """Stream output for any voter-pipeline main.py subcommand."""
    if cmd not in ALLOWED_CMDS:
        return JSONResponse({"error": f"invalid command: {cmd}"}, status_code=400)
    args = [sys.executable, str(VOTER_DIR / "main.py"), cmd]
    if extra:
        args += extra.split()   # e.g. "--ld 63" or "--full"

    return StreamingResponse(
        _stream(args, str(VOTER_DIR), _build_env()),
        media_type="text/plain",
    )


async def _stream(args: list[str], cwd: str, env: dict | None = None):
    import asyncio
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # Read in chunks (not lines) so \r progress prints flush through nginx
        # without waiting for a \n — avoids proxy_read_timeout on long syncs.
        while True:
            chunk = await proc.stdout.read(4096)
            if not chunk:
                break
            yield chunk.decode("utf-8", errors="replace")
        await proc.wait()
        yield f"\n[Exit code: {proc.returncode}]\n"
    except Exception as exc:
        yield f"\n[Error starting process: {exc}]\n"
