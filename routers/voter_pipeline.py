"""Voter Pipeline — CRM sync, Aiven sync, export (cloud-safe ops only)."""
import os
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from auth import require_user
from models import User
import portal_config

router = APIRouter(prefix="/voter-pipeline")
templates = Jinja2Templates(directory="templates")

PORTAL_DIR  = Path(__file__).parent.parent
VOTER_DIR   = PORTAL_DIR / "voter_pipeline"


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
    import pymysql
    from fastapi.responses import JSONResponse as _JSONResponse
    env = _build_env()
    try:
        conn = pymysql.connect(
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
    except Exception as exc:
        from fastapi.responses import JSONResponse as _JSONResponse
        return _JSONResponse({"error": f"DB connect failed: {exc}"}, status_code=500)

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

        from fastapi.responses import JSONResponse as _JSONResponse
        return _JSONResponse({
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
        from fastapi.responses import JSONResponse as _JSONResponse
        return _JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/export-unmatched")
def export_unmatched(current_user: User = Depends(require_user)):
    """Download unmatched CRM contacts as a CSV file."""
    import csv
    import io
    import pymysql
    from datetime import date
    env = _build_env()

    def _generate():
        try:
            conn = pymysql.connect(
                host=env.get("MYSQL_HOST", env.get("DB_HOST", "127.0.0.1")),
                port=int(env.get("MYSQL_PORT", env.get("DB_PORT", "3306"))),
                user=env.get("MYSQL_USER", env.get("DB_USER", "root")),
                password=env.get("MYSQL_PASSWORD", env.get("DB_PASSWORD", "")),
                database="crm_unified",
                charset="utf8mb4",
                autocommit=True,
            )
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

    from datetime import date
    filename = f"unmatched_contacts_{date.today().isoformat()}.csv"
    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/data-status")
def voter_data_status(current_user: User = Depends(require_user)):
    """Return structured file status for all donor source data files."""
    import time
    from fastapi.responses import JSONResponse as _JSONResponse

    now = time.time()

    def _file_info(p: Path) -> dict:
        if not p.exists():
            return {"name": p.name, "exists": False, "size_mb": None,
                    "age_seconds": None, "age_str": "missing", "mtime": None}
        stat = p.stat()
        age  = now - stat.st_mtime
        if age < 3600:    age_str = f"{int(age/60)}m ago"
        elif age < 86400: age_str = f"{age/3600:.1f}h ago"
        elif age < 86400*7: age_str = f"{age/86400:.0f}d ago"
        else:
            from datetime import datetime as _dt
            age_str = _dt.fromtimestamp(stat.st_mtime).strftime("%-d %b %Y")
        return {
            "name":       p.name,
            "exists":     True,
            "size_mb":    round(stat.st_size / 1_048_576, 1),
            "age_seconds": int(age),
            "age_str":    age_str,
            "mtime":      stat.st_mtime,
        }

    # Reconstruct the same file lists as voter_pipeline/main.py
    import datetime as _datetime
    _cur_year  = _datetime.datetime.now().year
    _cur_cycle = _cur_year if _cur_year % 2 == 0 else _cur_year + 1
    fec_cycles = [_cur_cycle - (i * 2) for i in range(6)]

    boe_dir = VOTER_DIR / "data" / "boe_donors"
    fec_dir = VOTER_DIR / "data" / "fec_downloads"
    cfb_dir = VOTER_DIR / "data" / "cfb"

    groups = [
        {
            "label": "BOE State Campaign Finance",
            "key":   "boe",
            "files": [_file_info(boe_dir / f) for f in [
                "ALL_REPORTS_StateCandidate.zip",
                "ALL_REPORTS_CountyCandidate.zip",
                "ALL_REPORTS_StateCommittee.zip",
                "ALL_REPORTS_CountyCommittee.zip",
            ]],
        },
        {
            "label": "FEC Federal Contributions",
            "key":   "fec",
            "files": [_file_info(fec_dir / f"indiv{str(c)[-2:]}.zip")
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

    return _JSONResponse({"groups": groups})


@router.post("/run")
async def voter_run_stream(
    cmd:      str = Form(...),
    extra:    str = Form(""),
    current_user: User = Depends(require_user),
):
    """Stream output for any voter-pipeline main.py subcommand."""
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
