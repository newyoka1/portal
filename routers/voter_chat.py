"""Voter Chat — Claude-powered natural language interface to the voter file."""
import json
import logging
import os
import time

import pymysql
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse

from auth import require_user
from models import User
import portal_config

router = APIRouter(prefix="/voter-pipeline/chat", tags=["voter-chat"])

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
_DB_NAME = "nys_voter_tagging"


def _voter_connect() -> pymysql.Connection:
    """Open a read-only-safe connection to the voter DB."""
    env = {}
    if time.time() - portal_config._cache_ts > portal_config._CACHE_TTL:
        portal_config._refresh_cache()
    env.update(portal_config._cache)
    for k, v in os.environ.items():
        if k not in env or not env[k]:
            env[k] = v
    return pymysql.connect(
        host=env.get("MYSQL_HOST", env.get("DB_HOST", "127.0.0.1")),
        port=int(env.get("MYSQL_PORT", env.get("DB_PORT", "3306"))),
        user=env.get("MYSQL_USER", env.get("DB_USER", "root")),
        password=env.get("MYSQL_PASSWORD", env.get("DB_PASSWORD", "")),
        database=_DB_NAME,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=60,
        autocommit=True,
    )


def _get_schema() -> str:
    """Fetch voter_file column names and types from INFORMATION_SCHEMA."""
    conn = _voter_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, COLUMN_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'voter_file' "
                "ORDER BY ORDINAL_POSITION",
                (_DB_NAME,),
            )
            cols = cur.fetchall()

            # Also grab a few related tables for context
            cur.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME != 'voter_file'",
                (_DB_NAME,),
            )
            other_tables = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    lines = [f"  {name} {dtype}" for name, dtype in cols]
    schema = "TABLE voter_file (\n" + ",\n".join(lines) + "\n)"

    if other_tables:
        schema += f"\n\nOther tables in database: {', '.join(other_tables)}"

    return schema


# Cache schema for 10 minutes
_schema_cache = {"text": "", "ts": 0}
_SCHEMA_TTL = 600


def _cached_schema() -> str:
    if not _schema_cache["text"] or (time.time() - _schema_cache["ts"]) > _SCHEMA_TTL:
        _schema_cache["text"] = _get_schema()
        _schema_cache["ts"] = time.time()
    return _schema_cache["text"]


# ---------------------------------------------------------------------------
# Claude API
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
You are a data analyst assistant for a New York State voter file database.
The database contains ~12.7 million voter records with detailed demographics,
voting history, donor information, and contact details.

DATABASE SCHEMA:
{schema}

RULES:
1. Generate MySQL-compatible SQL queries to answer the user's question.
2. ALWAYS use SELECT — never INSERT, UPDATE, DELETE, DROP, ALTER, or any DDL/DML.
3. ALWAYS add LIMIT 500 to queries returning rows (not aggregates).
4. For large counts, use COUNT(*), SUM(), AVG(), GROUP BY — prefer aggregates over raw rows.
5. Common columns:
   - Districts: CDName (congressional), SDName (senate), ADName (assembly), CountyName
   - Demographics: OfficialParty, Age, AgeRange, Gender, StateEthnicity, ModeledEthnicity
   - Voting: GeneralFrequency, PrimaryFrequency, RegistrationStatus, LastVoterActivity
   - Contact: PrimaryPhone, Mobile, Landline (plus TRC/DNC flags)
   - Donors: boe_total_D_amt, boe_total_R_amt, is_national_donor, national_total_amount
   - Geography: PrimaryCity, PrimaryAddress1, PrimaryZip, Latitude, Longitude
6. District values look like: "SD 15", "CD 12", "AD 36" — include the prefix + space.
7. Respond with a brief explanation of what you're querying, then the SQL, then interpret the results.
8. If the user's question is ambiguous, make reasonable assumptions and state them.
9. Format numbers with commas for readability.
10. If the query returns tabular data, format it as a Markdown table.

Respond ONLY with valid JSON in this format:
{{"explanation": "Brief description of what we're looking for", "sql": "SELECT ...", "error": null}}

If you cannot produce a valid query, respond with:
{{"explanation": null, "sql": null, "error": "Reason why"}}
"""


def _call_claude(messages: list[dict], schema: str) -> dict:
    """Call Claude API and return parsed JSON response."""
    import anthropic

    api_key = portal_config.get_setting("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"explanation": None, "sql": None, "error": "ANTHROPIC_API_KEY not configured. Add it in Settings."}

    client = anthropic.Anthropic(api_key=api_key)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=SYSTEM_PROMPT.format(schema=schema),
        messages=messages,
    )

    text = response.content[0].text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    return json.loads(text)


def _execute_sql(sql: str) -> dict:
    """Execute a read-only SQL query and return results."""
    sql_upper = sql.strip().upper()
    # Safety: block anything that isn't a SELECT
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        return {"error": "Only SELECT queries are allowed."}

    blocked = ["INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE ",
               "TRUNCATE ", "REPLACE ", "GRANT ", "REVOKE "]
    for kw in blocked:
        if kw in sql_upper:
            return {"error": f"Blocked keyword: {kw.strip()}"}

    # Enforce LIMIT
    if "LIMIT" not in sql_upper:
        sql = sql.rstrip(";") + " LIMIT 500"

    conn = _voter_connect()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return {"columns": columns, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Conversation memory (per-session, last N turns)
# ---------------------------------------------------------------------------
_MAX_HISTORY = 10
_conversations: dict[str, list] = {}  # session_id → messages


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.post("/ask")
def chat_ask(
    request: Request,
    message: str = Form(...),
    current_user: User = Depends(require_user),
):
    if not (current_user.is_admin or current_user.voter_role in ("full",)):
        return JSONResponse({"error": "Access denied"}, status_code=403)

    session_id = str(current_user.id)
    if session_id not in _conversations:
        _conversations[session_id] = []

    history = _conversations[session_id]
    history.append({"role": "user", "content": message})

    # Keep only last N turns
    if len(history) > _MAX_HISTORY * 2:
        history[:] = history[-_MAX_HISTORY * 2:]

    try:
        schema = _cached_schema()
        claude_resp = _call_claude(history, schema)
    except json.JSONDecodeError:
        return JSONResponse({"reply": "I had trouble parsing the response. Please try rephrasing your question.", "sql": None, "data": None})
    except Exception as e:
        log.exception("Claude API error")
        return JSONResponse({"reply": f"API error: {e}", "sql": None, "data": None})

    if claude_resp.get("error"):
        reply = claude_resp["error"]
        history.append({"role": "assistant", "content": json.dumps(claude_resp)})
        return JSONResponse({"reply": reply, "sql": None, "data": None})

    sql = claude_resp.get("sql")
    explanation = claude_resp.get("explanation", "")

    if not sql:
        history.append({"role": "assistant", "content": json.dumps(claude_resp)})
        return JSONResponse({"reply": explanation or "I couldn't generate a query for that.", "sql": None, "data": None})

    # Execute the query
    result = _execute_sql(sql)

    if result.get("error"):
        # Let Claude retry with the error
        history.append({"role": "assistant", "content": json.dumps(claude_resp)})
        history.append({"role": "user", "content": f"That SQL returned an error: {result['error']}. Please fix the query."})

        try:
            claude_resp2 = _call_claude(history, schema)
            sql2 = claude_resp2.get("sql")
            if sql2:
                result = _execute_sql(sql2)
                if not result.get("error"):
                    sql = sql2
                    explanation = claude_resp2.get("explanation", explanation)
        except Exception:
            pass

        if result.get("error"):
            reply = f"{explanation}\n\nSQL error: {result['error']}"
            history.append({"role": "assistant", "content": reply})
            return JSONResponse({"reply": reply, "sql": sql, "data": None})

    # Build response with data
    history.append({"role": "assistant", "content": json.dumps({"explanation": explanation, "sql": sql, "row_count": result["count"]})})

    return JSONResponse({
        "reply": explanation,
        "sql": sql,
        "data": {
            "columns": result["columns"],
            "rows": _serialize_rows(result["rows"]),
            "count": result["count"],
        },
    })


@router.post("/clear")
def chat_clear(current_user: User = Depends(require_user)):
    session_id = str(current_user.id)
    _conversations.pop(session_id, None)
    return JSONResponse({"ok": True})


def _serialize_rows(rows: list[dict]) -> list[dict]:
    """Ensure all values are JSON-serializable."""
    import decimal
    from datetime import date, datetime

    out = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if isinstance(v, decimal.Decimal):
                clean[k] = float(v)
            elif isinstance(v, (date, datetime)):
                clean[k] = v.isoformat()
            elif isinstance(v, bytes):
                clean[k] = v.decode("utf-8", errors="replace")
            else:
                clean[k] = v
        out.append(clean)
    return out
