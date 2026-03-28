"""Check crm_unified schema."""
import sys, traceback
sys.path.insert(0, r"D:\git\nys-voter-pipeline")

OUT = r"D:\git\nys-voter-pipeline\_crm_schema_out.txt"

lines = []
try:
    from utils.db import get_conn
    lines.append("import OK")
    conn = get_conn('crm_unified')
    lines.append("connected OK")
    cur  = conn.cursor()

    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    lines.append(f"Tables: {tables}")

    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM `{t}`")
        n = cur.fetchone()[0]
        lines.append(f"  {t}: {n:,} rows")

    lines.append("")
    cur.execute("SHOW COLUMNS FROM contacts")
    for row in cur.fetchall():
        lines.append(f"  col: {row[0]}  type: {row[1]}")

    cur.execute("SELECT id, first_name, last_name, email, source, state_voter_id FROM contacts WHERE email IS NOT NULL AND email != '' LIMIT 5")
    for r in cur.fetchall():
        lines.append(f"  sample: {r}")

    conn.close()
except Exception:
    lines.append(traceback.format_exc())

with open(OUT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
