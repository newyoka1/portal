import subprocess, sys
from pathlib import Path

GIT  = r"C:\Program Files\Git\cmd\git.exe"
BASE = Path(r"D:\git\nys-voter-pipeline")

lines = []

# Git log
r = subprocess.run([GIT, "-C", str(BASE), "log", "--oneline", "-10"],
                   capture_output=True, text=True)
lines.append("=== Git log (last 10) ===")
lines.append(r.stdout.strip())

# Git status
r2 = subprocess.run([GIT, "-C", str(BASE), "status", "-sb"],
                    capture_output=True, text=True)
lines.append("")
lines.append("=== Git status ===")
lines.append(r2.stdout.strip())

# crm_unified.contacts vf_* columns
sys.path.insert(0, str(BASE))
from utils.db import get_conn
conn = get_conn('crm_unified')
cur  = conn.cursor()

cur.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='crm_unified' AND TABLE_NAME='contacts'
    AND COLUMN_NAME LIKE 'vf_%'
    ORDER BY ORDINAL_POSITION
""")
vf_cols = [r[0] for r in cur.fetchall()]

lines.append("")
lines.append(f"=== vf_* columns on crm_unified.contacts ({len(vf_cols)}) ===")
if vf_cols:
    cur.execute("SELECT COUNT(*) FROM contacts WHERE vf_state_voter_id IS NOT NULL")
    matched = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM contacts WHERE vf_enriched_at IS NOT NULL")
    processed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM contacts")
    total = cur.fetchone()[0]
    lines.append(f"  Total contacts:    {total:,}")
    lines.append(f"  Processed:         {processed:,}")
    lines.append(f"  Matched to voter:  {matched:,} ({matched/total*100:.1f}%)")
else:
    lines.append("  No vf_* columns found — crm-enrich has NOT run yet")

conn.close()

out = "\n".join(lines)
with open(r"D:\git\nys-voter-pipeline\_session_state_out.txt", "w", encoding="utf-8") as fh:
    fh.write(out + "\n")
