#!/usr/bin/env python3
"""One-time cleanup script. Run: python _cleanup.py"""
import os, shutil
from pathlib import Path

BASE = Path(r"D:\git\nys-voter-pipeline")
ARCHIVE = BASE / "archive" / "2026-cleanup"
ARCHIVE.mkdir(parents=True, exist_ok=True)

ARCHIVE_FILES = [
    "check_boe_db.py", "check_donor_status.py", "check_donor_status2.py",
    "check_emails.py", "find_boe_donors.py", "schema_check.py",
    "vf_schema_check.py", "load_boe_data.py", "reclassify_party.py",
    "mills_campaign_donors.py", "mills_donor_analysis.py", "mills_donor_lookup.py",
    "mills_gaethics.py", "mills_indistrict.py", "mills_sd54.py",
    "nysed_check.py", "nysed_inspect.py", "nysed_inspect2.py",
    "tmp_check.py", "tmp_crm.py", "tmp_list_dbs.py",
    "test_dbs.py", "test_env.py", "test_tables.py",
    "_check_crm.py", "_match_emails.py", "_tmp_cols.py", "_tmp_query.py",
    "_deploy_export.py", "backup_databases.py", "FIX_MCP_POWERSHELL.py",
    "patch_export.py",
]

DELETE_FILES = [
    "boe_tables_check.txt", "col_inspect.txt", "filer_sample.txt",
    "fix_names_out.txt", "out.txt", "party_reclassify_log.txt",
    "patch_err.txt", "patch_out.txt", "patch_result.txt", "rebuild_out.txt",
    "schema_err.txt", "schema_out.txt", "schema_test_out.txt", "test_out.txt",
    "tmp_log.txt", "vf_schema_out.txt", "_crm_output.txt", "_match_output.txt",
    "schema_check.sql", "schema_test.sql", "tmp_q.sql", "vf_schema.sql",
    "APPLY_PATCH.bat", "FIX_MCP.bat", "run_patch.cmd", "tmp_run.bat",
    "export/export.py.bak",
]

out = []
archived = 0
for fname in ARCHIVE_FILES:
    src = BASE / fname
    dst = ARCHIVE / fname
    if src.exists():
        shutil.move(str(src), str(dst))
        out.append(f"  ARCHIVED: {fname}")
        archived += 1
    else:
        out.append(f"  SKIP:     {fname} (not found)")

deleted = 0
for fname in DELETE_FILES:
    fpath = BASE / fname
    if fpath.exists():
        fpath.unlink()
        out.append(f"  DELETED:  {fname}")
        deleted += 1
    else:
        out.append(f"  SKIP:     {fname} (not found)")

pc = BASE / "__pycache__"
if pc.exists():
    shutil.rmtree(pc)
    out.append("  DELETED:  __pycache__/")

summary = f"Archived {archived}, deleted {deleted} files."
out.append(summary)

with open(BASE / "_cleanup_log.txt", "w") as f:
    f.write("\n".join(out))

print(summary)
