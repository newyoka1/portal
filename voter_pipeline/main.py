#!/usr/bin/env python3
"""
main.py - NYS Voter Pipeline Entry Point
=========================================
Single command runner for all pipeline operations.

Usage:
    python main.py status                    # Show source file ages
    python main.py pipeline                  # Load all 13M voters + match all audiences
    python main.py export --ld 63            # Export LD 63 to Excel
    python main.py export --all-ld           # Export every Legislative District (one file each)
    python main.py export --all-sd           # Export every State Senate District
    python main.py export --all-cd           # Export every Congressional District
    python main.py donors                    # Full BOE + National + CFB donor pipeline
    python main.py donors --no-refresh       # Re-enrich only (skip downloads)
    python main.py boe-enrich                # BOE state donors only
    python main.py national-enrich           # National/FEC federal donors only
    python main.py cfb-enrich                # NYC CFB donors only
    python main.py hubspot-sync              # Incremental sync all HubSpot accounts
    python main.py hubspot-sync --full       # Full re-sync all accounts
    python main.py hubspot-sync --account X  # Sync only account "X"
    python main.py cm-sync                   # Incremental sync Campaign Monitor lists
    python main.py cm-sync --full            # Full re-sync all CM lists
    python main.py cm-sync --list ID         # Sync only one CM list
    python main.py cm-sync --skip-segments   # Sync lists only (skip segment tagging)
    python main.py crm-sync                  # Sync all CRM sources (HubSpot + CM)
    python main.py crm-sync --full           # Full re-sync all sources
    python main.py crm-enrich                # Append voter data to CRM contacts (incremental)
    python main.py crm-enrich --full         # Re-enrich all contacts
    python main.py crm-enrich --stats        # Show enrichment stats only
    python main.py fb-audiences                              # Interactive donor/audience → Facebook export
    python main.py crm-phone                 # Match unmatched CRM contacts to voter file by phone number
    python main.py fb-push --list-audiences  # List voter file audience names available for FB push
    python main.py fb-push --audience NYS_HARD_DEM          # Push a named voter audience to Facebook
    python main.py fb-push --audience NYS_SWING --ld 63     # Push audience filtered to a district
    python main.py fb-push --audience NYS_HARD_GOP --fb-audience-id 123 --replace  # Replace existing
    python main.py fb-audiences --list-audiences             # List available audiences
    python main.py fb-audiences --audience NYS_HARD_DEM      # Export audience to new FB Custom Audience
    python main.py fb-audiences --audience NYS_SWING --ld 63 # Filter to a single district
    python main.py fb-audiences --audience NYS_HARD_GOP --fb-audience-id 1234567890 --replace
    python main.py reset                     # Drop all donor DBs + re-run everything (clean slate)
    python main.py reset --db-only           # Drop donor DBs only, skip re-run
    python main.py sync                      # Push enriched tables to Aiven

Verbosity Flags (available for all commands):
    --verbose    Show detailed progress
    --debug      Show everything
    --quiet      Only show errors

Commands:
    status          Show source file ages and data freshness
    pipeline        Load all voters + match all audiences (statewide)
    export          Export a district to Excel
    donors          Full donor pipeline: BOE (state) + National (federal) + CFB (NYC)
    boe-enrich      BOE state pipeline: download -> load -> enrich voter_file
    national-enrich National/FEC pipeline: download -> extract -> load -> classify -> unify -> enrich
    cfb-enrich      NYC CFB pipeline: download -> load -> match -> enrich voter_file
    ethnicity       Build ModeledEthnicity on voter_file
    enrich-derived  Compute registration recency, turnout, donor cross-level, household stats
    district-scores Build district competitiveness scores table
    party-snapshot  Snapshot party registration and detect switchers
    both            pipeline + export in sequence
    reset           Drop all donor DBs and re-run full donor pipeline from scratch
    hubspot-sync    Sync HubSpot CRM contacts + deals to unified DB
    cm-sync         Sync Campaign Monitor subscribers to unified contacts
    crm-sync        Sync all CRM sources (HubSpot + Campaign Monitor)
    crm-enrich      Append voter file data to CRM contacts (party, districts, history, donors)
    crm-phone       Second-pass: match unmatched CRM contacts to voter file using phone numbers
    fb-audiences    Interactive export of donor/segment audiences to Facebook Custom Audiences
    fb-push         Non-interactive push of a named voter audience to Facebook Custom Audiences
    sync            Push final enriched tables to Aiven remote MySQL
"""

import argparse
import sys
import os
import subprocess
from pathlib import Path
import time
from datetime import datetime

BASE   = os.path.dirname(os.path.abspath(__file__))

# -- Auto-detect venv python, fall back to current interpreter
_venv_python = os.path.join(BASE, ".venv", "Scripts", "python.exe")
PYTHON = _venv_python if os.path.exists(_venv_python) else sys.executable

# 10-year window: 6 even-year FEC cycles ending at current/next cycle
_current_year  = datetime.now().year
_current_cycle = _current_year if _current_year % 2 == 0 else _current_year + 1
FEC_CYCLES = [_current_cycle - (i * 2) for i in range(6)]

# FEC extracted-dir hints for ask_refresh display (ZIPs deleted after extraction)
FEC_FILES = [
    os.path.join(BASE, "data", "fec_downloads", "extracted", f"indiv{str(c)[-2:]}")
    for c in FEC_CYCLES
]

BOE_FILES = [
    os.path.join(BASE, "data", "boe_donors", "ALL_REPORTS_StateCandidate.zip"),
    os.path.join(BASE, "data", "boe_donors", "ALL_REPORTS_CountyCandidate.zip"),
    os.path.join(BASE, "data", "boe_donors", "ALL_REPORTS_StateCommittee.zip"),
    os.path.join(BASE, "data", "boe_donors", "ALL_REPORTS_CountyCommittee.zip"),
]

CFB_FILES = [
    os.path.join(BASE, "data", "cfb", "2017_Contributions.csv"),
    os.path.join(BASE, "data", "cfb", "2021_Contributions.csv"),
    os.path.join(BASE, "data", "cfb", "2023_Contributions.csv"),
    os.path.join(BASE, "data", "cfb", "2025_Contributions.csv"),
]


class PipelineError(Exception):
    """Raised when a subprocess pipeline step fails."""
    def __init__(self, script, returncode):
        self.script = script
        self.returncode = returncode
        super().__init__(f"{script} exited with code {returncode}")


def run_fec_pipeline():
    """Download+extract, load, and classify FEC data (step1 now handles extraction)."""
    run("step1_download_fec.py")
    run("step3_load_fec.py")
    run("step4_classify_parties.py")


def run(script_path, extra_args=None, verbosity_level=None):
    cmd = [PYTHON, os.path.join(BASE, script_path)] + (extra_args or [])
    if verbosity_level == 'quiet':   cmd.append('--quiet')
    elif verbosity_level == 'verbose': cmd.append('--verbose')
    elif verbosity_level == 'debug':   cmd.append('--debug')
    print(f"\n>>> Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\nERROR: {script_path} exited with code {result.returncode}")
        raise PipelineError(script_path, result.returncode)


def file_age_str(path):
    p = Path(path)
    if not p.exists():
        return "missing"
    age = datetime.now().timestamp() - p.stat().st_mtime
    if age < 3600:    return f"{int(age/60)}m ago"
    if age < 86400:   return f"{age/3600:.1f}h ago"
    return f"{age/86400:.0f}d ago"


def ask_refresh(label, file_hints, refresh_flag):
    if refresh_flag is True:
        print(f"  [--refresh] Refreshing {label} data.")
        return True
    if refresh_flag is False:
        print(f"  [--no-refresh] Skipping download, enriching from existing DB.")
        return False
    print(f"\n{'='*60}")
    print(f"  {label} source files:")
    for fpath in file_hints:
        print(f"    {Path(fpath).name:<45} {file_age_str(fpath)}")
    print(f"{'='*60}")
    try:
        ans = input(f"  Refresh {label} data from source? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        ans = "n"
    print()
    return ans in ("y", "yes")


def district_args(args):
    if getattr(args, 'statewide', False): return ["--statewide"]
    if args.ld:     return ["--ld", args.ld]
    if args.sd:     return ["--sd", args.sd]
    if args.cd:     return ["--cd", args.cd]
    if args.county: return ["--county", args.county]
    print("ERROR: provide --ld, --sd, --cd, --county, or --statewide")
    sys.exit(1)


def run_reset(db_only=False, verbosity='normal', yes=False):
    """
    Drop all donor databases and clear enrichment columns on voter_file.
    Optionally re-run the full donor pipeline from scratch afterward.
    """
    import pymysql
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE, ".env"))

    DONOR_DBS  = ["boe_donors", "National_Donors", "cfb_donors"]
    EXTRACT_DIR = Path(BASE) / "data" / "fec_downloads" / "extracted"

    # Columns to NULL out on voter_file
    BOE_COLS = [
        "boe_total_amt", "boe_total_count", "boe_total_D_amt", "boe_total_D_count",
        "boe_total_R_amt", "boe_total_R_count", "boe_total_U_amt", "boe_total_U_count",
        "boe_last_date", "boe_last_filer",
    ]
    NATIONAL_COLS = [
        "is_national_donor", "national_total_amount", "national_total_count",
        "national_democratic_amount", "national_democratic_count",
        "national_republican_amount", "national_republican_count",
        "national_independent_amount", "national_independent_count",
        "national_unknown_amount", "national_unknown_count",
    ]
    CFB_COLS = [
        "cfb_total_amt", "cfb_total_count", "cfb_last_date", "cfb_last_cand",
        "cfb_last_office", "cfb_2017_amt", "cfb_2021_amt", "cfb_2023_amt", "cfb_2025_amt",
    ]

    print()
    print("=" * 70)
    print("  RESET: DONOR DATABASES + VOTER ENRICHMENT COLUMNS")
    print("=" * 70)
    print()
    print("  This will PERMANENTLY DROP:")
    for db in DONOR_DBS:
        print(f"    - {db} database")
    print()
    print("  And NULL out on voter_file:")
    print(f"    - {len(BOE_COLS)} BOE columns")
    print(f"    - {len(NATIONAL_COLS)} National columns")
    print(f"    - {len(CFB_COLS)} CFB columns")
    print()
    if EXTRACT_DIR.exists():
        print(f"  Extracted FEC files in {EXTRACT_DIR} will also be deleted.")
        print()
    print("  Downloaded source zips/CSVs will NOT be deleted.")
    print()

    if yes:
        print("  [--yes] Confirmation bypassed — proceeding with reset.")
    else:
        try:
            ans = input("  Type YES to confirm reset: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Reset cancelled.")
            return
        if ans != "YES":
            print("  Reset cancelled (type exactly YES to confirm).")
            return

    print()
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD"),
        charset="utf8mb4", autocommit=True
    )
    cur = conn.cursor()

    # 1. Drop donor databases
    print("  Step 1: Dropping donor databases...")
    for db in DONOR_DBS:
        cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
        print(f"    Dropped: {db}")

    # 2. NULL out enrichment columns on voter_file
    print()
    print("  Step 2: Clearing enrichment columns on voter_file...")
    all_cols = BOE_COLS + NATIONAL_COLS + CFB_COLS

    # Only null columns that actually exist
    placeholders = ",".join(["%s"] * len(all_cols))
    cur.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'nys_voter_tagging' AND TABLE_NAME = 'voter_file'
        AND COLUMN_NAME IN ({placeholders})
    """, all_cols)
    existing = [r[0] for r in cur.fetchall()]

    if existing:
        set_clause = ", ".join([f"`{c}` = NULL" for c in existing])

        # Raise lock timeout for large table (12.7M rows)
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

        # Chunk by CDName to avoid one mega-transaction timing out
        cur.execute("SELECT DISTINCT CDName FROM nys_voter_tagging.voter_file ORDER BY CDName")
        cd_names = [r[0] for r in cur.fetchall()]

        print(f"    Nulling {len(existing)} columns across {len(cd_names)} CD partitions...")
        t0 = time.time()
        for i, cd in enumerate(cd_names, 1):
            cur.execute(
                f"UPDATE nys_voter_tagging.voter_file SET {set_clause} WHERE CDName = %s",
                (cd,)
            )
            print(f"    {i}/{len(cd_names)}  {cd}  ({cur.rowcount:,} rows)  "
                  f"{time.time()-t0:.0f}s elapsed")
        print(f"    Done in {time.time()-t0:.1f}s")
    else:
        print("    No enrichment columns found on voter_file (already clean).")

    conn.close()

    # 3. Delete extracted FEC files
    print()
    print("  Step 3: Clearing extracted FEC files...")
    if EXTRACT_DIR.exists():
        import shutil
        shutil.rmtree(EXTRACT_DIR)
        EXTRACT_DIR.mkdir()
        print(f"    Cleared: {EXTRACT_DIR}")
    else:
        print("    No extracted directory found.")

    print()
    print("  Reset complete.")
    print("=" * 70)

    if db_only:
        print()
        print("  --db-only specified: stopping here.")
        print("  To re-run the donor pipeline:")
        print("    python main.py donors --refresh")
        print()
        return

    # 4. Re-run full donor pipeline
    print()
    print("  Step 4: Re-running full donor pipeline from scratch...")
    print()

    # BOE
    print("  --- BOE (state) ---")
    run("load_raw_boe.py")
    run("classify_boe_parties.py")
    run("pipeline/enrich_boe_donors.py", verbosity_level=verbosity)

    # National/FEC
    print("  --- National/FEC (federal) ---")
    run_fec_pipeline()
    run("pipeline/enrich_fec_donors.py", verbosity_level=verbosity)

    # CFB
    print("  --- NYC CFB ---")
    run("download_cfb.py")
    run("load_cfb_contributions.py", ["--force"], verbosity_level=verbosity)

    print()
    print("=" * 70)
    print("  RESET + REBUILD COMPLETE")
    print("=" * 70)
    print()


def main():
    t0 = time.time()
    print(f"Using Python: {PYTHON}")

    parser = argparse.ArgumentParser(description="NYS Voter Pipeline Runner")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--debug",   "-d", action="store_true")
    parser.add_argument("--quiet",   "-q", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    # status
    sub.add_parser("status", help="Show source file ages and data freshness")

    # pipeline
    sub.add_parser("pipeline", help="Load all voters + match all audiences (statewide)")

    # export
    p_exp = sub.add_parser("export", help="Export district to Excel")
    p_exp.add_argument("--ld");     p_exp.add_argument("--sd")
    p_exp.add_argument("--cd");     p_exp.add_argument("--county")
    p_exp.add_argument("--statewide", action="store_true", help="Export all voters (no geographic filter)")
    p_exp.add_argument("--all-ld",  action="store_true", help="Export every Legislative District")
    p_exp.add_argument("--all-sd",  action="store_true", help="Export every State Senate District")
    p_exp.add_argument("--all-cd",  action="store_true", help="Export every Congressional District")

    # voter-contact export
    p_vc = sub.add_parser("voter-contact", help="Export Voter Contact workbook (no turnout, full party tabs)")
    p_vc.add_argument("--ld");  p_vc.add_argument("--sd")
    p_vc.add_argument("--cd");  p_vc.add_argument("--county")

    # both
    p_both = sub.add_parser("both", help="Run pipeline then export")
    p_both.add_argument("--ld"); p_both.add_argument("--sd")
    p_both.add_argument("--cd"); p_both.add_argument("--county")

    # donors  (BOE + National + CFB)
    p_donors = sub.add_parser("donors", help="Full donor pipeline: BOE + National + CFB")
    p_donors_ref = p_donors.add_mutually_exclusive_group()
    p_donors_ref.add_argument("--refresh",    dest="refresh", action="store_const", const=True,
                              default=None, help="Re-download + reload all source data without prompting")
    p_donors_ref.add_argument("--no-refresh", dest="refresh", action="store_const", const=False,
                              help="Skip all downloads, enrich from existing DBs")

    # boe-enrich
    p_boe_enrich = sub.add_parser("boe-enrich", help="BOE state pipeline")
    p_boe_ref = p_boe_enrich.add_mutually_exclusive_group()
    p_boe_ref.add_argument("--refresh",    dest="refresh", action="store_const", const=True, default=None)
    p_boe_ref.add_argument("--no-refresh", dest="refresh", action="store_const", const=False)

    # national-enrich
    p_nat_enrich = sub.add_parser("national-enrich", help="National/FEC pipeline")
    p_nat_ref = p_nat_enrich.add_mutually_exclusive_group()
    p_nat_ref.add_argument("--refresh",    dest="refresh", action="store_const", const=True, default=None)
    p_nat_ref.add_argument("--no-refresh", dest="refresh", action="store_const", const=False)

    # cfb-enrich
    p_cfb_enrich = sub.add_parser("cfb-enrich", help="NYC CFB pipeline")
    p_cfb_ref = p_cfb_enrich.add_mutually_exclusive_group()
    p_cfb_ref.add_argument("--refresh",    dest="refresh", action="store_const", const=True, default=None)
    p_cfb_ref.add_argument("--no-refresh", dest="refresh", action="store_const", const=False)

    # boe-download
    p_boe_dl = sub.add_parser("boe-download", help="Download BOE bulk files (requires playwright)")
    p_boe_dl.add_argument("--force", action="store_true")

    # boe-load  (load + classify, no download, no enrich)
    sub.add_parser("boe-load",
        help="Load BOE ZIPs into boe_donors DB and classify parties (no download, no voter enrich)")

    # boe-enrich-only  (just the voter_file enrichment step)
    sub.add_parser("boe-enrich-only",
        help="Enrich voter_file from boe_donor_summary (skips download and load steps)")

    # fec-download  (step 1 only)
    sub.add_parser("fec-download",
        help="Download FEC bulk contribution ZIP files (6 cycles)")

    # fec-extract  (step 2 only)
    sub.add_parser("fec-extract",
        help="Extract downloaded FEC ZIP files")

    # fec-load  (steps 3 + 4: load + classify)
    sub.add_parser("fec-load",
        help="Load FEC data into National_Donors DB and classify committee parties")

    # fec-enrich  (just voter_file enrichment)
    sub.add_parser("fec-enrich",
        help="Enrich voter_file from FEC contributions (skips download/extract/load steps)")

    # cfb-download  (download only)
    p_cfb_dl = sub.add_parser("cfb-download",
        help="Download NYC CFB contribution CSVs (4 cycles)")
    p_cfb_dl.add_argument("--force", action="store_true",
        help="Re-download even if files are unchanged")

    # cfb-load  (load + enrich, no download)
    p_cfb_load = sub.add_parser("cfb-load",
        help="Load NYC CFB CSVs into cfb_donors DB and enrich voter_file (no download)")
    p_cfb_load.add_argument("--force", action="store_true",
        help="Force re-load even if file hash is unchanged")

    # ethnicity
    p_eth = sub.add_parser("ethnicity", help="Build ModeledEthnicity column")
    p_eth.add_argument("--dry-run",    action="store_true")
    p_eth.add_argument("--rebuild",    action="store_true")
    p_eth.add_argument("--batch-size", type=int, default=50000)

    # enrich-derived
    p_derived = sub.add_parser("enrich-derived",
        help="Compute registration recency, turnout score, donor cross-level, household stats")
    p_derived_ref = p_derived.add_mutually_exclusive_group()
    p_derived_ref.add_argument("--refresh",    dest="refresh", action="store_const", const=True,
                               default=None, help="Clear and recompute all derived columns")
    p_derived_ref.add_argument("--no-refresh", dest="refresh", action="store_const", const=False,
                               help="Only fill NULLs (default)")

    # district-scores
    sub.add_parser("district-scores", help="Build district competitiveness scores table")

    # party-snapshot
    sub.add_parser("party-snapshot", help="Snapshot party registration and detect switchers")

    # reset
    p_reset = sub.add_parser("reset",
        help="Drop all donor DBs + clear voter enrichment columns, then re-run everything")
    p_reset.add_argument("--db-only", action="store_true",
        help="Drop DBs and clear columns only — do NOT re-run the pipeline")
    p_reset.add_argument("--yes", action="store_true",
        help="Skip the interactive YES confirmation (required for non-interactive / portal use)")

    # hubspot-sync
    p_hs = sub.add_parser("hubspot-sync",
        help="Sync HubSpot CRM contacts + deals to local MySQL")
    p_hs.add_argument("--full", action="store_true",
        help="Force full re-sync (delete + reload all contacts and deals)")
    p_hs.add_argument("--account", type=str, default=None,
        help="Sync only this account (matches HUBSPOT_TOKEN_<NAME> suffix)")

    # cm-sync
    p_cm = sub.add_parser("cm-sync",
        help="Sync Campaign Monitor subscribers to unified contacts")
    p_cm.add_argument("--full", action="store_true",
        help="Force full re-sync (re-fetch all subscribers)")
    p_cm.add_argument("--account", type=str, default=None,
        help="Sync only this account (matches CM_API_KEY_<NAME> suffix)")
    p_cm.add_argument("--list", type=str, default=None,
        help="Sync only this list ID")
    p_cm.add_argument("--skip-segments", action="store_true",
        help="Skip segment discovery and tagging (faster)")

    # crm-sync
    p_crm = sub.add_parser("crm-sync",
        help="Sync all CRM sources (HubSpot + Campaign Monitor)")
    p_crm.add_argument("--full", action="store_true",
        help="Force full re-sync of all sources")

    # crm-enrich
    p_crm_enrich = sub.add_parser("crm-enrich",
        help="Append voter file data to CRM contacts")
    p_crm_enrich.add_argument("--full", action="store_true",
        help="Re-enrich all contacts (ignore watermark)")
    p_crm_enrich.add_argument("--stats", action="store_true",
        help="Show enrichment match stats only")

    # crm-phone
    p_crm_phone = sub.add_parser("crm-phone",
        help="Second-pass: match unmatched CRM contacts to voter file via phone number")
    p_crm_phone.add_argument("--stats", action="store_true",
        help="Show match stats only (no matching)")

    # crm-extended-match
    p_ext = sub.add_parser("crm-extended-match",
        help="Extended CRM voter matching: hyphenated names, first-word first name, inactive voters")
    p_ext.add_argument("--stats",   action="store_true",
        help="Show match-method breakdown only (no matching)")
    p_ext.add_argument("--dry-run", action="store_true",
        help="Count potential matches without writing any rows")

    # fb-push — non-interactive voter audience → Facebook Custom Audience
    p_fbpush = sub.add_parser(
        "fb-push",
        help="Push a named voter audience to Facebook Custom Audiences",
    )
    fb_push_mode = p_fbpush.add_mutually_exclusive_group()
    fb_push_mode.add_argument(
        "--audience", metavar="NAME",
        help="Audience name from voter_audience_bridge",
    )
    fb_push_mode.add_argument(
        "--list-audiences", action="store_true",
        help="List available audience names with voter counts",
    )
    fb_push_dist = p_fbpush.add_mutually_exclusive_group()
    fb_push_dist.add_argument("--ld",     metavar="NUM")
    fb_push_dist.add_argument("--sd",     metavar="NUM")
    fb_push_dist.add_argument("--cd",     metavar="NUM")
    fb_push_dist.add_argument("--county", metavar="NAME")
    p_fbpush.add_argument("--fb-audience-id", metavar="ID",
        help="Existing Facebook Custom Audience ID to update")
    p_fbpush.add_argument("--replace", action="store_true",
        help="Atomically replace audience (requires --fb-audience-id)")
    p_fbpush.add_argument("--dry-run", action="store_true",
        help="Hash and prepare records but do not upload to Facebook")

    # fb-audiences — interactive by default; optional CLI flags bypass the prompts
    p_fb = sub.add_parser(
        "fb-audiences",
        help="Export donor or voter-segment audiences to Facebook Custom Audiences",
    )
    fb_mode = p_fb.add_mutually_exclusive_group()
    fb_mode.add_argument(
        "--audience", metavar="NAME",
        help="Audience name from voter_audience_bridge — skips interactive source prompt",
    )
    fb_mode.add_argument(
        "--list-audiences", action="store_true",
        help="List all available audience names with voter counts",
    )
    fb_dist = p_fb.add_mutually_exclusive_group()
    fb_dist.add_argument("--ld",     metavar="NUM",  help="Filter to Assembly/Legislative District")
    fb_dist.add_argument("--sd",     metavar="NUM",  help="Filter to State Senate District")
    fb_dist.add_argument("--cd",     metavar="NUM",  help="Filter to Congressional District")
    fb_dist.add_argument("--county", metavar="NAME", help="Filter to county (e.g. Nassau)")
    p_fb.add_argument(
        "--fb-audience-id", metavar="ID",
        help="Existing Facebook Custom Audience ID to update (omit to create new)",
    )
    p_fb.add_argument(
        "--replace", action="store_true",
        help="Atomically replace all audience members (requires --fb-audience-id)",
    )
    p_fb.add_argument(
        "--audience-name", metavar="NAME", dest="fb_audience_name",
        help="Override name for the new Facebook Custom Audience",
    )
    p_fb.add_argument("--dry-run", action="store_true",
        help="Hash records but do not upload to Facebook")


    args = parser.parse_args()

    verbosity = 'normal'
    if args.debug:   verbosity = 'debug';   print("DEBUG MODE")
    elif args.verbose: verbosity = 'verbose'; print("VERBOSE MODE")
    elif args.quiet:   verbosity = 'quiet'

    try:
        _dispatch(args, verbosity)
    except PipelineError as e:
        sys.exit(e.returncode)

    elapsed = time.time() - t0
    if elapsed >= 60:
        print(f"\nTotal time: {elapsed/60:.1f} minutes")
    elif elapsed >= 5:
        print(f"\nTotal time: {elapsed:.0f} seconds")


def _dispatch(args, verbosity):
    """Route the parsed command to its handler."""
    # ── status ────────────────────────────────────────────────────────────────
    if args.command == "status":
        sources = [
            ("BOE state donors",          BOE_FILES),
            ("National/FEC federal donors", FEC_FILES),
            ("NYC CFB donors",            CFB_FILES),
        ]
        print("\nSource file status:")
        for label, files in sources:
            print(f"\n  {label}:")
            for f in files:
                print(f"    {Path(f).name:<45} {file_age_str(f)}")
        print()

    # ── pipeline ──────────────────────────────────────────────────────────────
    elif args.command == "pipeline":
        run("pipeline/pipeline.py", verbosity_level=verbosity)

    # ── export ────────────────────────────────────────────────────────────────
    elif args.command == "export":
        # Batch statewide export: loop through every district in the DB
        if getattr(args, 'all_ld', False) or getattr(args, 'all_sd', False) or getattr(args, 'all_cd', False):
            import pymysql
            from dotenv import load_dotenv
            load_dotenv(os.path.join(BASE, ".env"))
            conn = pymysql.connect(
                host=os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "127.0.0.1")),
                port=int(os.getenv("DB_PORT", os.getenv("MYSQL_PORT", "3306"))),
                user=os.getenv("DB_USER", os.getenv("MYSQL_USER", "root")),
                password=os.getenv("DB_PASSWORD", os.getenv("MYSQL_PASSWORD", "")),
                database="nys_voter_tagging", charset="utf8mb4", autocommit=True,
            )
            if args.all_ld:
                col, flag = "LDName", "--ld"
            elif args.all_sd:
                col, flag = "SDName", "--sd"
            else:
                col, flag = "CDName", "--cd"

            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT `{col}` FROM voter_file ORDER BY `{col}`")
                districts = [r[0] for r in cur.fetchall() if r[0]]
            conn.close()

            print(f"\nBatch export: {len(districts)} {col.replace('Name','')}s\n")
            for i, district in enumerate(districts, 1):
                print(f"\n[{i}/{len(districts)}] Exporting {col.replace('Name','')} {district}...")
                try:
                    run("export/export.py", [flag, str(district)], verbosity_level=verbosity)
                except PipelineError as e:
                    print(f"  WARNING: {col} {district} failed (code {e.returncode}) — continuing...")
        else:
            run("export/export.py", district_args(args), verbosity_level=verbosity)

    # ── voter-contact ──────────────────────────────────────────────────────
    elif args.command == "voter-contact":
        run("export/export_contact.py", district_args(args), verbosity_level=verbosity)

    # ── both ──────────────────────────────────────────────────────────────────
    elif args.command == "both":
        dargs = district_args(args)
        run("pipeline/pipeline.py", verbosity_level=verbosity)
        run("export/export.py", dargs, verbosity_level=verbosity)

    # ── donors (BOE + National + CFB) ─────────────────────────────────────────
    elif args.command == "donors":
        refresh_flag = args.refresh
        print("Running full donor pipeline: BOE (state) + National (federal) + CFB (NYC)")
        resume_cmds = ["boe-enrich", "national-enrich", "cfb-enrich"]
        stage = 0
        try:
            print("\nStep 1/3: BOE state donors...")
            if ask_refresh("BOE", BOE_FILES, refresh_flag):
                run("load_raw_boe.py")
            run("classify_boe_parties.py")
            run("pipeline/enrich_boe_donors.py", verbosity_level=verbosity)

            stage = 1
            print("\nStep 2/3: National/FEC federal donors...")
            if ask_refresh("National/FEC", FEC_FILES, refresh_flag):
                run_fec_pipeline()
            run("pipeline/enrich_fec_donors.py", verbosity_level=verbosity)

            stage = 2
            print("\nStep 3/3: NYC CFB donors...")
            if ask_refresh("NYC CFB", CFB_FILES, refresh_flag):
                run("download_cfb.py")
                run("load_cfb_contributions.py", ["--force"])
            else:
                run("load_cfb_contributions.py", ["--skip-raw"])
        except PipelineError:
            print(f"\n  To resume the remaining stage(s):")
            for cmd in resume_cmds[stage:]:
                print(f"    python main.py {cmd}")
            raise

    # ── boe-enrich ────────────────────────────────────────────────────────────
    elif args.command == "boe-enrich":
        refresh_flag = args.refresh
        if ask_refresh("BOE", BOE_FILES, refresh_flag):
            run("load_raw_boe.py")
        run("classify_boe_parties.py")
        run("pipeline/enrich_boe_donors.py", verbosity_level=verbosity)

    # ── national-enrich ───────────────────────────────────────────────────────
    elif args.command == "national-enrich":
        refresh_flag = args.refresh
        if ask_refresh("National/FEC", FEC_FILES, refresh_flag):
            run_fec_pipeline()
        run("pipeline/enrich_fec_donors.py", verbosity_level=verbosity)

    # ── cfb-enrich ────────────────────────────────────────────────────────────
    elif args.command == "cfb-enrich":
        refresh_flag = args.refresh
        if ask_refresh("NYC CFB", CFB_FILES, refresh_flag):
            run("download_cfb.py")
            run("load_cfb_contributions.py", ["--force"], verbosity_level=verbosity)
        else:
            run("load_cfb_contributions.py", ["--skip-raw"], verbosity_level=verbosity)

    # ── boe-download ──────────────────────────────────────────────────────────
    elif args.command == "boe-download":
        extra = ["--force"] if args.force else []
        run("download_boe.py", extra, verbosity_level=verbosity)

    # ── boe-load ──────────────────────────────────────────────────────────────
    elif args.command == "boe-load":
        run("load_raw_boe.py", verbosity_level=verbosity)
        run("classify_boe_parties.py", verbosity_level=verbosity)

    # ── boe-enrich-only ───────────────────────────────────────────────────────
    elif args.command == "boe-enrich-only":
        run("pipeline/enrich_boe_donors.py", verbosity_level=verbosity)

    # ── fec-download ──────────────────────────────────────────────────────────
    elif args.command == "fec-download":
        run("step1_download_fec.py", verbosity_level=verbosity)

    # ── fec-extract ───────────────────────────────────────────────────────────
    elif args.command == "fec-extract":
        run("step2_extract_fec.py", verbosity_level=verbosity)

    # ── fec-load ──────────────────────────────────────────────────────────────
    elif args.command == "fec-load":
        run("step3_load_fec.py", verbosity_level=verbosity)
        run("step4_classify_parties.py", verbosity_level=verbosity)

    # ── fec-enrich ────────────────────────────────────────────────────────────
    elif args.command == "fec-enrich":
        run("pipeline/enrich_fec_donors.py", verbosity_level=verbosity)

    # ── cfb-download ──────────────────────────────────────────────────────────
    elif args.command == "cfb-download":
        extra = ["--force"] if args.force else []
        run("download_cfb.py", extra, verbosity_level=verbosity)

    # ── cfb-load ──────────────────────────────────────────────────────────────
    elif args.command == "cfb-load":
        extra = ["--force"] if args.force else ["--skip-raw"]
        run("load_cfb_contributions.py", extra, verbosity_level=verbosity)

    # ── ethnicity ─────────────────────────────────────────────────────────────
    elif args.command == "ethnicity":
        extra = []
        if args.dry_run:  extra += ["--dry-run"]
        if args.rebuild:  extra += ["--rebuild"]
        extra += ["--batch-size", str(args.batch_size)]
        run("voter/ethnicity.py", extra, verbosity_level=verbosity)

    # ── enrich-derived ───────────────────────────────────────────────────────
    elif args.command == "enrich-derived":
        extra = []
        if args.refresh is True:   extra.append("--refresh")
        elif args.refresh is False: extra.append("--no-refresh")
        run("voter/enrich_derived.py", extra, verbosity_level=verbosity)

    # ── district-scores ──────────────────────────────────────────────────────
    elif args.command == "district-scores":
        run("voter/district_scores.py", verbosity_level=verbosity)

    # ── party-snapshot ───────────────────────────────────────────────────────
    elif args.command == "party-snapshot":
        run("voter/party_snapshot.py", verbosity_level=verbosity)

    # ── hubspot-sync ──────────────────────────────────────────────────────────
    elif args.command == "hubspot-sync":
        extra = []
        if args.full: extra.append("--full")
        if args.account: extra.extend(["--account", args.account])
        run("load_hubspot_contacts.py", extra, verbosity_level=verbosity)

    # ── cm-sync ───────────────────────────────────────────────────────────────
    elif args.command == "cm-sync":
        extra = []
        if args.full: extra.append("--full")
        if args.account: extra.extend(["--account", args.account])
        if getattr(args, 'list', None): extra.extend(["--list", args.list])
        if getattr(args, 'skip_segments', False): extra.append("--skip-segments")
        run("load_cm_subscribers.py", extra, verbosity_level=verbosity)

    # ── crm-sync (all sources) ───────────────────────────────────────────────
    elif args.command == "crm-sync":
        extra = ["--full"] if args.full else []
        run("load_hubspot_contacts.py", extra, verbosity_level=verbosity)
        run("load_cm_subscribers.py", extra, verbosity_level=verbosity)

    # ── crm-enrich ────────────────────────────────────────────────────────────
    elif args.command == "crm-enrich":
        extra = []
        if args.full:  extra.append("--full")
        if args.stats: extra.append("--stats")
        run("pipeline/enrich_crm_contacts.py", extra, verbosity_level=verbosity)

    # ── fb-audiences ──────────────────────────────────────────────────────────
    elif args.command == "fb-audiences":
        extra = []
        if getattr(args, 'list_audiences', False):
            extra.append("--list-audiences")
        elif getattr(args, 'audience', None):
            extra += ["--audience", args.audience]
        if getattr(args, 'ld', None):     extra += ["--ld", args.ld]
        elif getattr(args, 'sd', None):   extra += ["--sd", args.sd]
        elif getattr(args, 'cd', None):   extra += ["--cd", args.cd]
        elif getattr(args, 'county', None): extra += ["--county", args.county]
        if getattr(args, 'fb_audience_id', None): extra += ["--fb-audience-id", args.fb_audience_id]
        if getattr(args, 'replace', False):       extra.append("--replace")
        if getattr(args, 'fb_audience_name', None): extra += ["--audience-name", args.fb_audience_name]
        if getattr(args, 'dry_run', False):       extra.append("--dry-run")
        run("export/facebook_donor_audience.py", extra, verbosity_level=verbosity)

    # ── crm-extended-match ────────────────────────────────────────────────────
    elif args.command == "crm-extended-match":
        extra = []
        if args.stats:   extra.append("--stats")
        if args.dry_run: extra.append("--dry-run")
        run("pipeline/extended_match.py", extra, verbosity_level=verbosity)

    # ── crm-phone ─────────────────────────────────────────────────────────────
    elif args.command == "crm-phone":
        extra = []
        if args.stats: extra.append("--stats")
        run("pipeline/phone_match_crm.py", extra, verbosity_level=verbosity)

    # ── fb-push ───────────────────────────────────────────────────────────────
    elif args.command == "fb-push":
        extra = []
        if getattr(args, "list_audiences", False):
            extra.append("--list-audiences")
        elif getattr(args, "audience", None):
            extra += ["--audience", args.audience]
        if getattr(args, "ld",     None): extra += ["--ld",     args.ld]
        elif getattr(args, "sd",   None): extra += ["--sd",     args.sd]
        elif getattr(args, "cd",   None): extra += ["--cd",     args.cd]
        elif getattr(args, "county", None): extra += ["--county", args.county]
        if getattr(args, "fb_audience_id", None):
            extra += ["--fb-audience-id", args.fb_audience_id]
        if getattr(args, "replace",  False): extra.append("--replace")
        if getattr(args, "dry_run",  False): extra.append("--dry-run")
        run("export/facebook_audience.py", extra, verbosity_level=verbosity)

    # ── reset ─────────────────────────────────────────────────────────────────
    elif args.command == "reset":
        run_reset(db_only=args.db_only, verbosity=verbosity, yes=getattr(args, 'yes', False))



if __name__ == "__main__":
    main()
