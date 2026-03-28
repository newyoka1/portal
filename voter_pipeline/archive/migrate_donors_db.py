#!/usr/bin/env python3
"""
migrate_donors_db.py
====================
Copies all tables from donors_2024 into nys_voter_tagging.
Verifies row counts match - if not, drops and reloads that table.
Retries up to MAX_RETRIES times per table.

Usage:
    python migrate_donors_db.py --dry-run   # preview only
    python migrate_donors_db.py             # run migration
    python migrate_donors_db.py --drop      # migrate then drop donors_2024
"""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

SRC_DB     = "donors_2024"
TGT_DB     = "nys_voter_tagging"
MAX_RETRIES = 3

def log(msg): print(msg, flush=True)

def get_tables(cur, db):
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name", (db,))
    return [r[0] for r in cur.fetchall()]

def row_count(cur, db, table):
    cur.execute(f"SELECT COUNT(*) FROM `{db}`.`{table}`")
    return cur.fetchone()[0]

def table_exists(cur, db, table):
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s", (db, table))
    return cur.fetchone()[0] > 0

def copy_table(cur, conn, table):
    """Drop target if exists, copy structure + data from src. Returns (ok, src_cnt, tgt_cnt)."""
    if table_exists(cur, TGT_DB, table):
        cur.execute(f"DROP TABLE `{TGT_DB}`.`{table}`")
        conn.commit()
    cur.execute(f"CREATE TABLE `{TGT_DB}`.`{table}` LIKE `{SRC_DB}`.`{table}`")
    cur.execute(f"INSERT INTO `{TGT_DB}`.`{table}` SELECT * FROM `{SRC_DB}`.`{table}`")
    conn.commit()
    src_cnt = row_count(cur, SRC_DB, table)
    tgt_cnt = row_count(cur, TGT_DB, table)
    return src_cnt == tgt_cnt, src_cnt, tgt_cnt

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no changes")
    parser.add_argument("--drop", action="store_true", help="Drop donors_2024 after successful migration")
    parser.add_argument("--force", action="store_true", help="Re-copy tables that already exist in target")
    args = parser.parse_args()

    conn = get_conn(TGT_DB, autocommit=False)
    cur  = conn.cursor()

    src_tables = get_tables(cur, SRC_DB)
    tgt_tables = get_tables(cur, TGT_DB)

    log(f"\n{'='*65}")
    log(f"  DONOR DB MIGRATION: {SRC_DB} -> {TGT_DB}")
    log(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'} | Force re-copy: {args.force}")
    log(f"{'='*65}\n")

    # Preview
    log(f"{'Table':<45} {'Src Rows':>12}  Status")
    log(f"{'-'*45} {'-'*12}  {'-'*30}")
    for t in src_tables:
        cnt = row_count(cur, SRC_DB, t)
        already = t in tgt_tables
        if already and not args.force:
            tgt_cnt = row_count(cur, TGT_DB, t)
            match = "MATCH" if cnt == tgt_cnt else f"MISMATCH (tgt={tgt_cnt:,})"
            status = f"EXISTS in target - {match}"
        elif already and args.force:
            status = "EXISTS - will FORCE RE-COPY"
        else:
            status = "will COPY"
        log(f"  {t:<43} {cnt:>12,}  {status}")

    if args.dry_run:
        log("\nDry run complete. Run without --dry-run to execute.")
        cur.close(); conn.close()
        return

    log("\nStarting migration...\n")
    results = {"copied": [], "skipped": [], "failed": []}

    for table in src_tables:
        already = table_exists(cur, TGT_DB, table)

        # Check if already exists and matches - skip unless --force
        if already and not args.force:
            src_cnt = row_count(cur, SRC_DB, table)
            tgt_cnt = row_count(cur, TGT_DB, table)
            if src_cnt == tgt_cnt:
                log(f"  SKIP (already matches {src_cnt:,} rows): {table}")
                results["skipped"].append(table)
                continue
            else:
                log(f"  MISMATCH detected (src={src_cnt:,} tgt={tgt_cnt:,}) - will reload: {table}")

        # Copy with retry loop
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log(f"  {'Re-copying' if already else 'Copying'} (attempt {attempt}/{MAX_RETRIES}): {table} ...")
                ok, src_cnt, tgt_cnt = copy_table(cur, conn, table)
                if ok:
                    log(f"    OK: {tgt_cnt:,} rows verified")
                    results["copied"].append(table)
                    success = True
                    break
                else:
                    log(f"    MISMATCH after copy: src={src_cnt:,} tgt={tgt_cnt:,} - retrying...")
            except Exception as e:
                conn.rollback()
                log(f"    ERROR on attempt {attempt}: {e}")

        if not success:
            log(f"    FAILED after {MAX_RETRIES} attempts: {table}")
            results["failed"].append(table)

    # Summary
    log(f"\n{'='*65}")
    log(f"  RESULTS")
    log(f"{'='*65}")
    log(f"  Copied/reloaded : {len(results['copied'])} tables  -> {results['copied']}")
    log(f"  Skipped (matched): {len(results['skipped'])} tables")
    log(f"  Failed           : {len(results['failed'])} tables -> {results['failed']}")

    if results["failed"]:
        log(f"\n  Some tables FAILED - NOT dropping {SRC_DB}. Fix errors and rerun.")
    elif args.drop:
        confirm = input(f"\nAll tables migrated OK. Drop {SRC_DB}? Type 'yes' to confirm: ")
        if confirm.strip().lower() == "yes":
            log(f"Dropping {SRC_DB}...")
            cur.execute(f"DROP DATABASE `{SRC_DB}`")
            conn.commit()
            log(f"Dropped {SRC_DB}.")
        else:
            log("Drop cancelled.")
    else:
        log(f"\n  {SRC_DB} kept. Run with --drop to remove it once you have verified everything.")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()