#!/usr/bin/env python3
"""
extended_match.py — Additional high-confidence CRM-to-voter matching passes.

Designed to run AFTER the primary passes:
  1. enrich_crm_contacts.py  (name + zip, Active voters only)
  2. phone_match_crm.py      (phone number)

Targets still-unmatched contacts via three additional exact-match strategies:

  Pass A – Hyphenated last name (h1 / h2)
             Voter "Garcia-Lopez" has clean_last_h1="GARCIA", h2="LOPEZ"
             CRM contact with clean_last="GARCIA" matches via h1.
             Uses idx_clean_h1 / idx_clean_h2 indexes (created here if missing).

  Pass B – First-word first name (unique match only)
             CRM "Mary Ann" → first_word="Mary" → clean="MARY"
             Voter registered simply as "MARY" now matches.
             ONLY accepted when exactly ONE voter matches name+zip — the scalar
             subquery returns NULL on ties, so no ambiguous match is ever written.

  Pass C – Inactive / purged voter fallback
             Same name+zip logic as Pass 1 but WITHOUT RegistrationStatus filter.
             Captures contacts who are real people registered but currently inactive.
             Orders active records first so if both exist the active one wins.

All passes stamp vf_match_method on the contact row so you can audit what
fraction of your match rate came from each strategy.

Usage:
    python extended_match.py             # Run all 3 passes, print stats
    python extended_match.py --stats     # Show current match-method breakdown only
    python extended_match.py --dry-run   # Count matches without writing anything

Called by: python main.py crm-extended-match [--stats] [--dry-run]
"""

import argparse
import sys
import time
from pathlib import Path

# ── path bootstrap ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from enrich_crm_contacts import (
    CRM_DB, VOTER_DB, VOTER_TBL, ENRICHED_AT_COL,
    connect, discover_voter_columns,
)

MATCH_METHOD_COL = "vf_match_method"


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def ensure_match_method_col(cur):
    """Add vf_match_method to contacts if missing, then retroactively label
    pre-existing matches (they were all created by the name+zip pass)."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'contacts' AND COLUMN_NAME = %s",
        (CRM_DB, MATCH_METHOD_COL))
    if cur.fetchone()[0] == 0:
        cur.execute(
            f"ALTER TABLE {CRM_DB}.contacts "
            f"ADD COLUMN `{MATCH_METHOD_COL}` VARCHAR(30) DEFAULT NULL "
            f"AFTER `{ENRICHED_AT_COL}`")
        print(f"  + Added column: {MATCH_METHOD_COL}")

    # Retroactively label contacts matched before this column existed.
    # They were all matched by the name+zip pass.
    cur.execute(
        f"UPDATE {CRM_DB}.contacts "
        f"SET `{MATCH_METHOD_COL}` = 'name_zip' "
        f"WHERE vf_state_voter_id IS NOT NULL AND `{MATCH_METHOD_COL}` IS NULL")
    labelled = cur.rowcount
    if labelled:
        print(f"  Retroactively labelled {labelled:,} existing matches as 'name_zip'")


def ensure_voter_indexes(cur):
    """Create idx_clean_h1 and idx_clean_h2 on voter_file if missing.

    These composite indexes are required for Pass A to be fast (~1s per pass
    on 13M rows) rather than doing a full-table scan (~10+ min).
    The donor pipeline (classify_boe_parties, load_cfb_contributions) uses
    the same h1/h2 columns but JOINs them differently — these indexes are
    exclusively for the CRM matching direction.
    """
    for idx_name, part_col in [
        ("idx_clean_h1", "clean_last_h1"),
        ("idx_clean_h2", "clean_last_h2"),
    ]:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'voter_file' AND INDEX_NAME = %s",
            (VOTER_DB, idx_name))
        if cur.fetchone()[0] == 0:
            print(f"  Creating {idx_name} on voter_file (first run only — ~60-120s)...")
            t0 = time.time()
            cur.execute(
                f"ALTER TABLE {VOTER_DB}.{VOTER_TBL} "
                f"ADD INDEX {idx_name} (`{part_col}`(50), clean_first(50), PrimaryZip)")
            print(f"  + {idx_name} created in {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# SET clause builder
# ---------------------------------------------------------------------------

def _build_set(columns, method_label):
    """Build the SET clause for a bulk UPDATE, including the match method label."""
    parts = [f"c.`{crm_col}` = v.`{vf_col}`" for vf_col, crm_col, _ in columns]
    parts.append(f"c.`{ENRICHED_AT_COL}` = NOW()")
    parts.append(f"c.`{MATCH_METHOD_COL}` = '{method_label}'")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Pass A — Hyphenated last name (h1 and h2)
# ---------------------------------------------------------------------------

def pass_hyphenated(cur, columns, dry_run=False):
    """Match via voter_file.clean_last_h1 or .clean_last_h2.

    Handles the common case where the voter is registered with a hyphenated
    surname (e.g. "Garcia-Lopez") but the CRM contact only has one half
    (e.g. "Garcia").  Uses separate index-backed queries for h1 and h2 —
    the OR equivalent would prevent index use.
    """
    print("\n  Pass A: Hyphenated last name (h1 / h2 vs CRM clean_last)")
    set_clause = _build_set(columns, "hyph_zip")
    total_matched = 0

    for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
        t0 = time.time()
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_hyph_match")
        cur.execute(f"""
            CREATE TEMPORARY TABLE _ext_hyph_match AS
            SELECT c.id AS contact_id,
                   (SELECT v.StateVoterId
                    FROM {VOTER_DB}.{VOTER_TBL} v
                    WHERE v.`{part_col}` = c.clean_last
                      AND v.clean_first  = c.clean_first
                      AND v.PrimaryZip   LIKE CONCAT(c.zip5, '%')
                      AND v.RegistrationStatus = 'Active/Registered'
                    ORDER BY v.RegistrationDate DESC
                    LIMIT 1
                   ) AS matched_svid
            FROM {CRM_DB}.contacts c
            WHERE c.vf_state_voter_id IS NULL
              AND c.`{ENRICHED_AT_COL}` IS NOT NULL
              AND c.clean_last  IS NOT NULL
              AND c.clean_first IS NOT NULL
              AND c.zip5 IS NOT NULL
        """)

        if not dry_run:
            cur.execute(f"""
                UPDATE {CRM_DB}.contacts c
                JOIN _ext_hyph_match m ON m.contact_id = c.id
                JOIN {VOTER_DB}.{VOTER_TBL} v ON v.StateVoterId = m.matched_svid
                SET {set_clause}
                WHERE m.matched_svid IS NOT NULL
            """)
            matched = cur.rowcount
        else:
            cur.execute("SELECT COUNT(*) FROM _ext_hyph_match WHERE matched_svid IS NOT NULL")
            matched = cur.fetchone()[0]

        cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_hyph_match")
        print(f"    {part_label}: {matched:,} contacts matched  ({time.time()-t0:.1f}s)")
        total_matched += matched

    print(f"    Pass A total: {total_matched:,}")
    return total_matched


# ---------------------------------------------------------------------------
# Pass B — First-word first name (unique match only)
# ---------------------------------------------------------------------------

def pass_first_word(cur, columns, dry_run=False):
    """Match when CRM first_name has multiple words but voter uses only the first.

    Example:  CRM "Mary Ann Smith"  →  first_word clean = "MARY"
              Voter registered as  →  clean_first = "MARY"  ← matches

    The safety mechanism: `IF(COUNT(*) = 1, MIN(StateVoterId), NULL)` in the
    correlated scalar subquery.  If more than one voter matches the same last
    name + first-word + zip, NULL is returned and no match is written — so a
    common name like "John Smith" with 3 registrations in the same zip will
    never produce an incorrect match.

    Only considers contacts whose first_name contains a space (multi-word),
    and only when the first-word version differs from their full clean_first
    (to skip contacts who already matched via the standard pass or have a
    single-word first name).
    """
    print("\n  Pass B: First-word first name (unique match only, multi-word CRM names)")
    t0 = time.time()
    set_clause = _build_set(columns, "first_word_zip")

    # The scalar subquery returns NULL whenever there's more than one candidate,
    # making this pass completely safe against false-positive ambiguity.
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_fw_match")
    cur.execute(f"""
        CREATE TEMPORARY TABLE _ext_fw_match AS
        SELECT c.id AS contact_id,
               (SELECT IF(COUNT(*) = 1, MIN(v.StateVoterId), NULL)
                FROM {VOTER_DB}.{VOTER_TBL} v
                WHERE v.clean_last  = c.clean_last
                  AND v.clean_first = REGEXP_REPLACE(
                        UPPER(SUBSTRING_INDEX(c.first_name, ' ', 1)),
                        '[^A-Z]', '')
                  AND v.PrimaryZip LIKE CONCAT(c.zip5, '%')
                  AND v.RegistrationStatus = 'Active/Registered'
               ) AS matched_svid
        FROM {CRM_DB}.contacts c
        WHERE c.vf_state_voter_id IS NULL
          AND c.`{ENRICHED_AT_COL}` IS NOT NULL
          AND c.clean_last  IS NOT NULL
          AND c.zip5 IS NOT NULL
          AND c.first_name  LIKE '% %'
          AND REGEXP_REPLACE(UPPER(SUBSTRING_INDEX(c.first_name, ' ', 1)), '[^A-Z]', '')
              != COALESCE(c.clean_first, '')
    """)

    if not dry_run:
        cur.execute(f"""
            UPDATE {CRM_DB}.contacts c
            JOIN _ext_fw_match m ON m.contact_id = c.id
            JOIN {VOTER_DB}.{VOTER_TBL} v ON v.StateVoterId = m.matched_svid
            SET {set_clause}
            WHERE m.matched_svid IS NOT NULL
        """)
        matched = cur.rowcount
    else:
        cur.execute("SELECT COUNT(*) FROM _ext_fw_match WHERE matched_svid IS NOT NULL")
        matched = cur.fetchone()[0]

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_fw_match")
    print(f"    Matched: {matched:,}  ({time.time()-t0:.1f}s)")
    return matched


# ---------------------------------------------------------------------------
# Pass C — Inactive / purged voter fallback
# ---------------------------------------------------------------------------

def pass_inactive(cur, columns, dry_run=False):
    """Match contacts using name+zip WITHOUT the RegistrationStatus='Active/Registered' filter.

    Some people in the CRM are registered voters whose registration has lapsed
    (moved, age-purged, etc.) but they still have a StateVoterId in the voter
    file.  This pass finds them.

    When both an active AND inactive record exist for the same name+zip (rare),
    the ORDER BY in the correlated subquery prefers the active one.
    """
    print("\n  Pass C: Inactive / purged voter fallback (name+zip, no status filter)")
    t0 = time.time()
    set_clause = _build_set(columns, "inactive_name_zip")

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_inactive_match")
    cur.execute(f"""
        CREATE TEMPORARY TABLE _ext_inactive_match AS
        SELECT c.id AS contact_id,
               (SELECT v.StateVoterId
                FROM {VOTER_DB}.{VOTER_TBL} v
                WHERE v.clean_last  = c.clean_last
                  AND v.clean_first = c.clean_first
                  AND v.PrimaryZip  LIKE CONCAT(c.zip5, '%')
                ORDER BY
                  CASE v.RegistrationStatus
                    WHEN 'Active/Registered' THEN 0
                    ELSE 1
                  END,
                  v.RegistrationDate DESC
                LIMIT 1
               ) AS matched_svid
        FROM {CRM_DB}.contacts c
        WHERE c.vf_state_voter_id IS NULL
          AND c.`{ENRICHED_AT_COL}` IS NOT NULL
          AND c.clean_last  IS NOT NULL
          AND c.clean_first IS NOT NULL
          AND c.zip5 IS NOT NULL
    """)

    if not dry_run:
        cur.execute(f"""
            UPDATE {CRM_DB}.contacts c
            JOIN _ext_inactive_match m ON m.contact_id = c.id
            JOIN {VOTER_DB}.{VOTER_TBL} v ON v.StateVoterId = m.matched_svid
            SET {set_clause}
            WHERE m.matched_svid IS NOT NULL
        """)
        matched = cur.rowcount
    else:
        cur.execute("SELECT COUNT(*) FROM _ext_inactive_match WHERE matched_svid IS NOT NULL")
        matched = cur.fetchone()[0]

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _ext_inactive_match")
    print(f"    Matched: {matched:,}  ({time.time()-t0:.1f}s)")
    return matched


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def show_stats(cur):
    cur.execute(f"SELECT COUNT(*) FROM {CRM_DB}.contacts")
    total = cur.fetchone()[0]
    cur.execute(
        f"SELECT COUNT(*) FROM {CRM_DB}.contacts WHERE vf_state_voter_id IS NOT NULL")
    matched = cur.fetchone()[0]
    pct = matched / total * 100 if total else 0

    print(f"\n  Extended Match Stats")
    print(f"  {'='*45}")
    print(f"  Total contacts:    {total:,}")
    print(f"  Matched to voter:  {matched:,}  ({pct:.1f}%)")
    print(f"  Unmatched:         {total - matched:,}")

    # Check if method column exists before querying it
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{CRM_DB}' AND TABLE_NAME = 'contacts' "
        f"AND COLUMN_NAME = '{MATCH_METHOD_COL}'")
    has_col = cur.fetchone()[0] > 0

    if has_col:
        cur.execute(f"""
            SELECT COALESCE(`{MATCH_METHOD_COL}`, 'unlabelled') AS method,
                   COUNT(*) AS cnt
            FROM {CRM_DB}.contacts
            WHERE vf_state_voter_id IS NOT NULL
            GROUP BY method ORDER BY cnt DESC
        """)
        rows = cur.fetchall()
        if rows:
            print(f"\n  By match method:")
            for method, cnt in rows:
                pct_m = cnt / matched * 100 if matched else 0
                print(f"    {method:<25}  {cnt:>6,}  ({pct_m:.1f}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extended high-confidence CRM→voter matching (3 additional passes)"
    )
    parser.add_argument("--stats",   action="store_true",
                        help="Show current match-method breakdown only (no matching)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count potential matches without writing any rows")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  CRM Extended Match — additional high-confidence passes")
    print(f"{'='*60}\n")

    conn = connect()
    cur = conn.cursor()
    cur.execute("SET SESSION innodb_lock_wait_timeout = 600")

    columns = discover_voter_columns(cur)
    print(f"  Voter columns available: {len(columns)}")

    if args.stats:
        show_stats(cur)
        conn.close()
        return

    print()
    ensure_match_method_col(cur)
    ensure_voter_indexes(cur)

    if args.dry_run:
        print("\n  [DRY RUN — counts only, no rows written]\n")

    t_total = time.time()

    a = pass_hyphenated(cur, columns, dry_run=args.dry_run)
    b = pass_first_word(cur, columns, dry_run=args.dry_run)
    c = pass_inactive(cur, columns, dry_run=args.dry_run)

    mode = "would match" if args.dry_run else "new matches"
    print(f"\n  {'='*45}")
    print(f"  Pass A (hyphenated name):    {a:>5,}")
    print(f"  Pass B (first-word name):    {b:>5,}")
    print(f"  Pass C (inactive voter):     {c:>5,}")
    print(f"  Total {mode}:    {a+b+c:>5,}")
    print(f"  Elapsed: {time.time()-t_total:.1f}s")

    if not args.dry_run:
        show_stats(cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
