#!/usr/bin/env python3
"""
Derived Voter Enrichment
========================
Computes analytical columns on voter_file from existing data:

  A. Registration Recency  – registration_months, is_new_registrant
  B. Turnout Score         – turnout_score, voter_engagement
  C. Donor Cross-Level     – total_all_donations, is_multi_level_donor, donor_party_lean
  D. Household Clustering  – household_size, household_party_mix, household_has_donor

Called by: python main.py enrich-derived [--refresh/--no-refresh]
"""

import os, sys, time, argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

VOTER_TABLE = "voter_file"

# ── Column definitions ────────────────────────────────────────────────────────

REGISTRATION_COLUMNS = [
    ("registration_months", "INT           DEFAULT NULL"),
    ("is_new_registrant",   "TINYINT(1)    DEFAULT NULL"),
]

TURNOUT_COLUMNS = [
    ("turnout_score",       "DECIMAL(5,2)  DEFAULT NULL"),
    ("voter_engagement",    "VARCHAR(20)   DEFAULT NULL"),
]

DONOR_CROSS_COLUMNS = [
    ("total_all_donations",  "DECIMAL(14,2) DEFAULT NULL"),
    ("is_multi_level_donor", "TINYINT(1)    DEFAULT NULL"),
    ("donor_party_lean",     "VARCHAR(20)   DEFAULT NULL"),
]

HOUSEHOLD_COLUMNS = [
    ("household_size",       "INT           DEFAULT NULL"),
    ("household_party_mix",  "VARCHAR(20)   DEFAULT NULL"),
    ("household_has_donor",  "TINYINT(1)    DEFAULT NULL"),
]

ALL_COLUMNS = REGISTRATION_COLUMNS + TURNOUT_COLUMNS + DONOR_CROSS_COLUMNS + HOUSEHOLD_COLUMNS


# ── Helpers ───────────────────────────────────────────────────────────────────

def connect():
    return get_conn(database="nys_voter_tagging", autocommit=True)


def ensure_columns(cur, columns):
    """Add any missing columns to voter_file."""
    added = 0
    for col_name, col_def in columns:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = %s
              AND COLUMN_NAME  = %s
        """, (VOTER_TABLE, col_name))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE {VOTER_TABLE} ADD COLUMN {col_name} {col_def}")
            added += 1
    return added


def run_batched(conn, cur, label, sql, params=None, batch_size=50000):
    """Run UPDATE in batches; single-pass for JOIN updates (MySQL limitation)."""
    is_join = " JOIN " in sql.upper()
    t = time.time()
    if is_join:
        print(f"  Running (single pass, JOIN): {label} ...")
        cur.execute(sql, params or ())
        elapsed = time.time() - t
        print(f"    -> {cur.rowcount:,} rows affected ({elapsed:.1f}s)")
    else:
        batch_sql = sql.rstrip().rstrip(";") + f" LIMIT {batch_size}"
        total = 0
        print(f"  Running (batched {batch_size:,}/batch): {label} ...")
        while True:
            cur.execute(batch_sql, params or ())
            affected = cur.rowcount
            total += affected
            if affected < batch_size:
                break
            print(f"    ... {total:,} rows so far ({time.time()-t:.1f}s)")
        elapsed = time.time() - t
        print(f"    -> {total:,} rows total ({elapsed:.1f}s)")


def clear_columns(cur, columns):
    """NULL out a set of columns (for --refresh)."""
    col_names = [c for c, _ in columns]
    set_clause = ", ".join(f"{c} = NULL" for c in col_names)
    where_clause = " OR ".join(f"{c} IS NOT NULL" for c in col_names)
    cur.execute(f"UPDATE {VOTER_TABLE} SET {set_clause} WHERE {where_clause}")
    print(f"    Cleared {cur.rowcount:,} rows")


# ── Enrichment A: Registration Recency ────────────────────────────────────────

def enrich_registration(conn, cur, refresh):
    print("\n[A] Registration Recency")
    added = ensure_columns(cur, REGISTRATION_COLUMNS)
    if added:
        print(f"  Added {added} column(s)")

    if refresh:
        print("  Clearing existing values...")
        clear_columns(cur, REGISTRATION_COLUMNS)

    run_batched(conn, cur, "Registration months + new-registrant flag",
        f"""UPDATE {VOTER_TABLE}
            SET registration_months = TIMESTAMPDIFF(MONTH, RegDate, CURDATE()),
                is_new_registrant   = IF(TIMESTAMPDIFF(MONTH, RegDate, CURDATE()) <= 24, 1, 0)
            WHERE RegDate IS NOT NULL
              AND registration_months IS NULL""")


# ── Enrichment B: Turnout Score ───────────────────────────────────────────────

def enrich_turnout(conn, cur, refresh):
    print("\n[B] Turnout Score")
    added = ensure_columns(cur, TURNOUT_COLUMNS)
    if added:
        print(f"  Added {added} column(s)")

    if refresh:
        print("  Clearing existing values...")
        clear_columns(cur, TURNOUT_COLUMNS)

    # Generals weighted 2x: max score = (5*2 + 5) = 15 -> 100%
    # Score expression computed once in a derived subquery to avoid repeating it
    run_batched(conn, cur, "Turnout score from GeneralFrequency + PrimaryFrequency",
        f"""UPDATE {VOTER_TABLE} v
            JOIN (
                SELECT StateVoterId,
                    ROUND(
                        (CAST(COALESCE(GeneralFrequency, '0') AS UNSIGNED) * 2
                         + CAST(COALESCE(PrimaryFrequency, '0') AS UNSIGNED)
                        ) / 15.0 * 100, 2) AS raw_score
                FROM {VOTER_TABLE}
                WHERE turnout_score IS NULL
            ) s ON v.StateVoterId = s.StateVoterId
            SET v.turnout_score    = s.raw_score,
                v.voter_engagement = CASE
                    WHEN s.raw_score >= 80 THEN 'Super Voter'
                    WHEN s.raw_score >= 50 THEN 'Regular'
                    WHEN s.raw_score >= 20 THEN 'Occasional'
                    ELSE 'Rare/Never'
                END""")


# ── Enrichment C: Donor Cross-Level Analysis ─────────────────────────────────

def enrich_donor_cross(conn, cur, refresh):
    print("\n[C] Donor Cross-Level Analysis")
    added = ensure_columns(cur, DONOR_CROSS_COLUMNS)
    if added:
        print(f"  Added {added} column(s)")

    if refresh:
        print("  Clearing existing values...")
        clear_columns(cur, DONOR_CROSS_COLUMNS)

    # Total across all donation sources
    run_batched(conn, cur, "Total all donations",
        f"""UPDATE {VOTER_TABLE}
            SET total_all_donations = COALESCE(boe_total_amt, 0)
                                    + COALESCE(national_total_amount, 0)
                                    + COALESCE(cfb_total_amt, 0)
            WHERE total_all_donations IS NULL
              AND (boe_total_amt IS NOT NULL
                   OR national_total_amount IS NOT NULL
                   OR cfb_total_amt IS NOT NULL)""")

    # Multi-level donor: donates at 2+ levels (BOE, national, CFB)
    run_batched(conn, cur, "Multi-level donor flag",
        f"""UPDATE {VOTER_TABLE}
            SET is_multi_level_donor = IF(
                (IF(boe_total_amt IS NOT NULL AND boe_total_amt > 0, 1, 0)
                 + IF(national_total_amount IS NOT NULL AND national_total_amount > 0, 1, 0)
                 + IF(cfb_total_amt IS NOT NULL AND cfb_total_amt > 0, 1, 0)) >= 2,
                1, 0)
            WHERE is_multi_level_donor IS NULL
              AND total_all_donations IS NOT NULL
              AND total_all_donations > 0""")

    # Party lean from D vs R donation totals (BOE + national; CFB has no party split)
    run_batched(conn, cur, "Donor party lean",
        f"""UPDATE {VOTER_TABLE}
            SET donor_party_lean = CASE
                WHEN (COALESCE(boe_total_D_amt, 0) + COALESCE(national_democratic_amount, 0)) >
                     (COALESCE(boe_total_R_amt, 0) + COALESCE(national_republican_amount, 0)) * 2
                    THEN 'Strong D'
                WHEN (COALESCE(boe_total_D_amt, 0) + COALESCE(national_democratic_amount, 0)) >
                     (COALESCE(boe_total_R_amt, 0) + COALESCE(national_republican_amount, 0))
                    THEN 'Lean D'
                WHEN (COALESCE(boe_total_R_amt, 0) + COALESCE(national_republican_amount, 0)) >
                     (COALESCE(boe_total_D_amt, 0) + COALESCE(national_democratic_amount, 0)) * 2
                    THEN 'Strong R'
                WHEN (COALESCE(boe_total_R_amt, 0) + COALESCE(national_republican_amount, 0)) >
                     (COALESCE(boe_total_D_amt, 0) + COALESCE(national_democratic_amount, 0))
                    THEN 'Lean R'
                ELSE 'Mixed'
            END
            WHERE donor_party_lean IS NULL
              AND total_all_donations IS NOT NULL
              AND total_all_donations > 0""")


# ── Enrichment D: Household Clustering ────────────────────────────────────────

def enrich_household(conn, cur, refresh):
    print("\n[D] Household Clustering")
    added = ensure_columns(cur, HOUSEHOLD_COLUMNS)
    if added:
        print(f"  Added {added} column(s)")

    if refresh:
        print("  Clearing existing values...")
        clear_columns(cur, HOUSEHOLD_COLUMNS)

    # Build temp table with household aggregates
    print("  Building household stats temp table...")
    t = time.time()
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _hh_stats")
    cur.execute(f"""
        CREATE TEMPORARY TABLE _hh_stats (
            PRIMARY KEY (HHCode)
        ) ENGINE=InnoDB
        AS SELECT
            HHCode,
            COUNT(*)                             AS hh_size,
            COUNT(DISTINCT OfficialParty)         AS party_count,
            MAX(CASE WHEN COALESCE(boe_total_amt, 0)
                        + COALESCE(national_total_amount, 0)
                        + COALESCE(cfb_total_amt, 0) > 0
                     THEN 1 ELSE 0 END)           AS has_donor
        FROM {VOTER_TABLE}
        WHERE HHCode IS NOT NULL AND HHCode != ''
        GROUP BY HHCode
    """)
    print(f"    -> {cur.rowcount:,} households ({time.time()-t:.1f}s)")

    # JOIN update
    run_batched(conn, cur, "Household clustering (JOIN)",
        f"""UPDATE {VOTER_TABLE} v
            JOIN _hh_stats h ON v.HHCode = h.HHCode
            SET v.household_size      = h.hh_size,
                v.household_party_mix = IF(h.party_count > 1, 'Mixed', 'Uniform'),
                v.household_has_donor = h.has_donor
            WHERE v.household_size IS NULL""")

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _hh_stats")


# ── Summary ──────────────────────────────────────────────────────────────────

def print_summary(cur):
    print("\n" + "=" * 60)
    print("  Enrichment Summary")
    print("=" * 60)

    # Registration
    cur.execute(f"""
        SELECT COUNT(*) AS total,
               SUM(is_new_registrant = 1) AS new_reg,
               AVG(registration_months) AS avg_months
        FROM {VOTER_TABLE}
        WHERE registration_months IS NOT NULL
    """)
    total, new_reg, avg_months = cur.fetchone()
    print(f"\n  Registration Recency:")
    print(f"    Voters with RegDate:   {int(total or 0):>12,}")
    print(f"    New registrants (24m): {int(new_reg or 0):>12,}")
    print(f"    Avg months registered: {float(avg_months or 0):>12.1f}")

    # Turnout
    cur.execute(f"""
        SELECT voter_engagement, COUNT(*) AS cnt
        FROM {VOTER_TABLE}
        WHERE voter_engagement IS NOT NULL
        GROUP BY voter_engagement
        ORDER BY cnt DESC
    """)
    rows = cur.fetchall()
    print(f"\n  Voter Engagement:")
    for eng, cnt in rows:
        print(f"    {eng:<20} {int(cnt):>12,}")

    # Donor cross-level
    cur.execute(f"""
        SELECT COUNT(*) AS donors,
               SUM(is_multi_level_donor = 1) AS multi,
               SUM(donor_party_lean = 'Strong D') AS sd,
               SUM(donor_party_lean = 'Strong R') AS sr
        FROM {VOTER_TABLE}
        WHERE total_all_donations IS NOT NULL AND total_all_donations > 0
    """)
    donors, multi, sd, sr = cur.fetchone()
    print(f"\n  Donor Cross-Level:")
    print(f"    Total donors:          {int(donors or 0):>12,}")
    print(f"    Multi-level donors:    {int(multi or 0):>12,}")
    print(f"    Strong D donors:       {int(sd or 0):>12,}")
    print(f"    Strong R donors:       {int(sr or 0):>12,}")

    # Household
    cur.execute(f"""
        SELECT COUNT(DISTINCT HHCode) AS hh_count,
               AVG(household_size) AS avg_size,
               SUM(household_party_mix = 'Mixed') AS mixed_voters,
               SUM(household_has_donor = 1) AS donor_hh_voters
        FROM {VOTER_TABLE}
        WHERE household_size IS NOT NULL
    """)
    hh_count, avg_size, mixed, donor_hh = cur.fetchone()
    print(f"\n  Household Clustering:")
    print(f"    Households:            {int(hh_count or 0):>12,}")
    print(f"    Avg household size:    {float(avg_size or 0):>12.1f}")
    print(f"    Voters in mixed HH:   {int(mixed or 0):>12,}")
    print(f"    Voters in donor HH:   {int(donor_hh or 0):>12,}")

    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Derived voter enrichments")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--debug",   "-d", action="store_true")
    parser.add_argument("--quiet",   "-q", action="store_true")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--refresh",    dest="refresh", action="store_const", const=True,
                     default=None, help="Clear and recompute all derived columns")
    grp.add_argument("--no-refresh", dest="refresh", action="store_const", const=False,
                     help="Only compute where values are NULL (default behavior)")
    args = parser.parse_args()

    refresh = args.refresh is True

    print("=" * 60)
    print("  NYS Voter Tagging - Derived Enrichments")
    print("=" * 60)
    if refresh:
        print("  Mode: REFRESH (clear + recompute all)")
    else:
        print("  Mode: Incremental (fill NULLs only)")

    conn = connect()
    cur  = conn.cursor()

    enrich_registration(conn, cur, refresh)
    enrich_turnout(conn, cur, refresh)
    enrich_donor_cross(conn, cur, refresh)
    enrich_household(conn, cur, refresh)

    print_summary(cur)

    cur.close()
    conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
