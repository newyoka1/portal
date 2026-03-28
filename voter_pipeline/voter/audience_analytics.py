#!/usr/bin/env python3
"""
Audience Analytics Tool - Generate breakdowns by district, ethnicity, and turnout level

Features:
- Filter by specific LD, SD, or CD
- Break down ANY Causeway audience by ethnicity
- Create HT/MT versions of each audience (not just grouped ones)
- Export results to CSV

Usage examples:
  python audience_analytics.py --statewide                    # All audiences, all districts
  python audience_analytics.py --ld 63                        # Filter to LD 63 only
  python audience_analytics.py --sd "SD 05"                   # Filter to SD 05 only
  python audience_analytics.py --cd "CD 03"                   # Filter to CD 03 only
  python audience_analytics.py --audience "HT HARD GOP"       # Specific audience breakdown
  python audience_analytics.py --ld 63 --ethnicity            # LD 63 with ethnicity breakdown
  python audience_analytics.py --export-csv                   # Export all results to CSV
"""

import os
import sys
import argparse
import pymysql
from pathlib import Path
import csv
from typing import Optional

# MySQL Config (match pipeline.py)
DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )


def get_all_audiences(conn):
    """Get list of all unique audiences from the bridge table"""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT audience FROM voter_audience_bridge ORDER BY audience;")
        return [row[0] for row in cur.fetchall()]


def get_district_filter(ld: Optional[str] = None, sd: Optional[str] = None, cd: Optional[str] = None,
                       table_alias: str = "f") -> tuple:
    """Build WHERE clause for district filtering with table alias"""
    conditions = []
    params = []

    if ld:
        conditions.append(f"{table_alias}.LDName = %s")
        params.append(ld)
    if sd:
        conditions.append(f"{table_alias}.SDName = %s")
        params.append(sd)
    if cd:
        conditions.append(f"{table_alias}.CDName = %s")
        params.append(cd)

    if conditions:
        return " AND " + " AND ".join(conditions), params
    return "", []


def audience_counts_by_district(conn, audience: str, district_type: str = "LD",
                                ld: Optional[str] = None, sd: Optional[str] = None,
                                cd: Optional[str] = None):
    """Count voters in a specific audience, broken down by district"""

    district_col_map = {"LD": "LDName", "SD": "SDName", "CD": "CDName"}
    district_col = district_col_map[district_type]

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    query = f"""
        SELECT
            f.{district_col} AS district,
            COUNT(*) AS voters
        FROM voter_file f
        INNER JOIN voter_audience_bridge b ON b.StateVoterId = f.StateVoterId
        WHERE b.audience = %s {district_filter}
        GROUP BY f.{district_col}
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query, [audience] + filter_params)
        return cur.fetchall()


def audience_ethnicity_breakdown(conn, audience: str,
                                 ld: Optional[str] = None, sd: Optional[str] = None,
                                 cd: Optional[str] = None):
    """Get ethnicity breakdown for a specific audience with optional district filter"""

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    query = f"""
        SELECT
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        FROM voter_file f
        INNER JOIN voter_audience_bridge b ON b.StateVoterId = f.StateVoterId
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
        WHERE b.audience = %s {district_filter}
        GROUP BY e.dominant_ethnicity
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query, [audience] + filter_params)
        return cur.fetchall()


def get_turnout_variants(conn, base_audience_pattern: str,
                         ld: Optional[str] = None, sd: Optional[str] = None,
                         cd: Optional[str] = None):
    """Get HT/MT/LT variants of an audience pattern (e.g., 'HARD GOP' -> HT/MT/LT versions)"""

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    results = {}
    for turnout in ["HT", "MT", "LT"]:
        pattern = f"{turnout} {base_audience_pattern}%"

        query = f"""
            SELECT COUNT(DISTINCT f.StateVoterId) AS voters
            FROM voter_file f
            INNER JOIN voter_audience_bridge b ON b.StateVoterId = f.StateVoterId
            WHERE b.audience LIKE %s {district_filter};
        """

        with conn.cursor() as cur:
            cur.execute(query, [pattern] + filter_params)
            count = cur.fetchone()[0]
            results[turnout] = count

    return results


def unmatched_voters_ethnicity_breakdown(conn, ld: Optional[str] = None, sd: Optional[str] = None,
                                         cd: Optional[str] = None):
    """Get ethnicity breakdown for voters with NO audience match"""

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    query = f"""
        SELECT
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        FROM voter_file f
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
        WHERE (f.origin IS NULL OR TRIM(f.origin) = '') {district_filter}
        GROUP BY e.dominant_ethnicity
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query, filter_params)
        return cur.fetchall()


def matched_voters_ethnicity_breakdown(conn, ld: Optional[str] = None, sd: Optional[str] = None,
                                       cd: Optional[str] = None):
    """Get ethnicity breakdown for voters WITH audience match (all matched voters combined)"""

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    query = f"""
        SELECT
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        FROM voter_file f
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName)
        WHERE (f.origin IS NOT NULL AND TRIM(f.origin) != '') {district_filter}
        GROUP BY e.dominant_ethnicity
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query, filter_params)
        return cur.fetchall()


def get_total_voters_count(conn, ld: Optional[str] = None, sd: Optional[str] = None,
                           cd: Optional[str] = None):
    """Get total voter count with optional district filter"""

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    query = f"""
        SELECT COUNT(*) AS total_voters
        FROM voter_file f
        WHERE 1=1 {district_filter};
    """

    with conn.cursor() as cur:
        cur.execute(query, filter_params)
        return cur.fetchone()[0]


def list_all_audiences_with_counts(conn, ld: Optional[str] = None, sd: Optional[str] = None,
                                   cd: Optional[str] = None, include_unmatched: bool = True):
    """List all unique audiences with voter counts, optionally filtered by district

    Args:
        include_unmatched: If True, adds a row for voters with no audience match
    """

    district_filter, filter_params = get_district_filter(ld, sd, cd, table_alias="f")

    # Get matched audiences
    query = f"""
        SELECT
            b.audience,
            COUNT(DISTINCT f.StateVoterId) AS voters
        FROM voter_audience_bridge b
        INNER JOIN voter_file f ON f.StateVoterId = b.StateVoterId
        WHERE 1=1 {district_filter}
        GROUP BY b.audience
        ORDER BY b.audience;
    """

    with conn.cursor() as cur:
        cur.execute(query, filter_params)
        results = list(cur.fetchall())

    # Add unmatched voters count
    if include_unmatched:
        unmatched_query = f"""
            SELECT COUNT(*) AS voters
            FROM voter_file f
            WHERE (f.origin IS NULL OR TRIM(f.origin) = '') {district_filter};
        """

        with conn.cursor() as cur:
            cur.execute(unmatched_query, filter_params)
            unmatched_count = cur.fetchone()[0]

        if unmatched_count > 0:
            results.append(("** NO AUDIENCE MATCH **", unmatched_count))

    return results


def export_audience_summary_csv(conn, ld: Optional[str] = None, sd: Optional[str] = None,
                                cd: Optional[str] = None):
    """Export comprehensive audience summary to CSV"""

    filter_suffix = ""
    if ld:
        filter_suffix = f"_LD_{ld.replace(' ', '_')}"
    elif sd:
        filter_suffix = f"_SD_{sd.replace(' ', '_')}"
    elif cd:
        filter_suffix = f"_CD_{cd.replace(' ', '_')}"

    # 1. All audiences with counts
    audiences = list_all_audiences_with_counts(conn, ld, sd, cd)
    filename = OUTPUT_DIR / f"audience_summary{filter_suffix}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Audience", "Voters"])
        writer.writerows(audiences)
    print(f"OK Exported: {filename}")

    # 2. Ethnicity breakdown for each audience (top 10 audiences)
    top_audiences = [aud for aud, _ in audiences[:10] if aud != "** NO AUDIENCE MATCH **"]
    for audience in top_audiences:
        eth_data = audience_ethnicity_breakdown(conn, audience, ld, sd, cd)
        safe_name = audience.replace(" ", "_").replace("/", "-")[:50]
        filename = OUTPUT_DIR / f"ethnicity_{safe_name}{filter_suffix}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Ethnicity", "Voters", "Percentage"])
            writer.writerows(eth_data)
        print(f"OK Exported: {filename}")

    # 3. Ethnicity breakdown for unmatched voters
    unmatched_eth = unmatched_voters_ethnicity_breakdown(conn, ld, sd, cd)
    if unmatched_eth:
        filename = OUTPUT_DIR / f"ethnicity_NO_AUDIENCE_MATCH{filter_suffix}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Ethnicity", "Voters", "Percentage"])
            writer.writerows(unmatched_eth)
        print(f"OK Exported: {filename}")

    # 4. Ethnicity breakdown for all matched voters (combined)
    matched_eth = matched_voters_ethnicity_breakdown(conn, ld, sd, cd)
    if matched_eth:
        filename = OUTPUT_DIR / f"ethnicity_ALL_MATCHED{filter_suffix}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Ethnicity", "Voters", "Percentage"])
            writer.writerows(matched_eth)
        print(f"OK Exported: {filename}")

    # 5. Matched vs Unmatched comparison
    if matched_eth and unmatched_eth:
        matched_dict = {eth: (voters, pct) for eth, voters, pct in matched_eth}
        unmatched_dict = {eth: (voters, pct) for eth, voters, pct in unmatched_eth}
        all_ethnicities = sorted(set(matched_dict.keys()) | set(unmatched_dict.keys()))

        filename = OUTPUT_DIR / f"ethnicity_MATCHED_VS_UNMATCHED{filter_suffix}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Ethnicity", "Matched_Voters", "Matched_Pct", "Unmatched_Voters", "Unmatched_Pct", "Difference_Pct"])

            for ethnicity in all_ethnicities:
                matched_voters, matched_pct = matched_dict.get(ethnicity, (0, 0.0))
                unmatched_voters, unmatched_pct = unmatched_dict.get(ethnicity, (0, 0.0))
                diff = matched_pct - unmatched_pct
                writer.writerow([ethnicity, matched_voters, matched_pct, unmatched_voters, unmatched_pct, diff])

        print(f"OK Exported: {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze voter audiences by district, ethnicity, and turnout level",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # District filters (mutually exclusive)
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument("--ld", help="Filter by Legislative District (e.g., '063' or 'LD 063')")
    filter_group.add_argument("--sd", help="Filter by State Senate District (e.g., 'SD 05')")
    filter_group.add_argument("--cd", help="Filter by Congressional District (e.g., 'CD 03')")
    filter_group.add_argument("--statewide", action="store_true", help="Statewide analysis (no district filter)")

    # Analysis options
    parser.add_argument("--audience", help="Analyze specific audience (e.g., 'HT HARD GOP INDV NYS_001.csv')")
    parser.add_argument("--ethnicity", action="store_true", help="Include ethnicity breakdown")
    parser.add_argument("--turnout-split", help="Get HT/MT/LT breakdown for audience pattern (e.g., 'HARD GOP')")
    parser.add_argument("--list-audiences", action="store_true", help="List all available audiences with counts")
    parser.add_argument("--unmatched", action="store_true", help="Analyze voters with NO audience match")
    parser.add_argument("--matched-vs-unmatched", action="store_true",
                       help="Compare ethnicity: matched vs unmatched voters")
    parser.add_argument("--export-csv", action="store_true", help="Export all results to CSV files")
    parser.add_argument("--district-breakdown", choices=["LD", "SD", "CD"],
                       help="Break down audience by district type")

    args = parser.parse_args()

    conn = connect_db()

    try:
        # Determine district filter
        district_filter_desc = "STATEWIDE"
        if args.ld:
            district_filter_desc = f"LD {args.ld}"
        elif args.sd:
            district_filter_desc = f"SD {args.sd}"
        elif args.cd:
            district_filter_desc = f"CD {args.cd}"

        print("=" * 80)
        print(f"AUDIENCE ANALYTICS - {district_filter_desc}")
        print("=" * 80)

        # List all audiences
        if args.list_audiences or not any([args.audience, args.turnout_split, args.export_csv]):
            # Get summary stats
            total_voters = get_total_voters_count(conn, args.ld, args.sd, args.cd)
            audiences = list_all_audiences_with_counts(conn, args.ld, args.sd, args.cd)

            # Calculate matched vs unmatched
            matched_voters = sum(count for aud, count in audiences if aud != "** NO AUDIENCE MATCH **")
            unmatched_voters = next((count for aud, count in audiences if aud == "** NO AUDIENCE MATCH **"), 0)

            print(f"\nSummary ({district_filter_desc}):")
            print("-" * 80)
            print(f"  Total voters:           {total_voters:>10,}")
            print(f"  Matched to audiences:   {matched_voters:>10,} ({matched_voters/total_voters*100:>5.1f}%)")
            print(f"  No audience match:      {unmatched_voters:>10,} ({unmatched_voters/total_voters*100:>5.1f}%)")

            print(f"\nAll Audiences ({district_filter_desc}):")
            print("-" * 80)
            for aud, count in audiences:
                if aud == "** NO AUDIENCE MATCH **":
                    print(f"  {aud:60s}  {count:>10,} voters  [!]")
                else:
                    print(f"  {aud:60s}  {count:>10,} voters")

            matched_audience_count = len([a for a in audiences if a[0] != "** NO AUDIENCE MATCH **"])
            print(f"\nTotal unique audiences: {matched_audience_count}")

        # Specific audience analysis
        if args.audience:
            print(f"\n{args.audience} - {district_filter_desc}")
            print("-" * 80)

            # District breakdown
            if args.district_breakdown:
                print(f"\nBreakdown by {args.district_breakdown}:")
                results = audience_counts_by_district(
                    conn, args.audience, args.district_breakdown, args.ld, args.sd, args.cd
                )
                for district, voters in results:
                    print(f"  {district:20s}  {voters:>10,} voters")

            # Ethnicity breakdown
            if args.ethnicity:
                print(f"\nEthnicity Breakdown:")
                eth_results = audience_ethnicity_breakdown(conn, args.audience, args.ld, args.sd, args.cd)
                for ethnicity, voters, pct in eth_results:
                    print(f"  {ethnicity:12s}  {voters:>10,} voters ({pct:>5.1f}%)")

        # Turnout split analysis
        if args.turnout_split:
            print(f"\nTurnout Split for '{args.turnout_split}' - {district_filter_desc}")
            print("-" * 80)
            turnout_counts = get_turnout_variants(conn, args.turnout_split, args.ld, args.sd, args.cd)
            for level, count in turnout_counts.items():
                print(f"  {level:3s}  {count:>10,} voters")

        # Unmatched voters analysis
        if args.unmatched:
            print(f"\nUnmatched Voters (NO AUDIENCE) - {district_filter_desc}")
            print("-" * 80)

            total_voters = get_total_voters_count(conn, args.ld, args.sd, args.cd)
            unmatched_count = next(
                (count for aud, count in list_all_audiences_with_counts(conn, args.ld, args.sd, args.cd)
                 if aud == "** NO AUDIENCE MATCH **"), 0
            )

            print(f"\nTotal unmatched voters: {unmatched_count:,} ({unmatched_count/total_voters*100:.1f}% of all voters)")

            if args.ethnicity or not any([args.district_breakdown]):
                print(f"\nEthnicity Breakdown (Unmatched):")
                eth_results = unmatched_voters_ethnicity_breakdown(conn, args.ld, args.sd, args.cd)
                for ethnicity, voters, pct in eth_results:
                    print(f"  {ethnicity:12s}  {voters:>10,} voters ({pct:>5.1f}%)")

        # Matched vs Unmatched comparison
        if args.matched_vs_unmatched:
            print(f"\nMatched vs Unmatched Ethnicity Comparison - {district_filter_desc}")
            print("=" * 80)

            total_voters = get_total_voters_count(conn, args.ld, args.sd, args.cd)
            audiences = list_all_audiences_with_counts(conn, args.ld, args.sd, args.cd)
            matched_count = sum(count for aud, count in audiences if aud != "** NO AUDIENCE MATCH **")
            unmatched_count = next((count for aud, count in audiences if aud == "** NO AUDIENCE MATCH **"), 0)

            print(f"\nOverall Summary:")
            print("-" * 80)
            print(f"  Total voters:        {total_voters:>10,}")
            print(f"  Matched voters:      {matched_count:>10,} ({matched_count/total_voters*100:>5.1f}%)")
            print(f"  Unmatched voters:    {unmatched_count:>10,} ({unmatched_count/total_voters*100:>5.1f}%)")

            # Get ethnicity for both groups
            matched_eth = matched_voters_ethnicity_breakdown(conn, args.ld, args.sd, args.cd)
            unmatched_eth = unmatched_voters_ethnicity_breakdown(conn, args.ld, args.sd, args.cd)

            # Build combined comparison table
            matched_dict = {eth: (voters, pct) for eth, voters, pct in matched_eth}
            unmatched_dict = {eth: (voters, pct) for eth, voters, pct in unmatched_eth}

            all_ethnicities = sorted(set(matched_dict.keys()) | set(unmatched_dict.keys()))

            print(f"\nEthnicity Comparison:")
            print("-" * 80)
            print(f"  {'Ethnicity':<12s}  {'Matched':>15s}  {'Unmatched':>15s}  {'Difference':>12s}")
            print("-" * 80)

            for ethnicity in all_ethnicities:
                matched_voters, matched_pct = matched_dict.get(ethnicity, (0, 0.0))
                unmatched_voters, unmatched_pct = unmatched_dict.get(ethnicity, (0, 0.0))
                # Convert to float for arithmetic
                matched_pct_f = float(matched_pct) if matched_pct else 0.0
                unmatched_pct_f = float(unmatched_pct) if unmatched_pct else 0.0
                diff = matched_pct_f - unmatched_pct_f

                print(f"  {ethnicity:<12s}  {matched_voters:>8,} ({matched_pct_f:>4.1f}%)  "
                      f"{unmatched_voters:>8,} ({unmatched_pct_f:>4.1f}%)  {diff:>+6.1f}%")

        # CSV export
        if args.export_csv:
            print(f"\nExporting to CSV ({district_filter_desc})...")
            print("-" * 80)
            export_audience_summary_csv(conn, args.ld, args.sd, args.cd)
            print(f"\nOK All files exported to: {OUTPUT_DIR}")

        print("\n" + "=" * 80)

    finally:
        conn.close()


if __name__ == "__main__":
    main()