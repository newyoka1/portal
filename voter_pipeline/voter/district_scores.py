#!/usr/bin/env python3
"""
District Competitiveness Scores
===============================
Builds a `district_scores` table aggregating voter registration, turnout,
and donor metrics by district (CD, SD, AD, ED).

Table: district_scores
  district_type   VARCHAR(10)   -- CD, SD, AD, ED
  district_name   VARCHAR(100)
  total_voters    INT
  dem_count       INT
  rep_count       INT
  other_count     INT
  partisan_lean   DECIMAL(5,2)  -- negative = Dem, positive = Rep
  competitiveness VARCHAR(20)   -- Safe D / Lean D / Tossup / Lean R / Safe R
  avg_turnout     DECIMAL(5,2)
  donor_pct       DECIMAL(5,2)
  updated_at      TIMESTAMP

Called by: python main.py district-scores
"""

import os, sys, time, argparse
from dotenv import load_dotenv
import pymysql

load_dotenv()

MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


def connect():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database="nys_voter_tagging",
        charset="utf8mb4", autocommit=True
    )


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS district_scores (
    district_type   VARCHAR(10)   NOT NULL,
    district_name   VARCHAR(100)  NOT NULL,
    total_voters    INT           NOT NULL DEFAULT 0,
    dem_count       INT           NOT NULL DEFAULT 0,
    rep_count       INT           NOT NULL DEFAULT 0,
    other_count     INT           NOT NULL DEFAULT 0,
    partisan_lean   DECIMAL(5,2)  DEFAULT NULL,
    competitiveness VARCHAR(20)   DEFAULT NULL,
    avg_turnout     DECIMAL(5,2)  DEFAULT NULL,
    donor_pct       DECIMAL(5,2)  DEFAULT NULL,
    updated_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (district_type, district_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# District type -> column name in voter_file
DISTRICT_TYPES = [
    ("CD", "CDName"),
    ("SD", "SDName"),
    ("AD", "ADName"),
    ("ED", "ElectionDistrict"),
]

AGGREGATE_SQL = """
INSERT INTO district_scores
    (district_type, district_name, total_voters, dem_count, rep_count, other_count,
     partisan_lean, competitiveness, avg_turnout, donor_pct)
SELECT
    %s AS district_type,
    {col} AS district_name,
    COUNT(*) AS total_voters,
    SUM(OfficialParty = 'DEM') AS dem_count,
    SUM(OfficialParty = 'REP') AS rep_count,
    SUM(OfficialParty NOT IN ('DEM', 'REP')) AS other_count,
    ROUND((SUM(OfficialParty = 'REP') - SUM(OfficialParty = 'DEM'))
          / COUNT(*) * 100, 2) AS partisan_lean,
    CASE
        WHEN ABS((SUM(OfficialParty = 'REP') - SUM(OfficialParty = 'DEM'))
                  / COUNT(*) * 100) < 5  THEN 'Tossup'
        WHEN (SUM(OfficialParty = 'REP') - SUM(OfficialParty = 'DEM'))
              / COUNT(*) * 100 < -15     THEN 'Safe D'
        WHEN (SUM(OfficialParty = 'REP') - SUM(OfficialParty = 'DEM'))
              / COUNT(*) * 100 < 0       THEN 'Lean D'
        WHEN (SUM(OfficialParty = 'REP') - SUM(OfficialParty = 'DEM'))
              / COUNT(*) * 100 > 15      THEN 'Safe R'
        ELSE 'Lean R'
    END AS competitiveness,
    ROUND(AVG(turnout_score), 2) AS avg_turnout,
    ROUND(SUM(CASE WHEN COALESCE(boe_total_amt, 0)
                       + COALESCE(national_total_amount, 0)
                       + COALESCE(cfb_total_amt, 0) > 0
                   THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2) AS donor_pct
FROM voter_file
WHERE {col} IS NOT NULL AND {col} != ''
GROUP BY {col}
ON DUPLICATE KEY UPDATE
    total_voters    = VALUES(total_voters),
    dem_count       = VALUES(dem_count),
    rep_count       = VALUES(rep_count),
    other_count     = VALUES(other_count),
    partisan_lean   = VALUES(partisan_lean),
    competitiveness = VALUES(competitiveness),
    avg_turnout     = VALUES(avg_turnout),
    donor_pct       = VALUES(donor_pct)
"""


def main():
    parser = argparse.ArgumentParser(description="Build district competitiveness scores")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--debug",   "-d", action="store_true")
    parser.add_argument("--quiet",   "-q", action="store_true")
    parser.parse_args()

    print("=" * 60)
    print("  NYS Voter Tagging - District Competitiveness Scores")
    print("=" * 60)

    conn = connect()
    cur  = conn.cursor()

    print("\n[1] Creating district_scores table...")
    cur.execute(CREATE_TABLE)

    for dtype, col in DISTRICT_TYPES:
        print(f"\n[{dtype}] Aggregating by {col}...")
        t = time.time()
        sql = AGGREGATE_SQL.format(col=col)
        cur.execute(sql, (dtype,))
        elapsed = time.time() - t
        print(f"  -> {cur.rowcount:,} districts ({elapsed:.1f}s)")

    # Summary
    print("\n" + "=" * 60)
    print("  District Scores Summary")
    print("=" * 60)
    cur.execute("""
        SELECT district_type, COUNT(*) AS districts,
               MIN(partisan_lean) AS most_dem,
               MAX(partisan_lean) AS most_rep,
               SUM(competitiveness = 'Tossup') AS tossups
        FROM district_scores
        GROUP BY district_type
        ORDER BY FIELD(district_type, 'CD', 'SD', 'AD', 'ED')
    """)
    print(f"\n  {'Type':<6} {'Count':>7} {'Most D':>8} {'Most R':>8} {'Tossups':>8}")
    print(f"  {'-'*40}")
    for dtype, count, most_d, most_r, tossups in cur.fetchall():
        print(f"  {dtype:<6} {int(count):>7,} {float(most_d):>+8.1f} {float(most_r):>+8.1f} {int(tossups or 0):>8,}")

    # Top 10 most competitive
    cur.execute("""
        SELECT district_type, district_name, partisan_lean, competitiveness
        FROM district_scores
        WHERE district_type IN ('CD', 'SD', 'AD')
        ORDER BY ABS(partisan_lean) ASC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print(f"\n  Top 10 Most Competitive Districts:")
        print(f"  {'Type':<6} {'District':<30} {'Lean':>8} {'Rating':<12}")
        print(f"  {'-'*58}")
        for dtype, name, lean, comp in rows:
            print(f"  {dtype:<6} {name:<30} {float(lean):>+8.2f} {comp:<12}")

    print()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
