#!/usr/bin/env python3
"""
Build Materialized Tables for Individual Causeway Audiences

This script creates materialized tables for EACH individual Causeway audience file,
including ethnicity breakdowns by LD/SD/CD and HT/MT/LT variants.

Features:
- Creates a table for each unique audience in voter_audience_bridge
- Adds ethnicity breakdown tables for each audience
- Adds district-level summary tables (LD/SD/CD) for each audience
- Optional: Filter to create tables only for specific audiences

Usage:
  python build_causeway_audience_tables.py                          # Build all
  python build_causeway_audience_tables.py --audience "HT HARD GOP" # Build one
  python build_causeway_audience_tables.py --pattern "HARD GOP"     # Build matching pattern
  python build_causeway_audience_tables.py --rebuild                # Rebuild existing tables
"""

import os
import sys
import time
import argparse
import pymysql
from pathlib import Path
import re
import logging

# MySQL Config (match pipeline.py)
DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("causeway_tables")


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


def sanitize_table_name(audience_name: str) -> str:
    """Convert audience name to valid MySQL table name"""
    # Remove file extension
    name = audience_name.replace(".csv", "")
    # Replace spaces and special chars with underscores
    name = re.sub(r'[^A-Za-z0-9_]+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    # Ensure it starts with a letter or underscore
    if name and not name[0].isalpha() and name[0] != '_':
        name = 'aud_' + name
    # MySQL table name limit is 64 chars
    if len(name) > 64:
        name = name[:64]
    return name


def get_all_audiences(conn):
    """Get list of all unique audiences from bridge table"""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT audience FROM voter_audience_bridge ORDER BY audience;")
        return [row[0] for row in cur.fetchall()]


def create_audience_table(conn, audience: str, table_name: str):
    """Create materialized table for a specific audience"""
    logger.info(f"Creating table: {table_name} for audience: {audience}")

    # Drop if exists
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table_name}`;")

    # Create table with all voter data
    create_sql = f"""
        CREATE TABLE `{table_name}` AS
        SELECT f.*
        FROM voter_file f
        INNER JOIN voter_audience_bridge b ON b.StateVoterId = f.StateVoterId
        WHERE b.audience = %s;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql, (audience,))

    # Add indexes
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (StateVoterId);")
        cur.execute(f"ALTER TABLE `{table_name}` ADD KEY idx_cd (CDName);")
        cur.execute(f"ALTER TABLE `{table_name}` ADD KEY idx_sd (SDName);")
        cur.execute(f"ALTER TABLE `{table_name}` ADD KEY idx_ld (LDName);")

    # Get count
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table_name}`;")
        count = cur.fetchone()[0]

    logger.info(f"  ✓ {table_name}: {count:,} voters")
    return count


def create_ethnicity_tables(conn, audience: str, base_table: str):
    """Create ethnicity breakdown tables for an audience"""

    # Statewide ethnicity
    eth_table = f"{base_table}_ethnicity"
    logger.info(f"  Creating ethnicity table: {eth_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{eth_table}`;")

    create_sql = f"""
        CREATE TABLE `{eth_table}` AS
        SELECT
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM `{base_table}` v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY e.dominant_ethnicity
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{eth_table}` ADD KEY idx_eth (ethnicity);")

    # Ethnicity by CD
    eth_cd_table = f"{base_table}_ethnicity_by_cd"
    logger.info(f"  Creating ethnicity by CD: {eth_cd_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{eth_cd_table}`;")

    create_sql = f"""
        CREATE TABLE `{eth_cd_table}` AS
        SELECT
            v.CDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM `{base_table}` v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.CDName, e.dominant_ethnicity
        ORDER BY v.CDName, voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{eth_cd_table}` ADD KEY idx_cd_eth (CDName, ethnicity);")

    # Ethnicity by SD
    eth_sd_table = f"{base_table}_ethnicity_by_sd"
    logger.info(f"  Creating ethnicity by SD: {eth_sd_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{eth_sd_table}`;")

    create_sql = f"""
        CREATE TABLE `{eth_sd_table}` AS
        SELECT
            v.SDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM `{base_table}` v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.SDName, e.dominant_ethnicity
        ORDER BY v.SDName, voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{eth_sd_table}` ADD KEY idx_sd_eth (SDName, ethnicity);")

    # Ethnicity by LD
    eth_ld_table = f"{base_table}_ethnicity_by_ld"
    logger.info(f"  Creating ethnicity by LD: {eth_ld_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{eth_ld_table}`;")

    create_sql = f"""
        CREATE TABLE `{eth_ld_table}` AS
        SELECT
            v.LDName,
            COALESCE(e.dominant_ethnicity, 'UNKNOWN') AS ethnicity,
            COUNT(*) AS voters
        FROM `{base_table}` v
        LEFT JOIN ref_census_surnames e ON e.surname = UPPER(v.LastName)
        GROUP BY v.LDName, e.dominant_ethnicity
        ORDER BY v.LDName, voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{eth_ld_table}` ADD KEY idx_ld_eth (LDName, ethnicity);")


def create_district_summary_tables(conn, audience: str, base_table: str):
    """Create district-level summary tables (counts by LD/SD/CD)"""

    # By CD
    cd_table = f"{base_table}_by_cd"
    logger.info(f"  Creating CD summary: {cd_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{cd_table}`;")

    create_sql = f"""
        CREATE TABLE `{cd_table}` AS
        SELECT
            CDName,
            COUNT(*) AS voters
        FROM `{base_table}`
        GROUP BY CDName
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{cd_table}` ADD KEY idx_cd (CDName);")

    # By SD
    sd_table = f"{base_table}_by_sd"
    logger.info(f"  Creating SD summary: {sd_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{sd_table}`;")

    create_sql = f"""
        CREATE TABLE `{sd_table}` AS
        SELECT
            SDName,
            COUNT(*) AS voters
        FROM `{base_table}`
        GROUP BY SDName
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{sd_table}` ADD KEY idx_sd (SDName);")

    # By LD
    ld_table = f"{base_table}_by_ld"
    logger.info(f"  Creating LD summary: {ld_table}")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{ld_table}`;")

    create_sql = f"""
        CREATE TABLE `{ld_table}` AS
        SELECT
            LDName,
            COUNT(*) AS voters
        FROM `{base_table}`
        GROUP BY LDName
        ORDER BY voters DESC;
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"ALTER TABLE `{ld_table}` ADD KEY idx_ld (LDName);")


def main():
    parser = argparse.ArgumentParser(
        description="Build materialized tables for individual Causeway audiences",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("--audience", help="Build table for specific audience only")
    parser.add_argument("--pattern", help="Build tables for audiences matching pattern (e.g., 'HARD GOP')")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild existing tables")
    parser.add_argument("--skip-ethnicity", action="store_true", help="Skip ethnicity breakdown tables")
    parser.add_argument("--skip-districts", action="store_true", help="Skip district summary tables")
    parser.add_argument("--limit", type=int, help="Limit number of audiences to process (for testing)")

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("BUILD CAUSEWAY AUDIENCE TABLES")
    logger.info("=" * 80)

    conn = connect_db()

    try:
        # Get list of audiences to process
        all_audiences = get_all_audiences(conn)
        logger.info(f"Found {len(all_audiences)} unique audiences in database")

        # Filter audiences based on args
        audiences_to_process = all_audiences

        if args.audience:
            if args.audience in all_audiences:
                audiences_to_process = [args.audience]
                logger.info(f"Processing single audience: {args.audience}")
            else:
                logger.error(f"Audience '{args.audience}' not found in database")
                sys.exit(1)

        elif args.pattern:
            audiences_to_process = [a for a in all_audiences if args.pattern in a]
            logger.info(f"Processing {len(audiences_to_process)} audiences matching '{args.pattern}'")

        if args.limit:
            audiences_to_process = audiences_to_process[:args.limit]
            logger.info(f"Limited to first {args.limit} audiences")

        logger.info(f"\nProcessing {len(audiences_to_process)} audiences...")
        logger.info("=" * 80)

        created_tables = []
        start_time = time.time()

        for i, audience in enumerate(audiences_to_process, 1):
            table_name = sanitize_table_name(audience)

            logger.info(f"\n[{i}/{len(audiences_to_process)}] {audience}")
            logger.info(f"  Table name: {table_name}")

            # Create main audience table
            voter_count = create_audience_table(conn, audience, table_name)
            created_tables.append((table_name, voter_count))

            # Create ethnicity breakdown tables
            if not args.skip_ethnicity:
                create_ethnicity_tables(conn, audience, table_name)

            # Create district summary tables
            if not args.skip_districts:
                create_district_summary_tables(conn, audience, table_name)

        elapsed = time.time() - start_time

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Tables created: {len(created_tables)}")
        logger.info(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        logger.info("\nTop 10 audiences by voter count:")
        for table, count in sorted(created_tables, key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"  {table:60s}  {count:>10,} voters")

        logger.info("\n" + "=" * 80)
        logger.info("✓ Complete!")
        logger.info("=" * 80)

        # Show example queries
        if created_tables:
            example_table = created_tables[0][0]
            logger.info("\nExample queries:")
            logger.info(f"  -- Get all voters in audience:")
            logger.info(f"  SELECT * FROM `{example_table}`;")
            logger.info(f"\n  -- Ethnicity breakdown:")
            logger.info(f"  SELECT * FROM `{example_table}_ethnicity`;")
            logger.info(f"\n  -- District breakdown (LD 63 only):")
            logger.info(f"  SELECT * FROM `{example_table}` WHERE LDName = '063';")
            logger.info(f"\n  -- Ethnicity in LD 63:")
            logger.info(f"  SELECT * FROM `{example_table}_ethnicity_by_ld` WHERE LDName = '063';")

    finally:
        conn.close()


if __name__ == "__main__":
    main()