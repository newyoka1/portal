#!/usr/bin/env python3
"""
Simple Database Efficiency Analysis
Quick analysis of NYS_VOTER_TAGGING database
"""

import pymysql
import os
from datetime import datetime

# MySQL Config
DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

def run_query(conn, query, desc="Query"):
    """Run query and return results"""
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

def print_section(title):
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def main():
    print("\n" + "="*80)
    print("  NYS VOTER TAGGING DATABASE ANALYSIS")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*80)

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4"
    )

    try:
        # 1. Database Overview
        print_section("DATABASE OVERVIEW")

        results = run_query(conn, """
            SELECT
                COUNT(*) as table_count,
                SUM(TABLE_ROWS) as total_rows,
                ROUND(SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 / 1024, 2) as size_gb,
                ROUND(SUM(DATA_LENGTH) / 1024 / 1024 / 1024, 2) as data_gb,
                ROUND(SUM(INDEX_LENGTH) / 1024 / 1024 / 1024, 2) as index_gb
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
        """)

        row = results[0]
        print(f"Total tables: {row[0]:,}")
        print(f"Total rows: {row[1]:,}")
        print(f"Total size: {row[2]:.2f} GB")
        print(f"  Data: {row[3]:.2f} GB")
        print(f"  Indexes: {row[4]:.2f} GB")

        # 2. Top 10 Largest Tables
        print_section("TOP 10 LARGEST TABLES")

        results = run_query(conn, """
            SELECT
                TABLE_NAME,
                TABLE_ROWS,
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as size_mb
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
            ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
            LIMIT 10
        """)

        print(f"{'Table Name':<50} {'Rows':>12} {'Size (MB)':>12}")
        print("-" * 75)
        for row in results:
            print(f"{row[0]:<50} {row[1]:>12,} {row[2]:>12.2f}")

        # 3. Key Table Row Counts
        print_section("KEY TABLE ROW COUNTS")

        key_tables = [
            'voter_file',
            'voter_audience_bridge',
            'ref_census_surnames',
            'causeway_norm',
            'stg_voter_raw'
        ]

        for table in key_tables:
            try:
                results = run_query(conn, f"SELECT COUNT(*) FROM {table}")
                print(f"{table:<40} {results[0][0]:>15,} rows")
            except:
                print(f"{table:<40} {'ERROR':>15}")

        # 4. Index Check on voter_file
        print_section("INDEXES ON voter_file")

        results = run_query(conn, """
            SELECT DISTINCT INDEX_NAME, COLUMN_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
              AND TABLE_NAME = 'voter_file'
            ORDER BY INDEX_NAME, COLUMN_NAME
        """)

        if results:
            current_index = None
            for row in results:
                if row[0] != current_index:
                    print(f"\n{row[0]}:")
                    current_index = row[0]
                print(f"  - {row[1]}")
        else:
            print("[!] WARNING: No indexes found on voter_file!")

        # 5. Important Column Index Status
        print_section("KEY COLUMN INDEX STATUS (voter_file)")

        key_columns = ['StateVoterId', 'LDName', 'SDName', 'CDName', 'origin', 'LastName', 'FirstName', 'ZIP']

        indexed_columns = [row[1] for row in results]

        print(f"{'Column':<20} {'Status'}")
        print("-" * 40)
        for col in key_columns:
            status = "INDEXED" if col in indexed_columns else "[!] NOT INDEXED"
            print(f"{col:<20} {status}")

        # 6. MySQL Configuration
        print_section("MYSQL CONFIGURATION")

        config_vars = [
            'innodb_buffer_pool_size',
            'innodb_buffer_pool_instances',
            'max_connections',
            'innodb_thread_concurrency'
        ]

        print(f"{'Variable':<35} {'Value':<20} {'Recommended'}")
        print("-" * 80)

        recommendations = {
            'innodb_buffer_pool_size': '40G',
            'innodb_buffer_pool_instances': '16',
            'max_connections': '300',
            'innodb_thread_concurrency': '64'
        }

        for var in config_vars:
            results = run_query(conn, f"SHOW VARIABLES LIKE '{var}'")
            if results:
                value = results[0][1]
                rec = recommendations.get(var, 'OK')

                # Format large numbers
                if var == 'innodb_buffer_pool_size' and value.isdigit():
                    value_gb = int(value) / 1024 / 1024 / 1024
                    value = f"{value_gb:.1f}G"

                print(f"{var:<35} {value:<20} {rec}")

        # 7. Query Performance Test
        print_section("QUERY PERFORMANCE TEST")

        tests = [
            ("Count all voters", "SELECT COUNT(*) FROM voter_file"),
            ("Count LD 63", "SELECT COUNT(*) FROM voter_file WHERE LDName = '63'"),
            ("Count matched", "SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL"),
        ]

        print(f"{'Test':<30} {'Time (sec)':>12} {'Result':>15} {'Status'}")
        print("-" * 75)

        for desc, query in tests:
            start = datetime.now()
            try:
                results = run_query(conn, query)
                end = datetime.now()
                duration = (end - start).total_seconds()
                result = results[0][0]

                status = "OK"
                if duration > 5:
                    status = "[!] SLOW"
                elif duration > 2:
                    status = "[!] MODERATE"

                print(f"{desc:<30} {duration:>12.3f} {result:>15,} {status}")
            except Exception as e:
                print(f"{desc:<30} {'ERROR':>12} {str(e)[:15]:>15}")

        # 8. Materialized Tables Check
        print_section("MATERIALIZED AUDIENCE TABLES")

        results = run_query(conn, "SHOW TABLES LIKE 'ht_%'")
        ht_count = len(results)

        results = run_query(conn, "SHOW TABLES LIKE 'mt_%'")
        mt_count = len(results)

        results = run_query(conn, "SHOW TABLES LIKE 'lt_%'")
        lt_count = len(results)

        print(f"HT (High Turnout) tables: {ht_count}")
        print(f"MT (Medium Turnout) tables: {mt_count}")
        print(f"LT (Low Turnout) tables: {lt_count}")
        print(f"Total materialized tables: {ht_count + mt_count + lt_count}")

        if (ht_count + mt_count + lt_count) > 50:
            print("\n OK - Materialized tables are in use!")
        else:
            print("\n[!] RECOMMENDATION: Consider building more materialized tables")
            print("   Run: python build_causeway_audience_tables.py --pattern 'HT'")

        # 9. Table Fragmentation
        print_section("TABLE FRAGMENTATION (Top 10)")

        results = run_query(conn, """
            SELECT
                TABLE_NAME,
                ROUND((DATA_FREE / NULLIF(DATA_LENGTH, 0) * 100), 2) as frag_pct
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
              AND DATA_LENGTH > 0
            ORDER BY (DATA_FREE / DATA_LENGTH) DESC
            LIMIT 10
        """)

        print(f"{'Table Name':<50} {'Fragmentation %':>15}")
        print("-" * 70)

        fragmented = []
        for row in results:
            frag = row[1] or 0
            status = ""
            if frag > 20:
                status = " [!] HIGH"
                fragmented.append(row[0])
            elif frag > 10:
                status = " [!] MODERATE"

            print(f"{row[0]:<50} {frag:>15.2f}{status}")

        if fragmented:
            print(f"\n[!] {len(fragmented)} tables need optimization")
            print("   Run: OPTIMIZE TABLE <table_name>;")
        else:
            print("\nOK - No significant fragmentation detected")

        # 10. Optimization Recommendations
        print_section("OPTIMIZATION RECOMMENDATIONS")

        print("Based on analysis:")
        print()

        # Check buffer pool
        results = run_query(conn, "SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
        if results:
            buffer_gb = int(results[0][1]) / 1024 / 1024 / 1024
            if buffer_gb < 40:
                print(f"1. [HIGH PRIORITY] Increase buffer pool from {buffer_gb:.1f}GB to 40GB")
                print("   Action: Update my.ini with innodb_buffer_pool_size=40G")
            else:
                print(f"1. OK Buffer pool size is {buffer_gb:.1f}GB")

        # Check indexes
        results = run_query(conn, """
            SELECT COUNT(DISTINCT COLUMN_NAME)
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
              AND TABLE_NAME = 'voter_file'
              AND COLUMN_NAME IN ('LDName', 'SDName', 'CDName', 'origin')
        """)

        indexed_count = results[0][0]
        if indexed_count < 4:
            print(f"\n2. [MEDIUM PRIORITY] Add indexes to district columns ({indexed_count}/4 indexed)")
            print("   Action: Run ALTER TABLE voter_file ADD INDEX idx_ldname (LDName);")
        else:
            print(f"\n2. OK Key columns are indexed ({indexed_count}/4)")

        # Check materialized tables
        if (ht_count + mt_count + lt_count) < 20:
            print(f"\n3. [MEDIUM PRIORITY] Build more materialized tables ({ht_count + mt_count + lt_count} found)")
            print("   Action: python build_causeway_audience_tables.py --pattern 'HT'")
        else:
            print(f"\n3. OK Materialized tables in use ({ht_count + mt_count + lt_count} tables)")

        # Check fragmentation
        if fragmented:
            print(f"\n4. [LOW PRIORITY] Optimize {len(fragmented)} fragmented tables")
            print("   Action: OPTIMIZE TABLE voter_file;")
        else:
            print("\n4. OK No fragmentation issues")

        print("\n" + "="*80)
        print("  ANALYSIS COMPLETE")
        print("="*80 + "\n")

    finally:
        conn.close()

if __name__ == "__main__":
    main()