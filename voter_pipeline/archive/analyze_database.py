#!/usr/bin/env python3
"""
Database Efficiency Analysis Tool
Analyzes NYS_VOTER_TAGGING database for performance optimization opportunities
"""

import pymysql
from datetime import datetime
import sys
import os

# MySQL Config (match other scripts)
DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

def connect_db():
    """Connect to MySQL database"""
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def check_table_sizes(conn):
    """Analyze table sizes and row counts"""
    print_section("TABLE SIZES AND ROW COUNTS")

    query = """
        SELECT
            TABLE_NAME as table_name,
            TABLE_ROWS as table_rows,
            ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS size_mb,
            ROUND((DATA_LENGTH / 1024 / 1024), 2) AS data_mb,
            ROUND((INDEX_LENGTH / 1024 / 1024), 2) AS index_mb,
            ROUND((INDEX_LENGTH / NULLIF(DATA_LENGTH, 0) * 100), 2) AS index_ratio_pct
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'NYS_VOTER_TAGGING'
        ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("No tables found!")
            return

        print(f"{'Table Name':<50} {'Rows':>12} {'Total MB':>10} {'Data MB':>10} {'Index MB':>10} {'Idx%':>6}")
        print("-" * 110)

        total_size = 0
        total_data = 0
        total_index = 0
        total_rows = 0

        for row in results:
            table_name = row.get('table_name', row.get('TABLE_NAME', 'UNKNOWN'))
            table_rows = row.get('table_rows', row.get('TABLE_ROWS', 0)) or 0
            size_mb = row.get('size_mb', 0) or 0
            data_mb = row.get('data_mb', 0) or 0
            index_mb = row.get('index_mb', 0) or 0
            index_ratio = row.get('index_ratio_pct', 0) or 0

            print(f"{table_name:<50} {table_rows:>12,} {size_mb:>10.2f} {data_mb:>10.2f} {index_mb:>10.2f} {index_ratio:>6.1f}")

            total_size += size_mb
            total_data += data_mb
            total_index += index_mb
            total_rows += table_rows

        print("-" * 110)
        print(f"{'TOTAL':<50} {total_rows:>12,} {total_size:>10.2f} {total_data:>10.2f} {total_index:>10.2f}")
        print(f"\nDatabase total size: {total_size:,.2f} MB ({total_size/1024:.2f} GB)")
        print(f"Total rows across all tables: {total_rows:,}")

def check_indexes(conn):
    """Analyze indexes on key tables"""
    print_section("INDEX ANALYSIS")

    key_tables = ['voter_file', 'voter_audience_bridge', 'ref_census_surnames']

    query = """
        SELECT
            table_name,
            index_name,
            GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns,
            index_type,
            CASE WHEN non_unique = 0 THEN 'UNIQUE' ELSE 'NON-UNIQUE' END AS uniqueness,
            cardinality
        FROM information_schema.STATISTICS
        WHERE table_schema = 'NYS_VOTER_TAGGING'
          AND table_name = %s
        GROUP BY table_name, index_name, index_type, non_unique, cardinality
        ORDER BY table_name, index_name;
    """

    for table in key_tables:
        with conn.cursor() as cursor:
            cursor.execute(query, (table,))
            results = cursor.fetchall()

            if results:
                print(f"\nTable: {table}")
                print(f"{'Index Name':<30} {'Columns':<40} {'Type':<10} {'Unique':<12} {'Cardinality':>15}")
                print("-" * 110)

                for row in results:
                    print(f"{row['index_name']:<30} {row['columns']:<40} {row['index_type']:<10} {row['uniqueness']:<12} {row['cardinality'] or 0:>15,}")
            else:
                print(f"\nTable: {table}")
                print("  [!] WARNING: No indexes found!")

def check_missing_indexes(conn):
    """Check for missing indexes on frequently queried columns"""
    print_section("MISSING INDEX RECOMMENDATIONS")

    # Check if district columns have indexes on voter_file
    query = """
        SELECT
            column_name
        FROM information_schema.COLUMNS
        WHERE table_schema = 'NYS_VOTER_TAGGING'
          AND table_name = 'voter_file'
          AND column_name IN ('LDName', 'SDName', 'CDName', 'origin', 'LastName')
        ORDER BY column_name;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [row['column_name'] for row in cursor.fetchall()]

    # Check which columns have indexes
    index_query = """
        SELECT DISTINCT column_name
        FROM information_schema.STATISTICS
        WHERE table_schema = 'NYS_VOTER_TAGGING'
          AND table_name = 'voter_file'
          AND column_name IN ('LDName', 'SDName', 'CDName', 'origin', 'LastName');
    """

    with conn.cursor() as cursor:
        cursor.execute(index_query)
        indexed_columns = [row['column_name'] for row in cursor.fetchall()]

    missing = [col for col in columns if col not in indexed_columns]

    print("Table: voter_file")
    print("\nFrequently queried columns:")
    for col in columns:
        status = "OK - INDEXED" if col in indexed_columns else "[!] MISSING INDEX"
        print(f"  {col:<20} {status}")

    if missing:
        print("\n[!] RECOMMENDATION: Add indexes for these columns:")
        for col in missing:
            print(f"  ALTER TABLE voter_file ADD INDEX idx_{col.lower()} ({col});")
    else:
        print("\nOK - All key columns are indexed!")

def check_table_fragmentation(conn):
    """Check for table fragmentation"""
    print_section("TABLE FRAGMENTATION")

    query = """
        SELECT
            table_name,
            ROUND(data_length / 1024 / 1024, 2) AS data_mb,
            ROUND(data_free / 1024 / 1024, 2) AS free_mb,
            ROUND((data_free / data_length * 100), 2) AS fragmentation_pct
        FROM information_schema.TABLES
        WHERE table_schema = 'NYS_VOTER_TAGGING'
          AND data_length > 0
        ORDER BY fragmentation_pct DESC;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        print(f"{'Table Name':<50} {'Data MB':>10} {'Free MB':>10} {'Frag %':>8}")
        print("-" * 80)

        fragmented_tables = []

        for row in results:
            table_name = row['table_name']
            data_mb = row['data_mb'] or 0
            free_mb = row['free_mb'] or 0
            frag_pct = row['fragmentation_pct'] or 0

            status = ""
            if frag_pct > 20:
                status = " [!] HIGH FRAGMENTATION"
                fragmented_tables.append(table_name)
            elif frag_pct > 10:
                status = " [!] MODERATE FRAGMENTATION"

            print(f"{table_name:<50} {data_mb:>10.2f} {free_mb:>10.2f} {frag_pct:>8.2f}{status}")

        if fragmented_tables:
            print("\n[!] RECOMMENDATION: Optimize fragmented tables:")
            for table in fragmented_tables:
                print(f"  OPTIMIZE TABLE {table};")
        else:
            print("\nOK - No significant fragmentation detected!")

def check_query_cache(conn):
    """Check MySQL query cache status (if available)"""
    print_section("MYSQL CONFIGURATION")

    variables = [
        'innodb_buffer_pool_size',
        'innodb_buffer_pool_instances',
        'max_connections',
        'innodb_thread_concurrency',
        'innodb_read_io_threads',
        'innodb_write_io_threads',
        'table_open_cache',
        'tmp_table_size',
        'max_heap_table_size'
    ]

    print(f"{'Variable':<35} {'Current Value':<30} {'Recommendation'}")
    print("-" * 100)

    for var in variables:
        query = f"SHOW VARIABLES LIKE '{var}';"
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                value = result['Value']

                # Add recommendations
                recommendations = {
                    'innodb_buffer_pool_size': '40G (for 64GB RAM)',
                    'innodb_buffer_pool_instances': '16 (matches CPU cores)',
                    'max_connections': '300',
                    'innodb_thread_concurrency': '64',
                    'innodb_read_io_threads': '16',
                    'innodb_write_io_threads': '16',
                    'table_open_cache': '4000',
                    'tmp_table_size': '256M',
                    'max_heap_table_size': '256M'
                }

                rec = recommendations.get(var, 'OK')
                print(f"{var:<35} {value:<30} {rec}")

def check_slow_query_patterns(conn):
    """Analyze common query patterns that might be slow"""
    print_section("QUERY PATTERN ANALYSIS")

    # Test query performance for common patterns
    tests = [
        ("Count all voters", "SELECT COUNT(*) FROM voter_file;"),
        ("Count by LD 63", "SELECT COUNT(*) FROM voter_file WHERE LDName = '63';"),
        ("Count matched voters", "SELECT COUNT(*) FROM voter_file WHERE origin IS NOT NULL AND TRIM(origin) != '';"),
        ("Count with ethnicity join", "SELECT COUNT(*) FROM voter_file f LEFT JOIN ref_census_surnames e ON e.surname = UPPER(f.LastName) WHERE f.LDName = '63';"),
    ]

    print("Testing common query patterns:\n")
    print(f"{'Test Description':<30} {'Execution Time':<20} {'Status'}")
    print("-" * 80)

    for desc, query in tests:
        try:
            with conn.cursor() as cursor:
                start = datetime.now()
                cursor.execute(query)
                result = cursor.fetchone()
                end = datetime.now()

                duration = (end - start).total_seconds()

                status = "OK"
                if duration > 5:
                    status = "[!] SLOW"
                elif duration > 2:
                    status = "[!] MODERATE"

                print(f"{desc:<30} {duration:>10.3f} seconds      {status}")
        except Exception as e:
            print(f"{desc:<30} {'ERROR':<20} {str(e)[:30]}")

def check_duplicate_indexes(conn):
    """Check for duplicate or redundant indexes"""
    print_section("DUPLICATE/REDUNDANT INDEX CHECK")

    query = """
        SELECT
            table_name,
            GROUP_CONCAT(DISTINCT index_name ORDER BY index_name) AS indexes,
            GROUP_CONCAT(DISTINCT column_name ORDER BY column_name) AS columns,
            COUNT(*) AS index_count
        FROM information_schema.STATISTICS
        WHERE table_schema = 'NYS_VOTER_TAGGING'
        GROUP BY table_name, column_name
        HAVING COUNT(*) > 1
        ORDER BY table_name, column_name;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print("[!] WARNING: Duplicate indexes found:\n")
            for row in results:
                print(f"Table: {row['table_name']}")
                print(f"  Column: {row['columns']}")
                print(f"  Indexes: {row['indexes']}")
                print(f"  Count: {row['index_count']}")
                print()
            print("RECOMMENDATION: Review and remove redundant indexes to improve write performance.")
        else:
            print("OK - No duplicate indexes detected!")

def generate_optimization_report(conn):
    """Generate final optimization recommendations"""
    print_section("OPTIMIZATION RECOMMENDATIONS SUMMARY")

    print("Based on the analysis above, here are the priority recommendations:\n")

    recommendations = []

    # Check buffer pool size
    with conn.cursor() as cursor:
        cursor.execute("SHOW VARIABLES LIKE 'innodb_buffer_pool_size';")
        result = cursor.fetchone()
        if result:
            buffer_size = int(result['Value'])
            if buffer_size < 40 * 1024 * 1024 * 1024:  # Less than 40GB
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'MySQL Config',
                    'issue': f'Buffer pool size is {buffer_size / 1024 / 1024 / 1024:.1f}GB',
                    'recommendation': 'Increase innodb_buffer_pool_size to 40GB',
                    'action': 'Update my.ini and restart MySQL'
                })

    # Check for materialized tables
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES LIKE 'HT_%';")
        ht_tables = cursor.fetchall()

        if not ht_tables:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Performance',
                'issue': 'No materialized audience tables found',
                'recommendation': 'Create materialized tables for frequently queried audiences',
                'action': 'Run: python build_causeway_audience_tables.py --pattern "HT" --limit 5'
            })

    # Add general recommendations
    recommendations.append({
        'priority': 'LOW',
        'category': 'Maintenance',
        'issue': 'Regular maintenance not verified',
        'recommendation': 'Schedule regular OPTIMIZE TABLE operations',
        'action': 'Run OPTIMIZE TABLE monthly for large tables'
    })

    # Print recommendations
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. [{rec['priority']}] {rec['category']}: {rec['issue']}")
        print(f"   Recommendation: {rec['recommendation']}")
        print(f"   Action: {rec['action']}")
        print()

def main():
    """Main analysis function"""
    print("\n" + "="*80)
    print("  NYS VOTER TAGGING DATABASE EFFICIENCY ANALYSIS")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*80)

    conn = connect_db()

    try:
        check_table_sizes(conn)
        check_indexes(conn)
        check_missing_indexes(conn)
        check_table_fragmentation(conn)
        check_duplicate_indexes(conn)
        check_query_cache(conn)
        check_slow_query_patterns(conn)
        generate_optimization_report(conn)

        print("\n" + "="*80)
        print("  ANALYSIS COMPLETE")
        print("="*80 + "\n")

    finally:
        conn.close()

if __name__ == "__main__":
    main()