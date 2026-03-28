#!/usr/bin/env python3
"""
Clean up deleted audience files from the database
Removes references to audience files that no longer exist
"""

import pymysql
import os

# MySQL Config
DB_NAME = "NYS_VOTER_TAGGING"
if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")

def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=False  # We'll commit manually
    )

def main():
    print("\n" + "="*80)
    print("  CLEANING UP DELETED AUDIENCE: HT HARD DEM.csv")
    print("="*80 + "\n")

    conn = connect_db()
    cursor = conn.cursor()

    try:
        # First, check how many voters have this audience
        print("Checking current state...\n")

        # Check for exact match
        cursor.execute("""
            SELECT COUNT(*)
            FROM voter_file
            WHERE origin = 'HT HARD DEM.csv'
        """)
        exact_count = cursor.fetchone()[0]
        print(f"  Voters with ONLY 'HT HARD DEM.csv': {exact_count:,}")

        # Check for combined origins containing this audience
        cursor.execute("""
            SELECT COUNT(*)
            FROM voter_file
            WHERE origin LIKE '%HT HARD DEM.csv%'
        """)
        total_count = cursor.fetchone()[0]
        print(f"  Total voters referencing 'HT HARD DEM.csv': {total_count:,}")

        # Show some examples
        cursor.execute("""
            SELECT DISTINCT origin
            FROM voter_file
            WHERE origin LIKE '%HT HARD DEM.csv%'
            LIMIT 10
        """)
        examples = cursor.fetchall()

        print(f"\n  Examples of origin values to clean:")
        for (origin,) in examples:
            print(f"    - {origin}")

        print("\n" + "-"*80)
        print("CLEANUP PLAN:")
        print("-"*80)
        print(f"1. Remove 'HT HARD DEM.csv' from combined origin strings")
        print(f"2. Set origin to NULL for voters who ONLY have 'HT HARD DEM.csv'")
        print(f"3. Clean up any trailing/leading commas")
        print("\n")

        response = input("Proceed with cleanup? (yes/no): ").strip().lower()

        if response != 'yes':
            print("\nCanceled by user.")
            return

        print("\nExecuting cleanup...\n")

        # Step 1: For voters who ONLY have "HT HARD DEM.csv", set origin to NULL
        cursor.execute("""
            UPDATE voter_file
            SET origin = NULL
            WHERE origin = 'HT HARD DEM.csv'
        """)
        step1_count = cursor.rowcount
        print(f"  Step 1: Set origin to NULL for {step1_count:,} voters")

        # Step 2: Remove "HT HARD DEM.csv," from the beginning of combined strings
        cursor.execute("""
            UPDATE voter_file
            SET origin = TRIM(REPLACE(origin, 'HT HARD DEM.csv,', ''))
            WHERE origin LIKE 'HT HARD DEM.csv,%'
        """)
        step2_count = cursor.rowcount
        print(f"  Step 2: Removed from beginning of {step2_count:,} combined origins")

        # Step 3: Remove ",HT HARD DEM.csv" from the end of combined strings
        cursor.execute("""
            UPDATE voter_file
            SET origin = TRIM(REPLACE(origin, ',HT HARD DEM.csv', ''))
            WHERE origin LIKE '%,HT HARD DEM.csv'
        """)
        step3_count = cursor.rowcount
        print(f"  Step 3: Removed from end of {step3_count:,} combined origins")

        # Step 4: Remove ",HT HARD DEM.csv," from the middle of combined strings
        cursor.execute("""
            UPDATE voter_file
            SET origin = TRIM(REPLACE(origin, ',HT HARD DEM.csv,', ','))
            WHERE origin LIKE '%,HT HARD DEM.csv,%'
        """)
        step4_count = cursor.rowcount
        print(f"  Step 4: Removed from middle of {step4_count:,} combined origins")

        # Step 5: Clean up any double commas that might have been created
        cursor.execute("""
            UPDATE voter_file
            SET origin = REPLACE(origin, ',,', ',')
            WHERE origin LIKE '%,,%'
        """)
        step5_count = cursor.rowcount
        print(f"  Step 5: Cleaned up {step5_count:,} double commas")

        # Step 6: Clean up leading/trailing commas
        cursor.execute("""
            UPDATE voter_file
            SET origin = TRIM(BOTH ',' FROM origin)
            WHERE origin LIKE ',%' OR origin LIKE '%,'
        """)
        step6_count = cursor.rowcount
        print(f"  Step 6: Trimmed {step6_count:,} leading/trailing commas")

        # Step 7: Set empty strings to NULL
        cursor.execute("""
            UPDATE voter_file
            SET origin = NULL
            WHERE origin = '' OR origin IS NULL OR TRIM(origin) = ''
        """)
        step7_count = cursor.rowcount
        print(f"  Step 7: Set {step7_count:,} empty origins to NULL")

        # Verify cleanup
        print("\nVerifying cleanup...\n")

        cursor.execute("""
            SELECT COUNT(*)
            FROM voter_file
            WHERE origin LIKE '%HT HARD DEM.csv%'
        """)
        remaining = cursor.fetchone()[0]

        if remaining > 0:
            print(f"  [!] WARNING: {remaining:,} records still contain 'HT HARD DEM.csv'")

            cursor.execute("""
                SELECT DISTINCT origin
                FROM voter_file
                WHERE origin LIKE '%HT HARD DEM.csv%'
                LIMIT 5
            """)
            examples = cursor.fetchall()
            print("\n  Remaining examples:")
            for (origin,) in examples:
                print(f"    - {origin}")
        else:
            print("  OK All references to 'HT HARD DEM.csv' have been removed!")

        print("\n" + "-"*80)
        response = input("\nCommit these changes? (yes/no): ").strip().lower()

        if response == 'yes':
            conn.commit()
            print("\n  OK Changes committed successfully!")
            print(f"\nTotal records updated: {step1_count + step2_count + step3_count + step4_count + step5_count + step6_count + step7_count:,}")
        else:
            conn.rollback()
            print("\n  Changes rolled back. Database unchanged.")

    except Exception as e:
        print(f"\nERROR: {e}")
        conn.rollback()
        print("Changes rolled back.")

    finally:
        cursor.close()
        conn.close()

    print("\n" + "="*80)
    print("  CLEANUP COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()