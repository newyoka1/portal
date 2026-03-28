"""
BOE Complete Pipeline - Part 2: Match & Aggregate
==================================================
After running boe_import_comprehensive.py, this script:
1. Matches contributions to voters by name+zip
2. Classifies by party (D/R/U) using committee names
3. Aggregates by StateVoterId + party + year
4. Creates final donor_summary table ready for enrichment
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

def classify_party(committee_name):
    """Enhanced committee party classification with better detection"""
    name = committee_name.upper()
    
    # Strong Democratic indicators
    dem_strong = [
        'DEMOCRATIC', 'DEMOCRAT ', ' DEM ', 'DNC', 'DCCC', 'DSCC',
        'WORKING FAMILIES', 'WFP', 'WOMEN\'S EQUALITY',
    ]
    
    # Strong Republican indicators  
    rep_strong = [
        'REPUBLICAN', ' REP ', ' GOP', 'RNC', 'NRCC', 'NRSC',
        'CONSERVATIVE', ' CON ', 'REFORM PARTY',
    ]
    
    # Check strong indicators first
    for keyword in dem_strong:
        if keyword in name:
            return 'D'
    
    for keyword in rep_strong:
        if keyword in name:
            return 'R'
    
    # Known Democratic politicians/officials (NY specific)
    dem_names = [
        'BIDEN', 'HARRIS', 'CLINTON', 'SCHUMER', 'GILLIBRAND', 
        'HOCHUL', 'JAMES', 'CUOMO', 'AOC', 'OCASIO', 'NADLER',
        'MALONEY', 'JEFFRIES', 'VELAZQUEZ', 'MEEKS'
    ]
    
    # Known Republican politicians/officials (NY specific)
    rep_names = [
        'TRUMP', 'STEFANIK', 'ZELDIN', 'MOLINARO', 'LANGWORTHY',
        'TENNEY', 'MALLIOTAKIS', 'LAWLER', 'LALOTA', 'SANTOS'
    ]
    
    # Check for candidate committees
    if 'FRIENDS OF' in name or 'COMMITTEE TO ELECT' in name or 'FOR NY' in name:
        for pol in dem_names:
            if pol in name:
                return 'D'
        for pol in rep_names:
            if pol in name:
                return 'R'
    
    # County/State party committees
    if any(x in name for x in ['COUNTY DEMOCRATIC', 'STATE DEMOCRATIC', 'DEM COUNTY']):
        return 'D'
    if any(x in name for x in ['COUNTY REPUBLICAN', 'STATE REPUBLICAN', 'REP COUNTY', 'GOP COUNTY']):
        return 'R'
    
    # PACs with ideological indicators
    dem_ideological = [
        'PROGRESSIVE', 'LABOR', 'UNION', 'WORKERS', 'AFLCIO', 'SEIU',
        'PLANNED PARENTHOOD', 'ENVIRONMENTAL', 'SIERRA', 'LEAGUE OF CONSERVATION',
        'EQUALITY', 'CIVIL RIGHTS', 'LGBTQ', 'REPRODUCTIVE'
    ]
    
    rep_ideological = [
        'TAXPAYER', 'BUSINESS', 'CHAMBER OF COMMERCE', 'MANUFACTURERS',
        'PRO-LIFE', 'RIGHT TO LIFE', 'FAMILY VALUES', 'LIBERTY', 'FREEDOM',
        'CONSTITUTIONAL', 'PATRIOT', 'SECOND AMENDMENT', 'NRA'
    ]
    
    dem_count = sum(1 for kw in dem_ideological if kw in name)
    rep_count = sum(1 for kw in rep_ideological if kw in name)
    
    if dem_count > rep_count and dem_count > 0:
        return 'D'
    if rep_count > dem_count and rep_count > 0:
        return 'R'
    
    # Default to unaffiliated
    return 'U'

def main():
    print("="*80)
    print("BOE MATCHING & AGGREGATION")
    print("="*80)
    
    conn = get_conn()
    cur = conn.cursor()
    # Add performance indexes before matching
    print("\nStep 0: Adding performance indexes...")
    try:
        cur.execute("CREATE INDEX idx_voter_name_zip ON nys_voter_tagging.voter_file(LastName, FirstName, PrimaryZip)")
        print("  ? Created index on voter_file")
    except:
        print("  ? Index on voter_file already exists")
    
    try:
        cur.execute("CREATE INDEX idx_contrib_name_zip ON contributions_raw(last_name, first_name, zip)")
        print("  ? Created index on contributions_raw")
    except:
        print("  ? Index on contributions_raw already exists")

    
    # Step 1: Add party classification to contributions
    print("\nStep 1: Classifying committees by party...")
    cur.execute("USE boe_donors")
    
    # Add party column if not exists
    cur.execute("SHOW COLUMNS FROM contributions_raw LIKE 'party'")
    if not cur.fetchone():
        print("  Adding party column...")
        try:
            cur.execute("ALTER TABLE contributions_raw ADD COLUMN party CHAR(1) DEFAULT 'U'")
        except:
            pass  # Column already exists
        conn.commit()
    else:
        print("  Party column already exists")
    
    # Classify based on committee name
    cur.execute("SELECT DISTINCT committee_name FROM contributions_raw")
    committees = cur.fetchall()
    
    for (committee,) in committees:
        party = classify_party(committee)
        cur.execute("UPDATE contributions_raw SET party=%s WHERE committee_name=%s", (party, committee))
    
    conn.commit()
    
    # Show party breakdown
    cur.execute("""
        SELECT party, COUNT(*), SUM(amount) 
        FROM contributions_raw 
        GROUP BY party
    """)
    print("\n  Party breakdown:")
    for party, count, total in cur.fetchall():
        party_name = {'D': 'Democrat', 'R': 'Republican', 'U': 'Unaffiliated'}.get(party, party)
        print(f"    {party_name:15} {int(count):8,} contributions  ${total or 0:12,.2f}")
    
    # Step 2: Match to voters
    print("\nStep 2: Matching contributions to voters...")
    print("  (This may take 5-10 minutes)")
    
    cur.execute("DROP TABLE IF EXISTS contributions_matched")
    cur.execute("""
        CREATE TABLE contributions_matched AS
        SELECT 
            v.StateVoterId,
            c.year,
            c.party,
            SUM(c.amount) as total_amount,
            COUNT(*) as contribution_count
        FROM nys_voter_tagging.voter_file v
        JOIN contributions_raw c ON (
            UPPER(TRIM(c.last_name)) = UPPER(TRIM(v.LastName))
            AND LEFT(c.zip, 5) = LEFT(v.PrimaryZip, 5)
            AND (
                UPPER(TRIM(c.first_name)) = UPPER(TRIM(v.FirstName))
                OR SOUNDEX(c.first_name) = SOUNDEX(v.FirstName)
            )
        )
        WHERE c.amount > 0
        GROUP BY v.StateVoterId, c.year, c.party
    """)
    
    # Fix StateVoterId collation to match voter_file
    cur.execute("""
        ALTER TABLE contributions_matched
        MODIFY COLUMN StateVoterId VARCHAR(50)
        CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    """)
    conn.commit()
    
    cur.execute("SELECT COUNT(DISTINCT StateVoterId) FROM contributions_matched")
    matched_voters = cur.fetchone()[0]
    print(f"  ? Matched {matched_voters:,} unique voters")
    
    # Step 3: Create final donor summary table
    print("\nStep 3: Creating donor_summary table...")
    
    cur.execute("DROP TABLE IF EXISTS donor_summary")
    cur.execute("""
        CREATE TABLE donor_summary (
            StateVoterId VARCHAR(50) PRIMARY KEY,
            DEM_TOTAL DECIMAL(14,2) DEFAULT 0,
            REP_TOTAL DECIMAL(14,2) DEFAULT 0,
            UNA_TOTAL DECIMAL(14,2) DEFAULT 0,
            DEM_2018 DECIMAL(14,2) DEFAULT 0,
            DEM_2019 DECIMAL(14,2) DEFAULT 0,
            DEM_2020 DECIMAL(14,2) DEFAULT 0,
            DEM_2021 DECIMAL(14,2) DEFAULT 0,
            DEM_2022 DECIMAL(14,2) DEFAULT 0,
            DEM_2023 DECIMAL(14,2) DEFAULT 0,
            DEM_2024 DECIMAL(14,2) DEFAULT 0,
            REP_2018 DECIMAL(14,2) DEFAULT 0,
            REP_2019 DECIMAL(14,2) DEFAULT 0,
            REP_2020 DECIMAL(14,2) DEFAULT 0,
            REP_2021 DECIMAL(14,2) DEFAULT 0,
            REP_2022 DECIMAL(14,2) DEFAULT 0,
            REP_2023 DECIMAL(14,2) DEFAULT 0,
            REP_2024 DECIMAL(14,2) DEFAULT 0,
            UNA_2018 DECIMAL(14,2) DEFAULT 0,
            UNA_2019 DECIMAL(14,2) DEFAULT 0,
            UNA_2020 DECIMAL(14,2) DEFAULT 0,
            UNA_2021 DECIMAL(14,2) DEFAULT 0,
            UNA_2022 DECIMAL(14,2) DEFAULT 0,
            UNA_2023 DECIMAL(14,2) DEFAULT 0,
            UNA_2024 DECIMAL(14,2) DEFAULT 0,
            INDEX(DEM_TOTAL),
            INDEX(REP_TOTAL),
            INDEX(UNA_TOTAL)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    
    # Insert base records
    cur.execute("""
        INSERT INTO donor_summary (StateVoterId)
        SELECT DISTINCT StateVoterId FROM contributions_matched
    """)
    
    # Update totals by party
    for party, col_prefix in [('D', 'DEM'), ('R', 'REP'), ('U', 'UNA')]:
        cur.execute(f"""
            UPDATE donor_summary ds
            JOIN (
                SELECT StateVoterId, SUM(total_amount) as amt
                FROM contributions_matched
                WHERE party = %s
                GROUP BY StateVoterId
            ) c ON ds.StateVoterId = c.StateVoterId
            SET ds.{col_prefix}_TOTAL = c.amt
        """, (party,))
    
    # Update by year
    for year in range(2018, 2025):
        for party, col_prefix in [('D', 'DEM'), ('R', 'REP'), ('U', 'UNA')]:
            cur.execute(f"""
                UPDATE donor_summary ds
                JOIN (
                    SELECT StateVoterId, SUM(total_amount) as amt
                    FROM contributions_matched
                    WHERE party = %s AND year = %s
                    GROUP BY StateVoterId
                ) c ON ds.StateVoterId = c.StateVoterId
                SET ds.{col_prefix}_{year} = c.amt
            """, (party, year))
    
    conn.commit()
    
    # Final stats
    print("\nFinal Statistics:")
    cur.execute("""
        SELECT 
            COUNT(*) as donors,
            SUM(DEM_TOTAL) as dem_total,
            SUM(REP_TOTAL) as rep_total,
            SUM(UNA_TOTAL) as una_total
        FROM donor_summary
    """)
    
    row = cur.fetchone()
    donors, dem, rep, una = row
    total = (dem or 0) + (rep or 0) + (una or 0)
    
    print(f"  Total donors: {int(donors):,}")
    print(f"  Total contributed: ${total:,.2f}")
    print(f"    Democratic:   ${dem or 0:,.2f}")
    print(f"    Republican:   ${rep or 0:,.2f}")
    print(f"    Unaffiliated: ${una or 0:,.2f}")
    
    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)
    print("\nReady to enrich voter_file with:")
    print("  python main.py boe-enrich")
    
    conn.close()

if __name__ == "__main__":
    main()
