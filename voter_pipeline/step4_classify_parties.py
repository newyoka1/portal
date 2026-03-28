import pymysql, os
from dotenv import load_dotenv
load_dotenv()

DEM_KW = ['democratic', 'democrat', 'dccc', 'dscc', 'dnc', 'progressive', 'blue',
    'actblue', 'forward', 'people for', 'working families', 'labor', 'union',
    'seiu', 'afscme', 'afl-cio', 'teachers', 'nurses', 'planned parenthood',
    'emily', 'moveon', 'priorities usa', 'future forward']

REP_KW = ['republican', 'gop', 'nrcc', 'nrsc', 'rnc', 'conservative', 'red',
    'winred', 'freedom', 'liberty', 'american action', 'club for growth',
    'heritage', 'tea party', 'maga', 'trump', 'america first', 'patriots',
    'chamber of commerce', 'job creators']

OFFICIAL = {
    'C00000935': 'Democratic', 'C00428052': 'Democratic', 'C00042366': 'Democratic',
    'C00003418': 'Republican', 'C00075820': 'Republican', 'C00027466': 'Republican'
}

def classify_by_name(cid, name, party):
    if cid in OFFICIAL: return OFFICIAL[cid], 'Official'
    if party == 'DEM': return 'Democratic', 'FEC'
    if party == 'REP': return 'Republican', 'FEC'
    if party in ['IND','LIB','GRE']: return 'Independent', 'FEC'
    
    if name:
        nl = name.lower()
        hd = [k for k in DEM_KW if k in nl]
        hr = [k for k in REP_KW if k in nl]
        if hd and not hr: return 'Democratic', f'Keywords'
        if hr and not hd: return 'Republican', f'Keywords'
    
    return 'Unknown', 'No_Match'

def analyze_giving_via_candidates(conn):
    """Analyze committees by which party's CANDIDATES they support."""
    cur = conn.cursor()
    
    # Check if we have candidate data
    cur.execute("SHOW TABLES LIKE 'committee_to_candidate'")
    if not cur.fetchone():
        print("\n⚠ No committee-to-candidate data (pas2.zip not loaded)")
        return 0
    
    cur.execute("SELECT COUNT(*) FROM committee_to_candidate")
    if cur.fetchone()[0] == 0:
        print("\n⚠ Committee-to-candidate table empty")
        return 0
    
    print("\nAnalyzing giving patterns via candidate support...")
    
    # For each Unknown committee, see which party's candidates they support
    cur.execute("""
        SELECT cm.committee_id, cm.committee_name
        FROM fec_committees cm
        WHERE cm.classified_party = 'Unknown'
    """)
    
    unknown = cur.fetchall()
    print(f"  Analyzing {len(unknown)} Unknown committees...")
    
    reclassified = 0
    
    for cmte_id, cmte_name in unknown:
        # Get candidate party breakdown for this committee
        cur.execute("""
            SELECT 
                cand.party,
                SUM(ctc.contribution_amount) as total,
                COUNT(*) as cnt
            FROM committee_to_candidate ctc
            JOIN fec_candidates cand ON ctc.candidate_id = cand.candidate_id
            WHERE ctc.committee_id = %s
            GROUP BY cand.party
        """, (cmte_id,))
        
        results = cur.fetchall()
        if not results:
            continue
        
        # Calculate totals by party
        dem_amt, rep_amt, total_amt = 0, 0, 0
        for party, amt, cnt in results:
            total_amt += amt
            if party == 'DEM':
                dem_amt += amt
            elif party == 'REP':
                rep_amt += amt
        
        if total_amt < 5000:  # Need at least $5K to be confident
            continue
        
        dem_pct = (dem_amt / total_amt * 100) if total_amt > 0 else 0
        rep_pct = (rep_amt / total_amt * 100) if total_amt > 0 else 0
        
        new_party = None
        
        # Strong pattern: 75%+ to one party's candidates
        if dem_pct >= 75:
            new_party = 'Democratic'
        elif rep_pct >= 75:
            new_party = 'Republican'
        
        if new_party:
            cur.execute("UPDATE fec_committees SET classified_party=%s WHERE committee_id=%s",
                       (new_party, cmte_id))
            reclassified += 1
    
    conn.commit()
    print(f"  ✓ Reclassified {reclassified} based on candidate support")
    return reclassified

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST','localhost'),
    port=int(os.getenv('MYSQL_PORT',3306)),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database='National_Donors'
)
cur = conn.cursor()

print("="*70)
print("ENHANCED PARTY CLASSIFICATION")
print("="*70)

# Initial classification
cur.execute("SELECT committee_id, committee_name, party_affiliation FROM fec_committees")
cmtes = cur.fetchall()
print(f"\nClassifying {len(cmtes):,} committees...")

updates = []
for cid, name, party in cmtes:
    classified, method = classify_by_name(cid, name, party)
    updates.append((classified, cid))

cur.executemany("UPDATE fec_committees SET classified_party=%s WHERE committee_id=%s", updates)
conn.commit()
print(f"✓ Initial classification complete")

# Enhanced: Analyze giving to candidates
reclassified = analyze_giving_via_candidates(conn)

# Apply to contributions
print("\nApplying to contributions...")
cur.execute("""
UPDATE fec_contributions c
JOIN fec_committees m ON c.committee_id = m.committee_id
SET c.party_signal = m.classified_party
WHERE m.classified_party IS NOT NULL
""")
conn.commit()
print(f"✓ Updated {cur.rowcount:,} contributions")

# Stats
print("\n" + "="*70)
print("RESULTS")
print("="*70)

cur.execute("""
SELECT classified_party, COUNT(*) as cmtes,
       (SELECT COUNT(*) FROM fec_contributions WHERE committee_id IN 
        (SELECT committee_id FROM fec_committees WHERE classified_party = c.classified_party)) as contribs
FROM fec_committees c
GROUP BY classified_party
ORDER BY cmtes DESC
""")

print(f"\n{'Party':<15} {'Committees':>12} {'Contributions':>15}")
print("-"*45)
for party, cmtes, contribs in cur.fetchall():
    print(f"{party:<15} {cmtes:>12,} {contribs:>15,}")

conn.close()
print("\n✓ COMPLETE - Next: python pipeline/enrich_fec_donors.py")
