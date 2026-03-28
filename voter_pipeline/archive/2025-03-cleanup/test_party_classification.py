"""
Improved Party Classification for BOE Committees
=================================================
Better detection of Democratic vs Republican committees based on:
- Party keywords
- NY minor parties (Conservative = R, Working Families = D)
- Candidate names (if committee name includes known politicians)
- Committee types
"""

def classify_party_improved(committee_name):
    """Enhanced committee party classification"""
    name = committee_name.upper()
    
    # Strong Democratic indicators
    dem_strong = [
        'DEMOCRATIC', 'DEMOCRAT ', ' DEM ', 'DNC', 'DCCC', 'DSCC',
        'WORKING FAMILIES', 'WFP',
        'WOMEN\'S EQUALITY',
        'INDEPENDENCE PARTY',  # NY Independence often leans D
    ]
    
    # Strong Republican indicators  
    rep_strong = [
        'REPUBLICAN', ' REP ', ' GOP', 'RNC', 'NRCC', 'NRSC',
        'CONSERVATIVE', ' CON ',
        'REFORM PARTY',
    ]
    
    # Check strong indicators first
    for keyword in dem_strong:
        if keyword in name:
            return 'D'
    
    for keyword in rep_strong:
        if keyword in name:
            return 'R'
    
    # Contextual patterns
    # "FRIENDS OF [NAME]" - check if name is known politician
    if 'COMMITTEE TO ELECT' in name or 'FRIENDS OF' in name:
        # Could look up candidate registry here
        # For now, check for party-specific terms nearby
        if any(x in name for x in ['BIDEN', 'CLINTON', 'SCHUMER', 'GILLIBRAND', 'HOCHUL', 'JAMES', 'CUOMO']):
            return 'D'
        if any(x in name for x in ['TRUMP', 'STEFANIK', 'ZELDIN', 'MOLINARO', 'LANGWORTHY']):
            return 'R'
    
    # County/State party committees
    if 'COUNTY DEMOCRATIC' in name or 'STATE DEMOCRATIC' in name:
        return 'D'
    if 'COUNTY REPUBLICAN' in name or 'STATE REPUBLICAN' in name:
        return 'R'
    
    # PACs with ideological names
    dem_ideological = [
        'PROGRESSIVE', 'LABOR', 'UNION', 'WORKERS',
        'PLANNED PARENTHOOD', 'ENVIRONMENTAL', 'ACLU',
        'EQUALITY', 'CIVIL RIGHTS'
    ]
    
    rep_ideological = [
        'TAXPAYER', 'BUSINESS', 'CHAMBER OF COMMERCE',
        'PRO-LIFE', 'FAMILY VALUES', 'LIBERTY',
        'CONSTITUTIONAL', 'PATRIOT'
    ]
    
    dem_count = sum(1 for kw in dem_ideological if kw in name)
    rep_count = sum(1 for kw in rep_ideological if kw in name)
    
    if dem_count > rep_count and dem_count > 0:
        return 'D'
    if rep_count > dem_count and rep_count > 0:
        return 'R'
    
    # Default to unaffiliated if no clear signals
    return 'U'

# Test it
test_committees = [
    "Friends of Kathy Hochul",
    "Conservative Party of NY",
    "Working Families Party",
    "NY State Democratic Committee",
    "Nassau County Republican Committee",
    "Committee to Elect John Smith",
    "Progressive Action Network",
    "Taxpayers for Lower Taxes"
]

print("Testing improved classification:")
print("="*60)
for comm in test_committees:
    party = classify_party_improved(comm)
    print(f"{party}  {comm}")
