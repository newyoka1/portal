# -*- coding: utf-8 -*-
import pymysql, dotenv, os, json
dotenv.load_dotenv()
conn = pymysql.connect(
    host="127.0.0.1",
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD")
)
cur = conn.cursor()
out = {}

CMTE = "C00942599"

# -- 1. ALL DONATIONS TO ZIOGAS CAMPAIGN --------------------------------------
cur.execute("""
    SELECT contributor_last_name, contributor_first_name,
           contributor_city, contributor_state, contributor_zip5,
           contribution_amount, contribution_date, party_signal
    FROM national_donors.fec_contributions
    WHERE committee_id = %s
    ORDER BY contribution_date ASC
""", (CMTE,))
rows = cur.fetchall()
out["total_contributions"] = len(rows)
out["total_raised"] = float(sum(r[5] for r in rows))
all_donors = [
    {"last": r[0], "first": r[1], "city": r[2], "state": r[3], "zip": r[4],
     "amount": float(r[5]), "date": str(r[6]), "party_signal": r[7]}
    for r in rows
]
out["all_donors"] = all_donors

# -- 2. GEOGRAPHIC BREAKDOWN ---------------------------------------------------
cur.execute("""
    SELECT contributor_state, COUNT(*) as cnt, SUM(contribution_amount) as total
    FROM national_donors.fec_contributions
    WHERE committee_id = %s
    GROUP BY contributor_state ORDER BY total DESC
""", (CMTE,))
out["geo_state"] = [{"state": r[0], "count": r[1], "total": float(r[2])} for r in cur.fetchall()]

# -- 3. CITY BREAKDOWN ---------------------------------------------------------
cur.execute("""
    SELECT contributor_state, contributor_city, COUNT(*) cnt, SUM(contribution_amount) total
    FROM national_donors.fec_contributions
    WHERE committee_id = %s
    GROUP BY contributor_state, contributor_city ORDER BY total DESC LIMIT 30
""", (CMTE,))
out["geo_city"] = [{"state": r[0], "city": r[1], "count": r[2], "total": float(r[3])} for r in cur.fetchall()]

# -- 4. TOP INDIVIDUAL DONORS --------------------------------------------------
cur.execute("""
    SELECT contributor_last_name, contributor_first_name,
           contributor_city, contributor_state,
           SUM(contribution_amount) total, COUNT(*) cnt
    FROM national_donors.fec_contributions
    WHERE committee_id = %s
    GROUP BY contributor_last_name, contributor_first_name, contributor_city, contributor_state
    ORDER BY total DESC LIMIT 30
""", (CMTE,))
out["top_donors"] = [
    {"last": r[0], "first": r[1], "city": r[2], "state": r[3], "total": float(r[4]), "count": r[5]}
    for r in cur.fetchall()
]

# -- 5. IN-DISTRICT vs OUT -----------------------------------------------------
NY11_ZIPS = {"10301","10302","10303","10304","10305","10306","10307","10308","10309","10310",
             "10311","10312","10313","11209","11214","11219","11220","11228","11232","11204","11218","11230","11223"}
in_dist  = [d for d in all_donors if d["zip"] in NY11_ZIPS]
ny_out   = [d for d in all_donors if d["state"] == "NY" and d["zip"] not in NY11_ZIPS]
non_ny   = [d for d in all_donors if d["state"] not in ("NY","")]
out["in_district_count"]          = len(in_dist)
out["in_district_total"]          = round(sum(d["amount"] for d in in_dist), 2)
out["ny_out_of_district_count"]   = len(ny_out)
out["ny_out_of_district_total"]   = round(sum(d["amount"] for d in ny_out), 2)
out["non_ny_count"]               = len(non_ny)
out["non_ny_total"]               = round(sum(d["amount"] for d in non_ny), 2)

# -- 6. DONATION SIZE BREAKDOWN ------------------------------------------------
def bucket(amt):
    if amt <= 25:   return "micro_0_25"
    if amt <= 100:  return "small_26_100"
    if amt <= 250:  return "medium_101_250"
    if amt <= 500:  return "large_251_500"
    if amt <= 1000: return "major_501_1000"
    return "maxout_1001plus"
buckets = {}
for d in all_donors:
    b = bucket(d["amount"])
    if b not in buckets: buckets[b] = {"count": 0, "total": 0}
    buckets[b]["count"] += 1
    buckets[b]["total"] = round(buckets[b]["total"] + d["amount"], 2)
out["size_buckets"] = buckets

# -- 7. ZIOGAS PERSONAL FEC GIVING --------------------------------------------
cur.execute("""
    SELECT fc.committee_id, fc.contributor_last_name, fc.contributor_first_name,
           fc.contributor_city, fc.contributor_state,
           fc.contribution_amount, fc.contribution_date, fc.cycle, fc.party_signal,
           cm.committee_name, cm.classified_party
    FROM national_donors.fec_contributions fc
    LEFT JOIN national_donors.fec_committees cm ON fc.committee_id = cm.committee_id
    WHERE fc.contributor_last_name = 'ZIOGAS'
    ORDER BY fc.contribution_date DESC
""")
out["ziogas_personal_fec"] = [
    {"cmte_id": r[0], "last": r[1], "first": r[2], "city": r[3], "state": r[4],
     "amount": float(r[5]), "date": str(r[6]), "cycle": r[7],
     "party": r[8], "cmte_name": r[9], "cmte_party": r[10]}
    for r in cur.fetchall()
]

# -- 8. BOE GIVING HISTORY -----------------------------------------------------
cur.execute("""
    SELECT year, date, filer, party, first, last, city, state, amount
    FROM boe_donors.contributions
    WHERE last LIKE 'ZIOGAS%'
    ORDER BY date DESC
""")
out["ziogas_boe_giving"] = [
    {"year": r[0], "date": str(r[1]), "filer": r[2], "party": r[3],
     "first": r[4], "last": r[5], "city": r[6], "state": r[7], "amount": float(r[8])}
    for r in cur.fetchall()
]

# -- 9. VOTER FILE MATCH -------------------------------------------------------
cur.execute("""
    SELECT StateVoterId, FirstName, LastName, DOB, PrimaryAddress1,
           PrimaryCity, PrimaryZip, OfficialParty, SDName, CDName,
           CountyName, RegistrationStatus, RegistrationDate, LastVoterActivity,
           GeneralFrequency, PrimaryFrequency, OverAllFrequency,
           GeneralRegularity, PrimaryRegularity, Age, Gender
    FROM nys_voter_tagging.voter_file
    WHERE LastName LIKE 'ZIOGAS%'
""")
out["voter_file_matches"] = [
    {"id": r[0], "first": r[1], "last": r[2], "dob": str(r[3]),
     "address": r[4], "city": r[5], "zip": r[6], "party": r[7],
     "sd": r[8], "cd": r[9], "county": r[10], "status": r[11],
     "reg_date": str(r[12]), "last_vote": str(r[13]),
     "gen_freq": r[14], "pri_freq": r[15], "overall_freq": r[16],
     "gen_regularity": r[17], "pri_regularity": r[18],
     "age": r[19], "gender": r[20]}
    for r in cur.fetchall()
]

conn.close()

with open("D:\\git\\nys-voter-pipeline\\logs\\ziogas_full.json","w") as f:
    json.dump(out, f, indent=2, default=str)

# -- PRINT SUMMARY -------------------------------------------------------------
print("=" * 65)
print(f"ZIOGAS FOR CONGRESS (C00942599) -- FINANCE SUMMARY")
print("=" * 65)
print(f"Total contributions:  {out['total_contributions']}")
print(f"Total raised:         ${out['total_raised']:>10,.2f}")
print(f"In-district:          {out['in_district_count']:>4} donors / ${out['in_district_total']:>10,.2f}")
print(f"NY out-of-district:   {out['ny_out_of_district_count']:>4} donors / ${out['ny_out_of_district_total']:>10,.2f}")
print(f"Non-NY:               {out['non_ny_count']:>4} donors / ${out['non_ny_total']:>10,.2f}")
print()
print("STATE BREAKDOWN:")
for g in out["geo_state"][:10]:
    pct = g["total"] / out["total_raised"] * 100 if out["total_raised"] else 0
    print(f"  {g['state'] or 'UNK':5}  {g['count']:>4} gifts  ${g['total']:>10,.2f}  ({pct:.1f}%)")
print()
print("TOP CITIES:")
for g in out["geo_city"][:15]:
    print(f"  {g['city'] or 'UNK':25} {g['state']:5}  {g['count']:>3} gifts  ${g['total']:>10,.2f}")
print()
print("TOP INDIVIDUAL DONORS:")
for d in out["top_donors"][:20]:
    print(f"  {d['last']:20} {d['first']:15}  {d['city']:20} {d['state']}  ${d['total']:>8,.2f}  ({d['count']}x)")
print()
print("DONATION SIZE BREAKDOWN:")
for k,v in out["size_buckets"].items():
    print(f"  {k:25}  {v['count']:>4} gifts  ${v['total']:>10,.2f}")
print()
print(f"ZIOGAS PERSONAL FEC GIVING: {len(out['ziogas_personal_fec'])} records")
for g in out["ziogas_personal_fec"]:
    print(f"  {g['date']}  ${g['amount']:>8,.2f}  {g['cmte_name']}  ({g['party']})")
print()
print(f"ZIOGAS BOE GIVING: {len(out['ziogas_boe_giving'])} records")
for g in out["ziogas_boe_giving"]:
    print(f"  {g['date']}  ${g['amount']:>8,.2f}  {g['filer']}  ({g['party']})")
print()
print(f"VOTER FILE MATCHES: {len(out['voter_file_matches'])} records")
for v in out["voter_file_matches"]:
    print(f"  {v['first']} {v['last']} | DOB:{v['dob']} | {v['address']}, {v['city']} {v['zip']}")
    print(f"    Party:{v['party']} | SD:{v['sd']} | CD:{v['cd']} | Status:{v['status']}")
    print(f"    Reg:{v['reg_date']} | LastVote:{v['last_vote']} | Age:{v['age']} | Gender:{v['gender']}")
    print(f"    GenFreq:{v['gen_freq']} | PriFreq:{v['pri_freq']} | Overall:{v['overall_freq']}")
    print(f"    GenReg:{v['gen_regularity']} | PriReg:{v['pri_regularity']}")
