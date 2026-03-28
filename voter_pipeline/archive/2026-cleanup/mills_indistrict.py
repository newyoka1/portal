import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="nys_voter_tagging"
)
cur = conn.cursor()
out = []

# SD-54 in-district donors to Mills -- cross-reference voter file
# SD-54 = NYSSenateDistrict = 54
# Join voter_file to boe_donors.contributions on name+address match for Mills campaign

# First: how many Mills campaign donors have NY addresses in SD-54 zip codes?
# Get all NY Mills donors
cur.execute("""
    SELECT DISTINCT c.first, c.last, c.address, c.city, c.state, c.zip5, SUM(c.amount) as total
    FROM boe_donors.contributions c
    WHERE c.filer = 'Mills for NY Senate' AND c.state = 'NY'
    GROUP BY c.first, c.last, c.address, c.city, c.state, c.zip5
""")
ny_donors = cur.fetchall()
out.append(f"Total unique NY address donors: {len(ny_donors)}")

# Check which are in SD-54 via voter file match on last name + zip
in_district = []
not_matched = []
for d in ny_donors:
    first, last, addr, city, state, zip5, total = d
    cur.execute("""
        SELECT StateVoterId, FirstName, LastName, ResidenceCity, NYSSenateDistrict, EnrollmentParty
        FROM voter_file
        WHERE LastName = %s AND zip5 = %s
        AND NYSSenateDistrict IS NOT NULL
        LIMIT 5
    """, (last.upper(), zip5))
    rows = cur.fetchall()
    if rows:
        # Check if any match is in SD-54
        sd54_matches = [r for r in rows if r[4] == 54]
        if sd54_matches:
            in_district.append((first, last, city, zip5, total))
        else:
            # Found in voter file but different district
            not_matched.append((first, last, city, zip5, total, rows[0][4]))
    else:
        # Zip not in voter file at all
        not_matched.append((first, last, city, zip5, total, 'NO_MATCH'))

out.append(f"\nIn-district (SD-54) donors by zip+lastname match: {len(in_district)}")
matchable = sum(min(float(d[4]),250) for d in in_district if float(d[4]) >= 10)
out.append(f"Matchable amount from in-district donors: ${matchable:,.2f}")

out.append(f"\nNot matched / different district: {len(not_matched)}")
out.append("\nSample non-SD54 NY donors:")
for d in not_matched[:15]:
    out.append(f"  {d[0]} {d[1]}, {d[2]} {d[3]} | ${float(d[4]):.2f} | dist: {d[5]}")

out.append("\nIn-district donors list:")
for d in in_district:
    out.append(f"  {d[0]} {d[1]}, {d[2]} {d[3]} | ${float(d[4]):.2f}")

result = "\n".join(out)
with open("logs/mills_indistrict.txt","w") as f:
    f.write(result)
print(result[:3000])
