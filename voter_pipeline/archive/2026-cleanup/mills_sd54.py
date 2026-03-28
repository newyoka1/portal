import pymysql, dotenv, os
dotenv.load_dotenv()
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST"), port=int(os.getenv("MYSQL_PORT",3306)),
    user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"),
    database="boe_donors"
)
cur = conn.cursor()

# Get all NY Mills donors with address detail
cur.execute("""
    SELECT first, last, address, city, state, zip5, SUM(amount) as total, COUNT(*) as cnt
    FROM contributions
    WHERE filer = 'Mills for NY Senate' AND state = 'NY'
    GROUP BY first, last, address, city, state, zip5
    ORDER BY total DESC
""")
ny_donors = cur.fetchall()

# SD-54 zip codes: Ontario, Livingston, Wayne counties + Monroe towns Chili/Rush/Riga/Mendon/Wheatland
# Ontario County zips: 14424,14425,14415,14418,14432,14437,14441,14445,14456,14461,14462,14463,14468,14469,14470,14472,14481,14485,14487,14502,14512,14513,14522,14527,14532,14533,14534,14544,14546,14548,14549,14550,14551,14560,14564,14586,14589,14597
# Wayne County zips: 14489,14504,14507,14508,14510,14511,14514,14519,14520,14521,14529,14530,14531,14536,14537,14538,14539,14542,14543,14545,14547,14555,14558,14568,14569,14571,14572,14580,14585,14590,14591,14592,14593,14594,14595,14596
# Livingston County zips: 14411,14414,14416,14423,14433,14435,14454,14466,14467,14482,14486,14506,14518,14525,14526,14535,14541,14557,14559,14561,14563,14585
# Monroe towns (Chili/Rush/Riga/Mendon/Wheatland): 14514,14428,14445,14559,14624,14626
SD54_ZIPS = set([
    '14424','14425','14415','14418','14432','14437','14441','14445','14456','14461','14462',
    '14463','14468','14469','14470','14472','14481','14485','14487','14502','14512','14513',
    '14522','14527','14532','14533','14534','14544','14546','14548','14549','14550','14551',
    '14560','14564','14586','14589','14597',
    '14489','14504','14507','14508','14510','14511','14514','14519','14520','14521','14529',
    '14530','14531','14536','14537','14538','14539','14542','14543','14545','14547','14555',
    '14558','14568','14569','14571','14572','14580','14585','14590','14591','14592','14593',
    '14594','14595','14596',
    '14411','14414','14416','14423','14433','14435','14454','14466','14467','14482','14486',
    '14506','14518','14525','14526','14535','14541','14557','14559','14561','14563',
    '14428','14624','14626'
])

in_district = []
outside = []
for d in ny_donors:
    first, last, addr, city, state, zip5, total, cnt = d
    z = str(zip5).strip() if zip5 else ''
    if z in SD54_ZIPS:
        in_district.append(d)
    else:
        outside.append(d)

print(f"=== IN-DISTRICT (SD-54) NY DONORS ===")
print(f"Count: {len(in_district)}")
match_total = sum(min(float(d[6]),250) for d in in_district if float(d[6])>=10)
qual_count = sum(1 for d in in_district if float(d[6])>=10)
print(f"Qualifying donors (>=$10): {qual_count}")
print(f"Matchable amount (capped $250): ${match_total:,.2f}")
print()
for d in in_district:
    print(f"  {d[0]} {d[1]}, {d[3]} {d[4]} {d[4]}, ${float(d[6]):.2f}")

print(f"\n=== OUTSIDE SD-54 NY DONORS ===")
print(f"Count: {len(outside)}")
for d in outside[:20]:
    print(f"  {d[0]} {d[1]}, {d[3]}, {d[4]}, ${float(d[6]):.2f}")
