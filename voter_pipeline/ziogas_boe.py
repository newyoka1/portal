import pymysql, dotenv, os, json
dotenv.load_dotenv()
conn = pymysql.connect(
    host="127.0.0.1",
    port=int(os.getenv("MYSQL_PORT", 3306)),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="boe_donors"
)
cur = conn.cursor()

out = {}

# 1. Search for Ziogas as a donor (her personal giving history)
cur.execute("""
    SELECT year, date, filer, party, first, last, city, state, amount
    FROM contributions
    WHERE last LIKE '%ZIOGAS%'
    ORDER BY date DESC
""")
rows = cur.fetchall()
out["ziogas_as_donor"] = [{"year":r[0],"date":str(r[1]),"filer":r[2],"party":r[3],"first":r[4],"last":r[5],"city":r[6],"state":r[7],"amount":float(r[8])} for r in rows]

# 2. Search BOE for any committee receiving money from Ziogas campaign (won't have — federal race — but check)
cur.execute("""
    SELECT DISTINCT filer FROM contributions WHERE filer LIKE '%ZIOGAS%'
""")
ziogas_filers = [r[0] for r in cur.fetchall()]
out["ziogas_filers"] = ziogas_filers

# 3. Search for donors from Orsted or wind energy affiliation
cur.execute("""
    SELECT year, date, filer, party, first, last, city, state, amount
    FROM contributions
    WHERE last LIKE '%ZIOGAS%' OR (first LIKE '%ALLISON%' AND last LIKE '%ZIOG%')
    ORDER BY date
""")
rows2 = cur.fetchall()
out["all_ziogas_hits"] = len(rows2)

conn.close()

_out = __import__('pathlib').Path(__file__).parent / "logs" / "ziogas_boe.json"
_out.parent.mkdir(exist_ok=True)
with open(_out, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(json.dumps(out, indent=2, default=str))
