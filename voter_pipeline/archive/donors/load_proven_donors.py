import os, sys, csv, time, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)
import mysql.connector

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE, "data", "boe_donors", "ProvenDonors2024OnePerInd.csv")
BATCH    = 50_000
YEARS    = list(range(2018, 2025))
AMT_COLS  = [f"D{y}amt" for y in YEARS] + [f"R{y}amt" for y in YEARS] + [f"U{y}amt" for y in YEARS]
CNT_COLS  = [f"D{y}cnt" for y in YEARS] + [f"R{y}cnt" for y in YEARS] + [f"U{y}cnt" for y in YEARS]
FLAG_COLS = ["ContribToRep","ContribToDem","ContribToUnk","Alist","Blist","Clist","ClistDEM","BlistDEM"]
STR_COLS  = ["sboeid","voterparty","LASTNAME","FIRSTNAME","ZIPCODE","Countyname","adval","sdval","cdval","email"]
ALL_COLS  = STR_COLS + AMT_COLS + CNT_COLS + FLAG_COLS

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST","127.0.0.1"), port=int(os.getenv("MYSQL_PORT",3306)),
        user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"),
        database="nys_voter_tagging", connection_timeout=600, autocommit=False)

def run(cur, sql, label=""):
    t = time.time(); cur.execute(sql)
    if label: print("  %s (%.1fs)" % (label, time.time()-t))

def ensure_tracking(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_file_tracking (
        filename VARCHAR(500) PRIMARY KEY, file_mtime DOUBLE NOT NULL,
        file_size BIGINT NOT NULL, row_count INT DEFAULT NULL,
        last_loaded DATETIME NOT NULL) ENGINE=InnoDB""")

def file_changed(cur, path):
    s = os.stat(path)
    cur.execute("SELECT file_mtime,file_size FROM pipeline_file_tracking WHERE filename=%s",
                (os.path.basename(path),))
    row = cur.fetchone()
    if row is None: return True
    return abs(s.st_mtime - row[0]) > 2 or s.st_size != row[1]

def mark_loaded(cur, path, n):
    s = os.stat(path)
    cur.execute("""INSERT INTO pipeline_file_tracking
        (filename,file_mtime,file_size,row_count,last_loaded) VALUES (%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE file_mtime=VALUES(file_mtime),file_size=VALUES(file_size),
        row_count=VALUES(row_count),last_loaded=NOW()""",
        (os.path.basename(path), s.st_mtime, s.st_size, n))

CREATE_SQL = """CREATE TABLE boe_proven_donors (
    sboeid VARCHAR(30), voterparty VARCHAR(10),
    LASTNAME VARCHAR(60), FIRSTNAME VARCHAR(40), ZIPCODE VARCHAR(10),
    Countyname VARCHAR(50), adval VARCHAR(10), sdval VARCHAR(10), cdval VARCHAR(10), email VARCHAR(120),
    D2018amt DECIMAL(12,2), D2019amt DECIMAL(12,2), D2020amt DECIMAL(12,2),
    D2021amt DECIMAL(12,2), D2022amt DECIMAL(12,2), D2023amt DECIMAL(12,2), D2024amt DECIMAL(12,2),
    R2018amt DECIMAL(12,2), R2019amt DECIMAL(12,2), R2020amt DECIMAL(12,2),
    R2021amt DECIMAL(12,2), R2022amt DECIMAL(12,2), R2023amt DECIMAL(12,2), R2024amt DECIMAL(12,2),
    U2018amt DECIMAL(12,2), U2019amt DECIMAL(12,2), U2020amt DECIMAL(12,2),
    U2021amt DECIMAL(12,2), U2022amt DECIMAL(12,2), U2023amt DECIMAL(12,2), U2024amt DECIMAL(12,2),
    D2018cnt SMALLINT, D2019cnt SMALLINT, D2020cnt SMALLINT, D2021cnt SMALLINT,
    D2022cnt SMALLINT, D2023cnt SMALLINT, D2024cnt SMALLINT,
    R2018cnt SMALLINT, R2019cnt SMALLINT, R2020cnt SMALLINT, R2021cnt SMALLINT,
    R2022cnt SMALLINT, R2023cnt SMALLINT, R2024cnt SMALLINT,
    U2018cnt SMALLINT, U2019cnt SMALLINT, U2020cnt SMALLINT, U2021cnt SMALLINT,
    U2022cnt SMALLINT, U2023cnt SMALLINT, U2024cnt SMALLINT,
    ContribToRep SMALLINT, ContribToDem SMALLINT, ContribToUnk SMALLINT,
    Alist SMALLINT, Blist SMALLINT, Clist SMALLINT, ClistDEM SMALLINT, BlistDEM SMALLINT,
    INDEX idx_sboeid (sboeid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""

INSERT_SQL = "INSERT INTO boe_proven_donors (%s) VALUES (%s)" % (
    ",".join("`"+c+"`" for c in ALL_COLS), ",".join(["%s"]*len(ALL_COLS)))

def parse_row(row):
    vals = []
    for c in STR_COLS:
        v = (row.get(c) or "").strip(); vals.append(v if v else None)
    for c in AMT_COLS:
        try: vals.append(float(row.get(c) or 0))
        except: vals.append(0.0)
    for c in CNT_COLS + FLAG_COLS:
        try: vals.append(int(float(row.get(c) or 0)))
        except: vals.append(0)
    return vals

def load_csv(conn, force=False):
    cur = conn.cursor()
    ensure_tracking(cur); conn.commit()
    if not force and not file_changed(cur, CSV_PATH):
        cur.execute("SELECT COUNT(*) FROM boe_proven_donors")
        n = cur.fetchone()[0]
        print("  File unchanged - %s rows loaded. Use --force to reload." % format(n,","))
        return n
    print("\nStep 1: Dropping and recreating boe_proven_donors...")
    run(cur, "DROP TABLE IF EXISTS boe_proven_donors", "dropped")
    run(cur, CREATE_SQL, "created")
    conn.commit()
    print("\nStep 2: Loading CSV in %s-row batches..." % format(BATCH,","))
    total = 0; batch = []; t0 = time.time()
    with open(CSV_PATH, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not (row.get("sboeid") or "").strip(): continue
            batch.append(parse_row(row))
            if len(batch) >= BATCH:
                cur.executemany(INSERT_SQL, batch); conn.commit()
                total += len(batch); batch = []
                print("  %9s rows  (%s rows/sec)" % (format(total,","), format(int(total/(time.time()-t0)),",")))
    if batch:
        cur.executemany(INSERT_SQL, batch); conn.commit(); total += len(batch)
    elapsed = time.time()-t0
    print("\n  Finished: %s rows in %.1fs (%s rows/sec)" % (format(total,","), elapsed, format(int(total/elapsed),",")))
    cur.execute("SELECT COUNT(*) FROM boe_proven_donors")
    db_n = cur.fetchone()[0]; print("  DB count: %s" % format(db_n,","))
    mark_loaded(cur, CSV_PATH, db_n); conn.commit()
    return db_n

DONOR_COLS = (
    [("donor_D_total","DECIMAL(14,2) DEFAULT NULL"),
     ("donor_R_total","DECIMAL(14,2) DEFAULT NULL"),
     ("donor_U_total","DECIMAL(14,2) DEFAULT NULL")]
    + [("donor_D%damt" % y,"DECIMAL(12,2) DEFAULT NULL") for y in YEARS]
    + [("donor_R%damt" % y,"DECIMAL(12,2) DEFAULT NULL") for y in YEARS]
    + [("donor_U%damt" % y,"DECIMAL(12,2) DEFAULT NULL") for y in YEARS]
    + [("donor_Alist","TINYINT DEFAULT NULL"),
       ("donor_Blist","TINYINT DEFAULT NULL"),
       ("donor_Clist","TINYINT DEFAULT NULL"),
       ("donor_ClistDEM","TINYINT DEFAULT NULL"),
       ("donor_BlistDEM","TINYINT DEFAULT NULL"),
       ("donor_email","VARCHAR(120) DEFAULT NULL")]
)

def enrich_voters(conn):
    cur = conn.cursor()
    print("\nStep 3: Adding donor columns to voter_file...")
    for col, typedef in DONOR_COLS:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
            ("nys_voter_tagging","voter_file",col))
        if cur.fetchone()[0] == 0:
            cur.execute("ALTER TABLE voter_file ADD COLUMN `%s` %s" % (col, typedef))
            print("  Added: %s" % col)
    conn.commit(); print("  All columns ready.")
    print("\nStep 4: Clearing old donor values...")
    run(cur, """UPDATE voter_file SET donor_D_total=NULL,donor_R_total=NULL,donor_U_total=NULL
        WHERE donor_D_total IS NOT NULL OR donor_R_total IS NOT NULL OR donor_U_total IS NOT NULL""", "cleared")
    conn.commit()
    print("\nStep 5: Enriching voter_file from boe_proven_donors...")
    d_sum = "+".join("COALESCE(d.D%damt,0)" % y for y in YEARS)
    r_sum = "+".join("COALESCE(d.R%damt,0)" % y for y in YEARS)
    u_sum = "+".join("COALESCE(d.U%damt,0)" % y for y in YEARS)
    yr_sets = []
    for y in YEARS:
        yr_sets += ["v.donor_D%damt=NULLIF(d.D%damt,0)" % (y,y),
                    "v.donor_R%damt=NULLIF(d.R%damt,0)" % (y,y),
                    "v.donor_U%damt=NULLIF(d.U%damt,0)" % (y,y)]
    update_sql = """UPDATE voter_file v
        INNER JOIN boe_proven_donors d ON d.sboeid = v.StateVoterId
        SET v.donor_D_total=NULLIF(%s,0),
            v.donor_R_total=NULLIF(%s,0),
            v.donor_U_total=NULLIF(%s,0),
            %s,
            v.donor_Alist=NULLIF(d.Alist,0),
            v.donor_Blist=NULLIF(d.Blist,0),
            v.donor_Clist=NULLIF(d.Clist,0),
            v.donor_ClistDEM=NULLIF(d.ClistDEM,0),
            v.donor_BlistDEM=NULLIF(d.BlistDEM,0),
            v.donor_email=NULLIF(d.email,'')""" % (d_sum, r_sum, u_sum, ",\n            ".join(yr_sets))
    run(cur, update_sql, "enriched")
    conn.commit()
    print("  Rows affected: %s" % format(cur.rowcount,","))
    print("\nStep 6: Summary...")
    for label, filt in [
        ("D donors","donor_D_total IS NOT NULL"),
        ("R donors","donor_R_total IS NOT NULL"),
        ("U donors","donor_U_total IS NOT NULL"),
        ("A-list","donor_Alist=1"),
        ("B-list","donor_Blist=1"),
    ]:
        cur.execute("SELECT COUNT(*) FROM voter_file WHERE %s" % filt)
        print("  %s: %s" % (label, format(cur.fetchone()[0],",")))
    cur.execute("SELECT SUM(donor_D_total),SUM(donor_R_total),SUM(donor_U_total) FROM voter_file")
    d,r,u = cur.fetchone()
    print("  Total $  D=%s  R=%s  U=%s" % (format(int(d or 0),","),format(int(r or 0),","),format(int(u or 0),",")))

def main(force=False, enrich_only=False):
    print("="*60)
    print("BOE Proven Donors: Load + Enrich voter_file")
    print("="*60)
    if not os.path.exists(CSV_PATH):
        print("ERROR: CSV not found: %s" % CSV_PATH); sys.exit(1)
    s = os.stat(CSV_PATH)
    print("CSV: %s | %.1f MB | %s" % (os.path.basename(CSV_PATH), s.st_size/1024/1024, time.ctime(s.st_mtime)))
    conn = get_conn()
    if not enrich_only:
        load_csv(conn, force=force)
    enrich_voters(conn)
    conn.close()
    print("\n" + "="*60 + "\nDone.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true")
    p.add_argument("--enrich-only", action="store_true")
    a = p.parse_args()
    main(force=a.force, enrich_only=a.enrich_only)
