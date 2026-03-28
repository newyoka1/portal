#!/usr/bin/env python3
"""
Reclassify U contributions with better party identification.
Two-layer approach:
  1. Expanded regex patterns (unions -> D, DGA/RGA -> D/R, etc.)
  2. Named filer overrides for top candidate committees + known PACs

After updating contributions.party, rebuilds voter_contribs and boe_donor_summary.
Writes progress to party_reclassify_log.txt
"""
import os, datetime, time
from dotenv import load_dotenv
import pymysql

load_dotenv()

LOG = open(r"D:\git\nys-voter-pipeline\party_reclassify_log.txt","w",encoding="ascii",errors="replace",buffering=1)
def log(msg=""):
    print(msg)
    LOG.write(msg + "\n")

YEAR_MAX = datetime.date.today().year
YEAR_MIN = YEAR_MAX - 9
YEARS    = list(range(YEAR_MIN, YEAR_MAX + 1))

def connect(db=None):
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST","127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT","3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=db, charset="utf8mb4", autocommit=True,
        connect_timeout=30, read_timeout=600, write_timeout=600
    )

# ---------------------------------------------------------------------------
# Layer 1: Regex patterns applied to filer name
# ---------------------------------------------------------------------------
# Each tuple: (SQL REGEXP pattern, party)
# Applied in order - first match wins (D before R for any overlap)
REGEX_PATTERNS = [
    # --- Democratic ---
    # Explicit party org names
    (r'DGA\b|DEMOCRATIC.GOVERNORS', 'D'),
    (r'WORKING.FAMILIES', 'D'),
    (r'\bDSA\b|DEMOCRATIC.SOCIALIST', 'D'),
    (r'ELEANOR.ROOSEVELT', 'D'),
    (r'COURAGE.TO.CHANGE', 'D'),
    (r'FOR.THE.MANY', 'D'),
    (r'VOTE.?COPE|VOICE.OF.TEACHERS', 'D'),
    (r'PLANNED.PARENTHOOD|NARAL|\bEMILY', 'D'),
    (r'NEW YORK CIVIL LIBERTIES|NYCLU', 'D'),
    (r'\bDGA\b', 'D'),
    (r'GREEN.PARTY', 'D'),          # Left/Green -> D-leaning in NY context
    # Labor unions -> universally D-leaning in NY
    (r'\bSEIU\b|SERVICE.EMPLOYEES', 'D'),
    (r'\bUFT\b|\bNYSUT\b|\bCSEA\b|\bPEF\b', 'D'),
    (r'\bAFL.?CIO\b', 'D'),
    (r'\bIBEW\b|ELECTRICAL.WORKERS', 'D'),
    (r'MASON.TENDERS|LABORERS|LABORER', 'D'),
    (r'PLUMBERS|STEAMFITTERS|PIPEFITTERS', 'D'),
    (r'IRONWORKERS|IRON.WORKERS', 'D'),
    (r'BOILERMAKERS', 'D'),
    (r'AMALGAMATED.TRANSIT|TRANSIT.UNION', 'D'),
    (r'TEAMSTERS', 'D'),
    (r'\bUAW\b|AUTOWORKERS', 'D'),
    (r'\bCWA\b|COMMUNICATIONS.WORKERS', 'D'),
    (r'CARPENTERS.LOCAL|CARPENTERS.DISTRICT', 'D'),
    (r'TEACHERS.UNION|TEACHERS.FEDERATION|TEACHERS.ASSOC', 'D'),
    (r'NURSES.ASSOC|NURSES.UNITED', 'D'),
    (r'OPERATING.ENGINEERS', 'D'),
    (r'SHEET.METAL.WORKERS', 'D'),
    (r'PAINTERS.DISTRICT|PAINTERS.LOCAL', 'D'),
    (r'BRICKLAYERS', 'D'),
    (r'SANDHOGS|TUNNEL.WORKERS', 'D'),
    (r'HOTEL.WORKERS|UNITE.HERE', 'D'),
    (r'\bCOPE\b', 'D'),             # Committee on Political Education (union PAC suffix)
    (r'POLITICAL.ACTION.COMMITTEE.*LOCAL|LOCAL.*POLITICAL.ACTION', 'D'),

    # --- Republican ---
    (r'\bRGA\b|REPUBLICAN.GOVERNORS', 'R'),
    (r'\bNRCC\b|\bNRSC\b', 'R'),
    (r'CONSERVATIVE.PARTY', 'R'),
    (r'TRUMP', 'R'),                 # belt-and-suspenders (already caught by existing regex)
    (r'\bMAGA\b|AMERICA.FIRST', 'R'),
    (r'FREEDOM.CAUCUS', 'R'),
]

# ---------------------------------------------------------------------------
# Layer 2: Named filer overrides
# Filer name must match exactly (case-insensitive). Partial matches use LIKE.
# Format: (partial_match_string, party)
# ---------------------------------------------------------------------------
FILER_OVERRIDES = [
    # --- Democratic candidates / committees ---
    ("Friends for Kathy Hochul",           "D"),
    ("Kathy Hochul",                        "D"),
    ("Andrew Cuomo",                        "D"),
    ("De Blasio",                           "D"),
    ("Eric Adams",                          "D"),
    ("Letitia James",                       "D"),
    ("James for NY",                        "D"),
    ("James For NY",                        "D"),
    ("Alvin Bragg",                         "D"),
    ("Cynthia For New York",                "D"),
    ("Zohran For Assembly",                 "D"),
    ("Zohran for NY",                       "D"),
    ("Jabari for State Senate",             "D"),
    ("Jabari 2017",                         "D"),
    ("Lander For Nyc",                      "D"),
    ("Jumaane",                             "D"),
    ("Eleanor Roosevelt Legacy",            "D"),
    ("Working Families Party",              "D"),
    ("Courage To Change",                   "D"),
    ("For the Many Action",                 "D"),
    ("Dsa For The Many",                    "D"),
    ("DGA New York",                        "D"),
    ("Brown For Buffalo",                   "D"),
    ("Gaughran for New York",               "D"),
    ("Alessandra Biaggi",                   "D"),
    ("New Yorkers for Equal Rights",        "D"),
    ("Corey 2021",                          "D"),
    ("Maud for Manhattan",                  "D"),
    ("Phara For Assembly",                  "D"),
    ("Zellnor For New York",                "D"),
    ("Ron Kim For New York",                "D"),
    ("Kristen for New York",                "D"),
    ("Sarahana for Assembly",               "D"),
    ("Sarahana for 103",                    "D"),
    ("Eliza Orlins",                        "D"),
    ("Dan Goldman",                         "D"),
    ("Van Bramer",                          "D"),
    ("Hinchey For Ny",                      "D"),
    ("Hinchey for New York",                "D"),
    ("Mannion For State Senate",            "D"),
    ("Pete Harckham",                       "D"),
    ("Gina For Assembly",                   "D"),
    ("Yvette 4 NY",                         "D"),
    ("Sean Ryan for Buffalo",               "D"),
    ("Stringer 2017",                       "D"),
    ("Lucy Lang For Ny",                    "D"),
    ("Eric Gonzalez For Brooklyn",          "D"),
    ("Dan Quart For Nyc",                   "D"),
    ("Kaminsky For New York",               "D"),
    ("Gustavo Rivera",                      "D"),
    ("Latimer For Westchester",             "D"),
    ("Maloney For New York",                "D"),
    ("Jack Schnirman",                      "D"),
    ("Mimi Rocah For DA",                   "D"),
    ("Tim Sini for DA",                     "D"),
    ("Bellone For Suffolk",                 "D"),
    ("Delgado For New York",                "D"),
    ("Delgado for New York",                "D"),
    ("Trailblazers Political Action",       "D"),
    ("Tahanie For New York",                "D"),
    ("Yuh-Line Niou",                       "D"),
    ("Zephyr For New York",                 "D"),
    ("Nicole For New York City",            "D"),
    ("Salazar For State Senate",            "D"),
    ("Ramos For State Senate",              "D"),
    ("Samra Brouk",                         "D"),
    ("Caban For Queens",                    "D"),
    ("Jackson For Senate",                  "D"),
    ("Espinal For Nyc",                     "D"),
    ("Dawn For New York",                   "D"),
    ("Elijah for Senate",                   "D"),
    ("People For Catalina Cruz",            "D"),
    ("Claire Cousin for Assembly",          "D"),
    ("Marcela For New York",                "D"),
    ("Diana for Queens",                    "D"),
    ("Danny For Nyc",                       "D"),
    ("DREAM for NYC",                       "D"),
    ("Khaleel Anderson",                    "D"),
    ("New Yorkers For Alex Bores",          "D"),
    ("Ydanis For Nyc",                      "D"),
    ("April 4 Erie County",                 "D"),
    ("April Baskin",                        "D"),
    ("Sarahana",                            "D"),
    ("Dickens For New York",                "D"),
    ("Friends Of Rachel May",               "D"),
    ("Friends Of Jen Metzger",              "D"),
    ("Friends of Jen Lunsford",             "D"),
    ("Curran for Nassau",                   "D"),   # Laura Curran - D
    ("David for State Senate",              "D"),
    ("Mcevoy For Assembly",                 "D"),
    ("Friends Of Ruth Walter",              "D"),
    ("Friends Of Christine Pellegrino",     "D"),
    ("Friends Of Karen S. Smythe",          "D"),
    ("Capital Women",                       "D"),
    ("Henry for New York",                  "D"),   # Charles Henry - D
    ("Skoufis for Senate",                  "D"),
    ("Friends Of Kevin Parker",             "D"),
    ("Friends Of Leslie",                   "D"),
    ("Nelson For Senate",                   "D"),
    ("Friends Of Robert Carroll",           "D"),
    ("Wright For NY",                       "D"),
    ("Grasso for Queens",                   "D"),
    ("Dana for Assembly",                   "D"),
    ("IWEN for New York",                   "D"),
    ("Cooney for New York",                 "D"),
    ("Garcia for Sheriff",                  "D"),   # Leticia Ramos Garcia - D
    ("Andrew For New York",                 "D"),   # Andrew Yang 2021 (D primary)
    ("Friends Of Monica Martinez",          "D"),
    ("Friends Of Shawyn Patterson-Howard",  "D"),
    ("Maud for Manhattan",                  "D"),
    ("Friends Of Kevin Thomas For Senate",  "D"),
    ("People For JGR",                      "D"),
    ("Liz Crotty For Manhattan",            "D"),
    ("Friends of Minita",                   "D"),
    ("Josh Lafazan",                        "D"),
    ("Nyc Diaz",                            "D"),
    ("Friends Of Rebecca Seawright",        "D"),
    ("Friends of April Baskin",             "D"),

    # --- Republican candidates / committees ---
    ("Zeldin for New York",                 "R"),
    ("Zeldin For New York",                 "R"),
    ("ELISE FOR GOVERNOR",                  "R"),
    ("Elise for Governor",                  "R"),
    ("Elise Stefanik",                      "R"),
    ("Friends Of Rob Astorino",             "R"),
    ("Astorino",                            "R"),
    ("Molinaro For New York",               "R"),
    ("Molinaro For Dutchess",               "R"),
    ("Andrew Giuliani",                     "R"),
    ("Citizens For Saladino",               "R"),
    ("Friends Of Don Clavin",               "R"),
    ("New Yorkers for Lower Costs",         "R"),
    ("Save New York",                       "R"),
    ("Martins For Nassau",                  "R"),
    ("Ray Tierney for District Attorney",   "R"),
    ("Andrew Giuliani 2022",                "R"),
    ("Gallivan For Senate",                 "R"),
    ("Borrello For Senate",                 "R"),
    ("Friends of Chris Tague",              "R"),
    ("Friends Of Peter Oberacker",          "R"),
    ("Rolison for NY",                      "R"),
    ("Norber For Assembly",                 "R"),
    ("People For Colavita",                 "R"),
    ("Friends Of Neil Foley",               "R"),
    ("Koslow for Nassau",                   "R"),
    ("Friends Of Steven J. Flotteron",      "R"),
    ("Friends Of Ed Wehrheim",              "R"),
    ("Friends Of Kevin Hardwick",           "R"),
    ("Citizens For Saladino",               "R"),
    ("Curran for Nassau County Executive",  "R"),  # if not Laura Curran
    ("Friends Of John Kennedy",             "R"),   # John Kennedy Jr. - Nassau R
    ("Garcia for Sheriff",                  "R"),   # overridden below if needed - check
    ("Friends Of Mark Poloncarz",           "R"),   # Mark Poloncarz is D actually - override
    ("Friends Of Christopher P Scanlon",    "R"),
    ("Jack Schnirman for Nassau",           "D"),   # D - override any R above
    ("FOCUS NY PAC",                        "R"),
    ("Friends Of Dana Remus",               "D"),
    ("Friends of Sheriff Kirk Imperati",    "R"),
    ("Friends Of Dan Panico",               "R"),
    ("Donnelly For District Attorney",      "R"),
    ("Friends Of Steve Neuhaus",            "R"),
    ("Grasso for Queens",                   "D"),
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
try:
    conn = connect("boe_donors")
    cur  = conn.cursor()

    log("=" * 70)
    log("PARTY RECLASSIFICATION - U -> D/R where identifiable")
    log("=" * 70)

    # Baseline counts
    cur.execute("SELECT party, COUNT(*), SUM(amount) FROM contributions GROUP BY party ORDER BY party")
    log("\nBaseline contributions by party:")
    for r in cur.fetchall():
        log(f"  {r[0]}:  {r[1]:>10,} contribs  ${r[2]:>14,.2f}")

    # --------------------------------------------------
    # Layer 1: Regex patterns
    # --------------------------------------------------
    log("\nLayer 1: Regex patterns...")
    total_regex_updated = 0
    for pattern, party in REGEX_PATTERNS:
        cur.execute(
            f"UPDATE contributions SET party='{party}'"
            f" WHERE party='U'"
            f" AND UPPER(filer) REGEXP '{pattern}'"
        )
        n = cur.rowcount
        if n > 0:
            total_regex_updated += n
            log(f"  {party}  +{n:>8,}  [{pattern[:60]}]")

    log(f"\n  Total regex updates: {total_regex_updated:,}")

    # --------------------------------------------------
    # Layer 2: Named filer overrides (LIKE match, case-insensitive)
    # --------------------------------------------------
    log("\nLayer 2: Named filer overrides...")
    total_override_updated = 0
    for filer_str, party in FILER_OVERRIDES:
        # Only update if currently U (or any wrong party for corrections)
        cur.execute(
            f"UPDATE contributions SET party='{party}'"
            f" WHERE filer LIKE %s",
            (f"%{filer_str}%",)
        )
        n = cur.rowcount
        if n > 0:
            total_override_updated += n
            log(f"  {party}  +{n:>8,}  [{filer_str}]")

    log(f"\n  Total override updates: {total_override_updated:,}")

    # Final counts
    cur.execute("SELECT party, COUNT(*), SUM(amount) FROM contributions GROUP BY party ORDER BY party")
    log("\nFinal contributions by party:")
    for r in cur.fetchall():
        log(f"  {r[0]}:  {r[1]:>10,} contribs  ${r[2]:>14,.2f}")

    # How many U remain
    cur.execute("SELECT COUNT(DISTINCT filer) FROM contributions WHERE party='U'")
    log(f"\n  Remaining unique U filers: {cur.fetchone()[0]:,}")
    log(f"  Total reclassified: {total_regex_updated + total_override_updated:,}")

    # --------------------------------------------------
    # Rebuild voter_contribs
    # --------------------------------------------------
    log("\nRebuilding voter_contribs...")
    t0 = time.time()
    cur.execute("DROP TABLE IF EXISTS voter_contribs")
    cur.execute("""CREATE TABLE voter_contribs (
        StateVoterId VARCHAR(50),
        year         INT,
        party        CHAR(1),
        total        DECIMAL(12,2),
        count        INT,
        PRIMARY KEY  (StateVoterId, year, party),
        INDEX idx_svid (StateVoterId)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")

    # Pass 1: exact match (uses pre-computed clean columns)
    cur.execute(
        "INSERT INTO voter_contribs (StateVoterId, year, party, total, count)"
        " SELECT v.StateVoterId, c.year, c.party, SUM(c.amount), COUNT(*)"
        " FROM nys_voter_tagging.voter_file v"
        " JOIN boe_donors.contributions c"
        "   ON v.clean_last  = c.last"
        "  AND v.clean_first = c.first"
        "  AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5"
        " WHERE v.clean_last IS NOT NULL"
        "  AND c.zip5 != ''"
        " GROUP BY v.StateVoterId, c.year, c.party"
    )
    matched = cur.rowcount
    # Pass 2: hyphenated fallback — two separate queries (avoids OR)
    for part_label, part_col in [("h1", "clean_last_h1"), ("h2", "clean_last_h2")]:
        cur.execute(
            "INSERT IGNORE INTO voter_contribs (StateVoterId, year, party, total, count)"
            " SELECT v.StateVoterId, c.year, c.party, SUM(c.amount), COUNT(*)"
            " FROM nys_voter_tagging.voter_file v"
            " JOIN boe_donors.contributions c"
            f"   ON v.{part_col}  = c.last"
            "  AND v.clean_first = c.first"
            "  AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5"
            f" WHERE v.{part_col} IS NOT NULL"
            "  AND c.zip5 != ''"
            " GROUP BY v.StateVoterId, c.year, c.party"
        )
        matched += cur.rowcount
    log(f"  {matched:,} matched records  ({time.time()-t0:.1f}s)")

    # --------------------------------------------------
    # Rebuild boe_donor_summary
    # --------------------------------------------------
    log("\nRebuilding boe_donor_summary...")

    year_col_defs = []
    for yr in YEARS:
        year_col_defs += [
            f"y{yr}_D_amt   DECIMAL(12,2) DEFAULT 0",
            f"y{yr}_D_count INT           DEFAULT 0",
            f"y{yr}_R_amt   DECIMAL(12,2) DEFAULT 0",
            f"y{yr}_R_count INT           DEFAULT 0",
            f"y{yr}_U_amt   DECIMAL(12,2) DEFAULT 0",
            f"y{yr}_U_count INT           DEFAULT 0",
        ]

    cur.execute("DROP TABLE IF EXISTS boe_donor_summary")
    cur.execute(
        "CREATE TABLE boe_donor_summary (\n"
        "    StateVoterId  VARCHAR(50) PRIMARY KEY,\n"
        "    " + ",\n    ".join(year_col_defs) + ",\n"
        "    total_D_amt   DECIMAL(12,2) DEFAULT 0,\n"
        "    total_D_count INT           DEFAULT 0,\n"
        "    total_R_amt   DECIMAL(12,2) DEFAULT 0,\n"
        "    total_R_count INT           DEFAULT 0,\n"
        "    total_U_amt   DECIMAL(12,2) DEFAULT 0,\n"
        "    total_U_count INT           DEFAULT 0,\n"
        "    total_amt     DECIMAL(12,2) DEFAULT 0,\n"
        "    total_count   INT           DEFAULT 0,\n"
        "    last_date     DATE,\n"
        "    last_filer    VARCHAR(255),\n"
        "    INDEX idx_total (total_amt),\n"
        "    INDEX idx_D     (total_D_amt),\n"
        "    INDEX idx_R     (total_R_amt)\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    )

    cur.execute("INSERT INTO boe_donor_summary (StateVoterId) SELECT DISTINCT StateVoterId FROM voter_contribs")
    cur.execute("SELECT COUNT(*) FROM boe_donor_summary")
    log(f"  Seeded {cur.fetchone()[0]:,} donors")

    for yr in YEARS:
        t0 = time.time()
        for party in ['D', 'R', 'U']:
            cur.execute(
                f"UPDATE boe_donor_summary s"
                f" JOIN (SELECT StateVoterId, SUM(total) AS amt, SUM(count) AS cnt"
                f"       FROM voter_contribs WHERE year={yr} AND party='{party}'"
                f"       GROUP BY StateVoterId) v ON s.StateVoterId=v.StateVoterId"
                f" SET s.y{yr}_{party}_amt=v.amt, s.y{yr}_{party}_count=v.cnt"
            )
        log(f"  Pivoted {yr}  ({time.time()-t0:.1f}s)")

    # Totals
    d_amt = "+".join([f"y{yr}_D_amt"   for yr in YEARS])
    d_cnt = "+".join([f"y{yr}_D_count" for yr in YEARS])
    r_amt = "+".join([f"y{yr}_R_amt"   for yr in YEARS])
    r_cnt = "+".join([f"y{yr}_R_count" for yr in YEARS])
    u_amt = "+".join([f"y{yr}_U_amt"   for yr in YEARS])
    u_cnt = "+".join([f"y{yr}_U_count" for yr in YEARS])
    cur.execute(f"UPDATE boe_donor_summary SET total_D_amt={d_amt},total_D_count={d_cnt},total_R_amt={r_amt},total_R_count={r_cnt},total_U_amt={u_amt},total_U_count={u_cnt},total_amt=({d_amt})+({r_amt})+({u_amt}),total_count=({d_cnt})+({r_cnt})+({u_cnt})")

    # last_date + last_filer
    log("  Building last_date/last_filer...")
    t0 = time.time()
    cur.execute("DROP TABLE IF EXISTS boe_donors.tmp_last")
    cur.execute("CREATE TABLE boe_donors.tmp_last (StateVoterId VARCHAR(50) PRIMARY KEY, last_date DATE, last_filer VARCHAR(255), INDEX idx_date(last_date)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
    # exact match (uses pre-computed clean columns)
    cur.execute("""
        INSERT INTO boe_donors.tmp_last (StateVoterId, last_date)
        SELECT v.StateVoterId, MAX(c.date)
        FROM nys_voter_tagging.voter_file v
        JOIN boe_donors.contributions c
          ON v.clean_last  = c.last
         AND v.clean_first = c.first
         AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
        WHERE v.clean_last IS NOT NULL
          AND c.zip5 != '' AND c.date IS NOT NULL
        GROUP BY v.StateVoterId
    """)
    # hyphenated fallback — two separate queries (avoids OR)
    for part_col in ["clean_last_h1", "clean_last_h2"]:
        cur.execute(f"""
            INSERT INTO boe_donors.tmp_last (StateVoterId, last_date)
            SELECT v.StateVoterId, MAX(c.date)
            FROM nys_voter_tagging.voter_file v
            JOIN boe_donors.contributions c
              ON v.{part_col}  = c.last
             AND v.clean_first = c.first
             AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
            WHERE v.{part_col} IS NOT NULL
              AND c.zip5 != '' AND c.date IS NOT NULL
            GROUP BY v.StateVoterId
            ON DUPLICATE KEY UPDATE last_date = GREATEST(last_date, VALUES(last_date))
        """)
    # filer at max date — exact match first
    cur.execute("""
        UPDATE boe_donors.tmp_last t
        JOIN (
            SELECT v.StateVoterId, MIN(c.filer) AS filer
            FROM boe_donors.tmp_last t2
            JOIN nys_voter_tagging.voter_file v ON v.StateVoterId = t2.StateVoterId
            JOIN boe_donors.contributions c
              ON v.clean_last  = c.last
             AND v.clean_first = c.first
             AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
             AND c.date = t2.last_date
            WHERE v.clean_last IS NOT NULL
              AND c.zip5 != ''
            GROUP BY v.StateVoterId
        ) x ON t.StateVoterId = x.StateVoterId
        SET t.last_filer = x.filer
    """)
    # hyphenated filer fallback
    for part_col in ["clean_last_h1", "clean_last_h2"]:
        cur.execute(f"""
            UPDATE boe_donors.tmp_last t
            JOIN (
                SELECT v.StateVoterId, MIN(c.filer) AS filer
                FROM boe_donors.tmp_last t2
                JOIN nys_voter_tagging.voter_file v ON v.StateVoterId = t2.StateVoterId
                JOIN boe_donors.contributions c
                  ON v.{part_col}  = c.last
                 AND v.clean_first = c.first
                 AND SUBSTRING(v.PrimaryZip, 1, 5) = c.zip5
                 AND c.date = t2.last_date
                WHERE v.{part_col} IS NOT NULL
                  AND c.zip5 != ''
                GROUP BY v.StateVoterId
            ) x ON t.StateVoterId = x.StateVoterId
            SET t.last_filer = COALESCE(t.last_filer, x.filer)
        """)
    cur.execute("UPDATE boe_donor_summary s JOIN boe_donors.tmp_last t ON s.StateVoterId=t.StateVoterId SET s.last_date=t.last_date, s.last_filer=t.last_filer")
    cur.execute("DROP TABLE IF EXISTS boe_donors.tmp_last")
    log(f"  last_date/filer done  ({time.time()-t0:.1f}s)")

    # --------------------------------------------------
    # Final summary table
    # --------------------------------------------------
    log("\n" + "=" * 70)
    log(f"SUMMARY  ({YEAR_MIN}-{YEAR_MAX})")
    log("=" * 70)
    hdr = f"{'Year':<8}  {'Dem $':>14}  {'(n)':>8}  {'Rep $':>14}  {'(n)':>8}  {'Unaf $':>14}  {'(n)':>8}  {'Total $':>14}  {'(n)':>8}"
    log(hdr)
    log("-" * len(hdr))
    for yr in YEARS:
        cur.execute(f"SELECT SUM(y{yr}_D_amt),SUM(y{yr}_D_count),SUM(y{yr}_R_amt),SUM(y{yr}_R_count),SUM(y{yr}_U_amt),SUM(y{yr}_U_count),SUM(y{yr}_D_amt+y{yr}_R_amt+y{yr}_U_amt),SUM(y{yr}_D_count+y{yr}_R_count+y{yr}_U_count) FROM boe_donor_summary")
        da,dc,ra,rc,ua,uc,ta,tc = cur.fetchone()
        log(f"{yr:<8}  ${da or 0:>13,.2f}  {int(dc or 0):>8,}  ${ra or 0:>13,.2f}  {int(rc or 0):>8,}  ${ua or 0:>13,.2f}  {int(uc or 0):>8,}  ${ta or 0:>13,.2f}  {int(tc or 0):>8,}")
    log("-" * len(hdr))
    cur.execute("SELECT SUM(total_D_amt),SUM(total_D_count),SUM(total_R_amt),SUM(total_R_count),SUM(total_U_amt),SUM(total_U_count),SUM(total_amt),SUM(total_count) FROM boe_donor_summary")
    da,dc,ra,rc,ua,uc,ta,tc = cur.fetchone()
    log(f"{'TOTAL':<8}  ${da or 0:>13,.2f}  {int(dc or 0):>8,}  ${ra or 0:>13,.2f}  {int(rc or 0):>8,}  ${ua or 0:>13,.2f}  {int(uc or 0):>8,}  ${ta or 0:>13,.2f}  {int(tc or 0):>8,}")

    cur.execute("SELECT COUNT(*), SUM(last_date IS NOT NULL) FROM boe_donor_summary")
    total, with_date = cur.fetchone()
    log(f"\nDone. {total:,} donors  |  {int(with_date):,} with last_date")

    conn.close()

except Exception as e:
    import traceback
    log(f"\nERROR: {e}")
    log(traceback.format_exc())

finally:
    LOG.close()
