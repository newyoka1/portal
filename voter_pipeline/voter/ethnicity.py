import os, sys, time, argparse
import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": "nys_voter_tagging",
    "charset":  "utf8mb4",
    "autocommit": False,
}

VOTER_TABLE  = "voter_file"
LOOKUP_TABLE = "ref_surname_lookup"

EASTERN_EUROPEAN_SURNAMES = {
    "KOWALSKI","KOWALSKA","NOWAK","WISNIEWSKI","WOJCIK","KOWALCZYK","KAMINSKI",
    "LEWANDOWSKI","ZIELINSKI","SZYMANSKI","WOZNIAK","DABROWSKI","KOZLOWSKI",
    "JANKOWSKI","MAZUR","WOJCIECHOWSKI","KWIATKOWSKI","KRAWCZYK","PIOTROWSKI",
    "GRABOWSKI","NOWAKOWSKI","PAWLAK","MICHALSKI","NOWACKI","ADAMCZYK",
    "DUDEK","ZAJAC","WIECZOREK","JABLONSKI","KROL","MAJEWSKI","OLSZEWSKI",
    "JAWORSKI","WROBEL","MALINOWSKI","PAWLOWSKI","WITKOWSKI","WALCZAK",
    "BARAN","SZCZEPANSKI","DOBROWOLSKI","KUBIAK","KAZMIERCZAK","RUTKOWSKI",
    "IVANOV","PETROV","SIDOROV","SOKOLOV","POPOV","VOLKOV","LEBEDEV","KOZLOV",
    "NOVIKOV","MOROZOV","SOLOVYOV","KOVALEV","NIKOLAEV","ORLOV","FEDOROV",
    "MIKHAILOV","STEPANOV","SMIRNOV","KUZNETSOV","VASILIEV","SEMYONOV",
    "SHEVCHENKO","BONDARENKO","KOVALENKO","TKACHENKO","MARCHENKO","KRAVCHENKO",
    "KOVALCHUK","SAVCHENKO","LYSENKO","PETRENKO","MOROZ","BONDAR","MELNYK",
    "POLISHCHUK","SYDORENKO","RUDENKO","LEVCHENKO","KHARCHENKO","ZAKHARCHENKO",
    "NOVAK","DVORAK","HORAK","BLAHA","CERMAK","DOSTAL","FIALA","HAJEK",
    "HOLUB","HRUSKA","KOLAR","KOPECKY","KRAL","KRATKY","KREJCI","KRIZ",
    "KUBICEK","MALEK","MARES","MASEK","MATOUSEK","MORAVEC","MUSIL","NECAS",
    "NEMEC","NOVOTNY","POSPICHAL","POSPISIL","PROCHAZKA","PRUSA","RUZICKA",
    "SIMEK","SLAVIK","SOUKUP","STANEK","STASTNY","STEPAN","SVEC","SYKORA","SVOBODA",
    "JOVANOVIC","PETROVIC","NIKOLIC","MARKOVIC","DJORDJEVIC","STOJANOVIC",
    "ILIC","POPOVIC","PERIC","MILOVANOVIC","MILOSEVIC","FILIPOVIC","STEFANOVIC",
    "SIMIC","MITIC","PAVLOVIC","SAVIC","KOVACEVIC","NOVAKOVIC","BOGDANOVIC",
    "ANDRIC","BABIC","KOVACIC","HORVAT","MATIC","JURIC","BOSNJAK","LUKIC","KNEZEVIC",
    "IONESCU","POPESCU","POPA","RADU","DUMITRU","STAN","STOICA","GHEORGHE",
    "CONSTANTIN","MOLDOVAN","MUNTEANU","MIHAI","DINU","SERBAN","OLTEANU",
    "MATEI","BARBU","TUDOR","COSTACHE","APOSTOL","MARINESCU","MANEA",
    "GEORGIEV","PETKOV","DIMITROV","TODOROV","KOLEV","NIKOLOV",
    "STOYANOV","BORISOV","HRISTOV","ANGELOV","ATANASOV","TSONEV","MLADENOV",
    "NAGY","KOVACS","TOTH","SZABO","HORVATH","VARGA","KISS","MOLNAR",
    "NEMETH","FARKAS","BALOGH","PAPP","TAKACS","JUHASZ","FEKETE","LAKATOS",
    "MESZAROS","SIMON","RACZ",
    "HOXHA","SHEHU","MUSA","KRASNIQI","BERISHA","GASHI","LIMAJ","RAMA",
    "META","BASHA","GJOSHI","HASANI","MALOKU","OSMANI","SADIKU","SELMANI",
    "GJAKOVA","BEQIRI","FAZLIU","MUSLIU","NEZIRI","SYLEJMANI",
}

SOUTH_ASIAN_SURNAMES = {
    "PATEL","SHARMA","SINGH","KUMAR","GUPTA","MEHTA","SHAH","KHAN","AHMED",
    "CHAUDHARY","VERMA","MISHRA","YADAV","JOSHI","PANDEY","AGARWAL","BOSE",
    "DAS","BANERJEE","MUKHERJEE","CHATTERJEE","GHOSH","SEN","DUTTA","ROY",
    "NAIR","PILLAI","MENON","KRISHNAN","IYER","NAIDU","REDDY","RAO","MURTHY",
    "RAJAN","SRINIVASAN","VENKATESH","SUBRAMANIAM","BALACHANDRAN","GOSWAMI",
    "CHAKRABORTY","BHATTACHARYA","SARKAR","MANDAL","BISWAS","MITRA","PAUL",
    "CHOWDHURY","BHUIYAN","RAHMAN","HOSSAIN","ISLAM","BEGUM","AKHTAR",
    "SIDDIQUI","ANSARI","SHAIKH","SHEIKH","MALIK","MIRZA","BAIG","QURESHI",
    "HUSSAIN","ALI","WAQAR","JAVED","NAQVI","ZAIDI","RIZVI","BUKHARI",
}

ITALIAN_SURNAMES = {
    "RUSSO","FERRARI","ESPOSITO","BIANCHI","ROMANO","COLOMBO","RICCI","MARINO",
    "GRECO","BRUNO","GALLO","CONTI","DELUCA","MANCINI","COSTA","GIORDANO",
    "RIZZO","LOMBARDI","MORETTI","BARBIERI","FONTANA","SANTORO","MARIANI",
    "RINALDI","CARUSO","FERRARA","GALLI","MARTINI","LEONE","LONGO","GENTILE",
    "MARTINELLI","VITALE","LOMBARDO","SERRA","COPPOLA","DEROSA","DAMICO",
    "MARINI","FERRETTI","PELLEGRINI","PALUMBO","PARISI","SANNA","FARINA",
    "RIZZI","MONTI","CATTANEO","ANDREOTTI","DEMARCO","DIMAIO","AMATO",
    "BATTAGLIA","CAPUTO","CATALANO","FERRARO","FIORE","GIULIANI",
    "LUPO","MACRI","MAGGIO","MANGANO","MARCHESE","MAURO","MAZZA",
    "MIRABELLA","MIRAGLIA","MONTALBANO","NAPOLITANO","ORLANDO","PAGANO",
    "PALERMO","PALMIERI","PAPPALARDO","PERNA","PETRONE","PIAZZA","PISANO",
    "PUGLISI","RAGUSA","RAIA","RENDA","RESTIVO","RUGGIERO",
    "SACCO","SALA","SALERNO","SALVO","SANSONE","SANTANGELO","SARNO","SCALIA",
    "SCALISE","SCIORTINO","SCOTTO","SILVESTRI","SODANO","SORRENTINO",
    "TARANTINO","TARANTO","TODARO","TOSCANO","TRAPANI","TRICOMI",
    "TROVATO","TUCCIO","TUMINO","VALENTI","VENEZIA","VERDE","VITIELLO","ZITO",
}

JEWISH_SURNAMES = {
    "GOLDBERG","GOLDSTEIN","SILVER","SILVERSTEIN","SILVERMAN","GOLD","GOLDMAN",
    "KLEIN","GROSSMAN","GROSS","STEIN","STERN","BERNSTEIN","STEINBERG",
    "ROSENBERG","ROSENFELD","ROSENTHAL","ROSEN","GREENBERG","GREENBAUM",
    "BLUM","BLUMENTHAL","BLOOM","WEISS","WEISSMAN","SCHWARTZ","SCHWARZMAN",
    "KATZ","KATZMAN","KOHN","COHEN","KAPLAN","SHAPIRO","SCHAPIRO",
    "LEVINE","LEVINSON","LEVIN","LEVY","LEVITT","FRIEDMAN","FRIED","FRIEDBERG",
    "FELDMAN","FEINBERG","FEIN","FEINSTEIN","HOROWITZ","HOROVITZ",
    "HIRSCH","HIRSCHMAN","HARTMAN","HECHT","WAXMAN","WASSERMAN",
    "RUBINSTEIN","RUBIN","RUBENSTEIN","LIPMAN","LIPPMAN","LIPTON","LITWIN",
    "LICHTMAN","LICHTENSTEIN","KAPLOWITZ","MARKOWITZ","RABINOWITZ","LEIBOWITZ",
    "HOCHSTEIN","LOWENSTEIN","LOWENTHAL","OPPENHEIMER","OPPENHEIM","SELIGMAN",
    "LOEB","LOEWY","LOEW","TANNENBAUM","TENENBAUM","EINHORN","EISEN",
    "EISENSTEIN","EISENBERG","EPSTEIN","APPLEBAUM","APPELBAUM","BIRNBAUM",
    "NEIMAN","NEUMANN","NEWMAN","ABRAMOWITZ","ABRAMSON","ABRAHAMSON",
    "MANDELBAUM","MANDEL","MANDELL","NUSSBAUM","ROTHSCHILD","ROTH",
    "ROTHSTEIN","ROTHMAN","BERMAN","BERKOWITZ","BERKMAN","MOSKOWITZ",
    "PEARLMAN","PERLMAN","PERELMAN","PEARL","PRAGER","PRESSMAN",
    "POLLACK","POLLOCK","WOLFF","WOLFE","WOLF","WOLFSON","SACKS","SACHS",
    "BRODSKY","BRODSKI","BRODER","BRODMAN","LEFKOWITZ","LEFKOFF",
    "SCHNEIDER","SNYDER","SCHREIBER","SCHREIER",
}

IRISH_SURNAMES = {
    "MURPHY","KELLY","SULLIVAN","WALSH","OBRIEN","BYRNE","RYAN",
    "OCONNOR","ONEILL","DOLAN","DOYLE","MOORE","MCCARTHY","QUINN","GALLAGHER",
    "KENNEDY","LYNCH","MURRAY","BARRY","HAYES","OMAHONY","MAHONEY",
    "NOLAN","DUNNE","BRENNAN","FLANAGAN","MCDONAGH","MCDONALD","MCDONNELL",
    "CONNELLY","CONNOLLY","FITZGERALD","POWER","GRIFFIN","OREILLY","REILLY",
    "DOHERTY","FOLEY","SHERIDAN","FARRELL","BOYLE","CALLAHAN","CALLAGHAN",
    "MCLAUGHLIN","MAGUIRE","MCCORMACK","MCCORMICK","CASEY","WARD","DONNELLY",
    "FAHY","FANNING","FINNEGAN","FLOOD","GLYNN","GORMAN","HANLON","HOGAN",
    "JORDAN","KEANE","KEATING","KEEGAN","KERRIGAN","KIRWAN","LARKIN",
    "LAWLOR","LONERGAN","LOUGHRAN","LOWRY","MADDEN","MALONE","MANNION",
    "MEEHAN","MOONEY","MORRISSEY","MULCAHY","MULLEN","MULVEY","NAGLE",
    "OCALLAGHAN","ODONOGHUE","ODONOVAN","ODRISCOLL","OLEARY",
    "OROURKE","OSHEA","OTOOLE","PHELAN","ROCHE","SHAUGHNESSY","SHEA",
    "SWEENEY","TIERNEY","TOBIN","TWOMEY","WHELAN","WREN",
}

MIDDLE_EASTERN_SURNAMES = {
    "HASSAN","HUSSEIN","MOHAMMAD","MOHAMMED","MUHAMMAD","ABDULLA","ABDULLAH",
    "ALHASAN","ALHARBI","ALOTAIBI","ALSHEHRI","ALMUTAIRI","ALQAHTANI",
    "ABUBAKAR","ABDELRAHIM","ABDELRAHMAN","ABDELAZIZ","ABDULRAHMAN",
    "NASSER","NASSAR","MANSOUR","KHALIL","KHOURY","HADDAD","HABIB",
    "NASR","NASRI","SARHAN","SALAH","SALEH","SALEM","SELIM",
    "IBRAHIM","ISMAEL","ISMAIL","SAID","SAAD","SAADEH",
    "TAHA","TAHIR","TALEB","TALIB","TAMIMI","TARAZI","TOUMA",
    "YOUSSEF","YOUSEF","YOUNIS","YUSUF","ZAKI","ZIAD","ZIDAN","ZOUBI",
    "AMER","AMIN","AMIR","AMIRI","ARSLAN","ATIYEH","ATTAR","AWAD",
    "AZIZ","AZZAM","BAKR","BARAKAT","BARGHOUTI","BAROUDI","BAZZI",
    "CHAABAN","DARWISH","DAOUD","DAOUDI","DOUAIHY",
    "FADEL","FARAG","FARAJ","FAROUK","GHALI","HAMDAN","HAMDI",
    "HAMOUD","HAMOUDA","HARB","HILAL","JABER","JABR","JARRAR",
    "JUBRAN","KARIMI","KASEM","KASSEM","KHALID","KHALED","KHODR",
    "KOBEISSI","MAHMOUD","MAHMOOD","MAKKI","MAKHOUL","MALAK",
    "NASEREDDIN","NAZARI","NOURI","QASIM","RAHHAL","RASHID","RASHEED",
    "RASSAM","RAZEK","SABRY","SABBAGH","SARROUF","SAYED","SEIF",
    "SHAMOUN","SHARAF","SHATARA","SHIBLI","SHIHAB",
    "SULEIMAN","SULAIMAN","SULTAN","TOUFIC","TOUFEILI",
}

def get_connection():
    return pymysql.connect(**DB_CONFIG)

def build_lookup_table(cur, rebuild=False):
    if rebuild:
        cur.execute(f"DROP TABLE IF EXISTS {LOOKUP_TABLE}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOOKUP_TABLE} (
            surname_upper  VARCHAR(60)  NOT NULL,
            ethnicity      VARCHAR(40)  NOT NULL,
            source         VARCHAR(20)  NOT NULL DEFAULT 'curated',
            PRIMARY KEY (surname_upper)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    cur.execute(f"SELECT COUNT(*) FROM {LOOKUP_TABLE}")
    if cur.fetchone()[0] > 0 and not rebuild:
        print(f"  {LOOKUP_TABLE} already populated. Use --rebuild to refresh.")
        return
    print("  Populating surname lookup table...")
    rows = []
    def add_group(name_set, ethnicity):
        for s in name_set:
            rows.append((s.upper(), ethnicity, "curated"))
    add_group(EASTERN_EUROPEAN_SURNAMES, "Eastern European")
    add_group(SOUTH_ASIAN_SURNAMES,      "South Asian")
    add_group(ITALIAN_SURNAMES,          "Italian")
    add_group(JEWISH_SURNAMES,           "Jewish")
    add_group(IRISH_SURNAMES,            "Irish")
    add_group(MIDDLE_EASTERN_SURNAMES,   "Middle Eastern")
    cur.executemany(f"INSERT IGNORE INTO {LOOKUP_TABLE} (surname_upper, ethnicity, source) VALUES (%s, %s, %s)", rows)
    print(f"  Inserted {len(rows)} curated surname entries.")

def ensure_column(cur, column, definition):
    cur.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{VOTER_TABLE}' AND COLUMN_NAME = '{column}'")
    if cur.fetchone()[0] == 0:
        print(f"  Adding column: {column}")
        cur.execute(f"ALTER TABLE {VOTER_TABLE} ADD COLUMN {column} {definition}")
    else:
        print(f"  Column already exists: {column}")

def run_update(cur, label, sql, params=None):
    print(f"  Running: {label} ...")
    t = time.time()
    cur.execute(sql, params or ())
    elapsed = time.time() - t
    print(f"    -> {cur.rowcount:,} rows affected ({elapsed:.1f}s)")

def run_update_batched(conn, cur, label, sql, params=None, batch_size=50000):
    """Run UPDATE in batches. MySQL forbids LIMIT on JOIN updates, so those run as single pass."""
    is_join = " JOIN " in sql.upper()
    t = time.time()
    if is_join:
        print(f"  Running (single pass, JOIN): {label} ...")
        cur.execute(sql, params or ())
        conn.commit()
        elapsed = time.time() - t
        print(f"    -> {cur.rowcount:,} rows affected ({elapsed:.1f}s)")
    else:
        batch_sql = sql.rstrip().rstrip(";") + f" LIMIT {batch_size}"
        total = 0
        print(f"  Running (batched {batch_size:,}/batch): {label} ...")
        while True:
            cur.execute(batch_sql, params or ())
            affected = cur.rowcount
            conn.commit()
            total += affected
            if affected < batch_size:
                break
            print(f"    ... {total:,} rows so far ({time.time()-t:.1f}s)")
        elapsed = time.time() - t
        print(f"    -> {total:,} rows total ({elapsed:.1f}s)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--batch-size", type=int, default=50000, help="Rows per batch")
    args = parser.parse_args()
    batch_size = args.batch_size
    print("=" * 60)
    print("  NYS Voter Tagging - ModeledEthnicity Builder")
    print("=" * 60)
    conn = get_connection()
    cur  = conn.cursor()
    print("\n[Step 1] Building surname lookup table...")
    if not args.dry_run:
        build_lookup_table(cur, rebuild=args.rebuild)
        conn.commit()
    print("\n[Step 2] Ensuring ModeledEthnicity column on voter table...")
    if not args.dry_run:
        ensure_column(cur, "ModeledEthnicity", "VARCHAR(40) NULL DEFAULT NULL")
        conn.commit()
    print("\n[Step 3] Applying curated surname classifications...")
    if not args.dry_run:
        run_update_batched(conn, cur, "Curated surname match",
            f"UPDATE {VOTER_TABLE} v JOIN {LOOKUP_TABLE} l ON UPPER(v.LastName) COLLATE utf8mb4_0900_ai_ci = l.surname_upper COLLATE utf8mb4_0900_ai_ci SET v.ModeledEthnicity = l.ethnicity",
            batch_size=batch_size)
    print("\n[Step 4] Suffix-based fallback...")
    suffix_rules = [
        ("Eastern European", ["SKI","SKA","CKI","CKA","WICZ","ICZ","EWICZ","OWICZ","CZYK","ENKO","CHUK","SHUK","OVSKY","EVSKY","OVIC","EVIC","JEVIC","ESCU","EANU"]),
        ("Italian", ["ELLO","ELLA","ETTI","ETTA","IONI","IONE","UCCI","UCCA"]),
    ]
    if not args.dry_run:
        for ethnicity, suffixes in suffix_rules:
            like_clauses = " OR ".join([f"UPPER(v.LastName) LIKE %s" for _ in suffixes])
            params = [f"%{s}" for s in suffixes]
            run_update_batched(conn, cur, f"Suffix fallback -> {ethnicity}",
                f"UPDATE {VOTER_TABLE} v SET v.ModeledEthnicity = %s WHERE v.ModeledEthnicity IS NULL AND ({like_clauses})",
                params=[ethnicity] + params, batch_size=batch_size)
    print("\n[Step 5] Checking for ref_census_surnames table...")
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ref_census_surnames'")
    census_available = cur.fetchone()[0] > 0
    if census_available:
        print("  Census table found - applying dominant_ethnicity for unmatched voters...")
        if not args.dry_run:
            # Use dominant_ethnicity column directly; join on 'surname' (not 'name')
            run_update_batched(conn, cur, "Census dominant_ethnicity",
                f"UPDATE {VOTER_TABLE} v "
                f"JOIN ref_census_surnames c ON UPPER(v.LastName) = c.normalized_surname "
                f"SET v.ModeledEthnicity = c.dominant_ethnicity "
                f"WHERE v.ModeledEthnicity IS NULL",
                batch_size=batch_size)
    else:
        print("  ref_census_surnames not found - skipping.")
    print("\n[Step 6] Setting unclassified to Unknown...")
    if not args.dry_run:
        run_update_batched(conn, cur, "Default -> Unknown",
            f"UPDATE {VOTER_TABLE} SET ModeledEthnicity = 'Unknown' WHERE ModeledEthnicity IS NULL",
            batch_size=batch_size)
    print("\n[Step 7] Ethnicity distribution summary:")
    cur.execute(f"SELECT ModeledEthnicity, COUNT(*) AS cnt FROM {VOTER_TABLE} GROUP BY ModeledEthnicity ORDER BY cnt DESC")
    rows = cur.fetchall()
    total = sum(r[1] for r in rows)
    print(f"\n  {'Ethnicity':<22}  {'Count':>12}  {'Pct':>7}")
    print(f"  {'-'*22}  {'-'*12}  {'-'*7}")
    for eth, cnt in rows:
        pct = cnt / total * 100
        print(f"  {str(eth):<22}  {cnt:>12,}  {pct:>6.2f}%")
    print(f"  {'TOTAL':<22}  {total:>12,}")
    cur.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()





