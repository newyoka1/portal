"""
Load U.S. Census Bureau 2010 Surname data into MySQL.

Source: https://www2.census.gov/topics/genealogy/2010surnames/names.zip
Contains 162,254 surnames with race/ethnicity percentages.

Creates table: ref_census_surnames
  - surname          VARCHAR(100) PRIMARY KEY
  - pctwhite         DECIMAL(5,2) NULL  -- % White
  - pctblack         DECIMAL(5,2) NULL  -- % Black
  - pctapi           DECIMAL(5,2) NULL  -- % Asian/Pacific Islander
  - pctaian          DECIMAL(5,2) NULL  -- % American Indian/Alaska Native
  - pct2prace        DECIMAL(5,2) NULL  -- % Two or More Races
  - pcthispanic      DECIMAL(5,2) NULL  -- % Hispanic
  - dominant_ethnicity VARCHAR(30)       -- highest-probability ethnicity
"""
import os
import csv
import pymysql
from pymysql.constants import CLIENT

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is required")
MYSQL_DB = os.getenv("MYSQL_DB", "NYS_VOTER_TAGGING")

CSV_PATH = os.path.join(os.path.dirname(__file__), "census_surnames", "Names_2010Census.csv")

ETHNICITY_COLS = ["pctwhite", "pctblack", "pctapi", "pctaian", "pct2prace", "pcthispanic"]
ETHNICITY_LABELS = {
    "pctwhite": "WHITE",
    "pctblack": "BLACK",
    "pctapi": "ASIAN_PI",
    "pctaian": "AIAN",
    "pct2prace": "MULTI",
    "pcthispanic": "HISPANIC",
}


def parse_pct(val):
    """Parse a percentage value; return None for suppressed '(S)' values."""
    if val is None or val.strip() == "" or "(S)" in val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def dominant(row_pcts):
    """Return the ethnicity label with the highest percentage, or None."""
    best_label = None
    best_val = -1
    for col in ETHNICITY_COLS:
        v = row_pcts.get(col)
        if v is not None and v > best_val:
            best_val = v
            best_label = ETHNICITY_LABELS[col]
    return best_label


def main():
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        client_flag=CLIENT.MULTI_STATEMENTS,
    )

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS ref_census_surnames;")
        cur.execute("""
CREATE TABLE ref_census_surnames (
  surname           VARCHAR(100) NOT NULL PRIMARY KEY,
  census_count      INT UNSIGNED NULL,
  pctwhite          DECIMAL(5,2) NULL,
  pctblack          DECIMAL(5,2) NULL,
  pctapi            DECIMAL(5,2) NULL,
  pctaian           DECIMAL(5,2) NULL,
  pct2prace         DECIMAL(5,2) NULL,
  pcthispanic       DECIMAL(5,2) NULL,
  dominant_ethnicity VARCHAR(30) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")
    conn.commit()

    batch = []
    total = 0

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip().upper()
            count_val = row.get("count", "").replace(",", "")
            try:
                census_count = int(count_val)
            except (ValueError, TypeError):
                census_count = None

            pcts = {}
            for col in ETHNICITY_COLS:
                pcts[col] = parse_pct(row.get(col, ""))

            dom = dominant(pcts)

            batch.append((
                name,
                census_count,
                pcts["pctwhite"],
                pcts["pctblack"],
                pcts["pctapi"],
                pcts["pctaian"],
                pcts["pct2prace"],
                pcts["pcthispanic"],
                dom,
            ))

            if len(batch) >= 5000:
                _insert_batch(conn, batch)
                total += len(batch)
                batch = []

    if batch:
        _insert_batch(conn, batch)
        total += len(batch)

    # Add index on dominant_ethnicity for fast filtering
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE ref_census_surnames ADD INDEX idx_dominant (dominant_ethnicity);")
    conn.commit()

    print(f"Loaded {total:,} surnames into ref_census_surnames")

    # Show stats
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dominant_ethnicity, COUNT(*) AS cnt
            FROM ref_census_surnames
            GROUP BY dominant_ethnicity
            ORDER BY cnt DESC;
        """)
        print("\nDominant ethnicity distribution:")
        for row in cur.fetchall():
            print(f"  {row[0] or 'NULL':12s}  {row[1]:>8,}")

    conn.close()


def _insert_batch(conn, batch):
    sql = """
INSERT INTO ref_census_surnames
  (surname, census_count, pctwhite, pctblack, pctapi, pctaian, pct2prace, pcthispanic, dominant_ethnicity)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
    with conn.cursor() as cur:
        cur.executemany(sql, batch)
    conn.commit()


if __name__ == "__main__":
    main()