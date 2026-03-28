#!/usr/bin/env python3
# ============================================================
# load_fullnyvoter.py
# FULL SCRIPT
# ============================================================

import os
import re
import time
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------- CONFIG ----------------
CSV_PATH = "fullnyvoter.csv"

MYSQL_DSN = (
    "mysql+pymysql://root:!#goAmerica99@127.0.0.1:3306/"
    "nys_audaince_causway?charset=utf8mb4"
)

DB = "nys_audaince_causway"
TABLE = "voter_file"

READ_CHUNK = 100_000
WRITE_CHUNK = 10_000
SENTINEL_DATE = "1905-01-01"

# ---------------- HELPERS ----------------
def clean_alpha(val):
    return re.sub(r"[^A-Za-z]", "", str(val or "")).lower()

def zip5(val):
    return re.sub(r"[^0-9]", "", str(val or ""))[:5]

def parse_date(val):
    if val is None:
        return SENTINEL_DATE
    s = str(val).strip()
    if s == "" or s in ("0000-00-00", "00/00/0000"):
        return SENTINEL_DATE
    if re.match(r"^\\d{1,2}/\\d{1,2}/\\d{4}$", s):
        m, d, y = s.split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    if re.match(r"^\\d{4}-\\d{2}-\\d{2}$", s):
        return s
    return SENTINEL_DATE

def make_voter_key(row):
    fn = clean_alpha(row.get("FirstName"))
    ln = clean_alpha(row.get("LastName"))
    zp = zip5(row.get("PrimaryZip"))
    dob = re.sub(r"[^0-9]", "", str(row.get("DOB") or ""))
    return f"{fn}|{ln}|{zp}|{dob}" if dob else f"{fn}|{ln}|{zp}"

# ---------------- MAIN ----------------
def main():
    if not os.path.isfile(CSV_PATH):
        raise SystemExit(f"CSV not found: {CSV_PATH}")

    engine = create_engine(MYSQL_DSN, future=True)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB}"))
        conn.execute(text(f"USE {DB}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        conn.execute(text(f"""
            CREATE TABLE {TABLE} (
              StateVoterId VARCHAR(50) NOT NULL,
              FirstName VARCHAR(100),
              LastName VARCHAR(100),
              MiddleName VARCHAR(100),
              SuffixName VARCHAR(50),

              PrimaryAddress1 VARCHAR(255),
              PrimaryCity VARCHAR(100),
              PrimaryState CHAR(2),
              PrimaryZip CHAR(5),
              PrimaryZip4 CHAR(4),

              DOB DATE NOT NULL DEFAULT '{SENTINEL_DATE}',
              RegistrationDate DATE NOT NULL DEFAULT '{SENTINEL_DATE}',
              LastVoterActivity DATE NOT NULL DEFAULT '{SENTINEL_DATE}',

              Gender VARCHAR(10),
              ObservedParty VARCHAR(20),
              OfficialParty VARCHAR(20),
              CalculatedParty VARCHAR(20),

              CountyName VARCHAR(50),
              CountyNumber SMALLINT UNSIGNED,
              PrecinctNumber VARCHAR(20),

              Latitude DECIMAL(9,6),
              Longitude DECIMAL(9,6),

              voter_key VARCHAR(255) NOT NULL,

              PRIMARY KEY (StateVoterId),
              KEY idx_voter_key (voter_key),
              KEY idx_zip (PrimaryZip),
              KEY idx_name (LastName, FirstName)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

    seen = set()
    total = 0
    start = time.time()

    reader = pd.read_csv(
        CSV_PATH,
        dtype=str,
        keep_default_na=False,
        chunksize=READ_CHUNK
    )

    for chunk_num, chunk in enumerate(reader, start=1):
        chunk["StateVoterId"] = chunk["StateVoterId"].astype(str).str.strip()
        chunk = chunk[chunk["StateVoterId"] != ""]

        if chunk["StateVoterId"].duplicated().any():
            raise SystemExit(f"Duplicate StateVoterId in chunk {chunk_num}")

        overlap = set(chunk["StateVoterId"]).intersection(seen)
        if overlap:
            raise SystemExit(
                f"Duplicate StateVoterId across chunks at {chunk_num}: "
                f"{list(overlap)[:10]}"
            )

        seen.update(chunk["StateVoterId"])

        chunk["PrimaryZip"] = chunk["PrimaryZip"].apply(zip5)
        chunk["DOB"] = chunk["DOB"].apply(parse_date)
        chunk["RegistrationDate"] = chunk["RegistrationDate"].apply(parse_date)
        chunk["LastVoterActivity"] = chunk["LastVoterActivity"].apply(parse_date)
        chunk["voter_key"] = chunk.apply(make_voter_key, axis=1)

        chunk.to_sql(
            TABLE,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=WRITE_CHUNK
        )

        total += len(chunk)
        elapsed = time.time() - start
        print(f"Chunk {chunk_num} loaded, total {total:,}, {total/elapsed:,.0f} rows/sec")

    print(f"DONE. Loaded {total:,} voters")

if __name__ == "__main__":
    main()