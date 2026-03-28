#!/usr/bin/env python3
# ============================================================
# load_causeway_universe.py
# FULL SCRIPT
# ============================================================

import os
import re
import pandas as pd
from sqlalchemy import create_engine, text

FOLDER = "causeway_tags"

MYSQL_DSN = (
    "mysql+pymysql://root:!#goAmerica99@127.0.0.1:3306/"
    "nys_audaince_causway?charset=utf8mb4"
)

TABLE = "causeway_universe"

def clean_alpha(val):
    return re.sub(r"[^A-Za-z]", "", str(val or "")).lower()

def zip5(val):
    return re.sub(r"[^0-9]", "", str(val or ""))[:5]

def make_voter_key(row):
    fn = clean_alpha(row.get("FirstName"))
    ln = clean_alpha(row.get("LastName"))
    zp = zip5(row.get("PrimaryZip"))
    dob = re.sub(r"[^0-9]", "", str(row.get("DOB") or ""))
    return f"{fn}|{ln}|{zp}|{dob}" if dob else f"{fn}|{ln}|{zp}"

def main():
    if not os.path.isdir(FOLDER):
        raise SystemExit(f"Folder not found: {FOLDER}")

    frames = []
    for fname in os.listdir(FOLDER):
        if not fname.lower().endswith(".csv"):
            continue
        df = pd.read_csv(
            os.path.join(FOLDER, fname),
            dtype=str,
            keep_default_na=False
        )
        df["origin"] = fname
        df["voter_key"] = df.apply(make_voter_key, axis=1)
        frames.append(df[["voter_key", "origin"]])

    if not frames:
        raise SystemExit("No CSV files loaded")

    all_df = pd.concat(frames, ignore_index=True)

    engine = create_engine(MYSQL_DSN, future=True)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        conn.execute(text(f"""
            CREATE TABLE {TABLE} (
              voter_key VARCHAR(255) NOT NULL,
              origin VARCHAR(255),
              PRIMARY KEY (voter_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        all_df.to_sql(
            TABLE,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

    print(
        f"Loaded {len(all_df):,} rows, "
        f"{all_df['voter_key'].nunique():,} unique voters"
    )

if __name__ == "__main__":
    main()