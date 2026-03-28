"""
BOE Raw Contribution Ingestion - ALL 58 COLUMNS
================================================
Loads all 4 BOE zip files into MySQL (boe_contributions_raw table) with all
58 columns preserved. Filters to Schedule A only, years 2018-2024.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.db import get_conn
import zipfile
import io
import csv
import os
import time

BOE_DIR = r"D:\git\nys-voter-pipeline\data\boe_reports"

ZIP_FILES = [
    "ALL_REPORTS_StateCommittee.zip",
    "ALL_REPORTS_CountyCommittee.zip",
    "ALL_REPORTS_StateCandidate.zip",
    "ALL_REPORTS_CountyCandidate.zip",
]

# All 58 column names in CSV position order
COLUMNS = [
    "FILER_ID", "FILER_PREVIOUS_ID", "CAND_COMM_NAME", "ELECTION_YEAR",
    "ELECTION_TYPE", "COUNTY_DESC", "FILING_ABBREV", "FILING_DESC",
    "R_AMEND", "FILING_CAT_DESC", "FILING_SCHED_ABBREV", "FILING_SCHED_DESC",
    "LOAN_LIB_NUMBER", "TRANS_NUMBER", "TRANS_MAPPING", "SCHED_DATE",
    "ORG_DATE", "CNTRBR_TYPE_DESC", "CNTRBN_TYPE_DESC", "TRANSFER_TYPE_DESC",
    "RECEIPT_TYPE_DESC", "RECEIPT_CODE_DESC", "PURPOSE_CODE_DESC",
    "R_SUBCONTRACTOR", "FLNG_ENT_NAME", "FLNG_ENT_FIRST_NAME",
    "FLNG_ENT_MIDDLE_NAME", "FLNG_ENT_LAST_NAME", "FLNG_ENT_ADD1",
    "FLNG_ENT_CITY", "FLNG_ENT_STATE", "FLNG_ENT_ZIP", "FLNG_ENT_COUNTRY",
    "PAYMENT_TYPE_DESC", "PAY_NUMBER", "OWED_AMT", "ORG_AMT",
    "LOAN_OTHER_DESC", "TRANS_EXPLNTN", "R_ITEMIZED", "R_LIABILITY",
    "ELECTION_YEAR_R", "OFFICE_DESC", "DISTRICT", "DIST_OFF_CAND_BAL_PROP",
    "R_CLAIM", "R_IN_DISTRICT", "R_MINOR", "R_VENDOR", "R_LOBBYIST",
    "R_SUPPORT_OPPOSE", "R_CONTRBUTIONS", "EMPLOYER", "EMP_OCCUPATION",
    "EMP_ADDR_ADDR1", "EMP_ADDR_CITY", "EMP_ADDR_STATE", "EMP_ADDR_ZIP",
]

# Key column indices (0-based)
IDX_FILER_ID    = 0
IDX_YEAR        = 3
IDX_SCHED_ABBR  = 10   # 'A' = monetary contributions received
IDX_ORG_AMT     = 36

BATCH_SIZE = 5000

# Build the CREATE TABLE DDL dynamically from column list
# Most cols are VARCHAR; ELECTION_YEAR and ORG_AMT get proper types
TYPE_OVERRIDES = {
    "FILER_ID":        "VARCHAR(20)",
    "FILER_PREVIOUS_ID": "VARCHAR(20)",
    "ELECTION_YEAR":   "SMALLINT",
    "FILING_ABBREV":   "CHAR(1)",
    "R_AMEND":         "CHAR(1)",
    "FILING_SCHED_ABBREV": "CHAR(1)",
    "SCHED_DATE":      "DATE",
    "ORG_DATE":        "DATE",
    "OWED_AMT":        "DECIMAL(14,2)",
    "ORG_AMT":         "DECIMAL(14,2)",
    "R_ITEMIZED":      "CHAR(1)",
    "R_LIABILITY":     "CHAR(1)",
    "R_SUBCONTRACTOR": "CHAR(1)",
    "R_CLAIM":         "CHAR(1)",
    "R_IN_DISTRICT":   "CHAR(1)",
    "R_MINOR":         "CHAR(1)",
    "R_VENDOR":        "CHAR(1)",
    "R_LOBBYIST":      "CHAR(1)",
    "R_SUPPORT_OPPOSE":"CHAR(1)",
    "R_CONTRBUTIONS":  "CHAR(1)",
    "ELECTION_YEAR_R": "SMALLINT",
    "CAND_COMM_NAME":  "VARCHAR(100)",
    "ELECTION_TYPE":   "VARCHAR(100)",
    "COUNTY_DESC":     "VARCHAR(255)",
    "FILING_DESC":     "VARCHAR(80)",
    "FILING_CAT_DESC": "VARCHAR(80)",
    "FILING_SCHED_DESC":"VARCHAR(80)",
    "LOAN_LIB_NUMBER": "VARCHAR(100)",
    "TRANS_NUMBER":    "VARCHAR(100)",
    "TRANS_MAPPING":   "VARCHAR(100)",
    "CNTRBR_TYPE_DESC":"VARCHAR(80)",
    "CNTRBN_TYPE_DESC":"VARCHAR(80)",
    "TRANSFER_TYPE_DESC":"VARCHAR(80)",
    "RECEIPT_TYPE_DESC":"VARCHAR(80)",
    "RECEIPT_CODE_DESC":"VARCHAR(80)",
    "PURPOSE_CODE_DESC":"VARCHAR(80)",
    "FLNG_ENT_NAME":   "VARCHAR(100)",
    "FLNG_ENT_FIRST_NAME":"VARCHAR(50)",
    "FLNG_ENT_MIDDLE_NAME":"VARCHAR(50)",
    "FLNG_ENT_LAST_NAME":"VARCHAR(50)",
    "FLNG_ENT_ADD1":   "VARCHAR(100)",
    "FLNG_ENT_CITY":   "VARCHAR(50)",
    "FLNG_ENT_STATE":  "VARCHAR(5)",
    "FLNG_ENT_ZIP":    "VARCHAR(15)",
    "FLNG_ENT_COUNTRY":"VARCHAR(50)",
    "PAYMENT_TYPE_DESC":"VARCHAR(80)",
    "PAY_NUMBER":      "VARCHAR(30)",
    "LOAN_OTHER_DESC": "VARCHAR(80)",
    "TRANS_EXPLNTN":   "VARCHAR(250)",
    "OFFICE_DESC":     "VARCHAR(100)",
    "DISTRICT":        "VARCHAR(40)",
    "DIST_OFF_CAND_BAL_PROP":"VARCHAR(500)",
    "EMPLOYER":        "VARCHAR(510)",
    "EMP_OCCUPATION":  "VARCHAR(160)",
    "EMP_ADDR_ADDR1":  "VARCHAR(100)",
    "EMP_ADDR_CITY":   "VARCHAR(40)",
    "EMP_ADDR_STATE":  "VARCHAR(5)",
    "EMP_ADDR_ZIP":    "VARCHAR(10)",
}

def create_table(cur):
    col_defs = []
    for col in COLUMNS:
        dtype = TYPE_OVERRIDES.get(col, "VARCHAR(255)")
        col_defs.append(f"  `{col}` {dtype}")
    col_defs.append("  `SOURCE_FILE` VARCHAR(60)")

    ddl = (
        "CREATE TABLE boe_contributions_raw (\n"
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,\n"
        + ",\n".join(col_defs) + ",\n"
        "  INDEX idx_filer (FILER_ID),\n"
        "  INDEX idx_year  (ELECTION_YEAR),\n"
        "  INDEX idx_name  (FLNG_ENT_LAST_NAME, FLNG_ENT_FIRST_NAME),\n"
        "  INDEX idx_zip   (FLNG_ENT_ZIP)\n"
        ") ENGINE=InnoDB ROW_FORMAT=COMPRESSED"
    )
    cur.execute("DROP TABLE IF EXISTS boe_contributions_raw")
    cur.execute(ddl)
    print("  Created boe_contributions_raw table with all 58 columns.")

def clean(val, maxlen=None):
    v = val.strip().strip('"').strip()
    if v.upper() in ('NULL', ''):
        v = None
    if v and maxlen:
        v = v[:maxlen]
    return v

def parse_decimal(val):
    try:
        v = val.strip().strip('"').replace(',', '')
        return float(v) if v and v.upper() != 'NULL' else None
    except:
        return None

def parse_date(val):
    try:
        v = val.strip().strip('"')
        if not v or v.upper() == 'NULL':
            return None
        return v[:10]
    except:
        return None

def parse_smallint(val):
    try:
        v = val.strip().strip('"')
        y = int(v)
        return y
    except:
        return None

# Build the INSERT statement once
INSERT_SQL = (
    "INSERT INTO boe_contributions_raw ("
    + ", ".join(f"`{c}`" for c in COLUMNS)
    + ", SOURCE_FILE) VALUES ("
    + ", ".join(["%s"] * (len(COLUMNS) + 1))
    + ")"
)

def parse_row(row, source_label):
    """Convert raw CSV row to a tuple of values matching COLUMNS + SOURCE_FILE."""
    # Pad to 58
    while len(row) < 58:
        row.append('')

    vals = []
    for i, col in enumerate(COLUMNS):
        raw = row[i]
        dtype = TYPE_OVERRIDES.get(col, "VARCHAR")

        if dtype == "DATE":
            vals.append(parse_date(raw))
        elif dtype in ("DECIMAL(14,2)",):
            vals.append(parse_decimal(raw))
        elif dtype == "SMALLINT":
            vals.append(parse_smallint(raw))
        elif dtype == "CHAR(1)":
            v = clean(raw, 1)
            vals.append(v)
        else:
            maxlen = None
            if "VARCHAR(" in dtype:
                try:
                    maxlen = int(dtype.split("(")[1].rstrip(")"))
                except:
                    pass
            vals.append(clean(raw, maxlen))

    vals.append(source_label)
    return tuple(vals)

def load_zip_file(cur, conn, zip_path, source_label):
    outer_name = os.path.basename(zip_path)
    print(f"\n  Opening {outer_name}...")

    with zipfile.ZipFile(zip_path) as outer:
        inner_zips = [n for n in outer.namelist() if n.endswith('.zip')]
        if not inner_zips:
            print("    No inner zip found â€” skipping.")
            return 0
        inner_bytes = outer.read(inner_zips[0])

    total_rows = 0
    skipped = 0
    batch = []

    with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
        csvs = [n for n in inner.namelist() if n.endswith('.csv')]
        if not csvs:
            print("    No CSV found â€” skipping.")
            return 0

        print(f"  Streaming {csvs[0]}...")
        with inner.open(csvs[0]) as raw:
            text = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
            reader = csv.reader(text)

            for row in reader:
                while len(row) < 58:
                    row.append('')

                # Schedule A = monetary contributions received
                sched = row[IDX_SCHED_ABBR].strip().strip('"')
                if sched != 'A':
                    skipped += 1
                    continue

                # Years 2018-2024
                year = parse_smallint(row[IDX_YEAR])
                if not year or year < 2018 or year > 2024:
                    skipped += 1
                    continue

                # Positive amount only
                amt = parse_decimal(row[IDX_ORG_AMT])
                if amt is None or amt <= 0:
                    skipped += 1
                    continue

                batch.append(parse_row(row, source_label))

                if len(batch) >= BATCH_SIZE:
                    cur.executemany(INSERT_SQL, batch)
                    conn.commit()
                    total_rows += len(batch)
                    batch = []
                    if total_rows % 200000 == 0:
                        print(f"    ...{total_rows:,} rows inserted")

        if batch:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            total_rows += len(batch)

    print(f"  Done: {total_rows:,} inserted, {skipped:,} skipped")
    return total_rows

def show_stats(cur):
    print("\n\n=== SUMMARY ===")
    cur.execute("SELECT COUNT(*), SUM(ORG_AMT) FROM boe_contributions_raw")
    cnt, total = cur.fetchone()
    print(f"Total rows: {cnt:,}  |  Total $: ${total:,.0f}")

    cur.execute("""
        SELECT COALESCE(f.party,'U') as party,
               COUNT(*) as txns,
               SUM(b.ORG_AMT) as dollars
        FROM boe_contributions_raw b
        LEFT JOIN (
            SELECT FILER_ID,
                   CASE FilerParty WHEN 1 THEN 'D' WHEN 2 THEN 'R' ELSE 'U' END as party
            FROM boe_filer_registry
        ) f ON b.FILER_ID = f.FILER_ID
        GROUP BY party ORDER BY dollars DESC
    """)
    print("\nBy party (via COMMCAND join):")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,} transactions  ${r[2]:,.0f}")

if __name__ == '__main__':
    start = time.time()
    print("=" * 60)
    print("BOE Contribution Ingestion â€” All 58 Columns")
    print("=" * 60)

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    print("\nStep 1: Creating table...")
    create_table(cur)
    conn.commit()

    print("\nStep 2: Loading data...")
    grand_total = 0
    for zf in ZIP_FILES:
        zpath = os.path.join(BOE_DIR, zf)
        if not os.path.exists(zpath):
            print(f"  SKIPPING {zf} (not found)")
            continue
        label = zf.replace('ALL_REPORTS_','').replace('.zip','')
        grand_total += load_zip_file(cur, conn, zpath, label)
        print(f"  Running total: {grand_total:,}")

    show_stats(cur)

    elapsed = (time.time() - start) / 60
    print(f"\nFinished in {elapsed:.1f} minutes")
    cur.close()
    conn.close()


