# NYS Voter Pipeline

Python + MySQL ETL pipeline for processing ~12.7M New York State voter records with donor enrichment (BOE state, FEC federal, NYC CFB), demographic classification, CRM integration, and district-level Excel exports.

---

## Requirements

- **Python 3.11+**
- **MySQL 8.0+** (local)
- pip (bundled with Python)

---

## Installation

### Windows

1. **Install Python 3.11+**
   Download from [python.org](https://www.python.org/downloads/). During install, check **"Add Python to PATH"**.

   Verify:
   ```powershell
   python --version
   ```

2. **Install MySQL 8.0+**
   Download MySQL Community Server from [dev.mysql.com/downloads](https://dev.mysql.com/downloads/mysql/).
   During setup, note the root password you set — you'll need it in `.env`.

3. **Clone the repo and set up a virtual environment**
   ```powershell
   cd D:\git\nys-voter-pipeline
   python -m venv .venv
   .venv\Scripts\activate
   ```

4. **Install Python dependencies**
   ```powershell
   pip install -r requirements.txt
   playwright install firefox
   ```

---

### macOS

1. **Install Python 3.11+**
   Using Homebrew (recommended):
   ```bash
   brew install python@3.11
   ```
   Or download from [python.org](https://www.python.org/downloads/).

   Verify:
   ```bash
   python3 --version
   ```

2. **Install MySQL 8.0+**
   ```bash
   brew install mysql
   brew services start mysql
   mysql_secure_installation
   ```

3. **Clone the repo and set up a virtual environment**
   ```bash
   cd ~/git/nys-voter-pipeline
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   playwright install firefox
   ```

---

## Python Packages

All packages are in `requirements.txt` and installed via `pip install -r requirements.txt`.

| Package          | Purpose                                          |
|------------------|--------------------------------------------------|
| `pymysql`        | MySQL database driver                            |
| `python-dotenv`  | Load credentials from `.env`                     |
| `requests`       | HTTP downloads (FEC, CFB)                        |
| `openpyxl`       | Excel export generation                          |
| `playwright`     | Automated BOE bulk ZIP download from NYS BOE     |

After `pip install`, run `playwright install firefox` once to download the browser binary.

---

## Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env       # macOS/Linux
   copy .env.example .env     # Windows
   ```

2. Edit `.env` and fill in your credentials:

   ```env
   # Local MySQL
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=your_password_here
   DB_PORT=3306

   # Aiven Remote MySQL (only needed for sync command)
   AIVEN_HOST=
   AIVEN_USER=avnadmin
   AIVEN_PASSWORD=
   AIVEN_PORT=3306
   AIVEN_DB=nys_voter_tagging
   AIVEN_SSL_CA=certs/ca.pem

   # HubSpot CRM (one token per account)
   HUBSPOT_TOKEN_1=pat-na1-xxxxxxxx

   # Campaign Monitor (one API key per account)
   CM_API_KEY_1=xxxxxxxx
   ```

---

## First Run (Clean Environment)

Follow these steps in order the first time you set up the pipeline.

### Step 1 — Enable MySQL local_infile

The pipeline uses `LOAD DATA LOCAL INFILE` to bulk-load the 13M-row voter file. This must be enabled in MySQL before running anything.

**Windows** — find your `my.ini` (usually `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini`) and add under `[mysqld]`:
```ini
[mysqld]
local_infile = 1
```
Then restart MySQL:
```powershell
net stop MySQL80
net start MySQL80
```

**macOS (Homebrew)** — find your `my.cnf` (usually `/opt/homebrew/etc/my.cnf`) and add:
```ini
[mysqld]
local_infile = 1
```
Then restart MySQL:
```bash
brew services restart mysql
```

A pre-tuned config for this pipeline is at `config/my.ini.optimized` — you can use it as a reference or copy it over your existing config.

---

### Step 2 — Place source data files

The pipeline expects these files to exist before running. **None of these are included in the repo.**

| File / Folder | Description |
|---|---|
| `data/full voter 2025/fullnyvoter.csv` | Full NYS voter file extract (provided by NYS BOE) |
| `data/zipped/` | Audience CSV or ZIP files (one per named segment, e.g. `NYS_HARD_DEM.csv`) |

All other data (BOE donors, FEC, CFB) is downloaded automatically by the pipeline commands.

---

### Step 3 — Configure credentials

```bash
cp .env.example .env      # macOS
copy .env.example .env    # Windows
```

Edit `.env` and set at minimum:
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_mysql_root_password
DB_PORT=3306
```
All other credentials (Aiven, HubSpot, Campaign Monitor) are only needed for their respective commands and can be left blank until needed.

---

### Step 4 — Run the core pipeline

Activate your virtual environment first:
```powershell
.venv\Scripts\activate    # Windows
source .venv/bin/activate # macOS
```

Then run in this order:

```bash
# 1. Verify source file status
python main.py status

# 2. Load voter file + match audiences (~13M rows, takes 10–30 min)
#    Auto-creates the nys_voter_tagging database and all tables.
python main.py pipeline

# 3. Export a district to Excel to confirm everything worked
python main.py export --ld 63
```

---

### Step 5 — Optional enrichment (run after pipeline, in any order)

```bash
# Donor data (BOE state + FEC federal + NYC CFB)
python main.py donors

# Demographic classification (~30–60 min)
python main.py ethnicity

# Registration recency, turnout scores, household stats
python main.py enrich-derived

# District competitiveness scores
python main.py district-scores

# Party registration snapshots
python main.py party-snapshot
```

---

### Step 6 — CRM integration (requires API credentials in .env)

```bash
python main.py hubspot-sync
python main.py cm-sync
python main.py crm-enrich
```

---

### Step 7 — Sync to Aiven (requires Aiven credentials in .env)

```bash
python main.py sync
```

---

## Usage

All operations run through `main.py`. Activate your virtual environment first.

**Windows:**
```powershell
.venv\Scripts\activate
python main.py <command> [options]
```

**macOS:**
```bash
source .venv/bin/activate
python main.py <command> [options]
```

### Verbosity flags (available on all commands)

```
--verbose    Detailed progress
--debug      Everything
--quiet      Errors only
```

---

### Commands

```bash
# Check source file freshness
python main.py status

# Load voters + match audiences (statewide, ~13M rows)
python main.py pipeline

# Export district to Excel
python main.py export --ld 63           # Legislative District
python main.py export --sd 23           # State Senate District
python main.py export --cd 11           # Congressional District
python main.py export --county Nassau   # County

# Voter contact export (party tabs + CRM enrichment, no turnout models)
python main.py voter-contact --ld 63
python main.py voter-contact --sd 23
python main.py voter-contact --county Nassau

# Pipeline + export in one step
python main.py both --ld 63

# Full donor pipeline: BOE (state) + FEC (federal) + CFB (NYC)
python main.py donors                   # interactive refresh prompt
python main.py donors --refresh         # re-download all source data
python main.py donors --no-refresh      # enrich from existing DBs only

# Individual donor pipelines
python main.py boe-enrich               # NYS BOE state donors
python main.py national-enrich          # FEC federal donors
python main.py cfb-enrich               # NYC CFB donors

# Download BOE bulk ZIPs (requires playwright)
python main.py boe-download
python main.py boe-download --force     # force even if files are fresh

# Demographics
python main.py ethnicity                # build ModeledEthnicity (~30–60 min)
python main.py ethnicity --rebuild      # force full rebuild
python main.py enrich-derived           # registration recency, turnout, household stats
python main.py enrich-derived --refresh # clear and recompute all

# Scoring
python main.py district-scores          # district competitiveness scores
python main.py party-snapshot           # party registration snapshots + switcher detection

# CRM integration
python main.py hubspot-sync             # incremental HubSpot sync
python main.py hubspot-sync --full      # full re-sync
python main.py hubspot-sync --account X # sync only account "X"
python main.py cm-sync                  # incremental Campaign Monitor sync
python main.py cm-sync --full           # full re-sync
python main.py cm-sync --list ID        # sync one list only
python main.py cm-sync --skip-segments  # skip segment tagging
python main.py crm-sync                 # sync all CRM sources (HubSpot + CM)
python main.py crm-enrich               # append voter data to CRM contacts
python main.py crm-enrich --full        # re-enrich all contacts
python main.py crm-enrich --stats       # show match stats only

# Sync to Aiven remote MySQL
python main.py sync                          # push nys_voter_tagging
python main.py sync --tables voter           # voter_file table only
python main.py sync --tables summary         # summary tables only
python main.py sync --all-databases          # push ALL databases
python main.py sync --databases boe_donors National_Donors   # specific DBs
python main.py sync --full                   # force re-sync even if unchanged

# Maintenance
python main.py reset                    # drop donor DBs + clear enrichment + rebuild
python main.py reset --db-only          # drop DBs only, skip rebuild
```

---

## Project Structure

```
nys-voter-pipeline/
├── main.py                        # single entry point
├── requirements.txt
├── .env                           # credentials (never commit)
│
├── pipeline/
│   ├── pipeline.py                # voter tagging (StateVoterId direct match)
│   ├── enrich_boe_donors.py       # BOE state donor enrichment
│   ├── enrich_fec_donors.py       # FEC national donor enrichment
│   ├── enrich_crm_contacts.py     # append voter data to CRM contacts
│   ├── crm_merge.py               # merge CRM sources into unified table
│   └── extract_zip_files.py       # auto-extract audience zip files
│
├── voter/
│   ├── ethnicity.py               # layered ethnicity model (BISG + suffix rules)
│   ├── enrich_derived.py          # registration recency, turnout, household stats
│   ├── district_scores.py         # district competitiveness scores
│   ├── party_snapshot.py          # party snapshots + switcher detection
│   ├── audience_analytics.py      # audience overlap analytics
│   └── load_census_surnames.py    # Census 2010 surname data for ethnicity model
│
├── export/
│   ├── export.py                  # district Excel export
│   └── export_contact.py          # voter contact export (party tabs, CRM enriched)
│
├── sync/
│   └── aiven_sync.py              # push enriched tables to Aiven remote MySQL
│
├── utils/
│   └── db.py                      # shared DB connection helper
│
├── config/
│   ├── my.ini.optimized           # MySQL performance tuning config
│   ├── audience_group_queries.sql # audience SQL definitions
│   └── sql/                       # reference SQL scripts
│
├── data/                          # source data (gitignored)
│   ├── boe_donors/                # NYS BOE bulk ZIP files
│   ├── cfb/                       # NYC CFB contribution CSVs
│   ├── fec_downloads/             # FEC pas2 cycle files
│   ├── census_surnames/           # Census 2010 surname files
│   └── full voter 2025/           # NYS voter file extract
│
│── # Root-level loader/downloader scripts (called by main.py):
├── download_boe.py                # download BOE bulk ZIP files (requires playwright)
├── download_cfb.py                # download NYC CFB contribution CSVs
├── load_raw_boe.py                # parse BOE ZIPs → boe_donors DB
├── load_cfb_contributions.py      # load CFB CSVs → cfb_donors DB
├── load_hubspot_contacts.py       # sync HubSpot CRM → crm_unified DB
├── load_cm_subscribers.py         # sync Campaign Monitor → crm_unified DB
├── step1_download_fec.py          # download FEC pas2 bulk files
├── step2_extract_fec.py           # extract FEC ZIPs
├── step3_load_fec.py              # load FEC data → National_Donors DB
└── step4_classify_parties.py      # classify FEC donors by party
```

---

## Databases

| Database            | Purpose                                  |
|---------------------|------------------------------------------|
| `nys_voter_tagging` | Main voter file, audience tags, exports  |
| `boe_donors`        | NYS BOE state campaign contributions     |
| `National_Donors`   | FEC federal contributions                |
| `cfb_donors`        | NYC Campaign Finance Board contributions |
| `crm_unified`       | HubSpot + Campaign Monitor contacts      |

All databases use `utf8mb4_0900_ai_ci` collation. Local MySQL 8.4 is primary; Aiven MySQL 8.0.45 is the remote sync target.

---

## Ethnicity Model

Layered surname approach with no overlap between categories:

1. **BISG** — Hispanic, Black, Asian using surname + zip code
2. **Sub-European rules** — fires only on White-classified voters: Slavic suffixes, Italian, Irish heuristics, Jewish surname dictionary, Middle Eastern prefixes. Fallback is White (not Unknown).

Categories: `Hispanic | Black | Asian | South_Asian | Middle_Eastern | Eastern_European | Italian | Irish | Jewish | White | Other`

---

## Name Normalization

All three donor pipelines use identical normalization for matching:
```sql
REGEXP_REPLACE(UPPER(name), '[^A-Z]', '')
```
Pure alpha only — no spaces, punctuation, or digits.
