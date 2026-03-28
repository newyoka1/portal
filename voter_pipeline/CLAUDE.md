# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NYS Voter Pipeline is a Python + MySQL ETL system that ingests, enriches, and exports ~12.7 million New York State voter records. It integrates state/federal/NYC campaign donation data, CRM contacts (HubSpot + Campaign Monitor), and demographic models, producing district-level Excel exports and syncing to Aiven cloud MySQL.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Entry point for all operations
python main.py <command> [options]

# Verbosity flags available on all commands
python main.py <command> --verbose   # detailed progress
python main.py <command> --debug     # everything
python main.py <command> --quiet     # errors only
```

### Key Commands

```bash
python main.py status                     # Show source file ages
python main.py pipeline                   # Load all voters + match audiences
python main.py export --ld 63             # Export Legislative District to Excel
python main.py export --sd 23             # Export State Senate District
python main.py export --cd 11             # Export Congressional District
python main.py export --county Nassau     # Export by county
python main.py voter-contact --ld 63      # Export with contact methods + party tabs
python main.py donors                     # Full BOE + FEC + CFB pipeline (interactive)
python main.py donors --refresh           # Force re-download all donor data
python main.py donors --no-refresh        # Enrich from existing DBs only
python main.py boe-enrich                 # State BOE donors only
python main.py national-enrich            # FEC federal donors only
python main.py cfb-enrich                 # NYC CFB donors only
python main.py ethnicity                  # Build ModeledEthnicity (30-60 min)
python main.py enrich-derived             # Registration recency, turnout, household stats
python main.py district-scores            # District competitiveness scores
python main.py party-snapshot             # Party registration snapshots + switcher detection
python main.py hubspot-sync               # Incremental HubSpot sync
python main.py hubspot-sync --full        # Full re-sync
python main.py cm-sync                    # Incremental Campaign Monitor sync
python main.py crm-enrich                 # Append voter data to CRM contacts
python main.py sync                       # Push nys_voter_tagging to Aiven
python main.py sync --tables voter        # voter_file table only
python main.py sync --tables summary      # summary tables only
python main.py sync --all-databases       # Sync ALL databases to Aiven
python main.py sync --databases boe_donors National_Donors  # specific DBs
python main.py reset                      # Drop donor DBs + re-run everything
```

There is no formal test or lint suite.

## Architecture

### Entry Point & Dispatch

`main.py` is the single orchestrator. It dispatches to domain-specific scripts—many via subprocess—with each script handling a focused responsibility.

### Database Schema

Five MySQL 8.0+ databases (utf8mb4_0900_ai_ci):

| Database | Purpose |
|---|---|
| `nys_voter_tagging` | Main voter file (13M rows), audience bridge, export tables |
| `boe_donors` | NYS state campaign contributions |
| `National_Donors` | FEC federal contributions |
| `cfb_donors` | NYC Campaign Finance Board contributions |
| `crm_unified` | Unified HubSpot + Campaign Monitor contacts |

Shared connection factory: `utils/db.py` (supports local MySQL and Aiven SSL).

### Data Flow

```
Source Files (data/)
  → Download/Extract (Playwright BOE scraper, FEC ZIPs, CFB CSVs, CRM APIs)
  → Load into domain DBs (load_raw_boe.py, step3_load_fec.py, load_cfb_contributions.py, load_hubspot_contacts.py, load_cm_subscribers.py)
  → Voter base load (pipeline/pipeline.py → nys_voter_tagging.voter_file)
  → Enrichment, additive per-column (enrich_boe_donors.py, enrich_fec_donors.py, voter/ethnicity.py, voter/enrich_derived.py, etc.)
  → Export (export/export.py → Excel by district/county)
  → Sync (sync/aiven_sync.py → Aiven remote MySQL via mysqldump)
```

### Key Architectural Patterns

**StateVoterId direct matching** — All audience CSVs, donor records, and CRM contacts join on a single integer StateVoterId. No fuzzy/hash matching.

**Layered enrichment** — Each script in `pipeline/` and `voter/` adds columns to `voter_file` independently. Scripts can run in isolation or sequence.

**Hash-based change detection** — BOE, FEC, and CFB loaders hash source files and skip re-processing if unchanged. Stored in `load_metadata`.

**Performance** — `LOAD DATA LOCAL INFILE` for bulk loads; indexes built after data is loaded (not during); no intermediate staging tables.

**CRM merge logic** (`pipeline/crm_merge.py`) — Email-based dedup (`email_1` is UNIQUE), 5 email slots, 4 phone slots, source tracking.

### Audience Segmentation

SQL views in `config/audience_group_queries.sql` define named audiences (e.g., `NYS_HARD_GOP`, `NYS_HARD_DEM`, `NYS_SWING`). Audience membership is stored in `voter_audience_bridge`.

### Ethnicity Model

`voter/ethnicity.py` classifies voters into 10 categories using BISG (via `surgeo`) plus surname/suffix heuristics for sub-categories (Slavic, Italian, Irish, Jewish, South Asian, Middle Eastern, etc.).

## Configuration

Copy `.env.example` to `.env` and fill in credentials:

```
DB_HOST=localhost          # Local MySQL 8.4
DB_USER=root
DB_PASSWORD=...
DB_PORT=3306

AIVEN_HOST=...             # Optional, for sync command
AIVEN_USER=avnadmin
AIVEN_PASSWORD=...
AIVEN_PORT=3306
AIVEN_DB=nys_voter_tagging
AIVEN_SSL_CA=certs/ca.pem

HUBSPOT_TOKEN_1=...        # Multiple accounts supported (_1, _2, ...)
CM_API_KEY_1=...           # Multiple accounts supported (_1, _2, ...)
```

MySQL performance tuning config: `config/my.ini.optimized`.
