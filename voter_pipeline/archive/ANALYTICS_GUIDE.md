# Audience Analytics Guide

## Overview

You now have two powerful tools for analyzing your voter audiences:

1. **`audience_analytics.py`** - Query and analyze existing data with flexible filtering
2. **`build_causeway_audience_tables.py`** - Create materialized tables for every Causeway audience

---

## Quick Start

### 1. Analyze Existing Data (audience_analytics.py)

This tool lets you query your data interactively without creating new tables.

```bash
# List all audiences statewide
python audience_analytics.py --list-audiences

# Filter by Legislative District 63
python audience_analytics.py --ld "063" --list-audiences

# Get ethnicity breakdown for specific audience in LD 63
python audience_analytics.py --ld "063" --audience "HT HARD GOP INDV NYS_001.csv" --ethnicity

# Get HT/MT/LT breakdown for "HARD GOP" pattern
python audience_analytics.py --turnout-split "HARD GOP"

# Export all data for SD 05 to CSV
python audience_analytics.py --sd "SD 05" --export-csv
```

### 2. Build Materialized Tables (build_causeway_audience_tables.py)

This tool creates permanent tables in MySQL for fast querying.

```bash
# Build tables for ALL Causeway audiences (creates ~1000+ tables!)
python build_causeway_audience_tables.py

# Build for specific audience only
python build_causeway_audience_tables.py --audience "HT HARD GOP INDV NYS_001.csv"

# Build for all audiences matching "HARD GOP"
python build_causeway_audience_tables.py --pattern "HARD GOP"

# Test with first 10 audiences only
python build_causeway_audience_tables.py --limit 10

# Build without ethnicity tables (faster)
python build_causeway_audience_tables.py --skip-ethnicity
```

---

## Common Use Cases

### Use Case 1: "I want HT and MT breakdowns for LD 63"

```bash
# Option A: Interactive query
python audience_analytics.py --ld "063" --turnout-split "HARD GOP"
python audience_analytics.py --ld "063" --turnout-split "HARD DEM"
python audience_analytics.py --ld "063" --turnout-split "SWING"

# Option B: Build tables, then query in MySQL
python build_causeway_audience_tables.py --pattern "HT HARD GOP"
python build_causeway_audience_tables.py --pattern "MT HARD GOP"
```

Then in MySQL:
```sql
-- Get HT HARD GOP voters in LD 63
SELECT * FROM HT_HARD_GOP_INDV_NYS_001_csv WHERE LDName = '063';

-- Get count by ethnicity
SELECT * FROM HT_HARD_GOP_INDV_NYS_001_csv_ethnicity_by_ld WHERE LDName = '063';
```

### Use Case 2: "Ethnicity breakdown for all SWING voters in CD 03"

```bash
# Interactive query
python audience_analytics.py --cd "CD 03" --audience "NYS_SWING" --ethnicity

# Or build tables
python build_causeway_audience_tables.py --pattern "SWING"
```

Then in MySQL:
```sql
SELECT * FROM HT_SWING_INDV_NYS_XXX_csv_ethnicity_by_cd WHERE CDName = 'CD 03';
```

### Use Case 3: "Export all audiences filtered by SD 05 to CSV"

```bash
python audience_analytics.py --sd "SD 05" --export-csv
```

This creates CSV files in:
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

### Use Case 4: "Get district-level breakdown for specific audience"

```bash
# By LD
python audience_analytics.py --audience "HT HARD GOP INDV NYS_001.csv" --district-breakdown LD

# By SD
python audience_analytics.py --audience "HT HARD GOP INDV NYS_001.csv" --district-breakdown SD

# By CD
python audience_analytics.py --audience "HT HARD GOP INDV NYS_001.csv" --district-breakdown CD
```

---

## Table Naming Convention

When you run `build_causeway_audience_tables.py`, tables are created with this naming:

**Main audience table:**
- `HT_HARD_GOP_INDV_NYS_001_csv` (sanitized from "HT HARD GOP INDV NYS_001.csv")

**Related tables:**
- `HT_HARD_GOP_INDV_NYS_001_csv_ethnicity` - Statewide ethnicity breakdown
- `HT_HARD_GOP_INDV_NYS_001_csv_ethnicity_by_cd` - Ethnicity by CD
- `HT_HARD_GOP_INDV_NYS_001_csv_ethnicity_by_sd` - Ethnicity by SD
- `HT_HARD_GOP_INDV_NYS_001_csv_ethnicity_by_ld` - Ethnicity by LD
- `HT_HARD_GOP_INDV_NYS_001_csv_by_cd` - Count by CD
- `HT_HARD_GOP_INDV_NYS_001_csv_by_sd` - Count by SD
- `HT_HARD_GOP_INDV_NYS_001_csv_by_ld` - Count by LD

---

## SQL Query Examples (After Building Tables)

```sql
-- Get all HT HARD GOP voters in LD 63
SELECT FirstName, LastName, PrimaryZip, CDName, SDName, LDName
FROM HT_HARD_GOP_INDV_NYS_001_csv
WHERE LDName = '063';

-- Ethnicity breakdown for LD 63
SELECT ethnicity, voters
FROM HT_HARD_GOP_INDV_NYS_001_csv_ethnicity_by_ld
WHERE LDName = '063'
ORDER BY voters DESC;

-- Compare HT vs MT HARD GOP in LD 63
SELECT 'HT' AS turnout, COUNT(*) AS voters
FROM HT_HARD_GOP_INDV_NYS_001_csv
WHERE LDName = '063'
UNION ALL
SELECT 'MT', COUNT(*)
FROM MT_HARD_GOP_INDV_NYS_001_csv
WHERE LDName = '063';

-- Top 5 districts for specific audience
SELECT LDName, voters
FROM HT_HARD_GOP_INDV_NYS_001_csv_by_ld
ORDER BY voters DESC
LIMIT 5;

-- All audiences in LD 63 (using existing bridge table)
SELECT b.audience, COUNT(*) AS voters
FROM fullvoter_audience_bridge b
INNER JOIN fullnyvoter_2025 f ON f.StateVoterId = b.StateVoterId
WHERE f.LDName = '063'
GROUP BY b.audience
ORDER BY voters DESC;
```

---

## Performance Tips

### For Quick Ad-Hoc Queries:
Use `audience_analytics.py` - no table creation needed

### For Repeated Queries:
Run `build_causeway_audience_tables.py` once, then query MySQL directly

### For Specific Districts:
Filter with `--ld`, `--sd`, or `--cd` flags - much faster than querying all data

### For Large Builds:
- Test with `--limit 10` first
- Use `--skip-ethnicity` if you don't need ethnicity data
- Build only what you need with `--pattern` or `--audience`

---

## Integration with Existing Pipeline

Your current `pipeline.py` creates these tables:
- `NYS_HARD_GOP`, `NYS_HARD_DEM`, `NYS_SWING` (combined HT+MT+LT)
- `NYS_HT_HARD_GOP`, `NYS_MT_HARD_GOP`, `NYS_LT_HARD_GOP` (individual turnout levels)
- Ethnicity tables for the combined groups

The new scripts add:
- **Individual Causeway audience tables** (one per CSV file)
- **Ethnicity breakdowns** for each individual audience
- **District-level summaries** for each individual audience
- **Flexible filtering** by LD/SD/CD without rebuilding

---

## Example Workflow

```bash
# 1. Run main pipeline (as usual)
python pipeline.py

# 2. Build tables for audiences you care about
python build_causeway_audience_tables.py --pattern "HARD GOP" --pattern "SWING"

# 3. Query specific district interactively
python audience_analytics.py --ld "063" --list-audiences

# 4. Get detailed breakdown
python audience_analytics.py --ld "063" --audience "HT HARD GOP INDV NYS_001.csv" --ethnicity

# 5. Export to CSV for sharing
python audience_analytics.py --ld "063" --export-csv
```

---

## Troubleshooting

**Error: "Table already exists"**
- Use `--rebuild` flag to drop and recreate tables

**Error: "Audience not found"**
- Run with `--list-audiences` to see available audiences
- Check spelling - audience names are case-sensitive

**Slow performance**
- Use `--limit` to test with fewer audiences first
- Skip ethnicity/district tables if not needed
- Query materialized tables directly in MySQL instead of using Python tools

**Out of memory**
- Build tables in batches using `--pattern` or `--limit`
- Run during off-hours if MySQL buffer pool is under load

---

## Questions?

Check the help for each tool:
```bash
python audience_analytics.py --help
python build_causeway_audience_tables.py --help
```
