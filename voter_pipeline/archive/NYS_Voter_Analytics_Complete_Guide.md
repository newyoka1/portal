# NYS VOTER ANALYTICS TOOLKIT
## Complete Documentation Guide

**Version:** 1.0
**Last Updated:** February 10, 2026

---

# Table of Contents

1. [Welcome & Quick Start](#welcome-quick-start)
2. [Complete User Guide (INSTRUCTIONS)](#complete-user-guide)
3. [Build Script Guide](#build-script-guide)
4. [Feature Summary](#feature-summary)
5. [File Locations & System Information](#file-locations)

---

<a name="welcome-quick-start"></a>
# 1. Welcome & Quick Start

Welcome to the NYS Voter Analytics Toolkit!

This comprehensive guide contains all the tools and documentation you need to analyze voter data by district, ethnicity, and audience.

## Quick Start - READ THIS FIRST!

### NEW USER? Start here:
1. Read the Complete User Guide section below
2. Run: `python query_helper.py` (see what data is available)
3. Try: `python audience_analytics.py --ld "63" --list-audiences`

### EXPERIENCED USER? Quick commands:
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
python audience_analytics.py --ld "63" --export-csv
```

## Documentation Files

This guide consolidates the following documentation:

- **INSTRUCTIONS.txt** - Complete user guide (600 lines)
- **BUILD_SCRIPT_INSTRUCTIONS.txt** - Materialized table creation guide (400 lines)
- **README.txt** - Master index and quick reference
- **QUICK_START_UPDATED.md** - Quick reference guide
- **ANALYTICS_GUIDE.md** - Full documentation with SQL examples
- **FINAL_FEATURES.md** - Feature summary and highlights

## Python Scripts

### audience_analytics.py (MAIN TOOL)
Interactive analytics - use this for most tasks

**What it does:**
- List audiences by district
- Ethnicity breakdowns
- Matched vs unmatched comparison
- HT/MT/LT analysis
- CSV exports

**Quick example:**
```bash
python audience_analytics.py --ld "63" --list-audiences
```

### build_causeway_audience_tables.py (PERFORMANCE TOOL)
Create permanent tables for faster queries

**What it does:**
- Materializes audience tables in MySQL
- 10-50x faster than ad-hoc queries
- Creates ethnicity and district breakdowns

**Quick example:**
```bash
python build_causeway_audience_tables.py --pattern "HARD GOP"
```

### query_helper.py (DISCOVERY TOOL)
See what districts and audiences are available

**What it does:**
- Lists all available districts
- Shows audience statistics
- Generates sample SQL queries

**Quick example:**
```bash
python query_helper.py
```

### pipeline.py (DATA PIPELINE)
Main data processing pipeline

**What it does:**
- Loads voter data
- Matches with Causeway audiences
- Creates all base tables

**Run this FIRST before using other tools!**

## Key Features

- **Filter by District**
  - Legislative District (LD): `--ld "63"`
  - State Senate District (SD): `--sd "5"`
  - Congressional District (CD): `--cd "3"`

- **Ethnicity Analysis**
  - Breakdown for any audience
  - Matched vs unmatched comparison
  - Side-by-side percentages

- **Matched vs Unmatched Tracking**
  - See who's in Causeway audiences (matched)
  - See who's NOT in any audience (unmatched)
  - Compare ethnic composition
  - Identify targeting gaps

- **Turnout Level Analysis**
  - Compare HT/MT/LT variants
  - See distribution across turnout levels

- **CSV Exports**
  - All data exportable to CSV
  - Ready for Excel/Tableau
  - Shareable reports

- **Fast Performance**
  - Option A: Interactive queries (1-5 seconds)
  - Option B: Materialized tables (0.1 seconds)

## Typical Workflow

**SCENARIO:** Analyze LD 63 for campaign targeting

**Step 1:** Discover what's available
```bash
python query_helper.py
```

**Step 2:** Get overview of LD 63
```bash
python audience_analytics.py --ld "63" --list-audiences
```

Output shows:
- Total voters: 87,107
- Matched: 80,340 (92.2%)
- Unmatched: 13,904 (16.0%)

**Step 3:** Compare matched vs unmatched by ethnicity
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

Identifies targeting gaps (e.g., Asian/PI voters underrepresented)

**Step 4:** Analyze specific audience
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

**Step 5:** Export everything for team
```bash
python audience_analytics.py --ld "63" --export-csv
```

Files go to: `analytics_output` folder

**Step 6 (Optional):** Create permanent tables for repeated use
```bash
python build_causeway_audience_tables.py --pattern "HARD GOP"
```

Then query directly in MySQL for 10-50x faster performance

## Important Notes

### District Format
Use plain numbers WITHOUT leading zeros or prefixes:
- **Correct:** `--ld "63"  --sd "5"  --cd "3"`
- **Wrong:** `--ld "063" --sd "SD 05" --cd "CD 03"`

### Prerequisites
- Python 3.x installed
- pymysql library installed (`pip install pymysql`)
- MySQL running with NYS_VOTER_TAGGING database
- pipeline.py must be run first to populate data

### Performance
For repeated queries, use `build_causeway_audience_tables.py` to create permanent tables (10-50x faster!)

---

<a name="complete-user-guide"></a>
# 2. Complete User Guide (INSTRUCTIONS)

## Table of Contents - User Guide

1. Overview
2. Requirements
3. Getting Started
4. Commands and Usage
5. Advanced Features
6. CSV Export Guide
7. Understanding the Output
8. Common Use Cases
9. Troubleshooting
10. File Locations

---

## 1. OVERVIEW

### What is this tool?

The **audience_analytics.py** script is your main interface for analyzing NYS voter data with a focus on:
- **District filtering** (LD, SD, CD)
- **Ethnicity analysis** (WHITE, BLACK, HISPANIC, ASIAN_PI, UNKNOWN)
- **Causeway audience tracking** (which voters are in which audiences)
- **Matched vs Unmatched analysis** (voters in audiences vs not in any audience)
- **HT/MT/LT turnout variants**
- **CSV exports** for reports

### What data does it work with?

This tool queries the **NYS_VOTER_TAGGING** MySQL database, which contains:
- Full NYS voter file (fullnyvoter_2025 table)
- Causeway audience files (fullvoter_audience_bridge table)
- Census surname ethnicity data (census_surname_ethnicity table)

### Key concepts

**Matched voters:** Voters who appear in at least one Causeway audience file. These voters have the `origin` field populated in the database.

**Unmatched voters:** Voters in the NYS voter file who do NOT appear in any Causeway audience. These voters have `origin` field NULL or empty.

**Ethnicity:** Inferred from last name using census surname data. Categories:
- WHITE
- BLACK
- HISPANIC
- ASIAN_PI (Asian/Pacific Islander)
- UNKNOWN (no surname match or missing last name)

**Districts:**
- **LD** = Legislative District (State Assembly)
- **SD** = State Senate District
- **CD** = Congressional District

**Turnout levels:**
- **HT** = High Turnout
- **MT** = Medium Turnout
- **LT** = Low Turnout

---

## 2. REQUIREMENTS

### System Requirements
- Python 3.x
- MySQL 8.0 or higher
- 64GB RAM recommended (for large datasets)
- Windows, macOS, or Linux

### Python Libraries
Install required libraries:
```bash
pip install pymysql
```

### Database Requirements
- MySQL server running locally or remotely
- Database: **NYS_VOTER_TAGGING**
- Tables:
  - fullnyvoter_2025
  - fullvoter_audience_bridge
  - census_surname_ethnicity

### Prerequisites
1. **Run pipeline.py first** to populate the database
2. Ensure MySQL is running
3. Verify database credentials (currently hardcoded in script)

---

## 3. GETTING STARTED

### Step 1: Verify your setup

Check that your database is populated:
```bash
python query_helper.py
```

This shows:
- Available districts
- Audience statistics
- Sample queries

### Step 2: Try your first query

List all audiences in a district:
```bash
python audience_analytics.py --ld "63" --list-audiences
```

### Step 3: Explore ethnicity

Compare matched vs unmatched voters:
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

### Step 4: Export data

Export all data to CSV for analysis:
```bash
python audience_analytics.py --ld "63" --export-csv
```

CSV files are saved to:
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

---

## 4. COMMANDS AND USAGE

### 4.1 Basic Command Structure

```bash
python audience_analytics.py [FILTERS] [ACTIONS]
```

**FILTERS** (choose one):
- `--ld "63"` - Filter by Legislative District
- `--sd "5"` - Filter by State Senate District
- `--cd "3"` - Filter by Congressional District
- `--statewide` - No district filter (all NYS)

**ACTIONS** (choose one or more):
- `--list-audiences` - Show all audiences with counts
- `--matched-vs-unmatched` - Compare matched vs unmatched ethnicity
- `--unmatched` - Analyze unmatched voters only
- `--audience "NAME"` - Analyze specific audience
- `--ethnicity` - Show ethnicity breakdown
- `--turnout-split "PATTERN"` - Compare HT/MT/LT for audience
- `--export-csv` - Export all data to CSV

### 4.2 List All Audiences

**Command:**
```bash
python audience_analytics.py --ld "63" --list-audiences
```

**Output:**
```
Audience Summary - LD 63

Total voters in LD 63: 87,107
  Matched voters (in any audience): 80,340 (92.2%)
  Unmatched voters (not in any audience): 13,904 (16.0%)

Audiences available:
Rank  Audience                                      Voters    % of District
----  --------------------------------------------  --------  -------------
1     HT HARD DEM INDV NYS_2555.csv                32,187    37.0%
2     HT HARD GOP INDV NYS_1335.csv                18,504    21.2%
3     HT SWING INDV NYS_0983.csv                   12,457    14.3%
...
[!]   ** NO AUDIENCE MATCH **                      13,904    16.0%
```

### 4.3 Compare Matched vs Unmatched (KEY FEATURE!)

**Command:**
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

**Output:**
```
Matched vs Unmatched Ethnicity Comparison - LD 63

Overall Summary:
  Total voters:            87,107
  Matched voters:          80,340 ( 92.2%)
  Unmatched voters:        13,904 ( 16.0%)

Ethnicity Comparison:
  Ethnicity         Matched            Unmatched        Difference
  ---------------------------------------------------------------------
  WHITE          38,157 (52.1%)     5,946 (42.8%)    +9.4%  ← More likely matched
  ASIAN_PI        8,759 (12.0%)     2,179 (15.7%)    -3.7%  ← Less likely matched
  HISPANIC        9,739 (13.3%)     2,105 (15.1%)    -1.8%  ← Less likely matched
  BLACK           1,984 ( 2.7%)       473 ( 3.4%)    -0.7%  ← Less likely matched
  UNKNOWN        14,539 (19.9%)     3,199 (23.0%)    -3.2%  ← Less likely matched
```

**Key Insights:**
- **Positive difference (+)**: Group is MORE represented in Causeway audiences
- **Negative difference (-)**: Group is LESS represented (targeting gap!)

In this example:
- White voters are 9.4% more likely to be in an audience
- Asian/PI voters are 3.7% less likely (opportunity!)

### 4.4 Analyze Specific Audience

**Command:**
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

**Output:**
```
Ethnicity Breakdown - HT HARD GOP INDV NYS_1335.csv (LD 63)

Total voters: 18,504

Ethnicity         Voters      Percentage
-------------------------------------
WHITE            12,350       66.7%
UNKNOWN           3,200       17.3%
ASIAN_PI          1,850       10.0%
HISPANIC            890        4.8%
BLACK               214        1.2%
```

### 4.5 Compare Turnout Levels (HT vs MT vs LT)

**Command:**
```bash
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"
```

**Output:**
```
Turnout Level Comparison - LD 63
Pattern: "HARD GOP"

Audience                          Voters    % of District
--------------------------------------------------------
HT HARD GOP INDV NYS_1335.csv     18,504    21.2%
MT HARD GOP INDV NYS_3145.csv     24,890    28.6%
LT HARD GOP INDV NYS_5522.csv     31,255    35.9%

Total across all turnout levels:  74,649    85.7%
```

This shows how many additional voters you reach by including MT and LT turnout levels.

### 4.6 Analyze Unmatched Voters Only

**Command:**
```bash
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

**Output:**
```
Unmatched Voters (Not in Any Audience) - LD 63

Total unmatched voters: 13,904

Ethnicity         Voters      Percentage
-------------------------------------
WHITE             5,946       42.8%
UNKNOWN           3,199       23.0%
HISPANIC          2,105       15.1%
ASIAN_PI          2,179       15.7%
BLACK               473        3.4%
```

### 4.7 Statewide Analysis

**Command:**
```bash
python audience_analytics.py --statewide --matched-vs-unmatched
```

Analyzes all NYS voters without district filter.

### 4.8 Export to CSV

**Command:**
```bash
python audience_analytics.py --ld "63" --export-csv
```

**Creates 5 CSV files:**
1. **audience_summary_LD_63.csv** - All audiences with counts
2. **ethnicity_ALL_MATCHED_LD_63.csv** - Combined ethnicity of ALL matched voters
3. **ethnicity_NO_AUDIENCE_MATCH_LD_63.csv** - Ethnicity of unmatched voters
4. **ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv** - Side-by-side comparison
5. **ethnicity_[audience]_LD_63.csv** - Top 10 individual audiences (10 files)

**Output location:**
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

---

## 5. ADVANCED FEATURES

### 5.1 Combining Multiple Filters

You can combine district filters with actions:

```bash
# LD 63, matched vs unmatched, then export
python audience_analytics.py --ld "63" --matched-vs-unmatched --export-csv

# SD 5, list audiences only
python audience_analytics.py --sd "5" --list-audiences

# Statewide, specific audience with ethnicity
python audience_analytics.py --statewide --audience "HT HARD DEM INDV NYS_2555.csv" --ethnicity
```

### 5.2 Pattern Matching for Turnout Split

The `--turnout-split` option accepts partial patterns:

```bash
# All HARD GOP variants (HT, MT, LT)
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"

# All SWING variants
python audience_analytics.py --ld "63" --turnout-split "SWING"

# All HARD DEM variants
python audience_analytics.py --ld "63" --turnout-split "HARD DEM"
```

### 5.3 SQL Query Generation

Use `query_helper.py` to generate sample SQL queries:

```bash
python query_helper.py
```

This shows you the underlying SQL for direct database queries.

---

## 6. CSV EXPORT GUIDE

### 6.1 CSV Files Created

When you run `--export-csv`, these files are created:

#### audience_summary_[filter].csv
```csv
Audience,Voters,Percentage
HT HARD DEM INDV NYS_2555.csv,32187,37.0
HT HARD GOP INDV NYS_1335.csv,18504,21.2
** NO AUDIENCE MATCH **,13904,16.0
```

#### ethnicity_ALL_MATCHED_[filter].csv
```csv
Ethnicity,Voters,Percentage
WHITE,38157,52.12
UNKNOWN,14539,19.86
HISPANIC,9739,13.30
ASIAN_PI,8759,11.97
BLACK,1984,2.71
```

#### ethnicity_NO_AUDIENCE_MATCH_[filter].csv
```csv
Ethnicity,Voters,Percentage
WHITE,5946,42.76
UNKNOWN,3199,23.01
ASIAN_PI,2179,15.67
HISPANIC,2105,15.14
BLACK,473,3.40
```

#### ethnicity_MATCHED_VS_UNMATCHED_[filter].csv
```csv
Ethnicity,Matched_Voters,Matched_Pct,Unmatched_Voters,Unmatched_Pct,Difference_Pct
WHITE,38157,52.12,5946,42.76,9.36
ASIAN_PI,8759,11.97,2179,15.67,-3.70
HISPANIC,9739,13.30,2105,15.14,-1.84
BLACK,1984,2.71,473,3.40,-0.69
UNKNOWN,14539,19.86,3199,23.01,-3.15
```

#### ethnicity_[audience]_[filter].csv (top 10)
Individual ethnicity breakdowns for each of the top 10 audiences.

### 6.2 Opening CSV Files

- **Excel:** Double-click CSV file or File → Open in Excel
- **Google Sheets:** File → Import → Upload CSV
- **Tableau/Power BI:** Connect to CSV data source

### 6.3 CSV File Naming

Files are named with the filter applied:
- `audience_summary_LD_63.csv` - Filtered by LD 63
- `audience_summary_SD_5.csv` - Filtered by SD 5
- `audience_summary_STATEWIDE.csv` - No district filter

---

## 7. UNDERSTANDING THE OUTPUT

### 7.1 Matched Voters

**Definition:** Voters who appear in at least one Causeway audience file.

**Database indicator:** `origin` field is NOT NULL and NOT empty

**Typical percentage:** 85-95% of voters in a district

**Why are voters matched?**
- Meet Causeway targeting criteria
- Have valid DOB for matching
- Successfully matched on FirstName + LastName + ZIP + DOB

### 7.2 Unmatched Voters

**Definition:** Voters in NYS voter file but NOT in any Causeway audience.

**Database indicator:** `origin` field IS NULL or empty

**Typical percentage:** 5-15% of voters in a district

**Why are voters unmatched?**
- Don't meet Causeway targeting criteria
- Missing DOB or other matching fields
- Not targeted by any Causeway audience
- Data quality issues (name variations, ZIP changes, etc.)

### 7.3 Ethnicity Inference

Ethnicity is inferred from last name using Census Bureau surname data:

- **WHITE:** European surnames (Smith, Johnson, O'Brien, etc.)
- **HISPANIC:** Spanish/Latin surnames (Garcia, Rodriguez, etc.)
- **BLACK:** African-American surnames (Washington, Jefferson, etc.)
- **ASIAN_PI:** Asian/Pacific Islander surnames (Lee, Kim, Nguyen, etc.)
- **UNKNOWN:** No surname match or missing last name

**Accuracy:** ~80-90% for most groups, lower for ASIAN_PI due to surname overlap

### 7.4 Difference Percentage

In the matched vs unmatched comparison, the "Difference" column shows:

**Positive (+):** Group is MORE represented in matched voters
- Example: WHITE +9.4% means white voters are 9.4 percentage points more likely to be in an audience

**Negative (-):** Group is LESS represented in matched voters (potential targeting gap!)
- Example: ASIAN_PI -3.7% means Asian/PI voters are 3.7 percentage points less likely to be in an audience

**Zero (0):** Group is equally represented in matched and unmatched

### 7.5 District Format

Districts are stored as plain numbers WITHOUT leading zeros:
- **Correct:** LD 63, SD 5, CD 3
- **Wrong:** LD 063, SD 05, CD 03

Always use: `--ld "63"` not `--ld "063"`

---

## 8. COMMON USE CASES

### Use Case 1: "Are we missing certain ethnic groups in our audiences?"

**Solution:**
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

Look for negative difference percentages. Example:
- ASIAN_PI -3.7% = Asian/PI voters are underrepresented
- HISPANIC -1.8% = Hispanic voters are underrepresented

**Action:** Consider creating targeted audiences for underrepresented groups.

### Use Case 2: "What's the ethnic composition of voters we're NOT reaching?"

**Solution:**
```bash
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

Shows ethnicity breakdown of unmatched voters only.

**Action:** Use this to identify opportunities for new audience creation.

### Use Case 3: "Compare multiple districts"

**Solution:**
```bash
python audience_analytics.py --ld "63" --export-csv
python audience_analytics.py --ld "64" --export-csv
python audience_analytics.py --ld "65" --export-csv
```

Then compare the CSV files in Excel or Tableau.

### Use Case 4: "How many more voters do we reach with MT and LT audiences?"

**Solution:**
```bash
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"
```

Shows:
- HT: 18,504 voters (high turnout only)
- MT: 24,890 voters (medium turnout)
- LT: 31,255 voters (low turnout)
- Total: 74,649 voters (all turnout levels)

**Insight:** Including MT and LT gives you 4x more voters than HT alone!

### Use Case 5: "Weekly report for campaign team"

**Solution:**
```bash
# Export all data for LD 63
python audience_analytics.py --ld "63" --export-csv

# Share the CSV files from:
# C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

**Files to share:**
- `audience_summary_LD_63.csv` - Overview
- `ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv` - Key targeting insights

### Use Case 6: "Statewide analysis for all NYS"

**Solution:**
```bash
python audience_analytics.py --statewide --matched-vs-unmatched
```

Shows statewide patterns in audience representation.

---

## 9. TROUBLESHOOTING

### Problem: "No audiences found"

**Possible causes:**
1. District format wrong (use "63" not "063")
2. No voters in that district
3. Database not populated

**Solution:**
```bash
# Check available districts
python query_helper.py

# Verify district format
python audience_analytics.py --ld "63" --list-audiences
```

### Problem: "Can't connect to database"

**Possible causes:**
1. MySQL not running
2. Wrong database credentials
3. Database doesn't exist

**Solution:**
1. Start MySQL service
2. Verify connection settings in script
3. Create database: `CREATE DATABASE NYS_VOTER_TAGGING;`

### Problem: "Missing data for some audiences"

**Possible causes:**
1. pipeline.py not run yet
2. Incomplete data load
3. Audience files not processed

**Solution:**
```bash
# Re-run the data pipeline
python pipeline.py
```

### Problem: "CSV files not created"

**Possible causes:**
1. Output directory doesn't exist
2. No write permissions
3. Path issues on Mac/Linux

**Solution:**
1. Create output directory manually
2. Check permissions: `chmod 755 analytics_output/`
3. Update OUTPUT_DIR in script if needed

### Problem: "Query is slow"

**Possible causes:**
1. Large dataset (millions of voters)
2. MySQL not optimized
3. Missing indexes

**Solution:**
1. Use materialized tables: `python build_causeway_audience_tables.py`
2. Optimize MySQL (see MySQL tuning guide)
3. Add indexes on LDName, SDName, CDName

### Problem: "Ethnicity shows mostly UNKNOWN"

**Possible causes:**
1. census_surname_ethnicity table not populated
2. LastName field has data quality issues
3. Census data not loaded

**Solution:**
1. Verify census table exists: `SELECT COUNT(*) FROM census_surname_ethnicity;`
2. Re-run pipeline.py to populate census data
3. Check for missing or malformed last names

---

## 10. FILE LOCATIONS

### Script Location
```
D:\git\audience_analytics.py
D:\git\build_causeway_audience_tables.py
D:\git\query_helper.py
D:\git\pipeline.py
```

### Documentation Location
```
D:\git\README.txt
D:\git\INSTRUCTIONS.txt
D:\git\BUILD_SCRIPT_INSTRUCTIONS.txt
D:\git\QUICK_START_UPDATED.md
D:\git\ANALYTICS_GUIDE.md
D:\git\FINAL_FEATURES.md
```

### CSV Export Location
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

### Database
- **Server:** localhost (default)
- **Database:** NYS_VOTER_TAGGING
- **Key tables:**
  - fullnyvoter_2025
  - fullvoter_audience_bridge
  - census_surname_ethnicity

---

<a name="build-script-guide"></a>
# 3. Build Script Guide

## Materialized Table Creation Tool

This section covers the **build_causeway_audience_tables.py** script, which creates permanent materialized tables in MySQL for EACH individual Causeway audience.

## What This Tool Does

For each audience, it creates:
- Main audience table (all voter data)
- Ethnicity breakdown table
- Ethnicity by CD table
- Ethnicity by SD table
- Ethnicity by LD table
- District summary tables (counts by LD/SD/CD)

**EXAMPLE:**
For "HT HARD GOP INDV NYS_1335.csv", it creates 7 tables:
1. HT_HARD_GOP_INDV_NYS_1335_csv
2. HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity
3. HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_cd
4. HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_sd
5. HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld
6. HT_HARD_GOP_INDV_NYS_1335_csv_by_cd
7. HT_HARD_GOP_INDV_NYS_1335_csv_by_ld
8. HT_HARD_GOP_INDV_NYS_1335_csv_by_sd

## When to Use It

**USE THIS TOOL WHEN:**
- You query the same audiences repeatedly
- You need faster query performance
- You want to create reports in tools like Excel/Tableau
- You need to share data with others via SQL
- You're doing complex analysis on specific audiences

**DON'T USE THIS TOOL WHEN:**
- You're doing one-time quick queries (use audience_analytics.py instead)
- You're still exploring different audiences
- Disk space is limited (creates many tables)

**PERFORMANCE COMPARISON:**
- audience_analytics.py: 1-5 seconds per query (joins every time)
- build tables + direct SQL: 0.1 seconds per query (pre-computed)

## How It Works

**STEP 1:** Reads all audiences from fullvoter_audience_bridge table

**STEP 2:** For each audience:
- a. Creates a table with all voter records for that audience
- b. Adds indexes for fast lookups (CDName, SDName, LDName)
- c. Creates ethnicity breakdown tables
- d. Creates district summary tables

**STEP 3:** Returns summary of all tables created

**IMPORTANT:** This does NOT modify your source data. It only creates new tables for faster querying.

## Basic Usage

### Build All Tables (WARNING: Creates 100+ tables!)

**Command:**
```bash
python build_causeway_audience_tables.py
```

**What it does:**
- Processes ALL 16+ audiences in your database
- Creates ~7 tables per audience
- Takes 5-15 minutes depending on data size

**Output:**
```
[1/16] HT HARD DEM INDV NYS_2555.csv
  Table name: HT_HARD_DEM_INDV_NYS_2555_csv
  OK HT_HARD_DEM_INDV_NYS_2555_csv: 2,179,760 voters
  Creating ethnicity tables...
  Creating district summary tables...

[2/16] HT HARD GOP INDV NYS_1335.csv
  ...

SUMMARY:
Tables created: 112
Total time: 8.3 minutes
```

### Build Specific Audience Only

**Command:**
```bash
python build_causeway_audience_tables.py --audience "HT HARD GOP INDV NYS_1335.csv"
```

**What it does:**
- Processes ONLY the specified audience
- Creates 7 tables for that audience
- Takes 10-30 seconds

**Use when:**
- You only need tables for one audience
- Testing the tool
- Quick turnaround needed

### Build by Pattern

**Command:**
```bash
python build_causeway_audience_tables.py --pattern "HARD GOP"
```

**What it does:**
- Processes all audiences matching "HARD GOP"
- Examples: "HT HARD GOP...", "MT HARD GOP...", "LT HARD GOP..."
- Creates tables for each matching audience

**Other examples:**
```bash
python build_causeway_audience_tables.py --pattern "HT"
python build_causeway_audience_tables.py --pattern "SWING"
python build_causeway_audience_tables.py --pattern "HARD DEM"
```

### Test with Limited Number

**Command:**
```bash
python build_causeway_audience_tables.py --limit 5
```

**What it does:**
- Processes only the first 5 audiences
- Good for testing before full build
- Verify everything works correctly

### Skip Ethnicity or District Tables

**Commands:**
```bash
python build_causeway_audience_tables.py --skip-ethnicity
python build_causeway_audience_tables.py --skip-districts
```

**What it does:**
- Creates only main audience tables
- Faster build time
- Less disk space used

**Use when:**
- You don't need ethnicity breakdowns
- You only need voter lists

## Advanced Options

### Rebuild Existing Tables

**Command:**
```bash
python build_causeway_audience_tables.py --rebuild
```

**What it does:**
- Drops existing tables before creating new ones
- Updates tables with latest data
- Use after running pipeline.py with new data

### Combine Multiple Options

**Examples:**

Test first 3 HARD GOP audiences without ethnicity:
```bash
python build_causeway_audience_tables.py --pattern "HARD GOP" --limit 3 --skip-ethnicity
```

Rebuild all HT audiences:
```bash
python build_causeway_audience_tables.py --pattern "HT" --rebuild
```

Build only main tables for SWING voters:
```bash
python build_causeway_audience_tables.py --pattern "SWING" --skip-ethnicity --skip-districts
```

## Tables Created

For audience "HT HARD GOP INDV NYS_1335.csv", these tables are created:

### Main Audience Table

**Table:** HT_HARD_GOP_INDV_NYS_1335_csv

**Contains:**
- All voter data (FirstName, LastName, Address, etc.)
- Only voters in this specific audience
- Primary key on StateVoterId
- Indexes on CDName, SDName, LDName

**Example query:**
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv WHERE LDName = '63';
```

### Ethnicity Table (Statewide)

**Table:** HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity

**Contains:**
```
Ethnicity, Voters
WHITE, 850000
HISPANIC, 150000
...
```

**Example query:**
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity ORDER BY Voters DESC;
```

### Ethnicity by District Tables

**Tables:**
- HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_cd
- HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_sd
- HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld

**Contains:**
```
District, Ethnicity, Voters
```

**Example query:**
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld
WHERE LDName = '63'
ORDER BY Voters DESC;
```

### District Summary Tables

**Tables:**
- HT_HARD_GOP_INDV_NYS_1335_csv_by_cd
- HT_HARD_GOP_INDV_NYS_1335_csv_by_sd
- HT_HARD_GOP_INDV_NYS_1335_csv_by_ld

**Contains:**
```
District, Voters
```

**Example query:**
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_by_ld
ORDER BY Voters DESC
LIMIT 10;
```

## Performance Benefits

**BEFORE (using audience_analytics.py):**
- Query time: 1-5 seconds per query
- Method: Joins tables on every query
- Good for: Ad-hoc exploration

**AFTER (using materialized tables):**
- Query time: 0.1 seconds per query (10-50x faster!)
- Method: Pre-computed tables
- Good for: Repeated analysis, reports, dashboards

**DISK SPACE:**
- Each main audience table: ~100-500 MB
- Each ethnicity table: ~1 KB
- Total for 16 audiences: ~5-10 GB

**EXAMPLE PERFORMANCE:**

Task: Get all HT HARD GOP voters in LD 63 with ethnicity

audience_analytics.py:
- Time: 3.2 seconds
- Joins 3 tables
- Computes on the fly

Materialized tables:
- Time: 0.15 seconds (21x faster!)
- Single table query
- Pre-computed

## Use Cases

### Use Case 1: Weekly Reports

**Scenario:**
You create weekly reports for 10 key audiences showing ethnicity breakdown by district.

**Solution:**
1. Build tables once:
```bash
python build_causeway_audience_tables.py --pattern "HT"
```

2. Each week, run fast SQL queries:
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld;
```

**Benefit:** Reduce report generation from 30 minutes to 2 minutes

### Use Case 2: Excel/Tableau Dashboards

**Scenario:**
Create interactive dashboards showing audience distribution

**Solution:**
1. Build tables for key audiences
2. Connect Excel/Tableau directly to MySQL tables
3. Build pivot tables and charts

**Benefit:** Fast, live data connections

### Use Case 3: Share Specific Data

**Scenario:**
Team member needs HT HARD GOP data for specific districts

**Solution:**
1. Build tables:
```bash
python build_causeway_audience_tables.py --audience "HT HARD GOP INDV NYS_1335.csv"
```

2. Share SQL queries or export CSV:
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv WHERE LDName IN ('63', '64', '65');
```

**Benefit:** Clean, organized data sharing

### Use Case 4: Complex Analysis

**Scenario:**
Analyze correlations between multiple audiences and demographics

**Solution:**
1. Build tables for all relevant audiences
2. Write complex SQL joins across tables
3. Fast query performance for iterative analysis

**Benefit:** Support for advanced analytics

## Maintenance

### Updating Tables

**When to update:**
- After running pipeline.py with new voter data
- After adding new Causeway audiences
- When data looks stale

**How to update:**
```bash
python build_causeway_audience_tables.py --rebuild
```

This drops old tables and creates fresh ones.

### Checking Disk Space

**Check table sizes:**
```sql
SELECT
  table_name,
  ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
FROM information_schema.TABLES
WHERE table_schema = 'NYS_VOTER_TAGGING'
  AND table_name LIKE 'HT_%'
ORDER BY size_mb DESC;
```

### Cleaning Up Old Tables

If you need to free space, drop tables you don't use:

```sql
-- Drop specific audience
DROP TABLE IF EXISTS HT_HARD_GOP_INDV_NYS_1335_csv;
DROP TABLE IF EXISTS HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity;
-- etc...
```

## Troubleshooting

### Problem: "Table already exists"

**Solution:**
Use --rebuild flag:
```bash
python build_causeway_audience_tables.py --rebuild
```

### Problem: "Out of disk space"

**Solutions:**
1. Build only what you need:
```bash
python build_causeway_audience_tables.py --pattern "HT" --skip-ethnicity
```

2. Free up space by dropping unused tables

3. Build in batches:
```bash
python build_causeway_audience_tables.py --limit 5
```
(Run multiple times with different audiences)

### Problem: "Slow table creation"

**Solutions:**
1. Make sure MySQL is optimized (see MySQL config update)
2. Run during off-hours
3. Use --skip-ethnicity and --skip-districts to speed up
4. Check MySQL buffer pool size (should be 40GB)

### Problem: "Can't find audience"

**Solution:**
Check available audiences:
```bash
python query_helper.py
```

Or:
```bash
python audience_analytics.py --list-audiences
```

### Problem: "Permission denied"

**Solution:**
1. Make sure you can connect to MySQL
2. Check that you have CREATE TABLE permissions
3. Verify database exists: NYS_VOTER_TAGGING

## Example Workflow

**SCENARIO:** You want to analyze HT and MT audiences with ethnicity data

**Step 1:** Build tables for HT and MT audiences
```bash
python build_causeway_audience_tables.py --pattern "HT"
python build_causeway_audience_tables.py --pattern "MT"
```

**Step 2:** Verify tables were created
```sql
USE NYS_VOTER_TAGGING;
SHOW TABLES LIKE 'HT_%';
SHOW TABLES LIKE 'MT_%';
```

**Step 3:** Query specific audience for LD 63
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv WHERE LDName = '63';
```

**Step 4:** Get ethnicity breakdown for LD 63
```sql
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld
WHERE LDName = '63';
```

**Step 5:** Compare HT vs MT
```sql
SELECT
  'HT' AS turnout,
  COUNT(*) AS voters
FROM HT_HARD_GOP_INDV_NYS_1335_csv
WHERE LDName = '63'
UNION ALL
SELECT
  'MT',
  COUNT(*)
FROM MT_HARD_GOP_INDV_NYS_3145_csv
WHERE LDName = '63';
```

## Quick Reference

```bash
# Build all audiences (WARNING: Takes time!)
python build_causeway_audience_tables.py

# Build specific audience
python build_causeway_audience_tables.py --audience "HT HARD GOP INDV NYS_1335.csv"

# Build by pattern
python build_causeway_audience_tables.py --pattern "HARD GOP"
python build_causeway_audience_tables.py --pattern "HT"
python build_causeway_audience_tables.py --pattern "SWING"

# Test with limit
python build_causeway_audience_tables.py --limit 5

# Rebuild existing tables
python build_causeway_audience_tables.py --rebuild

# Skip optional tables
python build_causeway_audience_tables.py --skip-ethnicity
python build_causeway_audience_tables.py --skip-districts

# Combined options
python build_causeway_audience_tables.py --pattern "HT" --limit 3 --skip-ethnicity

# Get help
python build_causeway_audience_tables.py --help
```

---

<a name="feature-summary"></a>
# 4. Feature Summary

## Complete Feature Set

Your `audience_analytics.py` now includes comprehensive ethnicity tracking for **BOTH** matched and unmatched voters!

## What You Can Do Now

### 1. Summary Statistics (Matched + Unmatched)
```bash
python audience_analytics.py --ld "63" --list-audiences
```

**Shows:**
- Total voters in district
- **Matched voters** (in any Causeway audience): 80,340 (92.2%)
- **Unmatched voters** (not in any audience): 13,904 (16.0%)

### 2. Compare Matched vs Unmatched Ethnicity (NEW!)
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

**Key Insights:**
- White voters are **9.4% more likely** to be in a Causeway audience
- Asian/PI voters are **3.7% less likely** to be matched
- Shows potential targeting gaps in your Causeway audiences

### 3. Analyze Just Unmatched Voters
```bash
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

### 4. Get Specific Audience Ethnicity
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

### 5. Export Everything to CSV (NEW FILES!)
```bash
python audience_analytics.py --ld "63" --export-csv
```

**Creates these CSV files:**

1. **audience_summary_LD_63.csv** - All audiences + unmatched count
2. **ethnicity_ALL_MATCHED_LD_63.csv** - Combined ethnicity of all matched voters
3. **ethnicity_NO_AUDIENCE_MATCH_LD_63.csv** - Ethnicity of unmatched voters
4. **ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv** - Side-by-side comparison
5. **ethnicity_[audience_name]_LD_63.csv** - Top 10 individual audiences

## Use Cases

### Use Case 1: "Are we missing certain ethnic groups?"
```bash
# Statewide analysis
python audience_analytics.py --statewide --matched-vs-unmatched

# By district
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

**Answer:** If a group shows negative difference, they're underrepresented in audiences.

### Use Case 2: "What's the ethnic makeup of matched vs unmatched?"
```bash
# Export both for comparison in Excel
python audience_analytics.py --ld "63" --export-csv
```

**Files to compare:**
- `ethnicity_ALL_MATCHED_LD_63.csv`
- `ethnicity_NO_AUDIENCE_MATCH_LD_63.csv`
- `ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv`

### Use Case 3: "Compare districts"
```bash
# Export multiple districts
python audience_analytics.py --ld "63" --export-csv
python audience_analytics.py --ld "64" --export-csv
python audience_analytics.py --ld "65" --export-csv

# Then compare the matched vs unmatched CSVs
```

### Use Case 4: "What % of Hispanic voters are matched?"
```bash
python audience_analytics.py --ld "63" --matched-vs-unmatched
```

Look at the "Difference" column - negative means less likely to be matched.

## All Available Commands

```bash
# Basic listing with summary
python audience_analytics.py --ld "63" --list-audiences

# Compare matched vs unmatched ethnicity
python audience_analytics.py --ld "63" --matched-vs-unmatched

# Unmatched voters only
python audience_analytics.py --ld "63" --unmatched --ethnicity

# Specific audience
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity

# Turnout split
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"

# Export everything
python audience_analytics.py --ld "63" --export-csv

# Statewide
python audience_analytics.py --statewide --matched-vs-unmatched

# By SD or CD
python audience_analytics.py --sd "5" --matched-vs-unmatched
python audience_analytics.py --cd "3" --matched-vs-unmatched
```

## Understanding the Data

### Matched Voters
- Appear in at least one Causeway audience file
- Have `origin` field populated
- Typically have valid DOB and matching criteria

### Unmatched Voters
- In fullnyvoter_2025 but NOT in any Causeway file
- Have `origin` field NULL or empty
- May be missing DOB, don't match criteria, or not targeted

### Difference Percentage
- **Positive (+)** = Group is MORE represented in matched voters
- **Negative (-)** = Group is LESS represented in matched voters (potential gap)

Example: If WHITE shows +9.4%, white voters are 9.4% more likely to be in a Causeway audience than unmatched voters.

## Key Insights from LD 63 Example

From the output:
```
WHITE     52.1% matched vs 42.8% unmatched  (+9.4% difference)
ASIAN_PI  12.0% matched vs 15.7% unmatched  (-3.7% difference)
HISPANIC  13.3% matched vs 15.1% unmatched  (-1.8% difference)
```

**Interpretation:**
- White voters are overrepresented in Causeway audiences
- Asian/PI and Hispanic voters are underrepresented
- This could indicate targeting opportunities

## Complete Feature List

- List all audiences with counts
- Summary statistics (total/matched/unmatched)
- Filter by LD/SD/CD
- Ethnicity breakdown for any audience
- Ethnicity breakdown for ALL matched voters
- Ethnicity breakdown for unmatched voters
- Side-by-side matched vs unmatched comparison
- HT/MT/LT turnout analysis
- District-level breakdowns
- CSV export with all data
- Statewide analysis

---

<a name="file-locations"></a>
# 5. File Locations & System Information

## Script Locations

**Main Scripts:**
```
D:\git\audience_analytics.py
D:\git\build_causeway_audience_tables.py
D:\git\query_helper.py
D:\git\pipeline.py
```

## Documentation Locations

**All Documentation:**
```
D:\git\README.txt
D:\git\INSTRUCTIONS.txt
D:\git\BUILD_SCRIPT_INSTRUCTIONS.txt
D:\git\QUICK_START_UPDATED.md
D:\git\ANALYTICS_GUIDE.md
D:\git\FINAL_FEATURES.md
D:\git\NYS_Voter_Analytics_Complete_Guide.md (this file)
```

## CSV Export Location

**Output Directory:**
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

**Files Created:**
- audience_summary_[filter].csv
- ethnicity_ALL_MATCHED_[filter].csv
- ethnicity_NO_AUDIENCE_MATCH_[filter].csv
- ethnicity_MATCHED_VS_UNMATCHED_[filter].csv
- ethnicity_[audience]_[filter].csv (top 10)

## Database Information

**MySQL Configuration:**
- Server: localhost (default)
- Database: NYS_VOTER_TAGGING
- Key Tables:
  - fullnyvoter_2025
  - fullvoter_audience_bridge
  - census_surname_ethnicity

## System Requirements

**Recommended Hardware:**
- 64GB RAM
- 16 cores / 24 threads
- 100GB+ free disk space

**MySQL Optimization:**
- Buffer pool: 40GB
- Max connections: 300
- Parallel threads: 16

## Version Information

**Toolkit Version:** 1.0
**Last Updated:** February 10, 2026

**Tools Included:**
- audience_analytics.py v1.0
- build_causeway_audience_tables.py v1.0
- query_helper.py v1.0

**Features:**
- Matched/Unmatched tracking
- Ethnicity analysis (all groups)
- District filtering
- CSV exports
- Performance optimization

## Getting Help

**In-tool help:**
```bash
python audience_analytics.py --help
python build_causeway_audience_tables.py --help
```

**Documentation:**
- Read INSTRUCTIONS.txt for complete guide
- See "Troubleshooting" section for common issues

**Quick questions:**
- "Which districts exist?" → `python query_helper.py`
- "What audiences do I have?" → `python audience_analytics.py --list-audiences`
- "How do I filter by district?" → Use `--ld "63"` (no leading zeros!)
- "How do I export?" → Add `--export-csv` to any command

---

# End of Complete Guide

**Thank you for using the NYS Voter Analytics Toolkit!**

For questions or issues, refer to the troubleshooting sections throughout this guide.
