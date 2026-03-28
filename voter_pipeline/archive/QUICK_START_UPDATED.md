# Quick Start Guide - Audience Analytics (UPDATED)

## 🎯 New Features Added!

✅ **Unmatched Voters Tracking** - Now shows voters who don't appear in ANY Causeway audience
✅ **Summary Statistics** - Shows matched vs unmatched percentages
✅ **Ethnicity Analysis** - Includes ethnicity breakdown for unmatched voters
✅ **CSV Exports** - Automatically includes unmatched voter data

---

## 📍 Important: District Names

District values are stored as **plain numbers** (without leading zeros):
- ✅ Use `--ld "63"` NOT `--ld "063"`
- ✅ Use `--sd "5"` NOT `--sd "SD 05"`
- ✅ Use `--cd "3"` NOT `--cd "CD 03"`

---

## 🚀 Common Commands

### 1. List All Audiences with Summary Statistics
```bash
python audience_analytics.py --ld "63" --list-audiences
```

**Output:**
```
Summary (LD 63):
  Total voters:               87,107
  Matched to audiences:       80,340 ( 92.2%)
  No audience match:          13,904 ( 16.0%)  <-- NEW!

All Audiences (LD 63):
  HT HARD DEM INDV NYS_2555.csv    13,114 voters
  HT HARD GOP INDV NYS_1335.csv     9,269 voters
  ...
  ** NO AUDIENCE MATCH **          13,904 voters  [!]  <-- NEW!
```

### 2. Analyze Unmatched Voters (NEW!)
```bash
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

**Output:**
```
Unmatched Voters (NO AUDIENCE) - LD 63:
  Total unmatched voters: 13,904 (16.0% of all voters)

  Ethnicity Breakdown (Unmatched):
    WHITE       5,946 voters ( 42.8%)
    UNKNOWN     3,199 voters ( 23.0%)
    ASIAN_PI    2,179 voters ( 15.7%)
    HISPANIC    2,105 voters ( 15.1%)
    BLACK         473 voters (  3.4%)
```

### 3. Get Ethnicity for Specific Audience
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

### 4. Compare Matched vs Unmatched Statewide
```bash
python audience_analytics.py --statewide --list-audiences
```

### 5. Export All Data Including Unmatched Voters
```bash
python audience_analytics.py --ld "63" --export-csv
```

**New CSV files created:**
- `audience_summary_LD_63.csv` - Includes unmatched count
- `ethnicity_NO_AUDIENCE_MATCH_LD_63.csv` - Ethnicity of unmatched voters

### 6. HT/MT/LT Turnout Split
```bash
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"
```

---

## 📊 Use Cases

### Use Case 1: "Who are the voters not in any audience?"
```bash
# Statewide
python audience_analytics.py --statewide --unmatched --ethnicity

# Specific district
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

### Use Case 2: "What % of voters are matched vs unmatched in each district?"
```bash
# LD 63
python audience_analytics.py --ld "63" --list-audiences

# SD 5
python audience_analytics.py --sd "5" --list-audiences

# CD 3
python audience_analytics.py --cd "3" --list-audiences
```

### Use Case 3: "Export everything for LD 63 including unmatched"
```bash
python audience_analytics.py --ld "63" --export-csv
```

Check the output folder:
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

Files created:
- `audience_summary_LD_63.csv` - All audiences + unmatched
- `ethnicity_HT_HARD_GOP_INDV_NYS_1335.csv_LD_63.csv` - Top audiences
- `ethnicity_NO_AUDIENCE_MATCH_LD_63.csv` - Unmatched voters ethnicity

### Use Case 4: "Compare matched vs unmatched by ethnicity"
```bash
# Matched voters (specific audience)
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity

# Unmatched voters
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

---

## 📁 File Locations

**Scripts:** `D:\git\`
- `audience_analytics.py` - Main analytics tool
- `build_causeway_audience_tables.py` - Build permanent tables
- `query_helper.py` - Discover available data
- `pipeline.py` - Main data pipeline

**CSV Exports:**
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

---

## 🔍 Understanding the Data

### Matched vs Unmatched Voters

**Matched:**
- Voters who appear in at least one Causeway audience file
- Have `origin` field populated in database
- Show up in audience-specific queries

**Unmatched:**
- Voters in fullnyvoter_2025 but NOT in any Causeway audience
- Have `origin` field NULL or empty
- May not have valid DOB or may not match Causeway criteria
- Still have ethnicity data (based on last name)

### Why Are Voters Unmatched?

Common reasons:
1. Missing or invalid Date of Birth (DOB)
2. Name/ZIP/DOB combo doesn't match any Causeway file
3. Multiple voters with same match key (ambiguous)
4. Voters not targeted by any Causeway audience

---

## 💡 Pro Tips

### See Statewide Summary
```bash
python audience_analytics.py --statewide --list-audiences
```

### Compare Multiple Districts
```bash
# Export each district separately
python audience_analytics.py --ld "63" --export-csv
python audience_analytics.py --ld "64" --export-csv
python audience_analytics.py --ld "65" --export-csv

# Then open the CSV files in Excel
```

### Quick Check of Your Data
```bash
# Run query helper to see what's available
python query_helper.py

# Then pick a district to analyze
python audience_analytics.py --ld "63" --list-audiences
```

---

## 📊 Example Workflow

```bash
# 1. Discover available districts
python query_helper.py

# 2. Get overview for LD 63
python audience_analytics.py --ld "63" --list-audiences

# 3. Analyze unmatched voters
python audience_analytics.py --ld "63" --unmatched --ethnicity

# 4. Get specific audience details
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity

# 5. Compare turnout levels
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"

# 6. Export everything
python audience_analytics.py --ld "63" --export-csv
```

---

## 🆘 Help

```bash
python audience_analytics.py --help
```

For more details, see:
- `D:\git\ANALYTICS_GUIDE.md` - Full documentation
- `D:\git\QUICK_START.md` - Original quick start
