# Quick Start Guide - Audience Analytics

## 🎯 What You Can Do Now

Your pipeline now supports:
- ✅ Filter by LD, SD, or CD
- ✅ Ethnicity breakdowns for any audience
- ✅ HT/MT/LT turnout analysis
- ✅ Export to CSV

---

## 📍 Important: District Names

District values in the database are stored as **plain numbers** (without leading zeros or prefixes):
- ✅ Use `--ld "63"` NOT `--ld "063"` or `--ld "LD 063"`
- ✅ Use `--sd "5"` NOT `--sd "SD 05"`
- ✅ Use `--cd "3"` NOT `--cd "CD 03"`

---

## 🚀 Common Commands

### 1. Discover What Districts Exist
```bash
cd D:\git
python query_helper.py
```

### 2. List All Audiences in LD 63
```bash
python audience_analytics.py --ld "63" --list-audiences
```

### 3. Get Ethnicity Breakdown for Specific Audience in LD 63
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

### 4. Compare HT/MT/LT Turnout Levels in LD 63
```bash
python audience_analytics.py --ld "63" --turnout-split "HARD GOP"
python audience_analytics.py --ld "63" --turnout-split "HARD DEM"
python audience_analytics.py --ld "63" --turnout-split "SWING"
```

### 5. Export All Data for LD 63 to CSV
```bash
python audience_analytics.py --ld "63" --export-csv
```
Output goes to: `C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\`

### 6. Get District Breakdown for Specific Audience
```bash
python audience_analytics.py --audience "HT HARD GOP INDV NYS_1335.csv" --district-breakdown LD
```

---

## 📊 Real Example Output

### Example 1: List audiences in LD 63
```bash
$ python audience_analytics.py --ld "63" --list-audiences

All Audiences (LD 63):
  HT HARD DEM INDV NYS_2555.csv        13,114 voters
  HT HARD GOP INDV NYS_1335.csv         9,269 voters
  HT SWING INDV NYS_0229.csv            6,981 voters
  MT HARD DEM INDV NYS_2831.csv         4,804 voters
  MT HARD GOP INDV NYS_3145.csv         3,358 voters
  ...
```

### Example 2: Ethnicity breakdown
```bash
$ python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity

Ethnicity Breakdown:
  WHITE       6,132 voters ( 66.2%)
  UNKNOWN     1,860 voters ( 20.1%)
  HISPANIC      715 voters (  7.7%)
  ASIAN_PI      505 voters (  5.4%)
  BLACK          57 voters (  0.6%)
```

### Example 3: Turnout split
```bash
$ python audience_analytics.py --ld "63" --turnout-split "HARD GOP"

Turnout Split for 'HARD GOP' - LD 63:
  HT    9,269 voters
  MT    3,358 voters
  LT    3,003 voters
```

---

## 🔧 Build Materialized Tables (Optional - for Speed)

If you query the same audiences repeatedly, build permanent tables:

### Build All Tables (WARNING: Creates 1000+ tables!)
```bash
python build_causeway_audience_tables.py
```

### Build Specific Pattern Only
```bash
python build_causeway_audience_tables.py --pattern "HARD GOP"
python build_causeway_audience_tables.py --pattern "HT"
```

### Test First with Limit
```bash
python build_causeway_audience_tables.py --limit 5
```

---

## 📂 File Locations

All scripts are in: `D:\git\`

- **audience_analytics.py** - Interactive queries (use this most)
- **build_causeway_audience_tables.py** - Build permanent tables
- **query_helper.py** - Discover available data
- **pipeline.py** - Your main pipeline (run first)

CSV exports go to:
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

---

## ⚡ Pro Tips

### Statewide Analysis
```bash
python audience_analytics.py --statewide --list-audiences
```

### Multiple Districts
Run separate commands for each district:
```bash
python audience_analytics.py --ld "63" --export-csv
python audience_analytics.py --ld "64" --export-csv
python audience_analytics.py --ld "65" --export-csv
```

### SQL Queries (After Building Tables)
```sql
-- All HT HARD GOP voters in LD 63
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv WHERE LDName = '63';

-- Ethnicity breakdown
SELECT * FROM HT_HARD_GOP_INDV_NYS_1335_csv_ethnicity_by_ld WHERE LDName = '63';
```

---

## 🆘 Troubleshooting

**"No audiences found"**
- Check district number format (use "63" not "063")
- Verify district exists: `python query_helper.py`

**"Audience not found"**
- List available audiences: `python audience_analytics.py --list-audiences`
- Audience names are case-sensitive

**Slow queries**
- Build materialized tables: `python build_causeway_audience_tables.py`
- Then query MySQL directly instead of using Python tools

---

## 📞 Need Help?

Run any command with `--help`:
```bash
python audience_analytics.py --help
python build_causeway_audience_tables.py --help
```

Or check the full guide:
```
D:\git\ANALYTICS_GUIDE.md
```
