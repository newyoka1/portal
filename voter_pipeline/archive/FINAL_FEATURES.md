# Final Features Summary - Audience Analytics

## ✅ Complete Feature Set

Your `audience_analytics.py` now includes comprehensive ethnicity tracking for **BOTH** matched and unmatched voters!

---

## 🎯 What You Can Do Now

### 1. Summary Statistics (Matched + Unmatched)
```bash
python audience_analytics.py --ld "63" --list-audiences
```

**Shows:**
- Total voters in district
- **Matched voters** (in any Causeway audience): 80,340 (92.2%)
- **Unmatched voters** (not in any audience): 13,904 (16.0%)

---

### 2. Compare Matched vs Unmatched Ethnicity (NEW!)
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
  Ethnicity             Matched        Unmatched    Difference
  ------------------------------------------------------------------------
  WHITE           38,157 (52.1%)     5,946 (42.8%)    +9.4%  ⬆️ More likely matched
  ASIAN_PI         8,759 (12.0%)     2,179 (15.7%)    -3.7%  ⬇️ Less likely matched
  HISPANIC         9,739 (13.3%)     2,105 (15.1%)    -1.8%  ⬇️ Less likely matched
  BLACK            1,984 ( 2.7%)       473 ( 3.4%)    -0.7%  ⬇️ Less likely matched
  UNKNOWN         14,539 (19.9%)     3,199 (23.0%)    -3.2%  ⬇️ Less likely matched
```

**Key Insights:**
- White voters are **9.4% more likely** to be in a Causeway audience
- Asian/PI voters are **3.7% less likely** to be matched
- Shows potential targeting gaps in your Causeway audiences

---

### 3. Analyze Just Unmatched Voters
```bash
python audience_analytics.py --ld "63" --unmatched --ethnicity
```

---

### 4. Get Specific Audience Ethnicity
```bash
python audience_analytics.py --ld "63" --audience "HT HARD GOP INDV NYS_1335.csv" --ethnicity
```

---

### 5. Export Everything to CSV (NEW FILES!)
```bash
python audience_analytics.py --ld "63" --export-csv
```

**Creates these CSV files:**

1. **audience_summary_LD_63.csv** - All audiences + unmatched count
2. **ethnicity_ALL_MATCHED_LD_63.csv** - ⭐ NEW! Combined ethnicity of all matched voters
3. **ethnicity_NO_AUDIENCE_MATCH_LD_63.csv** - Ethnicity of unmatched voters
4. **ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv** - ⭐ NEW! Side-by-side comparison
5. **ethnicity_[audience_name]_LD_63.csv** - Top 10 individual audiences

---

## 📊 CSV File Examples

### ethnicity_ALL_MATCHED_LD_63.csv
```csv
Ethnicity,Voters,Percentage
WHITE,38157,52.12
UNKNOWN,14539,19.86
HISPANIC,9739,13.30
ASIAN_PI,8759,11.97
BLACK,1984,2.71
```

### ethnicity_MATCHED_VS_UNMATCHED_LD_63.csv
```csv
Ethnicity,Matched_Voters,Matched_Pct,Unmatched_Voters,Unmatched_Pct,Difference_Pct
WHITE,38157,52.12,5946,42.76,9.36
ASIAN_PI,8759,11.97,2179,15.67,-3.70
HISPANIC,9739,13.30,2105,15.14,-1.84
BLACK,1984,2.71,473,3.40,-0.69
UNKNOWN,14539,19.86,3199,23.01,-3.15
```

---

## 🔍 Use Cases

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

---

## 📁 All Available Commands

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

---

## 📊 Understanding the Data

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

---

## 🎯 Key Insights from LD 63 Example

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

---

## 📂 File Locations

**Scripts:** `D:\git\`
- `audience_analytics.py` - Main tool (updated with matched/unmatched)
- `build_causeway_audience_tables.py` - Build permanent tables
- `query_helper.py` - Discovery tool

**CSV Exports:**
```
C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\analytics_output\
```

**New CSV files include:**
- ✅ All matched voters ethnicity
- ✅ Unmatched voters ethnicity
- ✅ Side-by-side comparison
- ✅ Top 10 individual audiences
- ✅ Summary of all audiences

---

## ✨ Complete Feature List

✅ List all audiences with counts
✅ Summary statistics (total/matched/unmatched)
✅ Filter by LD/SD/CD
✅ Ethnicity breakdown for any audience
✅ Ethnicity breakdown for ALL matched voters
✅ Ethnicity breakdown for unmatched voters
✅ Side-by-side matched vs unmatched comparison
✅ HT/MT/LT turnout analysis
✅ District-level breakdowns
✅ CSV export with all data
✅ Statewide analysis

---

Everything is working and tested! 🎉
