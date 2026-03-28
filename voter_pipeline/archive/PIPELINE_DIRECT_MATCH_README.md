# NYS VOTER TAGGING PIPELINE - StateVoterId Direct Matching
## UPDATED PIPELINE COMPLETE

File: D:\git\pipeline_direct_match.py

## KEY FEATURES
✅ **Auto-extracts zip files** - Automatically refreshes CSVs from ziped folder if changed
✅ Direct matching on StateVoterId (official NYS voter ID)
✅ Keeps ALL 87 columns from audience CSV files
✅ Much faster than old match key approach
✅ More accurate - no ambiguous matches
✅ Simpler code - 694 lines vs 1648 lines

## HOW IT WORKS

### Auto-Refresh from Zip Files (NEW!)
The pipeline automatically checks for updates:
1. Scans `ziped` folder for *.zip files
2. For each zip file:
   - Checks if corresponding CSV exists in `data` folder
   - Compares zip modification time vs CSV modification time
   - If zip is newer: Extracts and replaces CSV
   - If CSV missing: Extracts CSV
   - If CSV up-to-date: Skips extraction
3. Logs: "Extracted 3 files, skipped 14 (already up-to-date)"

**This means:** You can update audience data by simply replacing zip files in the `ziped` folder. 
The pipeline will automatically refresh the CSVs on next run!

### Old Pipeline (pipeline.py):
1. Load audience CSVs → extract ONLY 4 columns (FirstName, LastName, PrimaryZip, DOB)
2. Build match key: firstname|lastname|zip5|birthyear
3. Compute MD5 hash of match key
4. Match voters by hash WHERE unique (cnt=1)
5. Throw away 83 other columns from audience files

### New Pipeline (pipeline_direct_match.py):
1. Load full voter file → fullnyvoter_2025
2. For each audience CSV:
   - Create dynamic staging table with ALL CSV columns
   - Load entire CSV (all 87 columns preserved)
   - Simple JOIN: fullnyvoter_2025.StateVoterId = audience_staging.StateVoterId
   - Insert matches into bridge table
3. Update origin field with comma-separated audiences
4. Build district summary tables

## WORKFLOW DETAILS

STEP 0: Auto-Extract Updated Zip Files
- Check ziped folder for *.zip files
- Compare zip file timestamps with existing CSVs in data folder
- If zip is newer OR CSV doesn't exist:
  - Extract CSV from zip
  - Rename to match zip filename (e.g., "HT HARD GOP.zip" → "HT HARD GOP.csv")
  - Replace old CSV
- Log: "Extracted X files, skipped Y (already up-to-date)"
- This ensures CSVs are always fresh from latest zip files!

STEP 1: Database Setup
- Create database if not exists
- Connect to MySQL

STEP 2: Rebuild Tables
- Drop old tables
- Create stg_fullvoter_raw (staging)
- Create fullnyvoter_2025 (main table, partitioned by CDName)
- Create fullvoter_audience_bridge (many-to-many)

STEP 3: Load Full Voter
- Load all 13M+ voters into staging
- Keep all columns from fullnyvoter.csv

STEP 4: Copy to Main Table
- Transform data types (dates, zips, etc.)
- Copy all voters to partitioned main table

STEP 5: Load & Match Audience Files (ONE AT A TIME)
For each CSV in data folder:
  1. Read CSV header dynamically
  2. Verify StateVoterId column exists
  3. DROP TABLE audience_staging
  4. CREATE TABLE audience_staging with dynamic schema (ALL CSV columns)
  5. LOAD DATA from CSV into audience_staging
  6. INSERT INTO fullvoter_audience_bridge
     SELECT f.StateVoterId, f.SDName, f.LDName, f.CDName, 'audience.csv'
     FROM fullnyvoter_2025 f
     INNER JOIN audience_staging a ON a.StateVoterId = f.StateVoterId
  7. Log matches

STEP 6: Update Origin Field
- GROUP_CONCAT all matched audiences per voter
- Update fullnyvoter_2025.origin = comma-separated list

STEP 7: Build District Summaries
- fullvoter_sd_audience_counts (Senate District)
- fullvoter_ld_audience_counts (Assembly District)
- fullvoter_cd_audience_counts (Congressional District)
- fullvoter_state_audience_counts (Statewide)

## MATCHING LOGIC

OLD (Match Key Hash):
```sql
FROM fullnyvoter_2025 f
STRAIGHT_JOIN fullvoter_mk_counts mc 
    ON mc.match_key_hash = f.match_key_hash AND mc.cnt = 1
STRAIGHT_JOIN causeway_norm c 
    ON c.match_key_hash = f.match_key_hash
```
Problems: Slow, throws away data, ambiguous matches excluded

NEW (Direct StateVoterId):
```sql
FROM fullnyvoter_2025 f
INNER JOIN audience_staging a 
    ON a.StateVoterId = f.StateVoterId
```
Benefits: Fast primary key join, keeps all data, no ambiguity

## FILES PROCESSED

Source: C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\data\

Audience CSV Files (17 files):
- blue collar.csv (220 MB)
- border security crisis.csv (2.0 GB)
- children in home.csv (2.5 GB)
- environmental protection support.csv (115 MB)
- HT HARD GOP.csv (1.1 GB)
- HT LEAN DEM.csv (734 MB)
- HT LEAN GOP.csv (299 MB)
- HT SWING.csv (877 MB)
- LT HARD DEM.csv (254 MB)
- LT HARD GOP.csv (359 MB)
- LT LEAN DEM.csv (358 MB)
- LT LEAN GOP.csv (152 MB)
- LT SWING.csv (708 MB)
- MT HARD DEM.csv (533 MB)
- MT HARD GOP.csv (298 MB)
- MT LEAN GOP.csv (115 MB)

Full Voter: full voter 2025\fullnyvoter.csv (13M+ records)

## OUTPUT TABLES

1. fullnyvoter_2025
   - All 13M+ voters
   - All original columns + origin field
   - Partitioned by CDName (32 partitions)

2. fullvoter_audience_bridge
   - Many-to-many voter-audience relationships
   - (StateVoterId, audience) pairs

3. fullvoter_sd_audience_counts
4. fullvoter_ld_audience_counts
5. fullvoter_cd_audience_counts
6. fullvoter_state_audience_counts

## PERFORMANCE

Optimizations:
- 512MB bulk insert buffer
- Partitioned tables (32 partitions)
- Indexed on StateVoterId, CDName, LDName, SDName
- GROUP_CONCAT_MAX_LEN: 500KB for origin field
- Processes audience files one at a time (memory efficient)

Expected Runtime:
- Full voter load: ~5-10 minutes
- Each audience file: 30 seconds - 2 minutes
- Total: ~30-45 minutes for all 17 files

## COMMAND TO RUN

Windows PowerShell:
```powershell
cd D:\git
$env:MYSQL_PASSWORD="your_mysql_password_here"
python pipeline_direct_match.py
```

## VERIFICATION

After completion, check:
```sql
USE NYS_VOTER_TAGGING;

-- Total voters
SELECT COUNT(*) FROM fullnyvoter_2025;

-- Voters with audience matches
SELECT COUNT(*) FROM fullnyvoter_2025 WHERE origin IS NOT NULL;

-- Sample multi-audience voters
SELECT StateVoterId, FirstName, LastName, origin
FROM fullnyvoter_2025
WHERE origin LIKE '%,%'
LIMIT 10;

-- Audience counts
SELECT * FROM fullvoter_state_audience_counts ORDER BY voters DESC;
```
