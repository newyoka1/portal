# SPACE RENAME EXECUTION PLAN
## NYS Voter Pipeline - Database Cleanup

**Generated:** 2026-03-03
**Status:** READY FOR REVIEW

---

## SUMMARY

**Total Issues Found:** 202 items across 4 databases
- **Tables with spaces:** 4 tables
- **Columns with spaces:** 198 columns

**CODE SEARCH RESULT:** ✓ NO PYTHON REFERENCES FOUND
This means your Python code doesn't reference these tables/columns with spaces, so renaming is SAFE.

---

## AFFECTED DATABASES

### ⭐ KEY DATABASES (Used by nys-voter-pipeline)

#### politik1_fec
- **2 tables** with spaces:
  - `committees all` → `committees_all`
  - `county codes` → `county_codes`
- **0 columns** with spaces

#### politik1_nydata  
- **2 tables** with spaces:
  - `county codes` → `county_codes`
  - `town + fips` → `town_+_fips`
- **130 columns** with spaces (mostly HubSpot contact fields)

### Secondary Databases (May not be actively used)

#### county_matching_new
- **0 tables** with spaces
- **39 columns** with spaces (HubSpot + county data)

#### housefilenm
- **0 tables** with spaces  
- **22 columns** with spaces (New Mexico housefile)

---

## EXECUTION STEPS

### Step 1: BACKUP (CRITICAL - DO THIS FIRST!)

```powershell
# Create backup directory
New-Item -ItemType Directory -Path "D:\git\nys-voter-pipeline\backups" -Force

# Backup key databases
$date = Get-Date -Format "yyyy-MM-dd_HHmmss"
$backupPath = "D:\git\nys-voter-pipeline\backups\$date"
New-Item -ItemType Directory -Path $backupPath -Force

# Backup politik1_fec
& "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysqldump.exe" `
  -h 127.0.0.1 -u root -p"!#goAmerica99" `
  --single-transaction --routines --triggers `
  politik1_fec > "$backupPath\politik1_fec.sql"

# Backup politik1_nydata
& "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysqldump.exe" `
  -h 127.0.0.1 -u root -p"!#goAmerica99" `
  --single-transaction --routines --triggers `
  politik1_nydata > "$backupPath\politik1_nydata.sql"
```

### Step 2: EXECUTE RENAME SQL

```powershell
cd D:\git\nys-voter-pipeline

# Run the complete rename script
& "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe" `
  -h 127.0.0.1 -u root -p"!#goAmerica99" `
  < rename_complete.sql
```

### Step 3: VERIFY CHANGES

```powershell
# Check that tables were renamed
C:\Python314\python.exe scan_spaces.py

# Should show: "TOTAL ISSUES FOUND: 0"
```

### Step 4: TEST DONOR ENRICHMENT

```powershell
# Test BOE donors
C:\Python314\python.exe main.py --operation boe_donors --verbose

# Test FEC donors  
C:\Python314\python.exe main.py --operation fec_donors --verbose
```

---

## FILES GENERATED

1. **space_issues.json** - Raw scan results (202 issues)
2. **rename_complete.sql** - Complete SQL script with proper column types
3. **SPACE_RENAME_PLAN.md** - This document

---

## RISK ASSESSMENT

**Risk Level:** LOW

**Why Low Risk:**
- ✓ No Python code references found to tables/columns with spaces
- ✓ SQL script includes proper column type definitions
- ✓ Backup strategy documented
- ✓ Only 4 tables need renaming (rest are columns)
- ✓ Tables are in databases used for donor enrichment, not core voter file

**Caution Areas:**
- politik1_nydata has 130 columns to rename - bulk operation
- town + fips table has special character (+) in name
- Some columns have slashes/dashes in renamed names (will keep these)

---

## ROLLBACK PLAN

If something goes wrong:

```powershell
# Restore from backup
$backupPath = "D:\git\nys-voter-pipeline\backups\[TIMESTAMP]"

& "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe" `
  -h 127.0.0.1 -u root -p"!#goAmerica99" `
  politik1_fec < "$backupPath\politik1_fec.sql"

& "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe" `
  -h 127.0.0.1 -u root -p"!#goAmerica99" `
  politik1_nydata < "$backupPath\politik1_nydata.sql"
```

---

## RECOMMENDATIONS

1. **Focus on KEY databases first** (politik1_fec, politik1_nydata)
2. **Skip secondary databases** unless you actively use them
3. **Run backups before executing SQL**
4. **Test donor enrichment** after changes
5. **Update Aiven sync** if needed (may need to re-sync schema changes)

---

## NEXT STEPS

**After you approve:**
1. I'll create a simplified script that only renames tables/columns in KEY databases
2. I'll add verbosity flags to main.py for donor operations
3. I'll update any collation fix scripts to use the new table names

**What do you want to do?**
- [ ] Execute full rename (all 4 databases)
- [ ] Execute partial rename (only KEY databases)
- [ ] Skip secondary databases (county_matching_new, housefilenm)
- [ ] Review SQL script first

---

## CONTACT

If you have questions or want to modify the plan, just ask!
