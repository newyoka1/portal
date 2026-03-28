path = r"D:\git\nys-voter-pipeline\voter\ethnicity.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Fix census step - replace the 4-loop approach with single dominant_ethnicity join
old_census_block = """    print("\\n[Step 5] Checking for ref_census_surnames table...")
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ref_census_surnames'")
    census_available = cur.fetchone()[0] > 0
    if census_available:
        print("  Census table found - applying for unmatched voters...")
        if not args.dry_run:
            for eth, col in [("Hispanic","pct_hispanic"),("Black","pct_black"),("Asian","pct_api"),("White","pct_white")]:
                run_update_batched(conn, cur, f"Census -> {eth}",
                    f"UPDATE {VOTER_TABLE} v JOIN ref_census_surnames c ON UPPER(v.LastName) = UPPER(c.name) SET v.ModeledEthnicity = %s WHERE v.ModeledEthnicity IS NULL AND c.{col} >= 50",
                    params=[eth], batch_size=batch_size)
    else:
        print("  ref_census_surnames not found - skipping.")"""

new_census_block = """    print("\\n[Step 5] Checking for ref_census_surnames table...")
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ref_census_surnames'")
    census_available = cur.fetchone()[0] > 0
    if census_available:
        print("  Census table found - applying dominant_ethnicity for unmatched voters...")
        if not args.dry_run:
            # Use dominant_ethnicity column directly; join on 'surname' (not 'name')
            run_update_batched(conn, cur, "Census dominant_ethnicity",
                f"UPDATE {VOTER_TABLE} v "
                f"JOIN ref_census_surnames c ON UPPER(v.LastName) = c.normalized_surname "
                f"SET v.ModeledEthnicity = c.dominant_ethnicity "
                f"WHERE v.ModeledEthnicity IS NULL",
                batch_size=batch_size)
    else:
        print("  ref_census_surnames not found - skipping.")"""

if old_census_block in content:
    content = content.replace(old_census_block, new_census_block)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Fixed census block")
else:
    print("ERROR: old block not found - searching for fragments...")
    if "c.name" in content:
        print("  Found c.name - needs manual fix")
    if "pct_hispanic" in content:
        print("  Found pct_hispanic - needs manual fix")
    # Show surrounding context
    idx = content.find("ref_census_surnames c ON")
    if idx >= 0:
        print("Context:", repr(content[idx-20:idx+120]))