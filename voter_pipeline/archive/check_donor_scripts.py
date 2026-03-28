import os, sys

base = r"D:\git\nys-voter-pipeline"
dirs = ["donors", "voter", "pipeline", "export", "."]
lines = []

for d in dirs:
    folder = os.path.join(base, d)
    if not os.path.isdir(folder):
        continue
    for f in os.listdir(folder):
        if not f.endswith(".py"):
            continue
        path = os.path.join(folder, f)
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()
        hits = []
        for ref in ["donors_2024", "politik1", "county_matching", "housefilenm", "DB_NAME", "database"]:
            if ref.lower() in content.lower():
                hits.append(ref)
        if hits:
            lines.append(f"\n{d}/{f}: {hits}")
            # Show actual db references
            for line in content.splitlines():
                if any(r.lower() in line.lower() for r in ["donors_2024","politik1","county_matching","db_name =","database ="]):
                    lines.append(f"    {line.strip()}")

with open(r"D:\git\nys-voter-pipeline\logs\donor_scripts.log", "w") as f:
    f.write("\n".join(lines))
print("done")