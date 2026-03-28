path = r"D:\git\nys-voter-pipeline\voter\ethnicity.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

result_file = r"D:\git\nys-voter-pipeline\logs\fix_result.log"
lines = []

lines.append(f"c.name present: {'c.name' in content}")
lines.append(f"pct_hispanic present: {'pct_hispanic' in content}")
lines.append(f"normalized_surname present: {'normalized_surname' in content}")
lines.append(f"dominant_ethnicity present: {'dominant_ethnicity' in content}")

# Find census block
idx = content.find("[Step 5]")
if idx >= 0:
    lines.append("\nStep 5 block:")
    lines.append(repr(content[idx:idx+600]))

with open(result_file, "w") as f:
    f.write("\n".join(lines))