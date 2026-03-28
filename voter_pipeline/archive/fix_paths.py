import os

base = r"D:\git\nys-voter-pipeline"
fixes = {
    r"pipeline\pipeline.py": [
        (
            'BASE_DIR = Path(r"C:\\Users\\georg_2r965zq\\OneDrive\\Desktop\\AUDIANCE DATABASE")\nDATA_DIR = BASE_DIR / "data"\nZIPPED_DIR = BASE_DIR / "ziped"\nFULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"\n\nLOG_DIR = BASE_DIR / "logs"\nLOG_DIR.mkdir(exist_ok=True)',
            'BASE_DIR       = Path(__file__).parent.parent  # D:\\git\\nys-voter-pipeline\nDATA_DIR       = BASE_DIR / "data"\nZIPPED_DIR     = DATA_DIR / "zipped"\nFULLVOTER_PATH = DATA_DIR / "full voter 2025" / "fullnyvoter.csv"\n\nLOG_DIR = BASE_DIR / "logs"\nLOG_DIR.mkdir(exist_ok=True)'
        ),
    ],
    r"pipeline\extract_zip_files.py": [
        (
            'zip_folder = r"C:\\Users\\georg_2r965zq\\OneDrive\\Desktop\\AUDIANCE DATABASE\\ziped"',
            'zip_folder = str(Path(__file__).parent.parent / "data" / "zipped")'
        ),
        (
            'data_folder = r"C:\\Users\\georg_2r965zq\\OneDrive\\Desktop\\AUDIANCE DATABASE\\data"',
            'data_folder = str(Path(__file__).parent.parent / "data")'
        ),
    ],
}

for rel, patches in fixes.items():
    path = os.path.join(base, rel)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    changed = 0
    for old, new in patches:
        if old in content:
            content = content.replace(old, new)
            changed += 1
        else:
            print(f"  WARNING: pattern not found in {rel}: {old[:60]!r}")

    # Make sure Path is imported in extract_zip_files.py
    if "extract_zip_files.py" in rel and "from pathlib import Path" not in content:
        content = content.replace("import os", "import os\nfrom pathlib import Path")
        changed += 1

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Patched {changed} change(s): {rel}")

print("\nVerifying...")
import subprocess, sys
for rel in fixes.keys():
    path = os.path.join(base, rel)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    remaining = content.count("georg_2r965zq") + content.count("AUDIANCE DATABASE") + content.count("OneDrive")
    status = "OK" if remaining == 0 else f"STILL HAS {remaining} hardcoded refs"
    print(f"  {rel}: {status}")