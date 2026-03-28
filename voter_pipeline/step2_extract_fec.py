import zipfile
from pathlib import Path

DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")
EXTRACT_DIR = DATA_DIR / "extracted"

print("="*70)
print("EXTRACTING FEC FILES")
print("="*70)

EXTRACT_DIR.mkdir(exist_ok=True)
zip_files = list(DATA_DIR.glob("*.zip"))

if not zip_files:
    print("\n❌ No zip files found")
    print("   Run: python step1_download_fec.py")
    exit(1)

print(f"\nExtracting {len(zip_files)} files...")

for zf in zip_files:
    print(f"\n{zf.name}")
    sub_dir = EXTRACT_DIR / zf.stem
    sub_dir.mkdir(exist_ok=True)
    try:
        with zipfile.ZipFile(zf, 'r') as z:
            z.extractall(sub_dir)
            for txt in sub_dir.glob("*.txt"):
                print(f"  ✓ {txt.name}: {txt.stat().st_size/1024/1024:.1f} MB")
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\n" + "="*70)
print("EXTRACTION COMPLETE")
print("="*70)
print("\nNext: python step3_load_fec.py")
