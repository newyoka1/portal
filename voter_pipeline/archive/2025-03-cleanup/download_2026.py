import requests
from pathlib import Path

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")

def download_file(url, dest):
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        downloaded = 0
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = (downloaded / total) * 100
                        print(f"\r  Progress: {pct:.1f}% ({downloaded/1024/1024:.1f}/{total/1024/1024:.1f} MB)", end='', flush=True)
        print(f"\n✓ Downloaded: {dest.name}")
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

print("="*70)
print("DOWNLOADING 2026 CYCLE DATA (2025-2026)")
print("="*70)

# Download indiv26.zip
dest = DATA_DIR / "indiv26.zip"

if dest.exists():
    print(f"\n✓ Already exists: {dest.name}")
else:
    url = f"{FEC_BASE_URL}/2026/indiv26.zip"
    download_file(url, dest)

# Show all files
print("\n" + "="*70)
print("CURRENT FILES")
print("="*70)
files = sorted(DATA_DIR.glob("*.zip"))
for f in files:
    year_range = {
        'indiv20.zip': '2019-2020 (DELETE - not needed)',
        'indiv22.zip': '2021-2022 ✓',
        'indiv24.zip': '2023-2024 ✓',
        'indiv26.zip': '2025-2026 ✓'
    }
    label = year_range.get(f.name, '')
    print(f"  {f.name}: {f.stat().st_size/1024/1024:.1f} MB  {label}")

print("\n" + "="*70)
print("ACTION REQUIRED:")
print("="*70)
print("1. Delete indiv20.zip (not needed for 2021-2026 range)")
print("2. Run: python step2_extract_fec.py")
print("="*70)
