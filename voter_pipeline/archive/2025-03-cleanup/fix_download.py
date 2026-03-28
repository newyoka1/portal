import requests
from pathlib import Path

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")

def download_file(url, dest):
    print(f"\n  Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
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
                        print(f"\r    {pct:.1f}% ({downloaded/1024/1024:.1f}/{total/1024/1024:.1f} MB)", end='', flush=True)
        print(f"\n  ✓ Saved: {dest.name}")
        return True
    except Exception as e:
        print(f"\n  ✗ Error: {e}")
        return False

# Try downloading committee master from different years
print("="*70)
print("DOWNLOADING COMMITTEE/CANDIDATE MASTER FILES")
print("="*70)

for fname in ["cm.zip", "cn.zip"]:
    dest = DATA_DIR / fname
    
    if dest.exists():
        print(f"\n✓ Already exists: {fname}")
        continue
    
    # Try multiple years
    for year in [2026, 2025, 2024, 2023, 2022]:
        url = f"{FEC_BASE_URL}/{year}/{fname}"
        print(f"\nTrying {year}...")
        if download_file(url, dest):
            break
    else:
        print(f"\n⚠ Could not download {fname} from any year")
        print(f"  You may need to download manually from:")
        print(f"  https://www.fec.gov/data/browse-data/?tab=bulk-data")

print("\n" + "="*70)
print("DOWNLOAD ATTEMPT COMPLETE")
print("="*70)

# Check what we have
files = list(DATA_DIR.glob("*.zip"))
print(f"\nFiles in directory: {len(files)}")
for f in sorted(files):
    print(f"  ✓ {f.name}: {f.stat().st_size/1024/1024:.1f} MB")

if len(files) >= 3:
    print("\n✓ Ready to proceed")
    print("  Next: python step2_extract_fec.py")
else:
    print("\n⚠ Missing some files")
    print("  Check if indiv files downloaded successfully")
