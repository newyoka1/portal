import requests
from pathlib import Path
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(__file__).parent / "data" / "fec_downloads"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 10-year window: 6 even-year cycles ending at current/next cycle
current_year = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1
CYCLES = [current_cycle - (i * 2) for i in range(6)]

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

print("="*70)
print("FEC BULK DATA DOWNLOAD")
print("="*70)

EXTRACT_DIR = DATA_DIR / "extracted"

def already_done(dest):
    """Skip if ZIP exists or was already extracted (and ZIP deleted to save space)."""
    if dest.exists():
        return True
    extracted = EXTRACT_DIR / dest.stem
    if extracted.exists() and any(extracted.iterdir()):
        return True
    return False

for cycle in CYCLES:
    year = str(cycle)[-2:]
    fname = f"indiv{year}.zip"
    url = f"{FEC_BASE_URL}/{cycle}/{fname}"
    dest = DATA_DIR / fname
    if already_done(dest):
        print(f"\n✓ Exists/extracted: {fname}")
        continue
    download_file(url, dest)

for cycle in CYCLES:
    yr = str(cycle)[-2:]
    for prefix in ["cm", "cn", "oth", "pas2"]:
        fname = f"{prefix}{yr}.zip"
        url   = f"{FEC_BASE_URL}/{cycle}/{fname}"
        dest  = DATA_DIR / fname
        if already_done(dest):
            print(f"\n  Exists/extracted: {fname}")
            continue
        download_file(url, dest)

print("\n" + "="*70)
print("DOWNLOAD COMPLETE")
print("="*70)
print("\nNext: python step2_extract_fec.py")
