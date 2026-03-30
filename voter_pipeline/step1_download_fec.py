import requests, zipfile
from pathlib import Path
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR     = Path(__file__).parent / "data" / "fec_downloads"
EXTRACT_DIR  = DATA_DIR / "extracted"
DATA_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# 10-year window: 6 even-year cycles ending at current/next cycle
current_year  = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1
CYCLES = [current_cycle - (i * 2) for i in range(6)]


def already_extracted(stem):
    """True if extraction directory exists and is non-empty."""
    d = EXTRACT_DIR / stem
    return d.exists() and any(d.iterdir())


def download_file(url, dest):
    print(f"\n  Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        total      = int(response.headers.get('content-length', 0))
        downloaded = 0
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = (downloaded / total) * 100
                        print(f"\r    {pct:.1f}% ({downloaded/1024/1024:.1f}/{total/1024/1024:.1f} MB)",
                              end='', flush=True)
        print(f"\n  ✓ Saved: {dest.name}")
        return True
    except Exception as e:
        print(f"\n  ✗ Error: {e}")
        return False


def extract_and_delete(dest):
    """Extract ZIP into extracted/<stem>/, then delete the ZIP."""
    sub_dir = EXTRACT_DIR / dest.stem
    sub_dir.mkdir(parents=True, exist_ok=True)
    size_mb = dest.stat().st_size / 1024 / 1024
    print(f"  Extracting {dest.name}...")
    try:
        with zipfile.ZipFile(dest, 'r') as z:
            z.extractall(sub_dir)
        for txt in sub_dir.glob("*.txt"):
            print(f"    ✓ {txt.name}: {txt.stat().st_size/1024/1024:.1f} MB")
        dest.unlink()
        print(f"  🗑  Deleted {dest.name} ({size_mb:.0f} MB freed)")
    except Exception as e:
        print(f"  ✗ Extract error: {e}")


print("=" * 70)
print("FEC BULK DATA DOWNLOAD + EXTRACT")
print("=" * 70)

for cycle in CYCLES:
    year  = str(cycle)[-2:]
    fname = f"indiv{year}.zip"
    dest  = DATA_DIR / fname
    if already_extracted(dest.stem):
        print(f"\n✓ Already extracted: {fname}")
        continue
    if dest.exists() or download_file(url := f"{FEC_BASE_URL}/{cycle}/{fname}", dest):
        if dest.exists():
            extract_and_delete(dest)

for cycle in CYCLES:
    yr = str(cycle)[-2:]
    for prefix in ["cm", "cn", "oth", "pas2"]:
        fname = f"{prefix}{yr}.zip"
        dest  = DATA_DIR / fname
        if already_extracted(dest.stem):
            print(f"\n  Already extracted: {fname}")
            continue
        if dest.exists() or download_file(url := f"{FEC_BASE_URL}/{cycle}/{fname}", dest):
            if dest.exists():
                extract_and_delete(dest)

print("\n" + "=" * 70)
print("DOWNLOAD + EXTRACT COMPLETE")
print("=" * 70)
print("\nNext: python step3_load_fec.py")
