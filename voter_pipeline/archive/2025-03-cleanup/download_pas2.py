import requests
from pathlib import Path
import time
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")

def download_file(url, dest, timeout=600):
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        downloaded = 0
        start = time.time()
        
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = (downloaded / total) * 100
                        mb_down = downloaded / 1024 / 1024
                        mb_total = total / 1024 / 1024
                        elapsed = time.time() - start
                        speed = mb_down / elapsed if elapsed > 0 else 0
                        eta = (mb_total - mb_down) / speed if speed > 0 else 0
                        print(f"\r  {pct:.1f}% - {mb_down:.0f}/{mb_total:.0f} MB @ {speed:.1f} MB/s - ETA: {eta/60:.1f}m", end='', flush=True)
        
        print(f"\n✓ Downloaded: {dest.name} ({total/1024/1024:.1f} MB)")
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

print("="*70)
print("DOWNLOAD COMMITTEE TRANSFER DATA (pas2.zip)")
print("For giving pattern analysis")
print("="*70)

# Try different years
current_year = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1

dest = DATA_DIR / "pas2.zip"

if dest.exists():
    print(f"\n✓ Already exists: {dest.name} ({dest.stat().st_size/1024/1024:.1f} MB)")
    resp = input("Re-download? (y/N): ").strip().lower()
    if resp != 'y':
        print("Keeping existing file")
        exit(0)
    dest.unlink()

print(f"\nTrying to download from recent cycles...")

for year in [current_cycle, current_cycle - 2, current_cycle - 4]:
    print(f"\nTrying {year}...")
    url = f"{FEC_BASE_URL}/{year}/pas2.zip"
    
    if download_file(url, dest, timeout=600):
        print(f"\n✓ SUCCESS! Downloaded from {year} cycle")
        break
else:
    print("\n✗ Could not download from any cycle")
    print("  Manual download: https://www.fec.gov/data/browse-data/?tab=bulk-data")
    print("  Look for: 'Committee contributions to candidates'")

print("\n" + "="*70)

if dest.exists():
    print("DOWNLOAD COMPLETE")
    print("="*70)
    print(f"\nFile: {dest.name}")
    print(f"Size: {dest.stat().st_size/1024/1024:.1f} MB")
    print("\nThis file contains committee-to-candidate transfers")
    print("Used for analyzing which party committees support")
    print("\nNext: Re-run extraction to include this file")
    print("  python step2_extract_fec.py")
else:
    print("DOWNLOAD FAILED")
    print("="*70)
    print("\nYou can proceed without pas2.zip")
    print("Classification will use keywords only (still good)")

print("="*70)
