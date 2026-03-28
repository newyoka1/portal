import requests
from pathlib import Path
import time
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Calculate last 4 cycles from current date
current_year = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1
CYCLES = [current_cycle - (i * 2) for i in range(4)]  # Last 4 cycles

def download_file(url, dest):
    print(f"\nDownloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        downloaded = 0
        start = time.time()
        
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = (downloaded / total) * 100
                        mb_down = downloaded / 1024 / 1024
                        mb_total = total / 1024 / 1024
                        elapsed = time.time() - start
                        speed = mb_down / elapsed if elapsed > 0 else 0
                        print(f"\r  {pct:.1f}% - {mb_down:.1f}/{mb_total:.1f} MB ({speed:.1f} MB/s)", end='', flush=True)
        
        print(f"\n✓ Saved: {dest.name} ({total/1024/1024:.1f} MB)")
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

print("="*70)
print("DOWNLOADING FEC DATA - LAST 4 CYCLES")
print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
print(f"Cycles to download: {', '.join(map(str, CYCLES))}")
print("="*70)

# Clear existing downloads
print("\nClearing old downloads...")
for old_file in DATA_DIR.glob("indiv*.zip"):
    print(f"  Deleting: {old_file.name}")
    old_file.unlink()

print("\n" + "="*70)
print("DOWNLOADING FILES")
print("="*70)

success_count = 0

for cycle in CYCLES:
    year_suffix = str(cycle)[-2:]
    filename = f"indiv{year_suffix}.zip"
    year_range = f"{cycle-1}-{cycle}"
    
    print(f"\n--- Cycle {cycle} ({year_range}) ---")
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    dest = DATA_DIR / filename
    
    if download_file(url, dest):
        success_count += 1
    else:
        print(f"Failed to download {filename}")

print("\n" + "="*70)
print("DOWNLOAD SUMMARY")
print("="*70)

files = sorted(DATA_DIR.glob("indiv*.zip"))
total_size = sum(f.stat().st_size for f in files)

print(f"\nDownloaded: {success_count}/4 files")
print(f"Total size: {total_size/1024/1024/1024:.2f} GB")
print(f"\nFiles:")

for f in files:
    print(f"  ✓ {f.name}: {f.stat().st_size/1024/1024:.1f} MB")

if success_count == 4:
    print("\n✓ All files downloaded successfully!")
    print("\nNext step: python step2_extract_fec.py")
else:
    print(f"\n⚠ Only {success_count}/4 files downloaded")
    print("  Some cycles may not be available yet")

print("="*70)
