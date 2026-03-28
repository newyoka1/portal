import requests
from pathlib import Path
import time
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Calculate last 4 cycles
current_year = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1
CYCLES = [current_cycle - (i * 2) for i in range(4)]

def download_file(url, dest, timeout=600):
    print(f"\nDownloading: {url}")
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
        
        file_size_mb = total / 1024 / 1024
        elapsed_time = time.time() - start
        avg_speed = file_size_mb / elapsed_time if elapsed_time > 0 else 0
        print(f"\n✓ Downloaded: {dest.name} ({file_size_mb:.1f} MB in {elapsed_time/60:.1f}m @ {avg_speed:.1f} MB/s)")
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        if dest.exists():
            dest.unlink()
        return False

print("="*70)
print("FRESH DOWNLOAD - ALL FEC DATA")
print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print(f"Cycles: {', '.join(map(str, CYCLES))}")
print("Timeout: 10 minutes per file")
print("="*70)

# DELETE ALL existing indiv files
print("\n1. Clearing existing downloads...")
deleted = 0
for old_file in DATA_DIR.glob("indiv*.zip"):
    print(f"   Deleting: {old_file.name}")
    old_file.unlink()
    deleted += 1

if deleted == 0:
    print("   (No existing files to delete)")
else:
    print(f"   ✓ Deleted {deleted} file(s)")

print("\n2. Starting fresh downloads...")
print("   (Large files may take 5-10 minutes each)")

success_count = 0
failed = []

for i, cycle in enumerate(CYCLES, 1):
    year_suffix = str(cycle)[-2:]
    filename = f"indiv{year_suffix}.zip"
    year_range = f"{cycle-1}-{cycle}"
    
    print(f"\n{'='*70}")
    print(f"FILE {i}/4: Cycle {cycle} ({year_range})")
    print(f"{'='*70}")
    
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    dest = DATA_DIR / filename
    
    # Try up to 2 times
    for attempt in range(2):
        if attempt > 0:
            print(f"\nRetry attempt {attempt + 1}/2...")
            time.sleep(3)
        
        if download_file(url, dest, timeout=600):
            success_count += 1
            break
    else:
        failed.append(filename)
        print(f"✗ Failed to download {filename} after 2 attempts")

print("\n" + "="*70)
print("DOWNLOAD COMPLETE")
print("="*70)

files = sorted(DATA_DIR.glob("indiv*.zip"))
total_size = sum(f.stat().st_size for f in files)

print(f"\nSuccessful: {success_count}/4 files")
if failed:
    print(f"Failed: {', '.join(failed)}")

print(f"Total downloaded: {total_size/1024/1024/1024:.2f} GB")
print(f"\nFiles:")

for f in files:
    print(f"  ✓ {f.name}: {f.stat().st_size/1024/1024:.0f} MB")

if success_count >= 3:
    print("\n✓ SUCCESS! Enough data to proceed")
    print("\nNext step:")
    print("  python step2_extract_fec.py")
elif success_count > 0:
    print(f"\n⚠ Only {success_count}/4 files downloaded")
    print("  You can proceed with partial data or retry failed files")
else:
    print("\n✗ No files downloaded successfully")
    print("  Check internet connection and try again")

print("="*70)
