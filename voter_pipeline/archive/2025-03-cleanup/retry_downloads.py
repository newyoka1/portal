import requests
from pathlib import Path
import time

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")

def download_file(url, dest, timeout=300):
    print(f"\nDownloading: {url}")
    print(f"Timeout: {timeout}s (5 minutes)")
    try:
        response = requests.get(url, stream=True, timeout=timeout)
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
                        eta = (mb_total - mb_down) / speed if speed > 0 else 0
                        print(f"\r  {pct:.1f}% - {mb_down:.1f}/{mb_total:.1f} MB ({speed:.1f} MB/s) ETA: {eta/60:.1f}min", end='', flush=True)
        
        print(f"\n✓ Saved: {dest.name} ({total/1024/1024:.1f} MB)")
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

print("="*70)
print("RETRY FAILED DOWNLOADS")
print("="*70)

# Check what's missing
cycles_needed = {
    2024: "indiv24.zip",
    2022: "indiv22.zip",
    2020: "indiv20.zip"
}

missing = []
for cycle, filename in cycles_needed.items():
    dest = DATA_DIR / filename
    if not dest.exists():
        missing.append((cycle, filename))

if not missing:
    print("\n✓ All files present!")
    exit(0)

print(f"\nMissing files: {len(missing)}")
for cycle, fname in missing:
    print(f"  - {fname}")

print("\nRetrying with 5 minute timeout per file...")

for cycle, filename in missing:
    print(f"\n--- Cycle {cycle} ---")
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    dest = DATA_DIR / filename
    
    # Try up to 3 times
    for attempt in range(3):
        if attempt > 0:
            print(f"\nAttempt {attempt + 1}/3...")
        
        if download_file(url, dest, timeout=300):
            break
        else:
            if attempt < 2:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print(f"Failed after 3 attempts")

print("\n" + "="*70)
print("DOWNLOAD STATUS")
print("="*70)

files = sorted(DATA_DIR.glob("indiv*.zip"))
total_size = sum(f.stat().st_size for f in files)

print(f"\nFiles downloaded: {len(files)}/4")
print(f"Total size: {total_size/1024/1024/1024:.2f} GB")

for f in files:
    print(f"  ✓ {f.name}: {f.stat().st_size/1024/1024:.1f} MB")

if len(files) >= 3:
    print("\n✓ Enough files to proceed!")
    print("\nNext: python step2_extract_fec.py")
else:
    print(f"\n⚠ Need at least 3 files to proceed")

print("="*70)
