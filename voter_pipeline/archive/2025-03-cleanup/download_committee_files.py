import requests
from pathlib import Path
import time
from datetime import datetime

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"
DATA_DIR = Path(r"D:\git\nys-voter-pipeline\data\fec_downloads")

# Calculate cycles
current_year = datetime.now().year
current_cycle = current_year if current_year % 2 == 0 else current_year + 1
CYCLES = [current_cycle - (i * 2) for i in range(4)]

def download_file(url, dest, timeout=600):
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        downloaded = 0
        start = time.time()
        
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(65536):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = (downloaded / total) * 100
                        mb = downloaded / 1024 / 1024
                        total_mb = total / 1024 / 1024
                        speed = mb / (time.time() - start) if time.time() > start else 0
                        print(f"\r  {pct:.1f}% - {mb:.0f}/{total_mb:.0f} MB @ {speed:.1f} MB/s", end='', flush=True)
        
        print(f"\n✓ Saved: {dest.name}")
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

print("="*70)
print("DOWNLOAD COMMITTEE/CANDIDATE/TRANSFER FILES")
print(f"Cycles: {', '.join(map(str, CYCLES))}")
print("="*70)

files_to_download = []

for cycle in CYCLES:
    yr = str(cycle)[-2:]
    files_to_download.append((cycle, f"cm{yr}.zip", "Committee master"))
    files_to_download.append((cycle, f"cn{yr}.zip", "Candidate master"))
    files_to_download.append((cycle, f"oth{yr}.zip", "Committee transfers"))

print(f"\nTotal files to download: {len(files_to_download)}")

success = 0
failed = []

for cycle, filename, description in files_to_download:
    dest = DATA_DIR / filename
    
    if dest.exists():
        print(f"\n✓ Already exists: {filename}")
        continue
    
    print(f"\n--- {description} ({cycle}) ---")
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    
    if download_file(url, dest):
        success += 1
    else:
        failed.append(filename)

print("\n" + "="*70)
print("DOWNLOAD SUMMARY")
print("="*70)

all_files = sorted(DATA_DIR.glob("*.zip"))
print(f"\nAll files in directory: {len(all_files)}")

# Group by type
cm_files = sorted(DATA_DIR.glob("cm*.zip"))
cn_files = sorted(DATA_DIR.glob("cn*.zip"))
oth_files = sorted(DATA_DIR.glob("oth*.zip"))
indiv_files = sorted(DATA_DIR.glob("indiv*.zip"))

print(f"\nCommittee master: {len(cm_files)}")
for f in cm_files:
    print(f"  ✓ {f.name}")

print(f"\nCandidate master: {len(cn_files)}")
for f in cn_files:
    print(f"  ✓ {f.name}")

print(f"\nCommittee transfers: {len(oth_files)}")
for f in oth_files:
    print(f"  ✓ {f.name}")

print(f"\nIndividual contributions: {len(indiv_files)}")
for f in indiv_files:
    print(f"  ✓ {f.name}")

if len(cm_files) >= 1 and len(cn_files) >= 1 and len(oth_files) >= 1:
    print("\n✓ SUCCESS! Have all file types needed")
    print("\nNext: python step2_extract_fec.py")
else:
    print(f"\n⚠ Missing some file types")
    if failed:
        print(f"  Failed: {', '.join(failed)}")

print("="*70)
