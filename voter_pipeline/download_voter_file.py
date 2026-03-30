#!/usr/bin/env python3
"""
Download a NYS voter file ZIP from a URL directly to data/zipped/.

Emits \r-based progress compatible with the portal's download progress bar.

Usage:
  python download_voter_file.py --url URL [--filename NAME]
"""
import argparse, sys, time
from pathlib import Path

import requests

BASE     = Path(__file__).parent
DEST_DIR = BASE / "data" / "zipped"
DEST_DIR.mkdir(parents=True, exist_ok=True)

ap = argparse.ArgumentParser()
ap.add_argument("--url",      required=True, help="Direct download URL for the voter file ZIP")
ap.add_argument("--filename", default=None,  help="Override the saved filename")
args = ap.parse_args()

url      = args.url
filename = args.filename or url.split("?")[0].rstrip("/").split("/")[-1] or "voter_file.zip"
dest     = DEST_DIR / filename

print("=" * 65)
print("  NYS VOTER FILE DOWNLOAD")
print("=" * 65)
print(f"  URL:  {url[:80]}{'…' if len(url) > 80 else ''}")
print(f"  Dest: {dest}")
print()

try:
    resp = requests.get(url, stream=True, timeout=60,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))

    # Emit the marker the JS progress bar detects
    if total:
        print(f"  -> {filename}  ({total // 1_000_000} MB expected)")
    else:
        print(f"  -> {filename}  (size unknown)")

    t0         = time.time()
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            elapsed = time.time() - t0 or 0.001
            mb  = downloaded / 1_000_000
            spd = mb / elapsed
            print(f"\r    {mb:.1f} MB  ({spd:.1f} MB/s)    ", end="", flush=True)

    elapsed = time.time() - t0 or 0.001
    mb  = downloaded / 1_000_000
    spd = mb / elapsed
    print(f"\r    {mb:.1f} MB in {elapsed:.0f}s  ({spd:.1f} MB/s)    ")
    print(f"\n  Saved: {dest.name}  ({dest.stat().st_size // 1_000_000} MB)")
    print()
    print("  Next: click 'Load Voter File' to run the pipeline.")

except KeyboardInterrupt:
    print("\n  Cancelled.")
    if dest.exists() and dest.stat().st_size == 0:
        dest.unlink()
    sys.exit(1)
except Exception as e:
    print(f"\n  ERROR: {e}")
    sys.exit(1)

print("=" * 65)
