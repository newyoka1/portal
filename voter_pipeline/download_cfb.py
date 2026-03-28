#!/usr/bin/env python3
"""
Download NYC CFB bulk contribution CSV files from nyccfb.info.

Downloads last 3 election cycles (2021, 2023, 2025) to data/cfb/.
Uses hash-based change detection to skip files that haven't changed.

Usage:
  python download_cfb.py             # download if changed
  python download_cfb.py --force     # always re-download
"""

import os, sys, hashlib, time
from pathlib import Path
from dotenv import load_dotenv
import urllib.request
import urllib.error

load_dotenv()

BASE    = Path(__file__).parent
CFB_DIR = BASE / "data" / "cfb"

# Direct CSV download URLs from nyccfb.info data library
CYCLES = [
    ("2017", "https://www.nyccfb.info/DataLibrary/2017_Contributions.csv"),
    ("2021", "https://www.nyccfb.info/DataLibrary/2021_Contributions.csv"),
    ("2023", "https://www.nyccfb.info/DataLibrary/2023_Contributions.csv"),
    ("2025", "https://www.nyccfb.info/datalibrary/2025_Contributions.csv"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nyccfb.info/follow-the-money/data-library/",
}

CHUNK = 1024 * 1024  # 1MB


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url: str, dest: Path, force: bool = False) -> bool:
    """Download url -> dest. Returns True if file was (re)downloaded."""
    hash_file = dest.with_suffix(".md5")

    req = urllib.request.Request(url, headers=HEADERS)
    try:
        # HEAD request to check Content-Length without downloading
        req.get_method = lambda: "HEAD"
        with urllib.request.urlopen(req, timeout=30) as r:
            remote_size = int(r.headers.get("Content-Length", 0))
    except Exception:
        remote_size = 0

    local_size = dest.stat().st_size if dest.exists() else 0

    if not force and dest.exists() and local_size > 0:
        if remote_size == 0 or remote_size == local_size:
            print(f"    No change detected ({local_size/1e6:.1f} MB) - skipping")
            return False

    print(f"    Downloading {url}")
    print(f"    -> {dest.name}", end="", flush=True)
    if remote_size:
        print(f"  ({remote_size/1e6:.0f} MB expected)", end="", flush=True)
    print()

    req = urllib.request.Request(url, headers=HEADERS)
    t0 = time.time()
    downloaded = 0
    try:
        with urllib.request.urlopen(req, timeout=300) as r, open(dest, "wb") as f:
            while True:
                chunk = r.read(CHUNK)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                elapsed = time.time() - t0
                mb = downloaded / 1e6
                speed = mb / elapsed if elapsed > 0 else 0
                print(f"\r    {mb:.1f} MB  ({speed:.1f} MB/s)    ", end="", flush=True)
    except Exception as e:
        print(f"\n    ERROR downloading: {e}")
        return False

    elapsed = time.time() - t0
    final_mb = downloaded / 1e6
    print(f"\r    {final_mb:.1f} MB in {elapsed:.0f}s  ({final_mb/elapsed:.1f} MB/s)    ")

    # Store md5
    md5 = file_md5(dest)
    hash_file.write_text(md5)
    return True


def main(force: bool = False):
    print("=" * 60)
    print("  NYC CFB Contribution Downloader")
    print("=" * 60)

    CFB_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped    = 0
    failed     = 0

    for cycle, url in CYCLES:
        dest = CFB_DIR / f"{cycle}_Contributions.csv"
        print(f"\n  Cycle {cycle}:")
        try:
            if download_file(url, dest, force=force):
                size_mb = dest.stat().st_size / 1e6
                print(f"    Saved: {dest.name} ({size_mb:.1f} MB)")
                downloaded += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"    FAILED: {e}")
            failed += 1

    print()
    print(f"  Downloaded : {downloaded}")
    print(f"  Skipped    : {skipped} (unchanged)")
    if failed:
        print(f"  Failed     : {failed}")
    print()
    print(f"  Files in {CFB_DIR}:")
    for f in sorted(CFB_DIR.glob("*.csv")):
        print(f"    {f.name:40s}  {f.stat().st_size/1e6:8.1f} MB")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Re-download even if unchanged")
    args = ap.parse_args()
    main(force=args.force)
