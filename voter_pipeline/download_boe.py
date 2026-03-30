#!/usr/bin/env python3
"""
download_boe.py - Automated BOE bulk campaign finance downloader.

Uses Playwright (real browser) to handle Cloudflare bot protection.
Downloads the 4 ALL_REPORTS zip files to data/boe_donors/.

Install once:
    pip install playwright
    playwright install firefox

Run:
    python download_boe.py
    python download_boe.py --force   # re-download even if files exist
"""

import argparse
import hashlib
import io
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BOE_DIR     = Path(__file__).parent / "data" / "boe_donors"
EXTRACT_DIR = BOE_DIR / "extracted"
BOE_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# Maps outer ZIP filename → (inner ZIP name, CSV name)
UNPACK = {
    "ALL_REPORTS_StateCandidate.zip":  ("STATE_CANDIDATE.zip",  "STATE_CANDIDATE.csv"),
    "ALL_REPORTS_CountyCandidate.zip": ("COUNTY_CANDIDATE.zip", "COUNTY_CANDIDATE.csv"),
    "ALL_REPORTS_StateCommittee.zip":  ("STATE_COMMITTEE.zip",  "STATE_COMMITTEE.csv"),
    "ALL_REPORTS_CountyCommittee.zip": ("COUNTY_COMMITTEE.zip", "COUNTY_COMMITTEE.csv"),
}

BASE_URL   = "https://publicreporting.elections.ny.gov"
PAGE_URL   = f"{BASE_URL}/DownloadCampaignFinanceData/DownloadCampaignFinanceData"
SET_URL    = f"{BASE_URL}/DownloadCampaignFinanceData/SetSessions/"
DL_URL     = (
    f"{BASE_URL}/DownloadCampaignFinanceData/DownloadZipFile"
    "?lstDateType=--lstDateType&lstUCYearDCF=--lstUCYearDCF&lstFilingDesc=--lstFilingDesc"
)

# The 4 bulk files we want
DOWNLOADS = [
    {
        "filename":    "ALL_REPORTS_StateCandidate.zip",
        "lstDateType": "Disclosure Report",
        "lstUCYearDCF":"All",
        "lstFilingDesc":"State Candidate",
    },
    {
        "filename":    "ALL_REPORTS_CountyCandidate.zip",
        "lstDateType": "Disclosure Report",
        "lstUCYearDCF":"All",
        "lstFilingDesc":"County Candidate",
    },
    {
        "filename":    "ALL_REPORTS_StateCommittee.zip",
        "lstDateType": "Disclosure Report",
        "lstUCYearDCF":"All",
        "lstFilingDesc":"State Committee",
    },
    {
        "filename":    "ALL_REPORTS_CountyCommittee.zip",
        "lstDateType": "Disclosure Report",
        "lstUCYearDCF":"All",
        "lstFilingDesc":"County Committee",
    },
]


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def run(force: bool = False):
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("ERROR: playwright not installed.")
        print("  Run:  pip install playwright")
        print("        playwright install firefox")
        sys.exit(1)

    print("=" * 60)
    print("BOE BULK CAMPAIGN FINANCE DOWNLOADER")
    print("=" * 60)
    print(f"Destination: {BOE_DIR}")
    print()

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) "
                "Gecko/20100101 Firefox/149.0"
            ),
            accept_downloads=True,
        )
        page = context.new_page()

        # ---- Visit the page once to establish session + Cloudflare cookies ----
        print("Connecting to BOE download page...")
        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=30_000)
            print(f"  Page loaded OK  (title: {page.title()[:60]})")
        except PWTimeout:
            print("  WARNING: page load timed out, continuing anyway...")
        print()

        results = []

        for i, dl in enumerate(DOWNLOADS, 1):
            fname             = dl["filename"]
            inner_zip, csv_nm = UNPACK[fname]
            csv_dest          = EXTRACT_DIR / csv_nm
            md5_file          = EXTRACT_DIR / (csv_nm + ".md5")
            tmp_dir           = BOE_DIR / "_tmp"
            tmp_dir.mkdir(exist_ok=True)

            print(f"[{i}/{len(DOWNLOADS)}] {fname}")

            # Change detection: compare MD5 of last downloaded outer ZIP
            # (stored as a tiny sidecar file so we don't need to keep the ZIP)
            old_hash = md5_file.read_text().strip() if md5_file.exists() else None
            if csv_dest.exists():
                print(f"  Extracted CSV: {fmt_size(csv_dest.stat().st_size)}")

            if csv_dest.exists() and not force:
                print("  Checking server for updates...")

            # Step 1: POST SetSessions via fetch() inside the browser page
            # Must run in-page so Cloudflare session cookies are included
            payload_json = (
                f'{{"lstDateType":"{dl["lstDateType"]}",'
                f'"lstUCYearDCF":"{dl["lstUCYearDCF"]}",'
                f'"lstFilingDesc":"{dl["lstFilingDesc"]}"}}'
            )
            set_status = page.evaluate(f"""
                async () => {{
                    const r = await fetch("{SET_URL}", {{
                        method: "POST",
                        headers: {{
                            "Content-Type": "application/json",
                            "X-Requested-With": "XMLHttpRequest",
                            "Accept": "application/json, text/javascript, */*; q=0.01"
                        }},
                        body: `{payload_json}`
                    }});
                    return r.status;
                }}
            """)
            if set_status != 200:
                print(f"  ERROR: SetSessions returned {set_status} - skipping")
                results.append((fname, "FAILED", f"SetSessions {set_status}"))
                continue

            # Step 2: Download the zip (server reads the session we just set)
            print(f"  Downloading...")
            t0 = time.time()
            try:
                with page.expect_download(timeout=600_000) as dl_info:
                    # Navigate to the download URL - triggers the file stream
                    page.evaluate(f"""
                        (() => {{
                            const f = document.createElement('form');
                            f.method = 'GET';
                            f.action = '{DL_URL}';
                            document.body.appendChild(f);
                            f.submit();
                            document.body.removeChild(f);
                        }})()
                    """)
                download = dl_info.value
                elapsed = time.time() - t0

                # Save to temp first
                tmp_path = tmp_dir / fname
                download.save_as(tmp_path)

                if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                    print(f"  ERROR: downloaded file is empty")
                    results.append((fname, "FAILED", "empty download"))
                    continue

                zip_size = tmp_path.stat().st_size
                new_hash = file_md5(tmp_path)

                print(f"  Downloaded: {fmt_size(zip_size)}  in {elapsed:.0f}s")

                # Change detection vs stored MD5 of last outer ZIP
                if old_hash and old_hash == new_hash and not force:
                    print(f"  UNCHANGED - skipping extract")
                    tmp_path.unlink()
                    results.append((fname, "UNCHANGED", fmt_size(csv_dest.stat().st_size) if csv_dest.exists() else ""))
                else:
                    # Extract nested CSV: outer ZIP → inner ZIP (BytesIO) → CSV
                    with zipfile.ZipFile(tmp_path) as outer:
                        inner_bytes = io.BytesIO(outer.read(inner_zip))
                    with zipfile.ZipFile(inner_bytes) as inner:
                        total_b = inner.getinfo(csv_nm).file_size
                    # Emit the "-> filename  (N MB expected)" marker the JS bar detects
                    print(f"  -> {csv_nm}  ({total_b // 1_000_000} MB expected)")
                    t1 = time.time()
                    written = 0
                    inner_bytes.seek(0)  # rewind BytesIO before second open
                    with zipfile.ZipFile(inner_bytes) as inner:
                        with inner.open(csv_nm) as src, open(csv_dest, "wb") as dst:
                            while True:
                                buf = src.read(4 * 1024 * 1024)
                                if not buf:
                                    break
                                dst.write(buf)
                                written += len(buf)
                                elapsed_x = time.time() - t1 or 0.001
                                mb  = written / 1_000_000
                                spd = mb / elapsed_x
                                print(f"\r    {mb:.1f} MB  ({spd:.1f} MB/s)    ", end="", flush=True)
                    elapsed_x = time.time() - t1 or 0.001
                    mb  = written / 1_000_000
                    spd = mb / elapsed_x
                    print(f"\r    {mb:.1f} MB in {elapsed_x:.0f}s  ({spd:.1f} MB/s)    ")
                    # Delete outer ZIP, save MD5 sidecar for next-run change detection
                    tmp_path.unlink()
                    md5_file.write_text(new_hash)
                    print(f"  🗑  Deleted {fname} ({fmt_size(zip_size)} freed)")
                    results.append((fname, "UPDATED" if old_hash else "NEW", fmt_size(written)))

            except PWTimeout:
                print(f"  ERROR: download timed out (10 min limit)")
                results.append((fname, "TIMEOUT", ""))
            except Exception as e:
                print(f"  ERROR: {e}")
                results.append((fname, "FAILED", str(e)))

            # Small pause between downloads to be polite
            if i < len(DOWNLOADS):
                time.sleep(3)
            print()

        # Cleanup temp dir
        tmp_dir_path = BOE_DIR / "_tmp"
        if tmp_dir_path.exists():
            shutil.rmtree(tmp_dir_path, ignore_errors=True)

        browser.close()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for fname, status, detail in results:
        print(f"  {status:<10}  {fname}  {detail}")
    print()

    failed = [r for r in results if r[1] in ("FAILED", "TIMEOUT")]
    if failed:
        print(f"ERRORS: {len(failed)} file(s) failed. Check output above.")
        sys.exit(1)
    else:
        print("All files complete.")
        print("\nNext: python main.py donors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download BOE bulk campaign finance files")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if files already exist and are unchanged")
    args = parser.parse_args()
    run(force=args.force)
