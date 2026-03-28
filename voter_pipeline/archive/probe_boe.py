#!/usr/bin/env python3
"""
probe_boe.py - Discover the actual BOE bulk download URLs.
Run: python probe_boe.py
Writes results to data/boe_donors/probe_results.txt
"""
import os, sys, time
import requests
from pathlib import Path

OUT = Path(r"D:\git\nys-voter-pipeline\data\boe_donors\probe_results.txt")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://publicreporting.elections.ny.gov/DownloadCampaignFinanceData/DownloadCampaignFinanceData",
}

BASE = "https://publicreporting.elections.ny.gov"

CANDIDATE_URLS = [
    # Direct file paths
    f"{BASE}/Content/BulkData/ALL_REPORTS_StateCandidate.zip",
    f"{BASE}/content/bulkdata/ALL_REPORTS_StateCandidate.zip",
    f"{BASE}/DownloadCampaignFinanceData/ALL_REPORTS_StateCandidate.zip",
    f"{BASE}/BulkData/ALL_REPORTS_StateCandidate.zip",
    # API patterns
    f"{BASE}/DownloadCampaignFinanceData/GetBulkFile?fileType=StateCandidate",
    f"{BASE}/DownloadCampaignFinanceData/GetBulkFile?type=StateCandidate",
    f"{BASE}/DownloadCampaignFinanceData/DownloadFile?fileName=ALL_REPORTS_StateCandidate.zip",
    f"{BASE}/DownloadCampaignFinanceData/GetFile?name=StateCandidate",
    f"{BASE}/api/DownloadCampaignFinanceData/GetBulkFile?fileType=StateCandidate",
]

lines = []

def log(msg):
    print(msg)
    lines.append(msg)

log("=" * 60)
log("BOE Download URL Probe")
log("=" * 60)

# Step 1: Try to GET the download page itself
log("\n--- Step 1: Probe the download page ---")
page_url = f"{BASE}/DownloadCampaignFinanceData/DownloadCampaignFinanceData"
try:
    s = requests.Session()
    r = s.get(page_url, headers=HEADERS, timeout=20, allow_redirects=True)
    log(f"Page status: {r.status_code}")
    log(f"Content-Type: {r.headers.get('Content-Type','?')}")
    log(f"Response size: {len(r.content)} bytes")
    if r.status_code == 200:
        html = r.text
        log(f"HTML snippet (first 2000 chars):\n{html[:2000]}")
        # Look for download-related patterns
        import re
        urls_in_html = re.findall(r'href=["\']([^"\']+zip[^"\']*)["\']', html, re.IGNORECASE)
        urls_in_html += re.findall(r'href=["\']([^"\']*GetBulk[^"\']*)["\']', html, re.IGNORECASE)
        urls_in_html += re.findall(r'href=["\']([^"\']*Download[^"\']*)["\']', html, re.IGNORECASE)
        urls_in_html += re.findall(r'action=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if urls_in_html:
            log(f"\nURLs found in HTML:")
            for u in set(urls_in_html):
                log(f"  {u}")
        else:
            log("No obvious download URLs found in HTML")
    session_cookies = s.cookies.get_dict()
    log(f"Session cookies: {session_cookies}")
except Exception as e:
    log(f"Page probe error: {e}")
    s = requests.Session()
    session_cookies = {}

# Step 2: Try candidate URL patterns (HEAD requests to avoid downloading)
log("\n--- Step 2: Try candidate URLs (HEAD) ---")
for url in CANDIDATE_URLS:
    try:
        r = s.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        ct = r.headers.get("Content-Type", "?")
        cl = r.headers.get("Content-Length", "?")
        loc = r.headers.get("Location", "")
        log(f"  {r.status_code}  CL={cl}  CT={ct}  {'-> '+loc if loc else ''}")
        log(f"    {url}")
    except Exception as e:
        log(f"  ERR {e}")
        log(f"    {url}")
    time.sleep(0.3)

# Step 3: Try POST to the download page
log("\n--- Step 3: Try POST to download page ---")
post_payloads = [
    {"fileType": "StateCandidate"},
    {"FileType": "StateCandidate"},
    {"type": "StateCandidate"},
    {"fileName": "ALL_REPORTS_StateCandidate.zip"},
]
for payload in post_payloads:
    try:
        r = s.post(page_url, headers=HEADERS, data=payload, timeout=10, allow_redirects=True)
        ct = r.headers.get("Content-Type", "?")
        cl = r.headers.get("Content-Length", "?")
        log(f"  POST {payload} -> {r.status_code}  CL={cl}  CT={ct}")
    except Exception as e:
        log(f"  POST {payload} -> ERR {e}")
    time.sleep(0.3)

# Step 4: Check response headers from page for any CSRF tokens
log("\n--- Step 4: Response headers from download page ---")
try:
    r2 = s.get(page_url, headers=HEADERS, timeout=20)
    for k, v in r2.headers.items():
        log(f"  {k}: {v}")
except Exception as e:
    log(f"  Error: {e}")

log("\n" + "=" * 60)
log("Probe complete. Check probe_results.txt")
log("=" * 60)

OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"\nResults written to: {OUT}")
