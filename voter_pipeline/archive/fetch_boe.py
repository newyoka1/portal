import subprocess
import sys

# Install requests if needed
subprocess.run([sys.executable, '-m', 'pip', 'install', 'requests', '--break-system-packages', '-q'], capture_output=True)

import requests
import re

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
})

url = 'https://publicreporting.elections.ny.gov/DownloadCampaignFinanceData/DownloadCampaignFinanceData'
print(f"Fetching {url}...")
r = session.get(url, timeout=30)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    html = r.text
    # Find all links
    links = re.findall(r'href=["\']([^"\']*)["\']', html, re.IGNORECASE)
    print("\nAll links:")
    for l in links:
        print(f"  {l}")
    
    # Look for download-specific patterns
    print("\nDownload-relevant links:")
    for l in links:
        if any(x in l.lower() for x in ['zip', 'csv', 'download', 'bulk', 'receipt', 'contrib', 'file', 'data']):
            print(f"  {l}")
    
    # Also look for onclick or data attributes
    onclicks = re.findall(r'onclick=["\']([^"\']*)["\']', html, re.IGNORECASE)
    if onclicks:
        print("\nOnclick handlers:")
        for o in onclicks[:20]:
            print(f"  {o}")
else:
    print(f"Failed: {r.status_code}")
    print(r.text[:500])
