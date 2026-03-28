import urllib.request
import re

url = 'https://www.elections.ny.gov/CFViewReports.html'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
html = urllib.request.urlopen(req).read().decode('utf-8', errors='replace')

links = re.findall(r'href="([^"]*)"', html, re.IGNORECASE)
for l in links:
    if any(x in l.lower() for x in ['bulk', 'csv', 'zip', 'receipt', 'contrib', 'download', 'data', 'efiled']):
        print(l)

print("\n--- ALL LINKS ---")
for l in links:
    print(l)
