import os

# Search D:\ for large CSV or txt files that could be raw BOE transaction data
print("Searching for large data files on D:\\ ...")
for root, dirs, files in os.walk('D:\\'):
    # Skip hidden/system dirs
    dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['$RECYCLE.BIN', 'System Volume Information']]
    for f in files:
        if f.lower().endswith(('.csv', '.txt', '.tsv', '.zip')):
            try:
                full = os.path.join(root, f)
                size = os.path.getsize(full)
                if size > 10_000_000:  # > 10MB
                    print(f"  {size/1_000_000:>8.1f} MB  {full}")
            except:
                pass

print("\nSearching OneDrive ...")
onedrive = os.path.expanduser(r'C:\Users\georg_2r965zq\OneDrive')
for root, dirs, files in os.walk(onedrive):
    dirs[:] = [d for d in dirs if not d.startswith('.')]
    for f in files:
        if f.lower().endswith(('.csv', '.txt', '.tsv', '.zip')):
            try:
                full = os.path.join(root, f)
                size = os.path.getsize(full)
                if size > 10_000_000:
                    print(f"  {size/1_000_000:>8.1f} MB  {full}")
            except:
                pass
print("Done.")
