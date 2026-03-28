"""
Backup key databases before rename operation
"""
import subprocess
import os
from datetime import datetime

# Backup path
backup_dir = r"D:\git\nys-voter-pipeline\backups\2026-03-03_132727"
os.makedirs(backup_dir, exist_ok=True)

# MySQL credentials from .env
MYSQL_PATH = r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe"
HOST = "127.0.0.1"
USER = "root"
PASSWORD = "!#goAmerica99"

databases = ["politik1_fec", "politik1_nydata"]

print("="*80)
print("BACKING UP KEY DATABASES")
print("="*80)

for db in databases:
    output_file = os.path.join(backup_dir, f"{db}.sql")
    print(f"\nBacking up {db}...")
    
    cmd = [
        MYSQL_PATH,
        f"-h{HOST}",
        f"-u{USER}",
        f"-p{PASSWORD}",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--result-file=" + output_file,
        db
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0 and os.path.exists(output_file):
            size = os.path.getsize(output_file)
            print(f"  ✓ Success: {output_file} ({size:,} bytes)")
        else:
            print(f"  ✗ Failed: {result.stderr}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\n" + "="*80)
print("BACKUP COMPLETE")
print("="*80)
