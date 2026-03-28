import subprocess, sys
from pathlib import Path

steps = [
    ("Download FEC Data", "step1_download_fec.py"),
    ("Extract Files", "step2_extract_fec.py"),
    ("Load FEC (NY only)", "step3_load_fec.py"),
    ("Classify Parties", "step4_classify_parties.py"),
    ("Build Unified Table", "step5_build_unified_table.py"),
]

print("="*70)
print("NATIONAL DONORS - COMPLETE PIPELINE")
print("="*70)
print("\nTotal time: ~30-45 minutes")
print("="*70)

resp = input("\nReady? (y/N): ").strip().lower()
if resp != 'y':
    print("Cancelled")
    exit(0)

for i, (title, script) in enumerate(steps, 1):
    print(f"\n\n### STEP {i}/{len(steps)}: {title} ###")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"\n❌ Failed at step {i}")
        print(f"   Resume: python {script}")
        exit(1)

print("\n" + "="*70)
print("✓ COMPLETE SUCCESS!")
print("="*70)
print("\nTable: National_Donors.ny_voters_with_donations")
print("\nColumns filled:")
print("  • democratic_amount & democratic_count")
print("  • republican_amount & republican_count")
print("  • independent_amount & independent_count")
print("="*70)
