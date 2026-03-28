"""
BOE Complete Import Pipeline
=============================
Master script that runs the complete BOE import process:
1. Import all contribution CSV files
2. Match to voters and aggregate by party/year
3. Create final donor_summary table

Run this once to rebuild the complete BOE donor database.
"""

import subprocess
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    print(f"\n{'='*80}")
    print(f"Running: {script_name}")
    print('='*80)
    result = subprocess.run([sys.executable, script_path], cwd=SCRIPT_DIR)
    if result.returncode != 0:
        print(f"\n❌ ERROR: {script_name} failed!")
        sys.exit(1)
    return result.returncode

def main():
    print("="*80)
    print("BOE COMPLETE IMPORT PIPELINE")
    print("="*80)
    print("\nThis will:")
    print("  1. Import all BOE contribution files (2023-2025)")
    print("  2. Match contributions to voters")
    print("  3. Aggregate by party and year")
    print("  4. Create donor_summary table for enrichment")
    print("\nEstimated time: 15-30 minutes")
    
    response = input("\nProceed? (y/N): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        sys.exit(0)
    
    # Run import
    run_script('boe_import_comprehensive.py')
    
    # Run matching and aggregation
    run_script('boe_match_aggregate.py')
    
    print("\n" + "="*80)
    print("✓ BOE IMPORT PIPELINE COMPLETE!")
    print("="*80)
    print("\nYour boe_donors database now contains:")
    print("  - contributions_raw: All individual contributions")
    print("  - contributions_matched: Matched to voters")
    print("  - donor_summary: Aggregated by StateVoterId + party + year")
    print("\nNext step:")
    print("  python main.py boe-enrich")
    print("\nThis will add BOE donor data to voter_file.")

if __name__ == "__main__":
    main()