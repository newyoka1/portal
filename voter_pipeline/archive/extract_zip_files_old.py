import zipfile
import os
from pathlib import Path

def extract_and_rename(zip_path, output_folder):
    """Extract CSV from zip and rename to match zip filename"""
    zip_name = os.path.basename(zip_path)
    base_name = os.path.splitext(zip_name)[0]  # Remove .zip extension
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get list of files in zip
        file_list = zip_ref.namelist()
        csv_files = [f for f in file_list if f.endswith('.csv')]
        
        if len(csv_files) == 0:
            return f"No CSV found in {zip_name}", False
        
        # Extract the CSV
        csv_file = csv_files[0]
        zip_ref.extract(csv_file, output_folder)
        
        # Rename to match zip file name
        old_path = os.path.join(output_folder, csv_file)
        new_path = os.path.join(output_folder, f"{base_name}.csv")
        
        # Remove existing file if it exists
        if os.path.exists(new_path):
            os.remove(new_path)
        
        # Handle if file exists or is in subfolder
        if os.path.dirname(csv_file):  # CSV was in a subfolder in the zip
            old_path = os.path.join(output_folder, csv_file)
            os.rename(old_path, new_path)
            # Clean up empty folder if it exists
            folder_path = os.path.dirname(old_path)
            if os.path.exists(folder_path) and not os.listdir(folder_path):
                os.rmdir(folder_path)
        else:
            if old_path != new_path:
                os.rename(old_path, new_path)
        
        # Get file size
        file_size = os.path.getsize(new_path) / (1024 * 1024)  # MB
        return f"{base_name}.csv ({file_size:.2f} MB)", True

# Main execution
zip_folder = r"C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\ziped"
data_folder = r"C:\Users\georg_2r965zq\OneDrive\Desktop\AUDIANCE DATABASE\data"

print("=" * 70)
print("EXTRACTING ZIP FILES WITH VOTERID")
print("=" * 70)
print(f"\nSource folder: {zip_folder}")
print(f"Target folder: {data_folder}\n")

# Get all zip files
zip_files = [f for f in os.listdir(zip_folder) if f.endswith('.zip')]
print(f"Found {len(zip_files)} zip files to process\n")

# Process each zip file
success_count = 0
fail_count = 0
results = []

for zip_file in sorted(zip_files):
    zip_path = os.path.join(zip_folder, zip_file)
    result, success = extract_and_rename(zip_path, data_folder)
    
    if success:
        print(f"[OK] {result}")
        success_count += 1
    else:
        print(f"[FAIL] {result}")
        fail_count += 1
    
    results.append((zip_file, result, success))

# Summary
print("\n" + "=" * 70)
print("EXTRACTION COMPLETE")
print("=" * 70)
print(f"[OK] Successfully extracted: {success_count}")
print(f"[FAIL] Failed: {fail_count}")
print(f"[FOLDER] CSV files saved to: {data_folder}\n")

# List all extracted files
if success_count > 0:
    print("\nExtracted files:")
    csv_files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]
    for csv_file in sorted(csv_files):
        csv_path = os.path.join(data_folder, csv_file)
        size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"  • {csv_file} ({size_mb:.2f} MB)")
