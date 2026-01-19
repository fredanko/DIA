"""
Simple script to extract all tar.gz archives from timetables and timetable_changes directories.
Run this once before running the main spark_job.py
"""
import os
import tarfile

def extract_all_archives(source_dir, target_dir):
    """
    Extract all .tar.gz files from source_dir into target_dir.
    
    Args:
        source_dir: Directory containing .tar.gz files
        target_dir: Directory where files should be extracted
    """
    if not os.path.exists(source_dir):
        print(f"Source directory not found: {source_dir}")
        return
    
    # Create target directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)
    
    # Find all tar.gz files
    tar_files = [f for f in os.listdir(source_dir) if f.endswith('.tar.gz')]
    
    if not tar_files:
        print(f"No .tar.gz files found in {source_dir}")
        return
    
    print(f"Found {len(tar_files)} archive(s) in {source_dir}")
    
    for tar_file in sorted(tar_files):
        tar_path = os.path.join(source_dir, tar_file)
        print(f"  Extracting: {tar_file}...")
        
        try:
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(target_dir)
            print(f"    ✓ Extracted successfully")
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    print(f"Extraction complete. Files extracted to: {target_dir}\n")


if __name__ == "__main__":
    print("=" * 80)
    print("Extracting DBahn Berlin Archives")
    print("=" * 80)
    print()
    
    # Extract timetables
    extract_all_archives(
        source_dir='DBahn-berlin/timetables',
        target_dir='DBahn-berlin/timetables_extracted'
    )
    
    # Extract timetable changes
    extract_all_archives(
        source_dir='DBahn-berlin/timetable_changes',
        target_dir='DBahn-berlin/timetable_changes_extracted'
    )
    
    print("=" * 80)
    print("Done! You can now run spark_job.py")
    print("=" * 80)
