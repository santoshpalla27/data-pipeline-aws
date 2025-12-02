import os
from pathlib import Path
import json

OUTPUT_DIR = Path('output/raw')

def analyze_data():
    total_files = 0
    total_size = 0
    service_stats = {}

    if not OUTPUT_DIR.exists():
        print("Output directory not found.")
        return

    for service_dir in OUTPUT_DIR.iterdir():
        if not service_dir.is_dir():
            continue
        
        service_name = service_dir.name
        service_files = 0
        service_size = 0
        
        for region_dir in service_dir.iterdir():
            if not region_dir.is_dir():
                continue
                
            pricing_file = region_dir / 'pricing.json'
            if pricing_file.exists():
                size = pricing_file.stat().st_size
                service_files += 1
                service_size += size
        
        service_stats[service_name] = {
            'files': service_files,
            'size_mb': service_size / (1024 * 1024)
        }
        total_files += service_files
        total_size += service_size

    print(f"Total Files: {total_files}")
    print(f"Total Size: {total_size / (1024 * 1024 * 1024):.2f} GB")
    print("-" * 40)
    print(f"{'Service':<25} {'Files':<10} {'Size (MB)':<10}")
    print("-" * 40)
    
    for service, stats in sorted(service_stats.items()):
        print(f"{service:<25} {stats['files']:<10} {stats['size_mb']:<10.2f}")

if __name__ == '__main__':
    analyze_data()
