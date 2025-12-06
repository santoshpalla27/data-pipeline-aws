"""
AWS Resource Downloader Orchestrator (V2).
"""
import argparse
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path

from loguru import logger
from aws_resource_downloader.registry import registry, ServiceConfig, ResourceConfig
from aws_resource_downloader.session import AwsSessionManager
from aws_resource_downloader.storage import StorageManager
from aws_resource_downloader.collector import BaseCollector

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("download.log", rotation="10 MB")

def parse_args():
    parser = argparse.ArgumentParser(description="AWS Resource Downloader V2")
    parser.add_argument("--services-file", type=Path, default=Path("services.txt"), help="Path to services list")
    parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent regions per service")
    parser.add_argument("--regions", nargs="+", help="Explicit list of regions to target (overrides discovery)")
    parser.add_argument("--profile", help="AWS Profile")
    parser.add_argument("--region", help="Default AWS Region")
    parser.add_argument("--compress", action="store_true", default=True, help="Compress output (default: True)")
    return parser.parse_args()

def load_services(file_path: Path) -> list[str]:
    if not file_path.exists():
        logger.error(f"Services file not found: {file_path}")
        return []
    with open(file_path) as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

def process_resource_region(
    service_name: str, # Registry Key (e.g. AmazonS3)
    resource_cfg: ResourceConfig,
    scan_region: str, # Region to CALL API
    save_region: str, # Region to SAVE data under (e.g. "global")
    session_manager: AwsSessionManager,
    storage: StorageManager
) -> dict:
    """
    Worker function to download a single resource in a single region.
    """
    stats = {"pages": 0, "status": "success", "error": None}
    
    try:
        collector = BaseCollector(session_manager, registry.get_service(service_name).service_name, resource_cfg)
        
        page_num = 0
        for page_data in collector.collect(scan_region):
            page_num += 1
            storage.save_page(
                service=service_name,
                resource=resource_cfg.name,
                region=save_region,
                data=page_data,
                page_num=page_num,
                metadata={
                    "scan_region": scan_region,
                    "api_method": resource_cfg.api_method
                }
            )
        
        stats["pages"] = page_num
        if page_num == 0:
             # Could be valid empty (no resources), or failed before yield
             pass

    except Exception as e:
        stats["status"] = "failed"
        stats["error"] = str(e)
        logger.error(f"Worker failed for {service_name}.{resource_cfg.name} in {scan_region}: {e}")
        
    return stats

def main():
    args = parse_args()
    
    # 1. Setup Run
    run_start = datetime.utcnow()
    run_id = f"run_{run_start.strftime('%Y-%m-%dT%H-%M-%SZ')}"
    logger.info(f"Starting Download Run: {run_id}")
    
    # 2. Init Components
    session_manager = AwsSessionManager(profile=args.profile, region=args.region)
    storage = StorageManager(Path("data/resource_metadata"), run_id, compress=args.compress)
    
    # 3. Load Targets
    target_service_keys = load_services(args.services_file)
    logger.info(f"Loaded {len(target_service_keys)} target services.")
    
    # 4. Processing Loop
    run_stats = defaultdict(lambda: {"success": 0, "failed": 0, "pages": 0})
    
    for service_key in target_service_keys:
        service_cfg = registry.get_service(service_key)
        
        if not service_cfg:
            logger.warning(f"Skipping unknown service: {service_key}")
            continue
            
        logger.info(f"Processing Service: {service_key} ({service_cfg.service_name})")
        
        # Determine Regions
        # If user provided explicit regions, use them. Else discover.
        # Note: Discovery intersects with Account Enabled Regions.
        if args.regions:
            available_regions = args.regions
        else:
            available_regions = session_manager.get_available_regions(service_cfg.service_name)

        if not available_regions:
            logger.warning(f"No available regions found for {service_key}")
            continue

        # Prepare Work Items
        futures = {}
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            
            for resource in service_cfg.resources:
                
                # CASE A: REGIONAL RESOURCE
                if resource.regional:
                    for reg in available_regions:
                        # Schedule worker
                        future = executor.submit(
                            process_resource_region,
                            service_key, resource, reg, reg, session_manager, storage
                        )
                        futures[future] = f"{service_key}.{resource.name} ({reg})"
                
                # CASE B: GLOBAL RESOURCE
                else:
                    # Determine where to call
                    scan_region = resource.forced_region or session_manager.region or "us-east-1"
                    save_region = "global"
                    
                    future = executor.submit(
                        process_resource_region,
                        service_key, resource, scan_region, save_region, session_manager, storage
                    )
                    futures[future] = f"{service_key}.{resource.name} (global)"
            
            # Collect Results
            for future in as_completed(futures):
                desc = futures[future]
                try:
                    res = future.result()
                    if res["status"] == "success":
                        run_stats[service_key]["success"] += 1
                        run_stats[service_key]["pages"] += res["pages"]
                    else:
                        run_stats[service_key]["failed"] += 1
                except Exception as e:
                    logger.error(f"Unhandled exception in future {desc}: {e}")
                    run_stats[service_key]["failed"] += 1

    # 5. Summary Report
    logger.info("="*60)
    logger.info("RUN SUMMARY")
    logger.info(f"Run ID: {run_id}")
    logger.info("-" * 60)
    logger.info(f"{'Service':<20} | {'Pages':<8} | {'Tasks OK':<8} | {'Failed':<8}")
    logger.info("-" * 60)
    
    total_pages = 0
    for svc, s in run_stats.items():
        logger.info(f"{svc:<20} | {s['pages']:<8} | {s['success']:<8} | {s['failed']:<8}")
        total_pages += s['pages']
        
    logger.info("="*60)
    logger.info(f"Total Pages Downloaded: {total_pages}")
    logger.info(f"Data stored in: {storage.run_dir}")

if __name__ == "__main__":
    main()
