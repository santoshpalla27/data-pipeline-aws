"""
Main CLI Orchestrator.
"""
import sys
import boto3
import click
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from rich.console import Console
from rich.progress import Progress

from aws_resource_downloader.session import AwsSessionManager
from aws_resource_downloader.registry import registry
from aws_resource_downloader.collector import BaseCollector
from aws_resource_downloader.storage import StorageManager

console = Console()

def setup_logging(debug: bool):
    """Configure Loguru."""
    logger.remove()
    level = "DEBUG" if debug else "INFO"
    logger.add(sys.stderr, level=level, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

def process_region(
    service_name: str,
    region: str,
    resources,
    session_manager,
    storage,
    timestamp
):
    """
    Worker function to process all resources for a service in a single region.
    """
    results = {"success": 0, "failed": 0, "pages": 0}
    
    for resource_cfg in resources:
        try:
            collector = BaseCollector(session_manager, service_name, resource_cfg)
            
            # Determine actual region to use for call
            # For non-regional resources (like S3), use session's default or us-east-1
            target_region = region
            if not resource_cfg.regional:
                target_region = session_manager.region or "us-east-1"
                
            page_count = 0
            for page_data in collector.collect(target_region):
                page_count += 1
                storage.save_page(
                    service=service_name,
                    resource=resource_cfg.name,
                    region=target_region,
                    data=page_data,
                    page_num=page_count,
                    timestamp=timestamp
                )
            
            results["success"] += 1
            results["pages"] += page_count
            logger.info(f"Completed {service_name}.{resource_cfg.name} in {target_region} ({page_count} pages)")
            
            # If not regional, we only run once. Break loop if this function was called in a region loop 
            # but we want to avoid duplicates? 
            # Actually, the orchestrator should handle "don't loop regions if not regional".
            
        except Exception as e:
            results["failed"] += 1
            logger.error(f"Failed {service_name}.{resource_cfg.name} in {region}: {e}")
            
    return results

@click.command()
@click.option("--services", "-s", multiple=True, help="Services to download (default: all)")
@click.option("--services-file", "-f", type=click.Path(exists=True), help="File containing list of services (one per line)")
@click.option("--regions", "-r", multiple=True, help="Specific regions to process (default: all available)")
@click.option("--exclude-regions", "-x", multiple=True, help="Regions to exclude")
@click.option("--concurrency", "-c", default=5, help="Max concurrent regions")
@click.option("--profile", "-p", help="AWS Profile")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def main(services, services_file, regions, exclude_regions, concurrency, profile, debug):
    """
    AWS Resource Metadata Downloader.
    """
    setup_logging(debug)
    timestamp = datetime.now(timezone.utc)
    
    # 1. Setup Session
    try:
        session_manager = AwsSessionManager(profile=profile)
        identity = session_manager.get_caller_identity()
        if not identity:
            logger.error("Failed to authenticate with AWS. Check credentials.")
            sys.exit(1)
        logger.info(f"Authenticated as {identity.get('Arn')}")
    except Exception as e:
        logger.critical(f"AWS Setup Failed: {e}")
        sys.exit(1)

    # 2. Setup Storage
    storage = StorageManager()

    # 3. Determine Services
    available_services = registry.list_services()
    target_services_set = set()

    # Add from CLI args
    if services:
        target_services_set.update(services)

    # Add from File
    if services_file:
        try:
            with open(services_file, "r") as f:
                file_services = [line.strip() for line in f if line.strip()]
                target_services_set.update(file_services)
        except Exception as e:
            logger.error(f"Failed to read services file: {e}")
            sys.exit(1)

    # If no services specified, default to ALL
    if not target_services_set:
        target_services = available_services
    else:
        # Validate and Filter
        target_services = [s for s in target_services_set if s in available_services]
        invalid = target_services_set - set(available_services)
        if invalid:
            logger.warning(f"Skipping unknown services: {invalid}")

    # 4. Processing Loop
    logger.info(f"Starting download for services: {target_services}")
    
    with Progress() as progress:
        task_id = progress.add_task("[cyan]Processing...", total=len(target_services))
        
        for service_name in target_services:
            service_config = registry.get_service(service_name)
            
            # Split resources into regional vs global
            regional_resources = [r for r in service_config.resources if r.regional]
            global_resources = [r for r in service_config.resources if not r.regional]
            
            # 4a. Process Global Resources (Once)
            if global_resources:
                logger.info(f"Processing global resources for {service_name}")
                process_region(service_name, "global", global_resources, session_manager, storage, timestamp)

            # 4b. Process Regional Resources
            if regional_resources:
                # Discover regions
                if regions:
                    target_regions = regions
                else:
                    target_regions = session_manager.get_available_regions(service_name)
                    # Filter exclusions
                    target_regions = [r for r in target_regions if r not in exclude_regions]
                
                logger.info(f"Processing {len(target_regions)} regions for {service_name}")
                
                # Use ThreadPool for regions
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = {
                        executor.submit(
                            process_region, 
                            service_name, 
                            r, 
                            regional_resources, 
                            session_manager, 
                            storage, 
                            timestamp
                        ): r for r in target_regions
                    }
                    
                    for future in as_completed(futures):
                        r = futures[future]
                        try:
                            res = future.result()
                            # logger.debug(f"Region {r} stats: {res}")
                        except Exception as e:
                            logger.error(f"Region task failed for {r}: {e}")

            progress.advance(task_id)

    logger.success("Download complete!")

if __name__ == "__main__":
    main()
