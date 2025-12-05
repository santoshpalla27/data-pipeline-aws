"""
Command-line interface for AWS Pricing Downloader.
"""

import asyncio
import sys
import signal
from pathlib import Path
from typing import Optional
import argparse

from aws_pricing_downloader.config import load_config
from aws_pricing_downloader.downloader import PricingDownloader
from aws_pricing_downloader.logger import get_logger


# Global downloader instance for signal handling
_downloader: Optional[PricingDownloader] = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger = get_logger("aws_pricing_downloader.cli")
    logger.warning(f"Received signal {signum}, shutting down gracefully...")
    
    if _downloader:
        _downloader._shutdown_event.set()
    
    sys.exit(0)


def create_parser() -> argparse.ArgumentParser:
    """
    Create argument parser.
    
    Returns:
        Configured ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog="aws-price",
        description="Enterprise AWS Pricing Downloader v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Download command
    download_parser = subparsers.add_parser(
        "download",
        help="Download AWS pricing data"
    )
    
    download_parser.add_argument(
        "--service",
        type=str,
        help="Download specific service only (e.g., AmazonEC2)",
    )
    
    download_parser.add_argument(
        "--services",
        type=str,
        nargs="+",
        help="Download multiple specific services (e.g., AmazonEC2 AmazonS3)",
    )
    
    download_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for pricing files",
    )
    
    download_parser.add_argument(
        "--metrics-dir",
        type=Path,
        help="Metrics directory",
    )
    
    download_parser.add_argument(
        "--max-concurrent",
        type=int,
        help="Maximum concurrent downloads",
    )
    
    download_parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size for streaming (bytes)",
    )
    
    download_parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Disable integrity verification",
    )
    
    download_parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    
    download_parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    
    return parser


async def download_command(args: argparse.Namespace):
    """
    Execute download command.
    
    Args:
        args: Parsed command-line arguments
    """
    global _downloader
    
    # Build config from arguments
    config_kwargs = {}
    
    if args.output_dir:
        config_kwargs["output_dir"] = args.output_dir
    if args.metrics_dir:
        config_kwargs["metrics_dir"] = args.metrics_dir
    if args.max_concurrent:
        config_kwargs["max_concurrent_downloads"] = args.max_concurrent
    if args.chunk_size:
        config_kwargs["chunk_size"] = args.chunk_size
    if args.log_level:
        config_kwargs["log_level"] = args.log_level
    if args.log_file:
        config_kwargs["log_file"] = args.log_file
    if args.no_verify:
        config_kwargs["verify_integrity"] = False
    
    config = load_config(**config_kwargs)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create downloader
    try:
        async with PricingDownloader(config) as downloader:
            _downloader = downloader
            logger = get_logger("aws_pricing_downloader.cli")
            
            if args.service:
                # Download single service
                logger.info(f"Downloading pricing for service: {args.service}")
                print(f"📥 Downloading {args.service}...")
                
                try:
                    output_path = await downloader.fetch_service_price(args.service)
                    logger.info(f"Successfully downloaded: {output_path}")
                    print(f"✓ Downloaded: {output_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to download service: {str(e)}", exc_info=True)
                    print(f"✗ Failed: {str(e)}", file=sys.stderr)
                    sys.exit(1)
                    
            elif args.services:
                # Download multiple specific services
                logger.info(f"Downloading pricing for {len(args.services)} services")
                print(f"📥 Downloading {len(args.services)} services...")
                
                try:
                    paths = await downloader.fetch_all_services(service_codes=args.services)
                    summary = downloader.metrics.get_summary()
                    
                    logger.info(f"Successfully downloaded {len(paths)} services")
                    print(f"\n✓ Downloaded {len(paths)}/{len(args.services)} services")
                    print(f"  Output directory: {config.output_dir}")
                    print(f"  Cache hit rate: {summary['cache_hit_rate']}")
                    print(f"  Total bytes: {summary['total_bytes']:,}")
                    
                except asyncio.CancelledError:
                    print("\n⚠ Download cancelled by user")
                    sys.exit(130)
                except Exception as e:
                    logger.error(f"Failed to download services: {str(e)}", exc_info=True)
                    print(f"✗ Failed: {str(e)}", file=sys.stderr)
                    sys.exit(1)
                    
            else:
                # Download all services
                logger.info("Downloading pricing for all services")
                print("📥 Downloading all AWS pricing data...")
                
                try:
                    paths = await downloader.fetch_all_services()
                    summary = downloader.metrics.get_summary()
                    
                    logger.info(f"Successfully downloaded {len(paths)} services")
                    print(f"\n✓ Downloaded {len(paths)} pricing files")
                    print(f"  Output directory: {config.output_dir}")
                    print(f"  Cache hit rate: {summary['cache_hit_rate']}")
                    print(f"  Success rate: {summary['success_rate']}")
                    print(f"  Total bytes: {summary['total_bytes']:,}")
                    print(f"  Avg duration: {summary['avg_duration_ms']}ms")
                    print(f"\n📊 Metrics: {config.metrics_dir}/latest.json")
                    
                except asyncio.CancelledError:
                    print("\n⚠ Download cancelled by user")
                    sys.exit(130)
                except Exception as e:
                    logger.error(f"Failed to download all services: {str(e)}", exc_info=True)
                    print(f"✗ Failed: {str(e)}", file=sys.stderr)
                    sys.exit(1)
    
    finally:
        _downloader = None


def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "download":
        try:
            asyncio.run(download_command(args))
        except KeyboardInterrupt:
            print("\n⚠ Download interrupted")
            sys.exit(130)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()