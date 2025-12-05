"""
Main pricing downloader with streaming, integrity checks, and metrics.
"""

import asyncio
import time
from pathlib import Path
from typing import List
import orjson

from aws_pricing_downloader.config import DownloaderConfig, load_config
from aws_pricing_downloader.logger import setup_logger, get_logger
from aws_pricing_downloader.http_client import HttpClient
from aws_pricing_downloader.storage import StorageManager
from aws_pricing_downloader.integrity import IntegrityVerifier
from aws_pricing_downloader.metrics import MetricsCollector
from aws_pricing_downloader.exceptions import DownloadError


class PricingDownloader:
    """Enterprise-grade AWS pricing downloader with streaming and integrity verification."""
    
    def __init__(self, config: DownloaderConfig | None = None):
        """
        Initialize pricing downloader.
        
        Args:
            config: Optional configuration override
        """
        self.config = config or load_config()
        self.logger = setup_logger(
            "aws_pricing_downloader",
            level=self.config.log_level,
            log_file=self.config.log_file,
        )
        
        self.storage = StorageManager(self.config)
        self.integrity = IntegrityVerifier(self.config.output_dir)
        self.metrics = MetricsCollector(self.config.metrics_dir)
        self.http_client: HttpClient | None = None
        
        self._shutdown_event = asyncio.Event()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Start the downloader (initialize HTTP client)."""
        if not self.http_client:
            self.http_client = HttpClient(self.config)
            await self.http_client.start()
            self.logger.info("Downloader started")
    
    async def close(self):
        """Close the downloader (cleanup HTTP client and export metrics)."""
        if self.http_client:
            await self.http_client.close()
            self.http_client = None
        
        # Export metrics
        try:
            metrics_path = self.metrics.export_json()
            self.logger.info(f"Metrics exported to {metrics_path}")
        except Exception as e:
            self.logger.warning(f"Failed to export metrics: {e}")
        
        self.logger.info("Downloader closed")
    
    async def fetch_offer_index(self) -> Path:
        """
        Fetch the AWS offer index.
        
        Returns:
            Path to the downloaded index file
            
        Raises:
            DownloadError: On download failure
        """
        service_code = "index"
        url = f"{self.config.base_url}/index.json"
        
        self.logger.info("Fetching offer index", extra={"url": url})
        
        start_time = time.time()
        
        try:
            # Get metadata first
            metadata = await self.http_client.head(url)
            etag = metadata.get("etag")
            
            # Check if download needed
            if self.config.verify_integrity:
                if not self.integrity.should_download(service_code, etag):
                    self.logger.info("Using cached offer index (integrity verified)")
                    
                    duration_ms = int((time.time() - start_time) * 1000)
                    size = self.storage.get_file_size(service_code) or 0
                    
                    self.metrics.record_download(
                        service_code=service_code,
                        success=True,
                        duration_ms=duration_ms,
                        size_bytes=size,
                        cache_hit=True,
                    )
                    
                    return self.storage.get_file_path(service_code)
            
            # Stream download
            content_iterator, stream_metadata = await self.http_client.stream_get(
                url=url,
                etag=etag,
            )
            
            # Handle cache hit
            if stream_metadata["cache_hit"]:
                duration_ms = int((time.time() - start_time) * 1000)
                size = self.storage.get_file_size(service_code) or 0
                
                self.metrics.record_download(
                    service_code=service_code,
                    success=True,
                    duration_ms=duration_ms,
                    size_bytes=size,
                    cache_hit=True,
                )
                
                return self.storage.get_file_path(service_code)
            
            # Save streamed content
            file_path, size_bytes = await self.storage.save_stream(
                service_code=service_code,
                content_iterator=content_iterator,
            )
            
            # Compute and save hash
            if self.config.verify_integrity:
                hash_value = self.integrity.compute_hash(file_path)
                self.integrity.save_hash(
                    service_code=service_code,
                    hash_value=hash_value,
                    etag=stream_metadata["etag"],
                )
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            self.metrics.record_download(
                service_code=service_code,
                success=True,
                duration_ms=duration_ms,
                size_bytes=size_bytes,
                cache_hit=False,
            )
            
            self.logger.info(
                "Offer index downloaded",
                extra={
                    "service_code": service_code,
                    "size_bytes": size_bytes,
                    "duration_ms": duration_ms,
                }
            )
            
            return file_path
            
        except asyncio.CancelledError:
            self.logger.warning("Offer index download cancelled")
            raise
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            
            self.metrics.record_download(
                service_code=service_code,
                success=False,
                duration_ms=duration_ms,
                error=str(e),
            )
            
            self.logger.error(
                "Failed to fetch offer index",
                extra={"url": url},
                exc_info=True
            )
            raise DownloadError(
                f"Failed to fetch offer index: {str(e)}",
                service_code=service_code,
                url=url,
            ) from e
    
    async def fetch_service_price(self, service_code: str) -> Path:
        """
        Fetch pricing for a specific service.
        
        Args:
            service_code: AWS service code (e.g., 'AmazonEC2')
            
        Returns:
            Path to the downloaded pricing file
            
        Raises:
            DownloadError: On download failure
        """
        url = f"{self.config.base_url}/{service_code}/current/index.json"
        
        self.logger.info(
            "Fetching service pricing",
            extra={"service_code": service_code, "url": url}
        )
        
        start_time = time.time()
        
        try:
            # Get metadata first
            metadata = await self.http_client.head(url)
            etag = metadata.get("etag")
            
            # Check if download needed
            if self.config.verify_integrity:
                if not self.integrity.should_download(service_code, etag):
                    self.logger.info(
                        "Using cached service pricing (integrity verified)",
                        extra={"service_code": service_code}
                    )
                    
                    duration_ms = int((time.time() - start_time) * 1000)
                    size = self.storage.get_file_size(service_code) or 0
                    
                    self.metrics.record_download(
                        service_code=service_code,
                        success=True,
                        duration_ms=duration_ms,
                        size_bytes=size,
                        cache_hit=True,
                    )
                    
                    return self.storage.get_file_path(service_code)
            
            # Stream download
            content_iterator, stream_metadata = await self.http_client.stream_get(
                url=url,
                etag=etag,
            )
            
            # Handle cache hit
            if stream_metadata["cache_hit"]:
                duration_ms = int((time.time() - start_time) * 1000)
                size = self.storage.get_file_size(service_code) or 0
                
                self.metrics.record_download(
                    service_code=service_code,
                    success=True,
                    duration_ms=duration_ms,
                    size_bytes=size,
                    cache_hit=True,
                )
                
                return self.storage.get_file_path(service_code)
            
            # Save streamed content
            file_path, size_bytes = await self.storage.save_stream(
                service_code=service_code,
                content_iterator=content_iterator,
            )
            
            # Compute and save hash
            if self.config.verify_integrity:
                hash_value = self.integrity.compute_hash(file_path)
                self.integrity.save_hash(
                    service_code=service_code,
                    hash_value=hash_value,
                    etag=stream_metadata["etag"],
                )
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            self.metrics.record_download(
                service_code=service_code,
                success=True,
                duration_ms=duration_ms,
                size_bytes=size_bytes,
                cache_hit=False,
            )
            
            self.logger.info(
                "Service pricing downloaded",
                extra={
                    "service_code": service_code,
                    "size_bytes": size_bytes,
                    "duration_ms": duration_ms,
                }
            )
            
            return file_path
            
        except asyncio.CancelledError:
            self.logger.warning(
                "Service pricing download cancelled",
                extra={"service_code": service_code}
            )
            raise
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            
            self.metrics.record_download(
                service_code=service_code,
                success=False,
                duration_ms=duration_ms,
                error=str(e),
            )
            
            self.logger.error(
                "Failed to fetch service pricing",
                extra={"service_code": service_code, "url": url},
                exc_info=True
            )
            raise DownloadError(
                f"Failed to fetch pricing for {service_code}: {str(e)}",
                service_code=service_code,
                url=url,
            ) from e
    
    def _parse_offer_index(self, index_path: Path) -> List[str]:
        """
        Parse offer index to extract service codes using orjson.
        
        Args:
            index_path: Path to the index file
            
        Returns:
            List of service codes
        """
        try:
            with open(index_path, "rb") as f:
                index_data = orjson.loads(f.read())
            
            service_codes = list(index_data.get("offers", {}).keys())
            
            self.logger.info(
                "Parsed offer index",
                extra={"service_count": len(service_codes)}
            )
            
            return service_codes
            
        except Exception as e:
            self.logger.error(
                "Failed to parse offer index",
                extra={"index_path": str(index_path)},
                exc_info=True
            )
            raise DownloadError(
                f"Failed to parse offer index: {str(e)}",
                service_code="index",
            ) from e
    
    async def fetch_all_services(self, service_codes: List[str] | None = None) -> List[Path]:
        """
        Fetch pricing for all AWS services or specific list.
        
        Args:
            service_codes: Optional list of specific service codes to download
        
        Returns:
            List of paths to downloaded pricing files
            
        Raises:
            DownloadError: On critical download failure
        """
        self.logger.info("Starting download of all services")
        
        # Fetch offer index if service codes not provided
        if service_codes is None:
            index_path = await self.fetch_offer_index()
            service_codes = self._parse_offer_index(index_path)
        
        self.logger.info(
            "Downloading service pricing files",
            extra={"total_services": len(service_codes)}
        )
        
        # Create download tasks with concurrency limit
        semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)
        
        async def download_with_semaphore(service_code: str) -> Path | None:
            """Download a service with semaphore control and cancellation support."""
            async with semaphore:
                try:
                    # Check for shutdown
                    if self._shutdown_event.is_set():
                        return None
                    
                    return await self.fetch_service_price(service_code)
                except asyncio.CancelledError:
                    self.logger.warning(
                        "Download cancelled",
                        extra={"service_code": service_code}
                    )
                    raise
                except Exception as e:
                    self.logger.error(
                        "Failed to download service",
                        extra={"service_code": service_code},
                        exc_info=True
                    )
                    return None
        
        # Execute all downloads concurrently with cancellation support
        try:
            tasks = [
                download_with_semaphore(service_code)
                for service_code in service_codes
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=False)
            
        except asyncio.CancelledError:
            self.logger.warning("Download cancelled by user")
            self._shutdown_event.set()
            
            # Cancel all pending tasks
            for task in asyncio.all_tasks():
                if not task.done():
                    task.cancel()
            
            raise
        
        # Filter out None results (failed downloads)
        downloaded_paths = [path for path in results if path is not None]
        
        failed_count = len(results) - len(downloaded_paths)
        
        # Log summary
        summary = self.metrics.get_summary()
        
        self.logger.info(
            "Completed downloading all services",
            extra={
                "total_services": len(service_codes),
                "successful": len(downloaded_paths),
                "failed": failed_count,
                **summary,
            }
        )
        
        return downloaded_paths