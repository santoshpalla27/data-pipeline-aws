"""
Metrics collection and reporting.
"""

import time
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
import orjson

from aws_pricing_downloader.logger import get_logger
from aws_pricing_downloader.exceptions import StorageError


logger = get_logger(__name__)


@dataclass
class DownloadMetrics:
    """Metrics for a single download."""
    service_code: str
    success: bool
    duration_ms: int
    size_bytes: int
    cache_hit: bool
    error: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


@dataclass
class AggregateMetrics:
    """Aggregate metrics for all downloads."""
    total_downloads: int = 0
    successful_downloads: int = 0
    failed_downloads: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_bytes_downloaded: int = 0
    total_duration_ms: int = 0
    average_duration_ms: float = 0.0
    cache_hit_rate: float = 0.0
    success_rate: float = 0.0
    start_time: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    end_time: str | None = None


class MetricsCollector:
    """Collect and export metrics."""
    
    def __init__(self, metrics_dir: Path):
        """
        Initialize metrics collector.
        
        Args:
            metrics_dir: Directory for metrics output
        """
        self.metrics_dir = metrics_dir
        self.download_metrics: list[DownloadMetrics] = []
        self.aggregate = AggregateMetrics()
        self.start_time = time.time()
        
        # Ensure metrics directory exists
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
    
    def record_download(
        self,
        service_code: str,
        success: bool,
        duration_ms: int,
        size_bytes: int = 0,
        cache_hit: bool = False,
        error: str | None = None,
    ):
        """
        Record metrics for a download.
        
        Args:
            service_code: Service code
            success: Whether download succeeded
            duration_ms: Duration in milliseconds
            size_bytes: Downloaded size in bytes
            cache_hit: Whether cache was used
            error: Error message if failed
        """
        metric = DownloadMetrics(
            service_code=service_code,
            success=success,
            duration_ms=duration_ms,
            size_bytes=size_bytes,
            cache_hit=cache_hit,
            error=error,
        )
        
        self.download_metrics.append(metric)
        
        # Update aggregates
        self.aggregate.total_downloads += 1
        
        if success:
            self.aggregate.successful_downloads += 1
        else:
            self.aggregate.failed_downloads += 1
        
        if cache_hit:
            self.aggregate.cache_hits += 1
        else:
            self.aggregate.cache_misses += 1
        
        self.aggregate.total_bytes_downloaded += size_bytes
        self.aggregate.total_duration_ms += duration_ms
        
        logger.debug(
            "Recorded download metrics",
            extra={
                "service_code": service_code,
                "success": success,
                "duration_ms": duration_ms,
                "cache_hit": cache_hit,
            }
        )
    
    def finalize(self):
        """Finalize metrics calculation."""
        self.aggregate.end_time = datetime.utcnow().isoformat() + "Z"
        
        # Calculate averages
        if self.aggregate.total_downloads > 0:
            self.aggregate.average_duration_ms = (
                self.aggregate.total_duration_ms / self.aggregate.total_downloads
            )
            self.aggregate.success_rate = (
                self.aggregate.successful_downloads / self.aggregate.total_downloads
            )
            self.aggregate.cache_hit_rate = (
                self.aggregate.cache_hits / self.aggregate.total_downloads
            )
    
    def export_json(self) -> Path:
        """
        Export metrics to JSON file.
        
        Returns:
            Path to exported metrics file
            
        Raises:
            StorageError: On export failure
        """
        self.finalize()
        
        metrics_data = {
            "aggregate": asdict(self.aggregate),
            "downloads": [asdict(m) for m in self.download_metrics],
        }
        
        metrics_path = self.metrics_dir / "latest.json"
        
        try:
            with open(metrics_path, "wb") as f:
                f.write(orjson.dumps(metrics_data, option=orjson.OPT_INDENT_2))
            
            logger.info(
                "Metrics exported",
                extra={
                    "path": str(metrics_path),
                    "total_downloads": self.aggregate.total_downloads,
                    "success_rate": self.aggregate.success_rate,
                    "cache_hit_rate": self.aggregate.cache_hit_rate,
                }
            )
            
            return metrics_path
            
        except Exception as e:
            raise StorageError(
                f"Failed to export metrics: {str(e)}",
                path=str(metrics_path)
            ) from e
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of metrics.
        
        Returns:
            Dictionary with key metrics
        """
        self.finalize()
        
        return {
            "total_downloads": self.aggregate.total_downloads,
            "successful": self.aggregate.successful_downloads,
            "failed": self.aggregate.failed_downloads,
            "cache_hits": self.aggregate.cache_hits,
            "cache_hit_rate": f"{self.aggregate.cache_hit_rate:.1%}",
            "success_rate": f"{self.aggregate.success_rate:.1%}",
            "total_bytes": self.aggregate.total_bytes_downloaded,
            "avg_duration_ms": int(self.aggregate.average_duration_ms),
        }
