"""
Storage layer for downloaded pricing files.
"""

import asyncio
from pathlib import Path
from typing import AsyncIterator
import aiofiles

from aws_pricing_downloader.config import DownloaderConfig
from aws_pricing_downloader.logger import get_logger
from aws_pricing_downloader.exceptions import StorageError


logger = get_logger(__name__)


class StorageManager:
    """Manage file storage for downloaded pricing data."""
    
    def __init__(self, config: DownloaderConfig):
        """
        Initialize storage manager.
        
        Args:
            config: Downloader configuration
        """
        self.config = config
        self.output_dir = config.output_dir
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories."""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(
                "Storage directories ready",
                extra={"output_dir": str(self.output_dir)}
            )
        except Exception as e:
            raise StorageError(
                f"Failed to create storage directories: {str(e)}",
                path=str(self.output_dir)
            ) from e
    
    def get_file_path(self, service_code: str) -> Path:
        """
        Get file path for a service.
        
        Args:
            service_code: Service code
            
        Returns:
            Path to output file
        """
        return self.output_dir / f"{service_code}.json"
    
    async def save_stream(
        self,
        service_code: str,
        content_iterator: AsyncIterator[bytes],
    ) -> tuple[Path, int]:
        """
        Save streamed content to file.
        
        Args:
            service_code: Service code
            content_iterator: Async iterator of content chunks
            
        Returns:
            Tuple of (file_path, total_bytes_written)
            
        Raises:
            StorageError: On write failure
        """
        file_path = self.get_file_path(service_code)
        temp_path = file_path.with_suffix(".tmp")
        
        total_bytes = 0
        
        try:
            async with aiofiles.open(temp_path, "wb") as f:
                async for chunk in content_iterator:
                    await f.write(chunk)
                    total_bytes += len(chunk)
            
            # Atomic rename
            temp_path.rename(file_path)
            
            logger.info(
                "File saved successfully",
                extra={
                    "service_code": service_code,
                    "path": str(file_path),
                    "size_bytes": total_bytes,
                }
            )
            
            return file_path, total_bytes
            
        except Exception as e:
            # Cleanup temp file
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            
            logger.error(
                "Failed to save file",
                extra={"service_code": service_code},
                exc_info=True
            )
            
            raise StorageError(
                f"Failed to save file for {service_code}: {str(e)}",
                path=str(file_path)
            ) from e
    
    def file_exists(self, service_code: str) -> bool:
        """
        Check if file exists for a service.
        
        Args:
            service_code: Service code
            
        Returns:
            True if file exists
        """
        return self.get_file_path(service_code).exists()
    
    def get_file_size(self, service_code: str) -> int | None:
        """
        Get file size for a service.
        
        Args:
            service_code: Service code
            
        Returns:
            File size in bytes, or None if file doesn't exist
        """
        file_path = self.get_file_path(service_code)
        if not file_path.exists():
            return None
        return file_path.stat().st_size
