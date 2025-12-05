"""
Async HTTP client with streaming, retry logic, and connection pooling.
"""

import asyncio
import logging  # ← ADD THIS LINE
import time
from typing import AsyncIterator
from pathlib import Path

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from aws_pricing_downloader.config import DownloaderConfig
from aws_pricing_downloader.logger import get_logger
from aws_pricing_downloader.exceptions import HttpError, DownloadError


logger = get_logger(__name__)


class HttpClient:
    """Async HTTP client with retry, streaming, and connection pooling."""
    
    def __init__(self, config: DownloaderConfig):
        """
        Initialize HTTP client.
        
        Args:
            config: Downloader configuration
        """
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._connector: aiohttp.TCPConnector | None = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Start the HTTP client session."""
        if self._session is None:
            self._connector = aiohttp.TCPConnector(
                limit=self.config.tcp_connector_limit,
                limit_per_host=30,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
            )
            
            timeout = aiohttp.ClientTimeout(
                total=self.config.total_timeout,
                sock_read=self.config.sock_read_timeout,
                sock_connect=self.config.sock_connect_timeout,
            )
            
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=timeout,
                headers={
                    "User-Agent": self.config.user_agent,
                    "Accept": "application/json",
                },
                raise_for_status=False,
            )
            
            logger.info(
                "HTTP client started",
                extra={
                    "tcp_limit": self.config.tcp_connector_limit,
                    "total_timeout": self.config.total_timeout,
                    "sock_read_timeout": self.config.sock_read_timeout,
                }
            )
    
    async def close(self):
        """Close the HTTP client session gracefully."""
        if self._session:
            await self._session.close()
            # Wait for proper cleanup
            await asyncio.sleep(0.25)
            self._session = None
            logger.info("HTTP client closed")
        
        if self._connector:
            await self._connector.close()
            self._connector = None
    
    def _should_retry(self, status_code: int) -> bool:
        """Determine if status code should be retried."""
        return status_code in self.config.retryable_status_codes
    
    def _create_retry_decorator(self):
        """Create retry decorator with configured settings."""
        def retry_condition(exception):
            """Custom retry condition."""
            if isinstance(exception, HttpError):
                # Only retry specific status codes
                if exception.status_code is not None:
                    return self._should_retry(exception.status_code)
                # Retry if no status code (network error)
                return True
            # Retry on network errors
            return isinstance(exception, (aiohttp.ClientError, asyncio.TimeoutError))
        
        return retry(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(
                min=self.config.retry_min_wait,
                max=self.config.retry_max_wait,
            ),
            retry=retry_condition,
            retry_error_callback=lambda retry_state: None,
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
    
    async def head(
        self,
        url: str,
    ) -> dict[str, str | int]:
        """
        Perform HEAD request to get metadata.
        
        Args:
            url: URL to request
            
        Returns:
            Dictionary with etag, last_modified, content_length, status
            
        Raises:
            HttpError: On HTTP errors
        """
        if not self._session:
            raise DownloadError("HTTP client not started. Call start() first.")
        
        retry_decorator = self._create_retry_decorator()
        
        @retry_decorator
        async def _head_with_retry():
            start_time = time.time()
            
            async with self._session.head(url) as response:
                duration_ms = int((time.time() - start_time) * 1000)
                
                if response.status >= 400 and not self._should_retry(response.status):
                    error_body = await response.text()
                    logger.error(
                        "HEAD request failed",
                        extra={
                            "url": url,
                            "status_code": response.status,
                            "duration_ms": duration_ms,
                        }
                    )
                    raise HttpError(
                        f"HTTP {response.status} error for {url}",
                        status_code=response.status,
                        url=url,
                        response_body=error_body,
                    )
                
                result = {
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "content_length": int(response.headers.get("Content-Length", 0)),
                    "status": response.status,
                }
                
                logger.debug(
                    "HEAD request successful",
                    extra={
                        "url": url,
                        "status_code": response.status,
                        "etag": result["etag"],
                        "content_length": result["content_length"],
                        "duration_ms": duration_ms,
                    }
                )
                
                return result
        
        try:
            return await _head_with_retry()
        except Exception as e:
            logger.error(
                "HEAD request failed after retries",
                extra={"url": url},
                exc_info=True
            )
            raise
    
    async def stream_get(
        self,
        url: str,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> tuple[AsyncIterator[bytes], dict]:
        """
        Stream GET request with conditional headers.
        
        Args:
            url: URL to fetch
            etag: Optional ETag for conditional request
            last_modified: Optional Last-Modified for conditional request
            
        Returns:
            Tuple of (content_iterator, metadata)
            metadata contains: etag, last_modified, content_length, status, cache_hit
            
        Raises:
            HttpError: On HTTP errors
        """
        if not self._session:
            raise DownloadError("HTTP client not started. Call start() first.")
        
        # Prepare conditional headers
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        
        retry_decorator = self._create_retry_decorator()
        
        @retry_decorator
        async def _stream_with_retry():
            start_time = time.time()
            
            response = await self._session.get(url, headers=headers)
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Handle 304 Not Modified
            if response.status == 304:
                await response.release()
                logger.info(
                    "Cache valid - not modified",
                    extra={
                        "url": url,
                        "status_code": 304,
                        "duration_ms": duration_ms,
                        "cache_hit": True,
                    }
                )
                
                async def empty_iterator():
                    return
                    yield  # Make it a generator
                
                metadata = {
                    "etag": etag,
                    "last_modified": last_modified,
                    "content_length": 0,
                    "status": 304,
                    "cache_hit": True,
                }
                
                return empty_iterator(), metadata
            
            # Handle error responses
            if response.status >= 400:
                error_body = await response.text()
                await response.release()
                
                logger.error(
                    "GET request failed",
                    extra={
                        "url": url,
                        "status_code": response.status,
                        "duration_ms": duration_ms,
                    }
                )
                
                # Raise with status code for retry logic
                raise HttpError(
                    f"HTTP {response.status} error for {url}",
                    status_code=response.status,
                    url=url,
                    response_body=error_body,
                )
            
            # Create streaming iterator
            async def content_iterator():
                try:
                    async for chunk in response.content.iter_chunked(self.config.chunk_size):
                        yield chunk
                finally:
                    await response.release()
            
            metadata = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": int(response.headers.get("Content-Length", 0)),
                "status": response.status,
                "cache_hit": False,
            )
            
            logger.info(
                "Streaming GET request started",
                extra={
                    "url": url,
                    "status_code": response.status,
                    "content_length": metadata["content_length"],
                    "duration_ms": duration_ms,
                    "cache_hit": False,
                }
            )
            
            return content_iterator(), metadata
        
        try:
            return await _stream_with_retry()
        except Exception as e:
            logger.error(
                "GET request failed after retries",
                extra={"url": url},
                exc_info=True
            )
            raise