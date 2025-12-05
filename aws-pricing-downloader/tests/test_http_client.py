"""
Tests for HTTP client module.
"""

import pytest
from unittest.mock import AsyncMock, patch
import aiohttp

from aws_pricing_downloader.http_client import HttpClient
from aws_pricing_downloader.exceptions import HttpError


@pytest.mark.asyncio
async def test_http_client_lifecycle(test_config):
    """Test HTTP client start and close."""
    client = HttpClient(test_config)
    
    # Should not be started
    assert client._session is None
    
    # Start
    await client.start()
    assert client._session is not None
    assert client._connector is not None
    
    # Close
    await client.close()
    assert client._session is None


@pytest.mark.asyncio
async def test_http_client_context_manager(test_config):
    """Test HTTP client as context manager."""
    async with HttpClient(test_config) as client:
        assert client._session is not None
    
    # Should be closed after context
    assert client._session is None


@pytest.mark.asyncio
async def test_head_success(test_config):
    """Test successful HEAD request."""
    async with HttpClient(test_config) as client:
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {
            "ETag": '"abc123"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "Content-Length": "1024",
        }
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()
        
        with patch.object(client._session, 'head', return_value=mock_response):
            result = await client.head("https://example.com/test.json")
        
        assert result["etag"] == '"abc123"'
        assert result["status"] == 200
        assert result["content_length"] == 1024


@pytest.mark.asyncio
async def test_stream_get_success(test_config):
    """Test successful streaming GET request."""
    async with HttpClient(test_config) as client:
        # Mock response
        test_data = b'{"test": "data"}'
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {
            "ETag": '"abc123"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "Content-Length": str(len(test_data)),
        }
        
        # Mock streaming content
        async def mock_iter_chunked(size):
            yield test_data
        
        mock_response.content.iter_chunked = mock_iter_chunked
        mock_response.release = AsyncMock()
        
        with patch.object(client._session, 'get', return_value=mock_response):
            iterator, metadata = await client.stream_get("https://example.com/test.json")
            
            # Consume iterator
            chunks = []
            async for chunk in iterator:
                chunks.append(chunk)
            
            content = b"".join(chunks)
        
        assert content == test_data
        assert metadata["etag"] == '"abc123"'
        assert metadata["status"] == 200
        assert metadata["cache_hit"] is False


@pytest.mark.asyncio
async def test_stream_get_cache_hit(test_config):
    """Test streaming GET with cache hit (304)."""
    async with HttpClient(test_config) as client:
        # Mock 304 response
        mock_response = AsyncMock()
        mock_response.status = 304
        mock_response.release = AsyncMock()
        
        with patch.object(client._session, 'get', return_value=mock_response):
            iterator, metadata = await client.stream_get(
                "https://example.com/test.json",
                etag='"abc123"',
            )
        
        assert metadata["status"] == 304
        assert metadata["cache_hit"] is True


@pytest.mark.asyncio
async def test_head_http_error(test_config):
    """Test HEAD with HTTP error."""
    async with HttpClient(test_config) as client:
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.text = AsyncMock(return_value="Not Found")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()
        
        with patch.object(client._session, 'head', return_value=mock_response):
            with pytest.raises(HttpError) as exc_info:
                await client.head("https://example.com/notfound.json")
        
        assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_stream_get_http_error(test_config):
    """Test streaming GET with HTTP error."""
    async with HttpClient(test_config) as client:
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_response.release = AsyncMock()
        
        with patch.object(client._session, 'get', return_value=mock_response):
            with pytest.raises(HttpError) as exc_info:
                await client.stream_get("https://example.com/error.json")
        
        assert exc_info.value.status_code == 500