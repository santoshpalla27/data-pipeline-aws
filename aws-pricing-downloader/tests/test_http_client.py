"""
Tests for HTTP client module.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
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
async def test_fetch_success(test_config):
    """Test successful fetch."""
    async with HttpClient(test_config) as client:
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {
            "ETag": '"abc123"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }
        mock_response.read = AsyncMock(return_value=b'{"test": "data"}')
        
        with patch.object(client._session, 'get', return_value=mock_response):
            result = await client.fetch("https://example.com/test.json")
        
        assert result["content"] == b'{"test": "data"}'
        assert result["etag"] == '"abc123"'
        assert result["status"] == 200
        assert result["cache_hit"] is False


@pytest.mark.asyncio
async def test_fetch_cache_hit(test_config):
    """Test fetch with cache hit (304)."""
    async with HttpClient(test_config) as client:
        # Mock 304 response
        mock_response = AsyncMock()
        mock_response.status = 304
        
        with patch.object(client._session, 'get', return_value=mock_response):
            result = await client.fetch(
                "https://example.com/test.json",
                etag='"abc123"',
            )
        
        assert result["content"] is None
        assert result["status"] == 304
        assert result["cache_hit"] is True


@pytest.mark.asyncio
async def test_fetch_http_error(test_config):
    """Test fetch with HTTP error."""
    async with HttpClient(test_config) as client:
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.text = AsyncMock(return_value="Not Found")
        
        with patch.object(client._session, 'get', return_value=mock_response):
            with pytest.raises(HttpError) as exc_info:
                await client.fetch("https://example.com/notfound.json")
        
        assert exc_info.value.status_code == 404
