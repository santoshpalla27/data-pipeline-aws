"""
Tests for downloader module.
"""

import pytest
import json
from unittest.mock import AsyncMock, patch

from aws_pricing_downloader.downloader import PricingDownloader
from aws_pricing_downloader.exceptions import DownloadError


@pytest.mark.asyncio
async def test_downloader_lifecycle(test_config):
    """Test downloader start and close."""
    downloader = PricingDownloader(test_config)
    
    assert downloader.http_client is None
    
    await downloader.start()
    assert downloader.http_client is not None
    
    await downloader.close()
    assert downloader.http_client is None


@pytest.mark.asyncio
async def test_downloader_context_manager(test_config):
    """Test downloader as context manager."""
    async with PricingDownloader(test_config) as downloader:
        assert downloader.http_client is not None
    
    assert downloader.http_client is None


@pytest.mark.asyncio
async def test_fetch_offer_index(test_config, mock_offer_index):
    """Test fetching offer index."""
    async with PricingDownloader(test_config) as downloader:
        # Mock HTTP client
        mock_fetch = AsyncMock(return_value={
            "content": json.dumps(mock_offer_index).encode(),
            "etag": '"abc123"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "status": 200,
            "cache_hit": False,
        })
        
        with patch.object(downloader.http_client, 'fetch', mock_fetch):
            output_path = await downloader.fetch_offer_index()
        
        assert output_path.exists()
        assert output_path.name == "index.json"
        
        # Verify content
        with open(output_path, "r") as f:
            data = json.load(f)
        assert "offers" in data


@pytest.mark.asyncio
async def test_fetch_service_price(test_config, mock_service_pricing):
    """Test fetching service pricing."""
    async with PricingDownloader(test_config) as downloader:
        # Mock HTTP client
        mock_fetch = AsyncMock(return_value={
            "content": json.dumps(mock_service_pricing).encode(),
            "etag": '"xyz789"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "status": 200,
            "cache_hit": False,
        })
        
        with patch.object(downloader.http_client, 'fetch', mock_fetch):
            output_path = await downloader.fetch_service_price("AmazonEC2")
        
        assert output_path.exists()
        assert output_path.name == "AmazonEC2.json"
        
        # Verify content
        with open(output_path, "r") as f:
            data = json.load(f)
        assert data["offerCode"] == "AmazonEC2"


@pytest.mark.asyncio
async def test_fetch_with_cache(test_config, mock_service_pricing):
    """Test fetching with cache hit."""
    async with PricingDownloader(test_config) as downloader:
        service_code = "AmazonEC2"
        content = json.dumps(mock_service_pricing).encode()
        
        # Pre-populate cache
        downloader.cache_manager.save_cache(
            service_code=service_code,
            content=content,
            etag='"xyz789"',
        )
        
        # Mock 304 response
        mock_fetch = AsyncMock(return_value={
            "content": None,
            "etag": '"xyz789"',
            "last_modified": None,
            "status": 304,
            "cache_hit": True,
        })
        
        with patch.object(downloader.http_client, 'fetch', mock_fetch):
            output_path = await downloader.fetch_service_price(service_code)
        
        assert output_path.exists()
        
        # Verify cached content was used
        with open(output_path, "rb") as f:
            assert f.read() == content


@pytest.mark.asyncio
async def test_parse_offer_index(test_config, mock_offer_index):
    """Test parsing offer index."""
    downloader = PricingDownloader(test_config)
    
    # Write mock index
    index_path = test_config.output_dir / "index.json"
    with open(index_path, "w") as f:
        json.dump(mock_offer_index, f)
    
    service_codes = downloader._parse_offer_index(index_path)
    
    assert len(service_codes) == 2
    assert "AmazonEC2" in service_codes
    assert "AmazonS3" in service_codes
