"""
Tests for downloader module.
"""

import pytest
import orjson
from unittest.mock import AsyncMock, patch, MagicMock

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
        # Mock HEAD request
        mock_head = AsyncMock(return_value={
            "etag": '"abc123"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "content_length": 1024,
            "status": 200,
        })
        
        # Mock streaming GET
        async def mock_iterator():
            yield orjson.dumps(mock_offer_index)
        
        mock_stream_get = AsyncMock(return_value=(
            mock_iterator(),
            {
                "etag": '"abc123"',
                "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
                "content_length": 1024,
                "status": 200,
                "cache_hit": False,
            }
        ))
        
        with patch.object(downloader.http_client, 'head', mock_head):
            with patch.object(downloader.http_client, 'stream_get', mock_stream_get):
                output_path = await downloader.fetch_offer_index()
        
        assert output_path.exists()
        assert output_path.name == "index.json"
        
        # Verify content
        with open(output_path, "rb") as f:
            data = orjson.loads(f.read())
        assert "offers" in data


@pytest.mark.asyncio
async def test_fetch_service_price(test_config, mock_service_pricing):
    """Test fetching service pricing."""
    async with PricingDownloader(test_config) as downloader:
        # Mock HEAD request
        mock_head = AsyncMock(return_value={
            "etag": '"xyz789"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "content_length": 2048,
            "status": 200,
        })
        
        # Mock streaming GET
        async def mock_iterator():
            yield orjson.dumps(mock_service_pricing)
        
        mock_stream_get = AsyncMock(return_value=(
            mock_iterator(),
            {
                "etag": '"xyz789"',
                "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
                "content_length": 2048,
                "status": 200,
                "cache_hit": False,
            }
        ))
        
        with patch.object(downloader.http_client, 'head', mock_head):
            with patch.object(downloader.http_client, 'stream_get', mock_stream_get):
                output_path = await downloader.fetch_service_price("AmazonEC2")
        
        assert output_path.exists()
        assert output_path.name == "AmazonEC2.json"
        
        # Verify content
        with open(output_path, "rb") as f:
            data = orjson.loads(f.read())
        assert data["offerCode"] == "AmazonEC2"


@pytest.mark.asyncio
async def test_fetch_with_cache_hit(test_config, mock_service_pricing):
    """Test fetching with cache hit (304)."""
    async with PricingDownloader(test_config) as downloader:
        service_code = "AmazonEC2"
        
        # Pre-save file
        content = orjson.dumps(mock_service_pricing)
        file_path = downloader.storage.get_file_path(service_code)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(content)
        
        # Save hash
        if downloader.config.verify_integrity:
            hash_value = downloader.integrity.compute_hash(file_path)
            downloader.integrity.save_hash(service_code, hash_value, '"xyz789"')
        
        # Mock HEAD request
        mock_head = AsyncMock(return_value={
            "etag": '"xyz789"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "content_length": len(content),
            "status": 200,
        })
        
        with patch.object(downloader.http_client, 'head', mock_head):
            output_path = await downloader.fetch_service_price(service_code)
        
        assert output_path.exists()
        
        # Verify metrics recorded cache hit
        assert downloader.metrics.aggregate.cache_hits > 0


@pytest.mark.asyncio
async def test_parse_offer_index(test_config, mock_offer_index):
    """Test parsing offer index."""
    downloader = PricingDownloader(test_config)
    
    # Write mock index
    index_path = test_config.output_dir / "index.json"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with open(index_path, "wb") as f:
        f.write(orjson.dumps(mock_offer_index))
    
    service_codes = downloader._parse_offer_index(index_path)
    
    assert len(service_codes) == 2
    assert "AmazonEC2" in service_codes
    assert "AmazonS3" in service_codes


@pytest.mark.asyncio
async def test_parse_offer_index_with_null_offers(test_config):
    """Test parsing offer index when offers is null."""
    downloader = PricingDownloader(test_config)
    
    # Write mock index with null offers
    index_path = test_config.output_dir / "index.json"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with open(index_path, "wb") as f:
        f.write(orjson.dumps({"offers": None}))
    
    service_codes = downloader._parse_offer_index(index_path)
    
    # Should return empty list, not crash
    assert service_codes == []