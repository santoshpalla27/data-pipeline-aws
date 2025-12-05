"""
Tests for storage module.
"""

import pytest
import asyncio

from aws_pricing_downloader.storage import StorageManager


@pytest.mark.asyncio
async def test_storage_initialization(test_config):
    """Test storage manager initialization."""
    storage = StorageManager(test_config)
    assert storage.output_dir.exists()
    assert storage.output_dir.is_dir()


@pytest.mark.asyncio
async def test_save_stream(test_config):
    """Test streaming save."""
    storage = StorageManager(test_config)
    
    service_code = "AmazonEC2"
    test_data = b'{"test": "data"}' * 1000  # 16KB
    
    async def content_iterator():
        # Yield in chunks
        chunk_size = 1024
        for i in range(0, len(test_data), chunk_size):
            yield test_data[i:i+chunk_size]
    
    file_path, size = await storage.save_stream(
        service_code=service_code,
        content_iterator=content_iterator(),
    )
    
    assert file_path.exists()
    assert size == len(test_data)
    
    # Verify content
    with open(file_path, "rb") as f:
        saved_data = f.read()
    assert saved_data == test_data


@pytest.mark.asyncio
async def test_file_exists(test_config):
    """Test file existence check."""
    storage = StorageManager(test_config)
    
    service_code = "AmazonEC2"
    assert not storage.file_exists(service_code)
    
    # Create file
    async def simple_iterator():
        yield b'test'
    
    await storage.save_stream(service_code, simple_iterator())
    
    assert storage.file_exists(service_code)


@pytest.mark.asyncio
async def test_get_file_size(test_config):
    """Test file size retrieval."""
    storage = StorageManager(test_config)
    
    service_code = "AmazonEC2"
    test_data = b'{"test": "data"}'
    
    async def simple_iterator():
        yield test_data
    
    await storage.save_stream(service_code, simple_iterator())
    
    size = storage.get_file_size(service_code)
    assert size == len(test_data)
