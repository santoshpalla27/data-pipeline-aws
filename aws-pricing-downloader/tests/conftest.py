"""
Pytest fixtures and configuration.
"""

import pytest
import tempfile
from pathlib import Path

from aws_pricing_downloader.config import load_config, DownloaderConfig


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_config(temp_dir: Path) -> DownloaderConfig:
    """Create test configuration."""
    return load_config(
        output_dir=temp_dir / "output",
        metrics_dir=temp_dir / "metrics",
        max_concurrent_downloads=5,
        tcp_connector_limit=10,
        total_timeout=30,
        max_retries=3,
        log_level="DEBUG",
    )


@pytest.fixture
def mock_offer_index() -> dict:
    """Mock offer index response."""
    return {
        "offers": {
            "AmazonEC2": {
                "offerCode": "AmazonEC2",
                "versionIndexUrl": "/offers/v1.0/aws/AmazonEC2/index.json",
                "currentVersionUrl": "/offers/v1.0/aws/AmazonEC2/current/index.json",
            },
            "AmazonS3": {
                "offerCode": "AmazonS3",
                "versionIndexUrl": "/offers/v1.0/aws/AmazonS3/index.json",
                "currentVersionUrl": "/offers/v1.0/aws/AmazonS3/current/index.json",
            },
        }
    }


@pytest.fixture
def mock_service_pricing() -> dict:
    """Mock service pricing response."""
    return {
        "formatVersion": "v1.0",
        "offerCode": "AmazonEC2",
        "version": "20231201000000",
        "publicationDate": "2023-12-01T00:00:00Z",
        "products": {
            "test-product-1": {
                "sku": "test-product-1",
                "productFamily": "Compute Instance",
            }
        },
        "terms": {
            "OnDemand": {},
            "Reserved": {},
        }
    }