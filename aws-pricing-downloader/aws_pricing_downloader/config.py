"""
Configuration management with Pydantic validation.
"""

from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, validator


class DownloaderConfig(BaseModel):
    """Configuration for AWS Pricing Downloader."""
    
    # AWS Pricing API base URL
    base_url: str = Field(
        default="https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws",
        description="Base URL for AWS Pricing API"
    )
    
    # Storage directory (no separate cache)
    output_dir: Path = Field(
        default=Path("data/aws_pricing"),
        description="Directory for storing downloaded pricing files"
    )
    
    # Metrics directory
    metrics_dir: Path = Field(
        default=Path("metrics"),
        description="Directory for metrics output"
    )
    
    # HTTP settings
    max_concurrent_downloads: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of concurrent downloads"
    )
    
    tcp_connector_limit: int = Field(
        default=100,
        ge=1,
        le=500,
        description="TCP connector pool limit"
    )
    
    # Streaming chunk size (64 KB)
    chunk_size: int = Field(
        default=65536,
        ge=8192,
        description="Chunk size for streaming downloads (bytes)"
    )
    
    # Socket-level timeouts
    sock_read_timeout: int = Field(
        default=30,
        ge=5,
        description="Socket read timeout (seconds)"
    )
    
    sock_connect_timeout: int = Field(
        default=10,
        ge=5,
        description="Socket connect timeout (seconds)"
    )
    
    total_timeout: int = Field(
        default=600,
        ge=60,
        description="Total request timeout (seconds)"
    )
    
    # Retry settings
    max_retries: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum number of retry attempts"
    )
    
    retry_min_wait: int = Field(
        default=2,
        ge=1,
        description="Minimum wait time between retries (seconds)"
    )
    
    retry_max_wait: int = Field(
        default=120,
        ge=1,
        description="Maximum wait time between retries (seconds)"
    )
    
    # Retryable status codes
    retryable_status_codes: set[int] = Field(
        default={429, 500, 502, 503, 504},
        description="HTTP status codes to retry"
    )
    
    # User agent
    user_agent: str = Field(
        default="AWS-Pricing-Downloader/2.0.0",
        description="User agent for HTTP requests"
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    log_file: Optional[Path] = Field(
        default=None,
        description="Optional log file path"
    )
    
    # Integrity verification
    verify_integrity: bool = Field(
        default=True,
        description="Enable SHA256 integrity verification"
    )
    
    @validator("output_dir", "metrics_dir")
    def validate_directory(cls, v: Path) -> Path:
        """Ensure directories are absolute paths."""
        return v.resolve()
    
    @validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v
    
    class Config:
        """Pydantic config."""
        validate_assignment = True
        arbitrary_types_allowed = True


def load_config(**kwargs) -> DownloaderConfig:
    """
    Load configuration with optional overrides.
    
    Args:
        **kwargs: Configuration overrides
        
    Returns:
        DownloaderConfig instance
    """
    return DownloaderConfig(**kwargs)