# AWS Pricing Downloader v2.0

Enterprise-grade Python package for downloading AWS pricing data with streaming, integrity verification, and metrics.

## Features

- ✅ **Async Downloads**: High-performance async I/O with aiohttp
- ✅ **Streaming**: Memory-efficient 64KB chunk streaming
- ✅ **SHA256 Integrity**: Detect silent AWS updates
- ✅ **Retry Logic**: Exponential backoff with status-aware retry
- ✅ **Structured Logging**: JSON-formatted logs for production
- ✅ **Concurrency Control**: Configurable concurrent download limits
- ✅ **Production Ready**: Docker support, comprehensive error handling
- ✅ **Type Safe**: Pydantic config validation
- ✅ **Metrics Export**: JSON metrics for monitoring

## Installation

### From Source

```bash
git clone https://github.com/example/aws-pricing-downloader.git
cd aws-pricing-downloader
pip install -e .
```

### With Docker

```bash
docker build -t aws-pricing-downloader .
```

## Quick Start

### Download All Services

```bash
aws-price download
```

### Download Specific Services

```bash
aws-price download --services AmazonEC2 AmazonS3 AWSLambda
```

### Download from File

```bash
aws-price download --services $(cat services.txt)
```

### Custom Configuration

```bash
aws-price download \
    --output-dir ./pricing-data \
    --metrics-dir ./metrics \
    --max-concurrent 30 \
    --log-level INFO
```

## Usage

### Python API

```python
import asyncio
from aws_pricing_downloader import PricingDownloader

async def main():
    async with PricingDownloader() as downloader:
        # Download all services
        paths = await downloader.fetch_all_services()
        print(f"Downloaded {len(paths)} pricing files")
        
        # Download specific service
        path = await downloader.fetch_service_price("AmazonEC2")
        print(f"EC2 pricing: {path}")

asyncio.run(main())
```

### Configuration

```python
from aws_pricing_downloader import PricingDownloader, load_config

config = load_config(
    output_dir="./data",
    metrics_dir="./metrics",
    max_concurrent_downloads=50,
    total_timeout=600,
    log_level="INFO",
)

downloader = PricingDownloader(config)
```

## Architecture

```
aws_pricing_downloader/
├── __init__.py          # Package initialization
├── config.py            # Pydantic configuration
├── logger.py            # Structured JSON logging
├── http_client.py       # Async HTTP client with streaming
├── storage.py           # File storage layer
├── integrity.py         # SHA256 verification
├── metrics.py           # Metrics collection
├── downloader.py        # Main downloader orchestration
├── exceptions.py        # Custom exceptions
└── cli.py              # Command-line interface
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `base_url` | `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws` | AWS Pricing API base URL |
| `output_dir` | `data/aws_pricing` | Output directory path |
| `metrics_dir` | `metrics` | Metrics directory path |
| `max_concurrent_downloads` | `50` | Max concurrent downloads |
| `tcp_connector_limit` | `100` | TCP connection pool limit |
| `chunk_size` | `65536` | Streaming chunk size (bytes) |
| `sock_read_timeout` | `30` | Socket read timeout (seconds) |
| `sock_connect_timeout` | `10` | Socket connect timeout (seconds) |
| `total_timeout` | `600` | Total request timeout (seconds) |
| `max_retries` | `5` | Maximum retry attempts |
| `retry_min_wait` | `2` | Minimum retry wait (seconds) |
| `retry_max_wait` | `120` | Maximum retry wait (seconds) |
| `retryable_status_codes` | `{429, 500, 502, 503, 504}` | Status codes to retry |
| `verify_integrity` | `True` | Enable SHA256 verification |
| `log_level` | `INFO` | Logging level |

## Integrity Verification

The downloader uses SHA256 hashing to detect silent AWS updates:

1. **First Download**: Computes SHA256 hash and stores with ETag
2. **Subsequent Downloads**: Checks ETag and verifies file hash
3. **Hash Mismatch**: Re-downloads file automatically
4. **ETag Changed**: Triggers re-download even if hash matches

Storage structure:

```
data/aws_pricing/
├── AmazonEC2.json       # Pricing data
├── AmazonEC2.sha256     # SHA256 + ETag metadata
├── AmazonS3.json
└── AmazonS3.sha256
```

## Error Handling

Custom exceptions with context:

- `DownloadError`: General download failures
- `HttpError`: HTTP-specific errors with status code
- `StorageError`: File storage failures
- `IntegrityError`: Hash verification failures

## Logging

Structured JSON logs:

```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "logger": "aws_pricing_downloader.downloader",
  "message": "Service pricing downloaded",
  "service_code": "AmazonEC2",
  "size_bytes": 524288000,
  "duration_ms": 5678,
  "cache_hit": false
}
```

## Metrics

Exported to `metrics/latest.json`:

```json
{
  "aggregate": {
    "total_downloads": 33,
    "successful_downloads": 33,
    "cache_hits": 15,
    "cache_hit_rate": 0.45,
    "success_rate": 1.0,
    "total_bytes_downloaded": 2456789012,
    "average_duration_ms": 3471.7
  },
  "downloads": [...]
}
```

## Testing

```bash
# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=aws_pricing_downloader --cov-report=html
```

## Performance

- **Memory**: ~100 MB for 50 concurrent 500MB files (streaming)
- **Concurrency**: 50 simultaneous downloads
- **Connection Pooling**: 100 TCP connections
- **Retry Logic**: Exponential backoff (2s - 120s)
- **Integrity**: SHA256 verification on every download

## Production Deployment

### Docker

```bash
docker run --rm \
    -v $(pwd)/data:/data/aws_pricing \
    -v $(pwd)/metrics:/metrics \
    aws-pricing-downloader:latest \
    download --services $(cat services.txt)
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: aws-pricing-downloader
spec:
  schedule: "0 0 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: downloader
            image: aws-pricing-downloader:2.0.0
            volumeMounts:
            - name: data
              mountPath: /data/aws_pricing
            - name: metrics
              mountPath: /metrics
          volumes:
          - name: data
            persistentVolumeClaim:
              claimName: pricing-data
          - name: metrics
            persistentVolumeClaim:
              claimName: pricing-metrics
          restartPolicy: OnFailure
```

## License

MIT License

## Support

- GitHub Issues: https://github.com/example/aws-pricing-downloader/issues
- Email: architect @example.com