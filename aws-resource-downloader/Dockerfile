# Production-ready Dockerfile for AWS Resource Downloader
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies (none currently needed, but good practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY aws_resource_downloader/ aws_resource_downloader/

# Create data directory
RUN mkdir -p /app/data/resource_metadata

# Default entrypoint
ENTRYPOINT ["python", "-m", "aws_resource_downloader.main"]

# Default command (can be overridden)
CMD ["--help"]
