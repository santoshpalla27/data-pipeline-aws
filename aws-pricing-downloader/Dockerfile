# Multi-stage build for production-ready container
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /build

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY aws_pricing_downloader/ ./aws_pricing_downloader/
COPY setup.py .
COPY pyproject.toml .
COPY README.md .

# Install application
RUN pip install --no-cache-dir -e .

# Create directories
RUN mkdir -p /data/aws_pricing /cache/aws_pricing

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import aws_pricing_downloader; print('healthy')" || exit 1

# Default command
ENTRYPOINT ["aws-price"]
CMD ["download"]

# Labels
LABEL maintainer="architect @example.com"
LABEL version="1.0.0"
LABEL description="Enterprise AWS Pricing Downloader"
