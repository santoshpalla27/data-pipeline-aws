FROM python:3.11-slim

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy scripts
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY config/ ./config/

# Create output directory
RUN mkdir -p output/raw output/normalized

ENTRYPOINT ["./scripts/run-all.sh"]

