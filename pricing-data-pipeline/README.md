# AWS Pricing Data Pipeline

**Standalone tool** for downloading and normalizing AWS pricing data.  
Run this **separately** from your main application whenever you want to update prices.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure AWS credentials
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-east-1"

# 3. Run the full pipeline
./scripts/run-all.sh

# 4. Sync to your database
./scripts/3-sync.sh --db-url "postgres://user:pass@host:5432/pricing_db"
```

## What It Does
*   Downloads pricing JSON from AWS Pricing API
*   Normalizes into SQL format (products + prices tables)
*   Generates database dumps you can import

## Output Files
`output/`
*   `pricing-dump.sql`          # PostgreSQL dump (full schema)
*   `pricing.db`                # SQLite database (portable)
*   `products.csv`              # Products as CSV
*   `prices.csv`                # Prices as CSV

## Usage Scenarios

### Scenario 1: Initial Setup
```bash
./scripts/run-all.sh
./scripts/3-sync.sh --db-url "$DATABASE_URL"
```

or 

python /scripts/1-download.py
python /scripts/2-normalize.py
python /scripts/3-sync.sh --db-url "$DATABASE_URL"


### Scenario 2: Monthly Update
```bash
# Run on your laptop or CI/CD
./scripts/run-all.sh

# Upload to production
./scripts/3-sync.sh --db-url "$PROD_DATABASE_URL"
```

### Scenario 3: Development
```bash
# Generate SQLite file for local dev
./scripts/run-all.sh

# Use in your app
cp output/normalized/pricing.db ../my-app/pricing.db
```

## Schedule Updates (Optional)

### Cron (Linux/Mac)
```bash
# Monthly on 1st at 2 AM
0 2 1 * * /opt/pricing-pipeline/scripts/run-all.sh
```

### GitHub Actions (CI/CD)
See `.github/workflows/update-pricing.yml` example in the project.

## Services Supported
*   ✅ All 25 services with API access
*   ✅ Services defined in `scripts/1-download.py`
