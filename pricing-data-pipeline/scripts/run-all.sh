#!/bin/bash
set -e

echo "=========================================="
echo "AWS Pricing Data Pipeline - Full Run"
echo "=========================================="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Step 1: Download
echo "Step 1/3: Downloading pricing data from AWS..."
python3 "$SCRIPT_DIR/1-download.py"

echo ""
echo "Step 2/3: Normalizing data..."
python3 "$SCRIPT_DIR/2-normalize.py"

echo ""
echo "Step 3/3: Pipeline complete!"
echo ""
echo "Next steps:"
echo "  1. Review output in: output/normalized/"
echo "  2. Sync to database:"
echo "     ./scripts/3-sync.sh --db-url postgres://user:pass@host/db"
echo ""
