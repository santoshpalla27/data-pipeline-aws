#!/bin/bash
set -e

echo "=========================================="
echo "AWS Pricing Data - Complete Validation"
echo "=========================================="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Step 1: Run data validation
echo "Step 1/2: Validating data integrity..."
python3 "$SCRIPT_DIR/4-validate.py"

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Data validation failed. Fix errors before proceeding."
    exit 1
fi

echo ""
echo "Step 2/2: Testing application queries..."
python3 "$SCRIPT_DIR/5-test-queries.py"

if [ $? -ne 0 ]; then
    echo ""
    echo "⚠️  Query tests had failures. Review results."
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ All validations passed!"
echo "Data is ready for production use"
echo "=========================================="
echo ""
echo "Reports available in: output/validation/"