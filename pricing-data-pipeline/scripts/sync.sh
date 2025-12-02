#!/bin/bash
set -e

# Sync normalized pricing data to PostgreSQL database

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/../output/normalized"

# Parse command-line arguments
# Loop through arguments and find --db-url
DB_URL="$DATABASE_URL"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --db-url) DB_URL="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ -z "$DB_URL" ]; then
    echo "Error: Database URL not provided"
    echo "Usage: $0 --db-url <database-url>"
    echo "   or: export DATABASE_URL=<database-url>"
    exit 1
fi

echo "=========================================="
echo "Syncing Pricing Data to Database"
echo "=========================================="
echo ""

# Check if dump file exists
DUMP_FILE="$OUTPUT_DIR/pricing-dump.sql"

if [ ! -f "$DUMP_FILE" ]; then
    echo "Error: pricing-dump.sql not found at $DUMP_FILE"
    echo "Run './scripts/2-normalize.py' first"
    exit 1
fi

echo "Database: (Hidden for security)" 
echo "Dump file: $DUMP_FILE"
echo ""

# Confirm before proceeding (unless CI is set)
if [ -z "$CI" ]; then
    read -p "This will REPLACE existing pricing data. Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted"
        exit 0
    fi
fi

echo ""
echo "Syncing..."

# Import SQL dump
# Using psql. Assumes psql is installed.
psql "$DB_URL" -f "$DUMP_FILE"

echo ""
echo "=========================================="
echo "Sync completed successfully!"
echo "=========================================="
echo ""

# Show stats (optional, might fail if psql fails or permissions issue, so we mask error)
psql "$DB_URL" -c "SELECT 
    (SELECT COUNT(*) FROM services) as services,
    (SELECT COUNT(*) FROM regions) as regions,
    (SELECT COUNT(*) FROM products) as products,
    (SELECT COUNT(*) FROM prices) as prices;" || echo "Could not fetch stats."

echo ""
