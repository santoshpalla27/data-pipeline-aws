#!/usr/bin/env python3
"""
Sync normalized pricing data to PostgreSQL database.
Loads configuration from .env file.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR.parent / 'output' / 'normalized'
DUMP_FILE = OUTPUT_DIR / 'pricing-dump.sql'

def check_psql():
    """Check if psql is available in PATH"""
    try:
        subprocess.run(['psql', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def main():
    # Load environment variables
    load_dotenv()
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.error("DATABASE_URL not found in .env file or environment variables")
        logger.info("Please add DATABASE_URL=postgres://user:pass@host:5432/dbname to your .env file")
        sys.exit(1)
        
    if not DUMP_FILE.exists():
        logger.error(f"Dump file not found at {DUMP_FILE}")
        logger.info("Run '2-normalize.py' first to generate the dump file")
        sys.exit(1)

    # Check for psql
    if not check_psql():
        logger.error("'psql' command not found. Please ensure PostgreSQL client tools are installed and in your PATH.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Syncing Pricing Data to Database")
    logger.info("=" * 60)
    logger.info(f"Dump file: {DUMP_FILE}")
    
    # Mask password for display
    masked_url = db_url
    if '@' in db_url:
        prefix = db_url.split('@')[0]
        suffix = db_url.split('@')[1]
        if ':' in prefix:
            proto_user = prefix.split(':')[0] + ':' + prefix.split(':')[1]
            masked_url = f"{proto_user}:****@{suffix}"
    
    logger.info(f"Target DB: {masked_url}")
    
    # Confirmation (skip if CI or force flag)
    if not os.getenv('CI') and '--force' not in sys.argv:
        response = input("\nThis will REPLACE existing pricing data. Continue? (y/n) ")
        if response.lower() != 'y':
            logger.info("Aborted")
            sys.exit(0)
            
    logger.info("\nImporting data (this may take a while)...")
    
    try:
        # Run psql command
        # We pass the DB URL directly to psql
        cmd = ['psql', db_url, '-f', str(DUMP_FILE)]
        
        # On Windows, we might need shell=True if psql is not in path directly but via batch file, 
        # but check_psql should have caught that.
        subprocess.run(cmd, check=True)
        
        logger.info("\n✓ Sync completed successfully!")
        
        # Show stats
        logger.info("\nVerifying import counts...")
        stats_query = """
        SELECT 
            (SELECT COUNT(*) FROM services) as services,
            (SELECT COUNT(*) FROM regions) as regions,
            (SELECT COUNT(*) FROM products) as products,
            (SELECT COUNT(*) FROM prices) as prices;
        """
        subprocess.run(['psql', db_url, '-c', stats_query], check=True)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"\n❌ Sync failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        logger.error(f"\n❌ An error occurred: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
