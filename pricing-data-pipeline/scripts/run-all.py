#!/usr/bin/env python3
"""
Master script to run the entire AWS Pricing Data Pipeline.
Executes steps sequentially and stops on error.
"""

import subprocess
import sys
import logging
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Define pipeline steps
PIPELINE_STEPS = [
    {
        'name': 'Download Data',
        'script': '1-download.py',
        'description': 'Downloading pricing data from AWS API...'
    },
    {
        'name': 'Validate Raw Data',
        'script': '1.5-validate-raw.py',
        'description': 'Validating downloaded JSON files...'
    },
    {
        'name': 'Normalize Data',
        'script': '2-normalize.py',
        'description': 'Normalizing data into SQLite database...'
    },
    {
        'name': 'Validate Normalized Data',
        'script': '4-validate.py',
        'description': 'Validating normalized database...'
    },
    {
        'name': 'Test Queries',
        'script': '5-test-queries.py',
        'description': 'Running sample queries to verify data access...'
    }
]

def run_step(step):
    """Run a single pipeline step"""
    script_path = SCRIPT_DIR / step['script']
    
    logger.info("=" * 60)
    logger.info(f"STEP: {step['name']}")
    logger.info(step['description'])
    logger.info(f"Script: {script_path.name}")
    logger.info("=" * 60)
    
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False
    
    start_time = time.time()
    
    try:
        # Run script using the same python interpreter
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,  # Run from project root
            check=False  # Don't raise exception immediately
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"✓ {step['name']} completed successfully in {duration:.2f}s")
            return True
        else:
            logger.error(f"❌ {step['name']} failed with exit code {result.returncode}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error executing {step['name']}: {e}")
        return False

def main():
    logger.info("Starting AWS Pricing Data Pipeline")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info(f"Steps to run: {len(PIPELINE_STEPS)}")
    
    total_start = time.time()
    
    for i, step in enumerate(PIPELINE_STEPS, 1):
        logger.info(f"\nRunning step {i}/{len(PIPELINE_STEPS)}...")
        success = run_step(step)
        
        if not success:
            logger.error("\nPipeline aborted due to failure.")
            sys.exit(1)
            
    total_duration = time.time() - total_start
    logger.info("\n" + "=" * 60)
    logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info("=" * 60)

if __name__ == '__main__':
    main()
