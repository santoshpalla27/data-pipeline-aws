#!/usr/bin/env python3
"""
Download AWS pricing data from AWS Pricing API.
Saves raw JSON files to output/raw/
"""

import boto3
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from dotenv import load_dotenv

# Load .env from project root
SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / '.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS services to download
SERVICES = [
    'AmazonEC2', 'AmazonRDS', 'AWSLambda', 'AmazonS3', 'AmazonDynamoDB',
    'AmazonElastiCache', 'AmazonES', 'AmazonCloudFront', 'AmazonRoute53',
    'AWSELB', 'AmazonEKS', 'AmazonECS', 'AWSSystemsManager', 'AmazonVPC',
    'AmazonApiGateway', 'AmazonEFS', 'AmazonMQ', 'AmazonMSK', 'AmazonSNS',
    'awskms', 'AWSSecretsManager', 'AmazonCloudWatch', 'AWSCloudTrail',
    'AWSXRay', 'AWSConfig'
]

# Standard AWS regions
REGIONS = [
    'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
    'eu-west-1', 'eu-west-2', 'eu-central-1',
    'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1',
    'ap-south-1', 'sa-east-1', 'ca-central-1'
]

# Determine output directory relative to script location or current working directory
SCRIPT_DIR = Path(__file__).parent
# If running from root, SCRIPT_DIR is pricing-data-pipeline/scripts
# We want pricing-data-pipeline/output/raw
OUTPUT_DIR = SCRIPT_DIR.parent / 'output' / 'raw'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class PricingDownloader:
    def __init__(self):
        # AWS Pricing API is only available in us-east-1 or ap-south-1
        self.client = boto3.client('pricing', region_name='us-east-1')
        self.stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def download_service_region(self, service, region):
        """Download pricing for a specific service and region"""
        try:
            # Get price list
            # Note: Some services might not have RegionCode filter or behave differently
            # But for standard services this is the common pattern.
            
            response = self.client.list_price_lists(
                ServiceCode=service,
                RegionCode=region,
                CurrencyCode='USD',
                EffectiveDate=datetime.now()
            )
            
            if not response.get('PriceLists'):
                logger.debug(f"No pricing for {service} in {region}")
                self.stats['skipped'] += 1
                return None
            
            price_list_arn = response['PriceLists'][0]['PriceListArn']
            
            # Get download URL
            file_response = self.client.get_price_list_file_url(
                PriceListArn=price_list_arn,
                FileFormat='json'
            )
            
            url = file_response['Url']
            
            # Download JSON
            import requests
            data = requests.get(url, timeout=300).json()
            
            # Save to file
            output_path = OUTPUT_DIR / service / region / 'pricing.json'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"✓ {service}/{region} - {len(data.get('products', {}))} products")
            self.stats['successful'] += 1
            return output_path
            
        except self.client.exceptions.ResourceNotFoundException:
            logger.debug(f"Service {service} not available in {region}")
            self.stats['skipped'] += 1
            return None
        except Exception as e:
            # Some services might fail if they don't support region filtering in list_price_lists
            # or if the API call fails for other reasons.
            logger.error(f"✗ {service}/{region} - {e}")
            self.stats['failed'] += 1
            return None
    
    def download_all(self, max_workers=5):
        """Download all services in parallel"""
        tasks = []
        
        for service in SERVICES:
            for region in REGIONS:
                tasks.append((service, region))
        
        self.stats['total'] = len(tasks)
        
        logger.info(f"Downloading {len(tasks)} service/region combinations...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.download_service_region, svc, rgn): (svc, rgn)
                for svc, rgn in tasks
            }
            
            for future in as_completed(futures):
                future.result()  # Wait for completion
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("="*60)
        logger.info(f"Total attempts: {self.stats['total']}")
        logger.info(f"Successful: {self.stats['successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Skipped (not available): {self.stats['skipped']}")
        logger.info("="*60)
        
        return self.stats['successful'] > 0

def main():
    logger.info("Starting AWS Pricing Download")
    logger.info(f"Services: {len(SERVICES)}")
    logger.info(f"Regions: {len(REGIONS)}")
    logger.info(f"Output directory: {OUTPUT_DIR.absolute()}")
    
    downloader = PricingDownloader()
    # Using fewer workers to avoid hitting API rate limits too hard
    success = downloader.download_all(max_workers=3)
    
    if not success:
        logger.error("No pricing data downloaded!")
        sys.exit(1)
    
    logger.info("Download completed successfully")

if __name__ == '__main__':
    main()
