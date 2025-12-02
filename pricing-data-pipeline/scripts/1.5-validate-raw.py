#!/usr/bin/env python3
"""
Validate raw AWS pricing JSON data after download.
Catches data quality issues before normalization.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR.parent / 'output' / 'raw'
REPORT_DIR = SCRIPT_DIR.parent / 'output' / 'validation'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Expected services (from download script)
EXPECTED_SERVICES = [
    'AmazonEC2', 'AmazonRDS', 'AWSLambda', 'AmazonS3', 'AmazonDynamoDB',
    'AmazonElastiCache', 'AmazonES', 'AmazonCloudFront', 'AmazonRoute53',
    'AWSELB', 'AmazonEKS', 'AmazonECS', 'AWSSystemsManager', 'AmazonVPC',
    'AmazonApiGateway', 'AmazonEFS', 'AmazonMQ', 'AmazonMSK', 'AmazonSNS',
    'awskms', 'AWSSecretsManager', 'AmazonCloudWatch', 'AWSCloudTrail',
    'AWSXRay', 'AWSConfig'
]

EXPECTED_REGIONS = [
    'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
    'eu-west-1', 'eu-west-2', 'eu-central-1',
    'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1',
    'ap-south-1', 'sa-east-1', 'ca-central-1'
]

class RawDataValidator:
    def __init__(self):
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'raw_data_path': str(RAW_DIR),
            'tests': [],
            'warnings': [],
            'errors': [],
            'critical_errors': [],
            'file_issues': [],
            'statistics': {},
            'passed': False
        }
        
        self.file_stats = defaultdict(lambda: {
            'total_files': 0,
            'valid_files': 0,
            'invalid_files': 0,
            'total_products': 0,
            'total_prices': 0,
            'total_size_bytes': 0,
            'regions': set()
        })
    
    def log_test(self, test_name, passed, message, severity='info'):
        """Log test result"""
        result = {
            'test': test_name,
            'passed': passed,
            'message': message,
            'severity': severity
        }
        
        self.validation_results['tests'].append(result)
        
        if not passed:
            if severity == 'critical':
                self.validation_results['critical_errors'].append(message)
                logger.error(f"❌ CRITICAL: {test_name} - {message}")
            elif severity == 'error':
                self.validation_results['errors'].append(message)
                logger.error(f"❌ {test_name} - {message}")
            elif severity == 'warning':
                self.validation_results['warnings'].append(message)
                logger.warning(f"⚠️  {test_name} - {message}")
        else:
            logger.info(f"✓ {test_name} - {message}")
    
    def test_raw_directory_exists(self):
        """Test 1: Verify raw data directory exists"""
        try:
            if not RAW_DIR.exists():
                self.log_test(
                    'Raw Directory Exists',
                    False,
                    f'Raw data directory not found at {RAW_DIR}',
                    'critical'
                )
                return False
            
            # Count total files
            json_files = list(RAW_DIR.rglob('*.json'))
            total_size = sum(f.stat().st_size for f in json_files)
            size_mb = total_size / (1024 * 1024)
            
            self.validation_results['statistics']['total_json_files'] = len(json_files)
            self.validation_results['statistics']['total_size_mb'] = round(size_mb, 2)
            
            if len(json_files) == 0:
                self.log_test(
                    'Raw Directory Exists',
                    False,
                    'Raw data directory is empty',
                    'critical'
                )
                return False
            
            self.log_test(
                'Raw Directory Exists',
                True,
                f'Found {len(json_files)} JSON files, total size: {size_mb:.2f} MB'
            )
            return True
            
        except Exception as e:
            self.log_test(
                'Raw Directory Exists',
                False,
                f'Error accessing raw directory: {e}',
                'critical'
            )
            return False
    
    def validate_json_file(self, file_path, service_code, region_code):
        """Validate a single JSON file"""
        file_issues = []
        
        try:
            # Check file size
            file_size = file_path.stat().st_size
            if file_size == 0:
                file_issues.append({
                    'file': str(file_path),
                    'issue': 'Empty file',
                    'severity': 'error'
                })
                return None, file_issues
            
            if file_size < 100:  # Suspiciously small
                file_issues.append({
                    'file': str(file_path),
                    'issue': f'Unusually small file ({file_size} bytes)',
                    'severity': 'warning'
                })
            
            # Parse JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate JSON structure
            if not isinstance(data, dict):
                file_issues.append({
                    'file': str(file_path),
                    'issue': 'Root element is not a JSON object',
                    'severity': 'error'
                })
                return None, file_issues
            
            # Check required top-level keys
            required_keys = ['formatVersion', 'products']
            missing_keys = [key for key in required_keys if key not in data]
            
            if missing_keys:
                file_issues.append({
                    'file': str(file_path),
                    'issue': f'Missing required keys: {missing_keys}',
                    'severity': 'error'
                })
                return None, file_issues
            
            # Validate products
            products = data.get('products', {})
            if not isinstance(products, dict):
                file_issues.append({
                    'file': str(file_path),
                    'issue': '"products" is not a dictionary',
                    'severity': 'error'
                })
                return None, file_issues
            
            product_count = len(products)
            
            # Check for empty products
            if product_count == 0:
                file_issues.append({
                    'file': str(file_path),
                    'issue': 'No products found',
                    'severity': 'warning'
                })
            
            # Validate product structure (sample first 10)
            invalid_products = 0
            for sku, product in list(products.items())[:10]:
                if not isinstance(product, dict):
                    invalid_products += 1
                    continue
                
                if 'productFamily' not in product and 'attributes' not in product:
                    invalid_products += 1
            
            if invalid_products > 0:
                file_issues.append({
                    'file': str(file_path),
                    'issue': f'{invalid_products}/10 sampled products have invalid structure',
                    'severity': 'warning'
                })
            
            # Validate pricing terms
            terms = data.get('terms', {})
            price_count = 0
            
            if isinstance(terms, dict):
                on_demand = terms.get('OnDemand', {})
                if isinstance(on_demand, dict):
                    for sku_terms in on_demand.values():
                        if isinstance(sku_terms, dict):
                            for term in sku_terms.values():
                                if isinstance(term, dict):
                                    price_dims = term.get('priceDimensions', {})
                                    price_count += len(price_dims)
            
            # Validate SKU format (should be alphanumeric)
            invalid_skus = 0
            for sku in list(products.keys())[:100]:  # Sample first 100
                if not sku or not isinstance(sku, str) or len(sku) < 5:
                    invalid_skus += 1
            
            if invalid_skus > 0:
                file_issues.append({
                    'file': str(file_path),
                    'issue': f'{invalid_skus}/100 sampled SKUs have invalid format',
                    'severity': 'warning'
                })
            
            # Return stats
            stats = {
                'products': product_count,
                'prices': price_count,
                'size': file_size
            }
            
            return stats, file_issues
            
        except json.JSONDecodeError as e:
            file_issues.append({
                'file': str(file_path),
                'issue': f'Invalid JSON: {e}',
                'severity': 'error'
            })
            return None, file_issues
        
        except Exception as e:
            file_issues.append({
                'file': str(file_path),
                'issue': f'Validation error: {e}',
                'severity': 'error'
            })
            return None, file_issues
    
    def test_json_file_validity(self):
        """Test 2: Validate all JSON files"""
        logger.info("\nValidating JSON files...")
        
        total_files = 0
        valid_files = 0
        invalid_files = 0
        
        for service_dir in RAW_DIR.iterdir():
            if not service_dir.is_dir():
                continue
            
            service_code = service_dir.name
            
            for region_dir in service_dir.iterdir():
                if not region_dir.is_dir():
                    continue
                
                region_code = region_dir.name
                pricing_file = region_dir / 'pricing.json'
                
                if not pricing_file.exists():
                    continue
                
                total_files += 1
                stats, issues = self.validate_json_file(pricing_file, service_code, region_code)
                
                if stats:
                    valid_files += 1
                    self.file_stats[service_code]['valid_files'] += 1
                    self.file_stats[service_code]['total_products'] += stats['products']
                    self.file_stats[service_code]['total_prices'] += stats['prices']
                    self.file_stats[service_code]['total_size_bytes'] += stats['size']
                    self.file_stats[service_code]['regions'].add(region_code)
                else:
                    invalid_files += 1
                    self.file_stats[service_code]['invalid_files'] += 1
                
                self.file_stats[service_code]['total_files'] += 1
                
                # Log issues
                for issue in issues:
                    self.validation_results['file_issues'].append(issue)
        
        self.validation_results['statistics']['total_files_checked'] = total_files
        self.validation_results['statistics']['valid_files'] = valid_files
        self.validation_results['statistics']['invalid_files'] = invalid_files
        
        if total_files == 0:
            self.log_test(
                'JSON File Validity',
                False,
                'No JSON files found to validate',
                'critical'
            )
            return False
        
        validity_pct = (valid_files / total_files * 100) if total_files > 0 else 0
        
        if invalid_files > 0:
            severity = 'error' if invalid_files > total_files * 0.1 else 'warning'
            self.log_test(
                'JSON File Validity',
                invalid_files == 0,
                f'{valid_files}/{total_files} files valid ({validity_pct:.1f}%), {invalid_files} invalid',
                severity
            )
            return invalid_files < total_files * 0.1  # Pass if < 10% invalid
        else:
            self.log_test(
                'JSON File Validity',
                True,
                f'All {valid_files} JSON files are valid'
            )
            return True
    
    def test_service_coverage(self):
        """Test 3: Check downloaded services coverage"""
        downloaded_services = set(self.file_stats.keys())
        expected_services = set(EXPECTED_SERVICES)
        
        missing_services = expected_services - downloaded_services
        extra_services = downloaded_services - expected_services
        
        coverage_pct = (len(downloaded_services & expected_services) / 
                       len(expected_services) * 100)
        
        self.validation_results['statistics']['service_coverage_pct'] = round(coverage_pct, 2)
        self.validation_results['statistics']['services_downloaded'] = len(downloaded_services)
        self.validation_results['statistics']['services_expected'] = len(expected_services)
        
        if coverage_pct < 50:
            self.log_test(
                'Service Coverage',
                False,
                f'Only {coverage_pct:.1f}% coverage. Missing: {missing_services}',
                'critical'
            )
            return False
        elif coverage_pct < 80:
            self.log_test(
                'Service Coverage',
                True,
                f'{coverage_pct:.1f}% coverage. Missing: {missing_services}',
                'warning'
            )
        else:
            msg = f'{coverage_pct:.1f}% coverage ({len(downloaded_services)}/{len(expected_services)} services)'
            if missing_services:
                msg += f'. Missing: {missing_services}'
            self.log_test('Service Coverage', True, msg)
        
        return True
    
    def test_region_distribution(self):
        """Test 4: Check region distribution per service"""
        logger.info("\nAnalyzing region distribution...")
        
        region_coverage = {}
        for service, stats in self.file_stats.items():
            regions = stats['regions']
            coverage = len(regions) / len(EXPECTED_REGIONS) * 100
            region_coverage[service] = {
                'regions': len(regions),
                'coverage_pct': round(coverage, 1)
            }
        
        # Check for services with low region coverage
        low_coverage_services = [
            service for service, cov in region_coverage.items() 
            if cov['coverage_pct'] < 50
        ]
        
        if low_coverage_services:
            self.log_test(
                'Region Distribution',
                True,
                f'{len(low_coverage_services)} services have <50% region coverage: ' 
                f'{low_coverage_services[:5]}',
                'warning'
            )
        else:
            avg_coverage = sum(c['coverage_pct'] for c in region_coverage.values()) / len(region_coverage)
            self.log_test(
                'Region Distribution',
                True,
                f'Average region coverage: {avg_coverage:.1f}%'
            )
        
        self.validation_results['statistics']['region_coverage_by_service'] = region_coverage
        return True
    
    def test_data_volume(self):
        """Test 5: Validate data volume is reasonable"""
        total_products = sum(stats['total_products'] for stats in self.file_stats.values())
        total_prices = sum(stats['total_prices'] for stats in self.file_stats.values())
        
        self.validation_results['statistics']['total_products'] = total_products
        self.validation_results['statistics']['total_prices'] = total_prices
        
        # Minimum thresholds
        min_products = 1000
        min_prices = 1000
        
        if total_products < min_products:
            self.log_test(
                'Data Volume',
                False,
                f'Only {total_products} products found (expected at least {min_products})',
                'error'
            )
            return False
        
        if total_prices < min_prices:
            self.log_test(
                'Data Volume',
                False,
                f'Only {total_prices} prices found (expected at least {min_prices})',
                'error'
            )
            return False
        
        self.log_test(
            'Data Volume',
            True,
            f'Found {total_products:,} products and {total_prices:,} prices'
        )
        return True
    
    def test_file_age(self):
        """Test 6: Check if downloaded data is recent"""
        try:
            # Find newest file
            json_files = list(RAW_DIR.rglob('*.json'))
            if not json_files:
                return True
            
            newest_file = max(json_files, key=lambda f: f.stat().st_mtime)
            newest_time = datetime.fromtimestamp(newest_file.stat().st_mtime)
            age_hours = (datetime.now() - newest_time).total_seconds() / 3600
            
            self.validation_results['statistics']['data_age_hours'] = round(age_hours, 2)
            self.validation_results['statistics']['newest_file_time'] = newest_time.isoformat()
            
            if age_hours > 720:  # 30 days
                self.log_test(
                    'Data Freshness',
                    True,
                    f'Data is {age_hours/24:.0f} days old. Consider re-downloading.',
                    'warning'
                )
            else:
                self.log_test(
                    'Data Freshness',
                    True,
                    f'Data is {age_hours:.1f} hours old'
                )
            
            return True
            
        except Exception as e:
            self.log_test(
                'Data Freshness',
                True,
                f'Could not determine data age: {e}',
                'warning'
            )
            return True
    
    def test_service_data_quality(self):
        """Test 7: Check data quality per service"""
        logger.info("\nAnalyzing service data quality...")
        
        quality_issues = []
        
        for service, stats in self.file_stats.items():
            # Check if service has any valid files
            if stats['valid_files'] == 0:
                quality_issues.append(f"{service}: No valid files")
                continue
            
            # Check products-to-files ratio
            if stats['valid_files'] > 0:
                products_per_file = stats['total_products'] / stats['valid_files']
                if products_per_file < 10:
                    quality_issues.append(
                        f"{service}: Low product count ({products_per_file:.0f} per file)"
                    )
            
            # Check if prices exist
            if stats['total_products'] > 0 and stats['total_prices'] == 0:
                quality_issues.append(f"{service}: Products without prices")
        
        if quality_issues:
            self.log_test(
                'Service Data Quality',
                True,
                f'Quality issues in {len(quality_issues)} services: {quality_issues[:3]}',
                'warning'
            )
        else:
            self.log_test(
                'Service Data Quality',
                True,
                'All services have good data quality'
            )
        
        return True
    
    def generate_service_report(self):
        """Generate per-service statistics"""
        logger.info("\n" + "="*60)
        logger.info("SERVICE BREAKDOWN")
        logger.info("="*60)
        logger.info(f"{ 'Service':<25} {'Files':<8} {'Products':<12} {'Prices':<12} {'Regions':<8}")
        logger.info("-" * 60)
        for service, stats in sorted(self.file_stats.items()):
            logger.info(
                f"{service:<25} "
                f"{stats['total_files']:<8} "
                f"{stats['total_products']:<12} "
                f"{stats['total_prices']:<12} "
                f"{len(stats['regions']):<8}"
            )
        logger.info("="*60)

def main():
    logger.info("Starting Raw Data Validation")
    
    validator = RawDataValidator()
    
    # Run tests
    if not validator.test_raw_directory_exists():
        sys.exit(1)
        
    validator.test_json_file_validity()
    validator.test_service_coverage()
    validator.test_region_distribution()
    validator.test_data_volume()
    validator.test_file_age()
    validator.test_service_data_quality()
    
    # Generate reports
    validator.generate_service_report()
    
    # Save detailed JSON report
    report_file = REPORT_DIR / f'raw-validation-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
    with open(report_file, 'w') as f:
        json.dump(validator.validation_results, f, indent=2)
    logger.info(f"\nDetailed report saved to: {report_file}")
    
    # Final status
    if validator.validation_results['critical_errors']:
        logger.error("\n❌ VALIDATION FAILED: Critical errors found")
        sys.exit(1)
    elif validator.validation_results['errors']:
        logger.error("\n❌ VALIDATION FAILED: Errors found")
        sys.exit(1)
    elif validator.validation_results['warnings']:
        logger.warning("\n⚠️  VALIDATION PASSED: With warnings")
        sys.exit(0)
    else:
        logger.info("\n✓ VALIDATION PASSED: All checks passed")
        sys.exit(0)

if __name__ == '__main__':
    main()