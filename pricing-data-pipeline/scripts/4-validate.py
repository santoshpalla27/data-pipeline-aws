#!/usr/bin/env python3
"""
Comprehensive validation of normalized AWS pricing data.
Checks data quality, completeness, and integrity before production use.
"""

import sqlite3
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
DB_PATH = SCRIPT_DIR.parent / 'output' / 'normalized' / 'pricing.db'
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

# Expected regions
EXPECTED_REGIONS = [
    'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
    'eu-west-1', 'eu-west-2', 'eu-central-1',
    'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1',
    'ap-south-1', 'sa-east-1', 'ca-central-1'
]

class PricingValidator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'database': str(db_path),
            'tests': [],
            'warnings': [],
            'errors': [],
            'critical_errors': [],
            'statistics': {},
            'passed': False
        }
    
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
    
    def test_database_exists(self):
        """Test 1: Database file exists and is readable"""
        try:
            if not self.db_path.exists():
                self.log_test(
                    'Database Exists',
                    False,
                    f'Database file not found at {self.db_path}',
                    'critical'
                )
                return False
            
            size_mb = self.db_path.stat().st_size / (1024 * 1024)
            self.log_test(
                'Database Exists',
                True,
                f'Database found, size: {size_mb:.2f} MB'
            )
            self.validation_results['statistics']['database_size_mb'] = round(size_mb, 2)
            return True
        except Exception as e:
            self.log_test(
                'Database Exists',
                False,
                f'Error accessing database: {e}',
                'critical'
            )
            return False
    
    def test_schema_integrity(self):
        """Test 2: All required tables and indexes exist"""
        required_tables = ['services', 'regions', 'products', 'prices']
        required_indexes = [
            'idx_products_sku',
            'idx_products_service',
            'idx_prices_product',
            'idx_prices_region'
        ]
        
        try:
            # Check tables
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            existing_tables = {row['name'] for row in self.cursor.fetchall()}
            
            missing_tables = set(required_tables) - existing_tables
            if missing_tables:
                self.log_test(
                    'Schema Integrity',
                    False,
                    f'Missing tables: {", ".join(missing_tables)}',
                    'critical'
                )
                return False
            
            # Check indexes
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
            existing_indexes = {row['name'] for row in self.cursor.fetchall()}
            
            missing_indexes = set(required_indexes) - existing_indexes
            if missing_indexes:
                self.log_test(
                    'Schema Integrity',
                    False,
                    f'Missing indexes: {", ".join(missing_indexes)}',
                    'warning'
                )
            else:
                self.log_test(
                    'Schema Integrity',
                    True,
                    'All required tables and indexes present'
                )
            return True
            
        except Exception as e:
            self.log_test(
                'Schema Integrity',
                False,
                f'Error checking schema: {e}',
                'critical'
            )
            return False
    
    def test_data_counts(self):
        """Test 3: Verify data exists in all tables"""
        try:
            counts = {}
            for table in ['services', 'regions', 'products', 'prices']:
                self.cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
                count = self.cursor.fetchone()['count']
                counts[table] = count
                self.validation_results['statistics'][f'{table}_count'] = count
            
            # Check if any table is empty
            empty_tables = [table for table, count in counts.items() if count == 0]
            
            if empty_tables:
                self.log_test(
                    'Data Counts',
                    False,
                    f'Empty tables: {", ".join(empty_tables)}',
                    'critical'
                )
                return False
            
            # Check minimum thresholds
            if counts['products'] < 100:
                self.log_test(
                    'Data Counts',
                    False,
                    f'Very few products ({counts["products"]}). Expected at least 100.',
                    'error'
                )
                return False
            
            self.log_test(
                'Data Counts',
                True,
                f'Services: {counts["services"]}, Regions: {counts["regions"]}, '
                f'Products: {counts["products"]}, Prices: {counts["prices"]}'
            )
            return True
            
        except Exception as e:
            self.log_test(
                'Data Counts',
                False,
                f'Error checking data counts: {e}',
                'critical'
            )
            return False
    
    def test_service_coverage(self):
        """Test 4: Check if expected services are present"""
        try:
            self.cursor.execute('SELECT code FROM services')
            existing_services = {row['code'] for row in self.cursor.fetchall()}
            
            missing_services = set(EXPECTED_SERVICES) - existing_services
            extra_services = existing_services - set(EXPECTED_SERVICES)
            
            coverage_pct = (len(existing_services & set(EXPECTED_SERVICES)) / 
                          len(EXPECTED_SERVICES) * 100)
            
            self.validation_results['statistics']['service_coverage_pct'] = round(coverage_pct, 2)
            self.validation_results['statistics']['services_found'] = len(existing_services)
            self.validation_results['statistics']['services_expected'] = len(EXPECTED_SERVICES)
            
            if coverage_pct < 50:
                self.log_test(
                    'Service Coverage',
                    False,
                    f'Only {coverage_pct:.1f}% service coverage. Missing: {missing_services}',
                    'critical'
                )
                return False
            elif coverage_pct < 80:
                self.log_test(
                    'Service Coverage',
                    True,
                    f'{coverage_pct:.1f}% service coverage. Missing: {missing_services}',
                    'warning'
                )
            else:
                self.log_test(
                    'Service Coverage',
                    True,
                    f'{coverage_pct:.1f}% service coverage ({len(existing_services)}/{len(EXPECTED_SERVICES)} services)'
                )
            
            return True
            
        except Exception as e:
            self.log_test(
                'Service Coverage',
                False,
                f'Error checking service coverage: {e}',
                'error'
            )
            return False
    
    def test_region_coverage(self):
        """Test 5: Check if expected regions are present"""
        try:
            self.cursor.execute('SELECT code FROM regions')
            existing_regions = {row['code'] for row in self.cursor.fetchall()}
            
            missing_regions = set(EXPECTED_REGIONS) - existing_regions
            
            coverage_pct = (len(existing_regions & set(EXPECTED_REGIONS)) / 
                          len(EXPECTED_REGIONS) * 100)
            
            self.validation_results['statistics']['region_coverage_pct'] = round(coverage_pct, 2)
            self.validation_results['statistics']['regions_found'] = len(existing_regions)
            
            if coverage_pct < 70:
                self.log_test(
                    'Region Coverage',
                    False,
                    f'Only {coverage_pct:.1f}% region coverage. Missing: {missing_regions}',
                    'error'
                )
                return False
            elif missing_regions:
                self.log_test(
                    'Region Coverage',
                    True,
                    f'{coverage_pct:.1f}% region coverage. Missing: {missing_regions}',
                    'warning'
                )
            else:
                self.log_test(
                    'Region Coverage',
                    True,
                    f'100% region coverage ({len(existing_regions)} regions)'
                )
            
            return True
            
        except Exception as e:
            self.log_test(
                'Region Coverage',
                False,
                f'Error checking region coverage: {e}',
                'error'
            )
            return False
    
    def test_price_validity(self):
        """Test 6: Validate price data quality"""
        try:
            # Check for NULL prices
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices 
                WHERE price_per_unit IS NULL
            ''')
            null_prices = self.cursor.fetchone()['count']
            
            # Check for negative prices
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices 
                WHERE price_per_unit < 0
            ''')
            negative_prices = self.cursor.fetchone()['count']
            
            # Check for zero prices (might be valid for free tier)
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices 
                WHERE price_per_unit = 0
            ''')
            zero_prices = self.cursor.fetchone()['count']
            
            # Check for extremely high prices (potential data error)
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices 
                WHERE price_per_unit > 10000
            ''')
            high_prices = self.cursor.fetchone()['count']
            
            self.validation_results['statistics']['null_prices'] = null_prices
            self.validation_results['statistics']['negative_prices'] = negative_prices
            self.validation_results['statistics']['zero_prices'] = zero_prices
            self.validation_results['statistics']['high_prices'] = high_prices
            
            issues = []
            severity = 'info'
            
            if null_prices > 0:
                issues.append(f'{null_prices} NULL prices')
                severity = 'warning'
            
            if negative_prices > 0:
                issues.append(f'{negative_prices} negative prices')
                severity = 'error'
            
            if high_prices > 100:
                issues.append(f'{high_prices} suspiciously high prices')
                severity = 'warning'
            
            if issues:
                self.log_test(
                    'Price Validity',
                    severity != 'error',
                    f'Price issues found: {", ".join(issues)}',
                    severity
                )
                return severity != 'error'
            else:
                self.log_test(
                    'Price Validity',
                    True,
                    f'All prices valid. {zero_prices} zero prices (likely free tier)'
                )
                return True
            
        except Exception as e:
            self.log_test(
                'Price Validity',
                False,
                f'Error checking price validity: {e}',
                'error'
            )
            return False
    
    def test_referential_integrity(self):
        """Test 7: Check foreign key relationships"""
        try:
            # Check orphaned products (products without valid service)
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM products p 
                LEFT JOIN services s ON p.service_id = s.id 
                WHERE s.id IS NULL
            ''')
            orphaned_products = self.cursor.fetchone()['count']
            
            # Check orphaned prices (prices without valid product or region)
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices pr 
                LEFT JOIN products p ON pr.product_id = p.id 
                WHERE p.id IS NULL
            ''')
            orphaned_prices_product = self.cursor.fetchone()['count']
            
            self.cursor.execute('''
                SELECT COUNT(*) as count 
                FROM prices pr 
                LEFT JOIN regions r ON pr.region_id = r.id 
                WHERE r.id IS NULL
            ''')
            orphaned_prices_region = self.cursor.fetchone()['count']
            
            total_orphaned = orphaned_products + orphaned_prices_product + orphaned_prices_region
            
            if total_orphaned > 0:
                issues = []
                if orphaned_products > 0:
                    issues.append(f'{orphaned_products} orphaned products')
                if orphaned_prices_product > 0:
                    issues.append(f'{orphaned_prices_product} prices without products')
                if orphaned_prices_region > 0:
                    issues.append(f'{orphaned_prices_region} prices without regions')
                
                self.log_test(
                    'Referential Integrity',
                    False,
                    f'Foreign key violations: {", ".join(issues)}',
                    'critical'
                )
                return False
            else:
                self.log_test(
                    'Referential Integrity',
                    True,
                    'All foreign key relationships valid'
                )
                return True
            
        except Exception as e:
            self.log_test(
                'Referential Integrity',
                False,
                f'Error checking referential integrity: {e}',
                'error'
            )
            return False
    
    def test_sku_uniqueness(self):
        """Test 8: Verify SKU uniqueness"""
        try:
            self.cursor.execute('''
                SELECT sku, COUNT(*) as count 
                FROM products 
                GROUP BY sku 
                HAVING count > 1
            ''')
            duplicate_skus = self.cursor.fetchall()
            
            if duplicate_skus:
                dup_count = len(duplicate_skus)
                sample = ', '.join([row['sku'] for row in duplicate_skus[:5]])
                self.log_test(
                    'SKU Uniqueness',
                    False,
                    f'{dup_count} duplicate SKUs found. Sample: {sample}',
                    'critical'
                )
                return False
            else:
                self.log_test(
                    'SKU Uniqueness',
                    True,
                    'All SKUs are unique'
                )
                return True
            
        except Exception as e:
            self.log_test(
                'SKU Uniqueness',
                False,
                f'Error checking SKU uniqueness: {e}',
                'error'
            )
            return False
    
    def test_json_attributes(self):
        """Test 9: Validate JSON attributes are parseable"""
        try:
            self.cursor.execute('SELECT id, attributes FROM products LIMIT 1000')
            
            invalid_json = 0
            for row in self.cursor.fetchall():
                try:
                    json.loads(row['attributes'])
                except:
                    invalid_json += 1
            
            if invalid_json > 0:
                self.log_test(
                    'JSON Attributes',
                    False,
                    f'{invalid_json} products with invalid JSON attributes',
                    'error'
                )
                return False
            else:
                self.log_test(
                    'JSON Attributes',
                    True,
                    'All sampled JSON attributes are valid'
                )
                return True
            
        except Exception as e:
            self.log_test(
                'JSON Attributes',
                False,
                f'Error checking JSON attributes: {e}',
                'error'
            )
            return False
    
    def test_service_distribution(self):
        """Test 10: Analyze product distribution across services"""
        try:
            self.cursor.execute('''
                SELECT s.code, COUNT(p.id) as product_count
                FROM services s
                LEFT JOIN products p ON s.id = p.service_id
                GROUP BY s.code
                ORDER BY product_count DESC
            ''')
            
            distribution = self.cursor.fetchall()
            services_without_products = [
                row['code'] for row in distribution if row['product_count'] == 0
            ]
            
            self.validation_results['statistics']['service_distribution'] = {
                row['code']: row['product_count'] for row in distribution
            }
            
            if services_without_products:
                self.log_test(
                    'Service Distribution',
                    True,
                    f'{len(services_without_products)} services without products: '
                    f'{", ".join(services_without_products[:5])}',
                    'warning'
                )
            else:
                top_service = distribution[0]
                self.log_test(
                    'Service Distribution',
                    True,
                    f'All services have products. Top: {top_service["code"]} '
                    f'({top_service["product_count"]} products)'
                )
            
            return True
            
        except Exception as e:
            self.log_test(
                'Service Distribution',
                False,
                f'Error checking service distribution: {e}',
                'error'
            )
            return False
    
    def test_price_distribution(self):
        """Test 11: Analyze price distribution"""
        try:
            self.cursor.execute('''
                SELECT 
                    MIN(price_per_unit) as min_price,
                    MAX(price_per_unit) as max_price,
                    AVG(price_per_unit) as avg_price,
                    COUNT(DISTINCT product_id) as products_with_prices
                FROM prices
                WHERE price_per_unit > 0
            ''')
            
            stats = self.cursor.fetchone()
            
            self.validation_results['statistics']['price_stats'] = {
                'min': float(stats['min_price']) if stats['min_price'] else 0,
                'max': float(stats['max_price']) if stats['max_price'] else 0,
                'avg': float(stats['avg_price']) if stats['avg_price'] else 0,
                'products_with_prices': stats['products_with_prices']
            }
            
            self.log_test(
                'Price Distribution',
                True,
                f'Price range: ${stats["min_price"]:.6f} - ${stats["max_price"]:.2f}, '
                f'Avg: ${stats["avg_price"]:.4f}'
            )
            return True
            
        except Exception as e:
            self.log_test(
                'Price Distribution',
                False,
                f'Error checking price distribution: {e}',
                'error'
            )
            return False
    
    def test_data_freshness(self):
        """Test 12: Check if database was recently updated"""
        try:
            db_modified = datetime.fromtimestamp(self.db_path.stat().st_mtime)
            age_days = (datetime.now() - db_modified).days
            
            self.validation_results['statistics']['database_age_days'] = age_days
            self.validation_results['statistics']['last_modified'] = db_modified.isoformat()
            
            if age_days > 60:
                self.log_test(
                    'Data Freshness',
                    False,
                    f'Database is {age_days} days old. Consider updating.',
                    'warning'
                )
            elif age_days > 30:
                self.log_test(
                    'Data Freshness',
                    True,
                    f'Database is {age_days} days old',
                    'warning'
                )
            else:
                self.log_test(
                    'Data Freshness',
                    True,
                    f'Database is {age_days} days old (last modified: {db_modified.strftime("%Y-%m-%d")})'
                )
            
            return True
            
        except Exception as e:
            self.log_test(
                'Data Freshness',
                False,
                f'Error checking data freshness: {e}',
                'warning'
            )
            return False
    
    def run_all_tests(self):
        """Run all validation tests"""
        logger.info("="*60)
        logger.info("Starting AWS Pricing Data Validation")
        logger.info("="*60)
        logger.info("")
        
        tests = [
            self.test_database_exists,
            self.test_schema_integrity,
            self.test_data_counts,
            self.test_service_coverage,
            self.test_region_coverage,
            self.test_price_validity,
            self.test_referential_integrity,
            self.test_sku_uniqueness,
            self.test_json_attributes,
            self.test_service_distribution,
            self.test_price_distribution,
            self.test_data_freshness
        ]
        
        passed_tests = 0
        total_tests = len(tests)
        
        for test in tests:
            if test():
                passed_tests += 1
        
        # Determine overall result
        has_critical_errors = len(self.validation_results['critical_errors']) > 0
        has_errors = len(self.validation_results['errors']) > 0
        
        if has_critical_errors:
            self.validation_results['passed'] = False
            self.validation_results['status'] = 'FAILED - Critical errors found'
        elif has_errors:
            self.validation_results['passed'] = False
            self.validation_results['status'] = 'FAILED - Errors found'
        elif self.validation_results['warnings']:
            self.validation_results['passed'] = True
            self.validation_results['status'] = 'PASSED - With warnings'
        else:
            self.validation_results['passed'] = True
            self.validation_results['status'] = 'PASSED - All tests passed'
        
        self.validation_results['statistics']['tests_passed'] = passed_tests
        self.validation_results['statistics']['tests_total'] = total_tests
        self.validation_results['statistics']['pass_rate'] = round(
            (passed_tests / total_tests * 100), 2
        )
        
        return self.validation_results
    
    def generate_report(self):
        """Generate validation report"""
        logger.info("")
        logger.info("="*60)
        logger.info("VALIDATION SUMMARY")
        logger.info("="*60)
        
        stats = self.validation_results['statistics']
        logger.info(f"Status: {self.validation_results['status']}")
        logger.info(f"Tests Passed: {stats['tests_passed']}/{stats['tests_total']} ({stats['pass_rate']}%)")
        logger.info("")
        
        logger.info("Data Statistics:")
        logger.info(f"  Services: {stats.get('services_count', 0)}")
        logger.info(f"  Regions: {stats.get('regions_count', 0)}")
        logger.info(f"  Products: {stats.get('products_count', 0)}")
        logger.info(f"  Prices: {stats.get('prices_count', 0)}")
        logger.info(f"  Database Size: {stats.get('database_size_mb', 0)} MB")
        logger.info(f"  Database Age: {stats.get('database_age_days', 0)} days")
        logger.info("")
        
        if self.validation_results['critical_errors']:
            logger.error(f"Critical Errors ({len(self.validation_results['critical_errors'])}):")
            for error in self.validation_results['critical_errors']:
                logger.error(f"  • {error}")
            logger.info("")
        
        if self.validation_results['errors']:
            logger.error(f"Errors ({len(self.validation_results['errors'])}):")
            for error in self.validation_results['errors']:
                logger.error(f"  • {error}")
            logger.info("")
        
        if self.validation_results['warnings']:
            logger.warning(f"Warnings ({len(self.validation_results['warnings'])}):")
            for warning in self.validation_results['warnings']:
                logger.warning(f"  • {warning}")
            logger.info("")
        
        # Save JSON report
        report_file = REPORT_DIR / f'validation-report-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
        
        logger.info(f"Detailed report saved: {report_file}")
        logger.info("="*60)
        
        return self.validation_results['passed']
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    if not DB_PATH.exists():
        logger.error(f"Database not found at {DB_PATH}")
        logger.error("Run './scripts/2-normalize.py' first")
        sys.exit(1)
    
    validator = PricingValidator(DB_PATH)
    
    try:
        validator.run_all_tests()
        passed = validator.generate_report()
        
        if not passed:
            logger.error("\n⚠️  VALIDATION FAILED - Data is NOT ready for production use")
            sys.exit(1)
        else:
            logger.info("\n✓ VALIDATION PASSED - Data is ready for production use")
            sys.exit(0)
    
    finally:
        validator.close()

if __name__ == '__main__':
    main()
