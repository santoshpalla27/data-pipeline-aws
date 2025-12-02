#!/usr/bin/env python3
"""
Test common pricing queries to ensure data is usable in applications.
Simulates real-world usage patterns.
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
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

class QueryTester:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'queries': [],
            'performance': {},
            'passed': 0,
            'failed': 0
        }
    
    def log_query_result(self, name, success, execution_time, result_count, message=''):
        """Log query test result"""
        result = {
            'query': name,
            'success': success,
            'execution_time_ms': round(execution_time * 1000, 2),
            'result_count': result_count,
            'message': message
        }
        
        self.test_results['queries'].append(result)
        
        if success:
            self.test_results['passed'] += 1
            logger.info(f"✓ {name}: {result_count} results in {result['execution_time_ms']}ms")
        else:
            self.test_results['failed'] += 1
            logger.error(f"✗ {name}: {message}")
    
    def test_query(self, name, query, expected_min_results=0):
        """Execute and test a query"""
        start_time = datetime.now()
        
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            result_count = len(results)
            success = result_count >= expected_min_results
            
            message = '' if success else f'Expected at least {expected_min_results} results'
            
            self.log_query_result(name, success, execution_time, result_count, message)
            
            return success, results
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_query_result(name, False, execution_time, 0, str(e))
            return False, []
    
    def run_application_queries(self):
        """Run queries that simulate real application usage"""
        
        logger.info("="*60)
        logger.info("Testing Application Queries")
        logger.info("="*60)
        logger.info("")
        
        # Query 1: Get all services
        logger.info("1. Fetch all services")
        self.test_query(
            'Get All Services',
            'SELECT * FROM services ORDER BY code',
            expected_min_results=1
        )
        
        # Query 2: Get EC2 prices for us-east-1
        logger.info("\n2. Get EC2 prices for us-east-1")
        success, results = self.test_query(
            'EC2 Prices - us-east-1',
            '''
            SELECT p.sku, p.product_family, pr.price_per_unit, pr.unit
            FROM products p
            JOIN services s ON p.service_id = s.id
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            WHERE s.code = 'AmazonEC2' AND r.code = 'us-east-1'
            LIMIT 100
            ''',
            expected_min_results=1
        )
        
        if success and results:
            logger.info(f"   Sample: {results[0]['sku']} - ${results[0]['price_per_unit']}/{results[0]['unit']}")
        
        # Query 3: Get Lambda prices
        logger.info("\n3. Get Lambda pricing")
        success, results = self.test_query(
            'Lambda Pricing',
            '''
            SELECT r.code as region, pr.price_per_unit, pr.unit, pr.description
            FROM products p
            JOIN services s ON p.service_id = s.id
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            WHERE s.code = 'AWSLambda'
            ORDER BY r.code
            LIMIT 50
            ''',
            expected_min_results=1
        )
        
        # Query 4: Get S3 storage prices
        logger.info("\n4. Get S3 storage prices")
        success, results = self.test_query(
            'S3 Storage Prices',
            '''
            SELECT r.code as region, p.attributes, pr.price_per_unit
            FROM products p
            JOIN services s ON p.service_id = s.id
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            WHERE s.code = 'AmazonS3' 
            AND p.product_family = 'Storage'
            ORDER BY pr.price_per_unit
            LIMIT 50
            ''',
            expected_min_results=1
        )
        
        # Query 5: Compare prices across regions for a service
        logger.info("\n5. Compare RDS prices across regions")
        success, results = self.test_query(
            'RDS Price Comparison',
            '''
            SELECT r.code as region, 
                   COUNT(DISTINCT p.id) as product_count,
                   AVG(pr.price_per_unit) as avg_price,
                   MIN(pr.price_per_unit) as min_price,
                   MAX(pr.price_per_unit) as max_price
            FROM products p
            JOIN services s ON p.service_id = s.id
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            WHERE s.code = 'AmazonRDS'
            GROUP BY r.code
            ORDER BY avg_price
            ''',
            expected_min_results=1
        )
        
        if success and results:
            for row in results[:5]:
                logger.info(f"   {row['region']}: ${row['avg_price']:.4f} avg, "
                          f"{row['product_count']} products")
        
        # Query 6: Find cheapest compute options
        logger.info("\n6. Find cheapest EC2 instances")
        success, results = self.test_query(
            'Cheapest EC2 Instances',
            '''
            SELECT p.sku, p.attributes, r.code as region, pr.price_per_unit, pr.unit
            FROM products p
            JOIN services s ON p.service_id = s.id
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            WHERE s.code = 'AmazonEC2' 
            AND pr.price_per_unit > 0
            ORDER BY pr.price_per_unit ASC
            LIMIT 10
            ''',
            expected_min_results=1
        )
        
        if success and results:
            logger.info(f"   Cheapest: {results[0]['sku']} - "
                       f"${results[0]['price_per_unit']}/{results[0]['unit']} in {results[0]['region']}")
        
        # Query 7: Get products by SKU (common lookup)
        logger.info("\n7. Lookup product by SKU")
        # First get a sample SKU
        self.cursor.execute('SELECT sku FROM products LIMIT 1')
        sample_sku = self.cursor.fetchone()
        
        if sample_sku:
            self.test_query(
                'Product Lookup by SKU',
                f'''
                SELECT p.*, s.code as service, pr.price_per_unit, r.code as region
                FROM products p
                JOIN services s ON p.service_id = s.id
                LEFT JOIN prices pr ON p.id = pr.product_id
                LEFT JOIN regions r ON pr.region_id = r.id
                WHERE p.sku = '{sample_sku[0]}'
                ''',
                expected_min_results=1
            )
        
        # Query 8: Get price history (per region)
        logger.info("\n8. Get prices for a product across regions")
        self.cursor.execute('''
            SELECT product_id 
            FROM prices 
            GROUP BY product_id 
            HAVING COUNT(*) > 1 
            LIMIT 1
        ''')
        sample_product = self.cursor.fetchone()
        
        if sample_product:
            success, results = self.test_query(
                'Multi-Region Price Check',
                f'''
                SELECT r.code as region, pr.price_per_unit, pr.unit
                FROM prices pr
                JOIN regions r ON pr.region_id = r.id
                WHERE pr.product_id = {sample_product[0]}
                ORDER BY pr.price_per_unit
                ''',
                expected_min_results=1
            )
            
            if success and results:
                logger.info(f"   Price range: ${results[0]['price_per_unit']} - "
                          f"${results[-1]['price_per_unit']}")
        
        # Query 9: Aggregate service costs
        logger.info("\n9. Aggregate pricing by service")
        success, results = self.test_query(
            'Service Cost Aggregation',
            '''
            SELECT s.code as service,
                   COUNT(DISTINCT p.id) as product_count,
                   COUNT(pr.id) as price_count,
                   AVG(pr.price_per_unit) as min_price,
                   MAX(pr.price_per_unit) as max_price
            FROM services s
            JOIN products p ON s.id = p.service_id
            LEFT JOIN prices pr ON p.id = pr.product_id
            GROUP BY s.code
            ORDER BY product_count DESC
            LIMIT 10
            ''',
            expected_min_results=1
        )
        
        if success and results:
            logger.info("\n   Top services by product count:")
            for row in results[:5]:
                logger.info(f"   {row['service']}: {row['product_count']} products, "
                          f"{row['price_count']} prices")
        
        # Query 10: Search by product attributes
        logger.info("\n10. Search products by attributes")
        self.test_query(
            'Product Attribute Search',
            '''
            SELECT p.sku, p.product_family, p.attributes
            FROM products p
            WHERE p.attributes LIKE '%memory%'
            OR p.attributes LIKE '%storage%'
            LIMIT 20
            ''',
            expected_min_results=0  # May not have results depending on data
        )
    
    def run_performance_tests(self):
        """Test query performance"""
        logger.info("\n" + "="*60)
        logger.info("Performance Tests")
        logger.info("="*60)
        logger.info("")
        
        # Test 1: Index usage on SKU lookup
        logger.info("1. Testing SKU index performance")
        start = datetime.now()
        self.cursor.execute('SELECT * FROM products WHERE sku = "DUMMY_SKU"')
        self.cursor.fetchall()
        sku_lookup_time = (datetime.now() - start).total_seconds()
        self.test_results['performance']['sku_lookup_ms'] = round(sku_lookup_time * 1000, 2)
        logger.info(f"   SKU lookup: {sku_lookup_time * 1000:.2f}ms")
        
        # Test 2: Join performance
        logger.info("\n2. Testing join performance")
        start = datetime.now()
        self.cursor.execute('''
            SELECT COUNT(*)
            FROM products p
            JOIN prices pr ON p.id = pr.product_id
            JOIN regions r ON pr.region_id = r.id
            JOIN services s ON p.service_id = s.id
        ''')
        self.cursor.fetchone()
        join_time = (datetime.now() - start).total_seconds()
        self.test_results['performance']['complex_join_ms'] = round(join_time * 1000, 2)
        logger.info(f"   Complex join: {join_time * 1000:.2f}ms")
        
        # Test 3: Aggregation performance
        logger.info("\n3. Testing aggregation performance")
        start = datetime.now()
        self.cursor.execute('''
            SELECT service_id, COUNT(*), AVG(CAST(id AS REAL))
            FROM products
            GROUP BY service_id
        ''')
        self.cursor.fetchall()
        agg_time = (datetime.now() - start).total_seconds()
        self.test_results['performance']['aggregation_ms'] = round(agg_time * 1000, 2)
        logger.info(f"   Aggregation: {agg_time * 1000:.2f}ms")
        
        # Performance thresholds
        if sku_lookup_time > 0.1:
            logger.warning("   ⚠️  SKU lookup is slow. Consider checking indexes.")
        if join_time > 5.0:
            logger.warning("   ⚠️  Joins are slow. Database might be too large for SQLite.")
        if agg_time > 2.0:
            logger.warning("   ⚠️  Aggregations are slow. Consider materialized views.")
    
    def generate_report(self):
        """Generate test report"""
        logger.info("\n" + "="*60)
        logger.info("QUERY TEST SUMMARY")
        logger.info("="*60)
        
        total_tests = self.test_results['passed'] + self.test_results['failed']
        pass_rate = (self.test_results['passed'] / total_tests * 100) if total_tests > 0 else 0
        
        logger.info(f"Queries Tested: {total_tests}")
        logger.info(f"Passed: {self.test_results['passed']}")
        logger.info(f"Failed: {self.test_results['failed']}")
        logger.info(f"Pass Rate: {pass_rate:.1f}%")
        logger.info("")
        
        logger.info("Performance Metrics:")
        for metric, value in self.test_results['performance'].items():
            logger.info(f"  {metric}: {value}ms")
        
        # Save JSON report
        report_file = REPORT_DIR / f'query-test-report-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(self.test_results, f, indent=2)
        
        logger.info(f"\nDetailed report saved: {report_file}")
        logger.info("="*60)
        
        return self.test_results['failed'] == 0
    
    def close(self):
        self.conn.close()

def main():
    if not DB_PATH.exists():
        logger.error(f"Database not found at {DB_PATH}")
        logger.error("Run './scripts/2-normalize.py' first")
        sys.exit(1)
    
    tester = QueryTester(DB_PATH)
    
    try:
        tester.run_application_queries()
        tester.run_performance_tests()
        passed = tester.generate_report()
        
        if not passed:
            logger.warning("\n⚠️  Some queries failed - review results")
            sys.exit(1)
        else:
            logger.info("\n✓ All queries passed - data is ready for use")
            sys.exit(0)
    
    finally:
        tester.close()

if __name__ == '__main__':
    main()
