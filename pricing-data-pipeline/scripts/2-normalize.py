#!/usr/bin/env python3
"""
Normalize downloaded AWS pricing JSON into SQL database format.
Outputs: PostgreSQL dump, SQLite database, CSV files
FIXED: Proper handling of product ID mapping to prevent orphaned prices
"""

import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
INPUT_DIR = SCRIPT_DIR.parent / 'output' / 'raw'
OUTPUT_DIR = SCRIPT_DIR.parent / 'output' / 'normalized'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class PricingNormalizer:
    def __init__(self):
        # Create SQLite database
        self.db_path = OUTPUT_DIR / 'pricing.db'
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.setup_database()
        
        self.stats = {
            'files_processed': 0,
            'products_imported': 0,
            'products_updated': 0,
            'prices_imported': 0,
            'prices_skipped': 0,
            'errors': 0
        }
    
    def setup_database(self):
        """Create database schema"""
        # Services table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )
        ''')
        
        # Regions table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )
        ''')
        
        # Products table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_id INTEGER NOT NULL,
                sku TEXT UNIQUE NOT NULL,
                product_family TEXT,
                attributes TEXT,  -- JSON
                FOREIGN KEY (service_id) REFERENCES services(id)
            )
        ''')
        
        # Prices table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                region_id INTEGER NOT NULL,
                term_type TEXT,
                unit TEXT,
                price_per_unit REAL,
                currency TEXT DEFAULT 'USD',
                description TEXT,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (region_id) REFERENCES regions(id),
                UNIQUE(product_id, region_id, term_type, unit, description)
            )
        ''')
        
        # Create indexes
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_products_service ON products(service_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_prices_product ON prices(product_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_prices_region ON prices(region_id)')
        
        self.conn.commit()
    
    def get_or_create_service(self, code):
        """Get service ID, create if doesn't exist"""
        self.cursor.execute('SELECT id FROM services WHERE code = ?', (code,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        
        self.cursor.execute('INSERT INTO services (code, name) VALUES (?, ?)', (code, code))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_or_create_region(self, code):
        """Get region ID, create if doesn't exist"""
        self.cursor.execute('SELECT id FROM regions WHERE code = ?', (code,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        
        self.cursor.execute('INSERT INTO regions (code, name) VALUES (?, ?)', (code, code))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_or_create_product(self, service_id, sku, product_family, attributes_json):
        """
        Get product ID, create or update if doesn't exist.
        FIXED: Properly returns existing product ID to prevent orphaned prices.
        """
        # First, try to find existing product by SKU
        self.cursor.execute('SELECT id FROM products WHERE sku = ?', (sku,))
        row = self.cursor.fetchone()
        
        if row:
            # Product exists, update it
            product_id = row[0]
            self.cursor.execute('''
                UPDATE products 
                SET service_id = ?, product_family = ?, attributes = ?
                WHERE id = ?
            ''', (service_id, product_family, attributes_json, product_id))
            self.stats['products_updated'] += 1
            return product_id
        else:
            # Product doesn't exist, insert it
            self.cursor.execute('''
                INSERT INTO products (service_id, sku, product_family, attributes)
                VALUES (?, ?, ?, ?)
            ''', (service_id, sku, product_family, attributes_json))
            self.stats['products_imported'] += 1
            return self.cursor.lastrowid
    
    def normalize_file(self, file_path, service_code, region_code):
        """Normalize a single pricing JSON file"""
        try:
            with open(file_path) as f:
                data = json.load(f)
            
            service_id = self.get_or_create_service(service_code)
            region_id = self.get_or_create_region(region_code)
            
            # Import products with proper ID tracking
            products = data.get('products', {})
            sku_to_id = {}
            
            for sku, product in products.items():
                product_id = self.get_or_create_product(
                    service_id,
                    sku,
                    product.get('productFamily'),
                    json.dumps(product.get('attributes', {}))
                )
                sku_to_id[sku] = product_id
            
            # Commit products before prices
            self.conn.commit()
            
            # Import prices
            terms = data.get('terms', {})
            on_demand = terms.get('OnDemand', {})
            
            for sku, term_data in on_demand.items():
                if sku not in sku_to_id:
                    logger.warning(f"SKU {sku} not found in products, skipping prices")
                    self.stats['prices_skipped'] += 1
                    continue
                
                product_id = sku_to_id[sku]
                
                for term_code, term in term_data.items():
                    for dim_key, dimension in term.get('priceDimensions', {}).items():
                        price_str = dimension.get('pricePerUnit', {}).get('USD', '0')
                        try:
                            price = float(price_str)
                        except:
                            price = 0.0
                        
                        # Use INSERT OR IGNORE to handle duplicates gracefully
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO prices 
                            (product_id, region_id, term_type, unit, price_per_unit, description)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            product_id,
                            region_id,
                            'OnDemand',
                            dimension.get('unit'),
                            price,
                            dimension.get('description')
                        ))
                        
                        if self.cursor.rowcount > 0:
                            self.stats['prices_imported'] += 1
            
            self.conn.commit()
            self.stats['files_processed'] += 1
            logger.info(f"✓ Processed {service_code}/{region_code} - "
                       f"{len(sku_to_id)} products")
            
        except Exception as e:
            logger.error(f"✗ Error processing {file_path}: {e}")
            self.stats['errors'] += 1
            self.conn.rollback()
    
    def normalize_all(self):
        """Normalize all downloaded pricing files"""
        logger.info("Starting normalization...")
        
        if not INPUT_DIR.exists():
             logger.error(f"Input directory {INPUT_DIR} does not exist. Run download script first.")
             return

        for service_dir in INPUT_DIR.iterdir():
            if not service_dir.is_dir():
                continue
            
            service_code = service_dir.name
            
            for region_dir in service_dir.iterdir():
                if not region_dir.is_dir():
                    continue
                
                region_code = region_dir.name
                pricing_file = region_dir / 'pricing.json'
                
                if pricing_file.exists():
                    self.normalize_file(pricing_file, service_code, region_code)
        
        self.conn.commit()
    
    def verify_integrity(self):
        """Verify data integrity after normalization"""
        logger.info("\nVerifying data integrity...")
        
        # Check for orphaned prices
        self.cursor.execute('''
            SELECT COUNT(*) 
            FROM prices pr 
            LEFT JOIN products p ON pr.product_id = p.id 
            WHERE p.id IS NULL
        ''')
        orphaned_prices = self.cursor.fetchone()[0]
        
        if orphaned_prices > 0:
            logger.error(f"❌ Found {orphaned_prices} orphaned prices!")
            return False
        else:
            logger.info("✓ No orphaned prices found")
            return True
    
    def export_postgresql_dump(self):
        """Export PostgreSQL dump"""
        logger.info("Generating PostgreSQL dump...")
        
        sql_path = OUTPUT_DIR / 'pricing-dump.sql'
        
        with open(sql_path, 'w', encoding='utf-8') as f:
            # Write schema
            f.write("-- AWS Pricing Database Dump\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")
            
            f.write("BEGIN;\n\n")
            
            # Create tables
            f.write('''
CREATE TABLE IF NOT EXISTS services (
    id SERIAL PRIMARY KEY,
    code VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    service_id INTEGER NOT NULL REFERENCES services(id),
    sku VARCHAR(100) UNIQUE NOT NULL,
    product_family VARCHAR(200),
    attributes JSONB
);

CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    region_id INTEGER NOT NULL REFERENCES regions(id),
    term_type VARCHAR(50),
    unit VARCHAR(50),
    price_per_unit NUMERIC(20,10),
    currency VARCHAR(3) DEFAULT 'USD',
    description TEXT,
    UNIQUE(product_id, region_id, term_type, unit, description)
);

CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_service ON products(service_id);
CREATE INDEX IF NOT EXISTS idx_prices_product ON prices(product_id);
CREATE INDEX IF NOT EXISTS idx_prices_region ON prices(region_id);

''')
            
            # Export data
            for table in ['services', 'regions', 'products', 'prices']:
                self.cursor.execute(f'SELECT * FROM {table}')
                rows = self.cursor.fetchall()
                
                if rows:
                    # Get column names
                    columns = [desc[0] for desc in self.cursor.description]
                    columns_str = ', '.join(columns)
                    
                    f.write(f"\n-- Data for {table}\n")
                    
                    for row in rows:
                        values = []
                        for val in row:
                            if val is None:
                                values.append('NULL')
                            elif isinstance(val, str):
                                # Escape single quotes
                                escaped = val.replace("'", "''")
                                values.append(f"'{escaped}'")
                            else:
                                values.append(str(val))
                        
                        values_str = ', '.join(values)
                        f.write(f"INSERT INTO {table} ({columns_str}) VALUES ({values_str});\n")
            
            f.write("\nCOMMIT;\n")
        
        logger.info(f"PostgreSQL dump created: {sql_path}")
    
    def export_csv(self):
        """Export CSV files"""
        logger.info("Generating CSV exports...")
        
        # Export products
        self.cursor.execute('SELECT * FROM products')
        products = self.cursor.fetchall()
        
        with open(OUTPUT_DIR / 'products.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'service_id', 'sku', 'product_family', 'attributes'])
            writer.writerows(products)
        
        # Export prices
        self.cursor.execute('SELECT * FROM prices')
        prices = self.cursor.fetchall()
        
        with open(OUTPUT_DIR / 'prices.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'product_id', 'region_id', 'term_type', 
                           'unit', 'price_per_unit', 'currency', 'description'])
            writer.writerows(prices)
        
        logger.info("CSV files created")
    
    def print_summary(self):
        """Print normalization summary"""
        logger.info("\n" + "="*60)
        logger.info("NORMALIZATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Products created: {self.stats['products_imported']}")
        logger.info(f"Products updated: {self.stats['products_updated']}")
        logger.info(f"Prices imported: {self.stats['prices_imported']}")
        logger.info(f"Prices skipped (duplicate): {self.stats['prices_skipped']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("="*60)
        logger.info(f"SQLite database: {self.db_path.absolute()}")
        logger.info("="*60)
    
    def close(self):
        self.conn.close()

def main():
    logger.info("Starting normalization...")
    
    normalizer = PricingNormalizer()
    
    try:
        normalizer.normalize_all()
        
        # Verify integrity before exporting
        if not normalizer.verify_integrity():
            logger.error("\n❌ Data integrity check failed!")
            logger.error("Please review errors and re-run normalization.")
            return False
        
        normalizer.export_postgresql_dump()
        normalizer.export_csv()
        normalizer.print_summary()
        
        logger.info("\n✓ Normalization completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Normalization failed: {e}")
        return False
    finally:
        normalizer.close()

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)