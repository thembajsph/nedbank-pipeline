#!/usr/bin/env python3
"""Stage 2 Silver Layer - Memory optimized with chunking."""

import sys
import os
import logging
import duckdb
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("Starting Stage 2 Silver Layer (Optimized)")
    logger.info("=" * 60)
    
    bronze_base = "/data/output/bronze"
    silver_base = "/data/output/silver"
    
    try:
        conn = duckdb.connect()
        
        # Set DuckDB to use disk for large operations
        conn.execute("PRAGMA memory_limit='1GB'")
        conn.execute("PRAGMA temp_directory='/tmp'")
        
        # Process accounts - simple dedup without temp tables
        accounts_file = f"{bronze_base}/accounts/data.parquet"
        if os.path.exists(accounts_file):
            logger.info("Deduplicating accounts...")
            os.makedirs(f"{silver_base}/accounts", exist_ok=True)
            
            # Use simpler dedup that's memory efficient
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{accounts_file}')
                    QUALIFY row_number() OVER (PARTITION BY account_id ORDER BY account_id) = 1
                ) TO '{silver_base}/accounts/data.parquet' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_base}/accounts/data.parquet')").fetchone()[0]
            logger.info(f"✓ Accounts: {count:,} records")
        
        # Process transactions - use a more memory-efficient approach
        tx_file = f"{bronze_base}/transactions/data.parquet"
        if os.path.exists(tx_file):
            logger.info("Deduplicating transactions (this may take a few minutes)...")
            os.makedirs(f"{silver_base}/transactions", exist_ok=True)
            
            # Process in chunks using DuckDB's built-in optimization
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{tx_file}')
                    QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY transaction_id) = 1
                ) TO '{silver_base}/transactions/data.parquet' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_base}/transactions/data.parquet')").fetchone()[0]
            logger.info(f"✓ Transactions: {count:,} records")
        
        # Process customers
        customers_file = f"{bronze_base}/customers/data.parquet"
        if os.path.exists(customers_file):
            logger.info("Deduplicating customers...")
            os.makedirs(f"{silver_base}/customers", exist_ok=True)
            
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{customers_file}')
                    QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
                ) TO '{silver_base}/customers/data.parquet' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_base}/customers/data.parquet')").fetchone()[0]
            logger.info(f"✓ Customers: {count:,} records")
        
        conn.close()
        logger.info("Silver layer completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Silver layer failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
