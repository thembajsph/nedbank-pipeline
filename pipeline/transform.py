#!/usr/bin/env python3
"""Silver Layer: Data cleansing with DuckDB (no pandas/pyarrow)."""

import sys
import os
import logging
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main entry point for Silver layer transformation."""
    logger.info("=" * 60)
    logger.info("Starting Silver Layer Transformation")
    logger.info("=" * 60)
    
    bronze_base = "/data/output/bronze"
    silver_base = "/data/output/silver"
    
    try:
        conn = duckdb.connect()
        
        # Process accounts - direct copy with minimal transformation
        accounts_input = f"{bronze_base}/accounts/data.parquet"
        if os.path.exists(accounts_input):
            logger.info("Processing accounts...")
            accounts_output = f"{silver_base}/accounts/data.parquet"
            os.makedirs(f"{silver_base}/accounts", exist_ok=True)
            
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{accounts_input}')
                ) TO '{accounts_output}' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{accounts_output}')").fetchone()[0]
            logger.info(f"✓ Accounts: {count:,} records")
        
        # Process transactions
        transactions_input = f"{bronze_base}/transactions/data.parquet"
        if os.path.exists(transactions_input):
            logger.info("Processing transactions...")
            transactions_output = f"{silver_base}/transactions/data.parquet"
            os.makedirs(f"{silver_base}/transactions", exist_ok=True)
            
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{transactions_input}')
                ) TO '{transactions_output}' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{transactions_output}')").fetchone()[0]
            logger.info(f"✓ Transactions: {count:,} records")
        
        # Process customers
        customers_input = f"{bronze_base}/customers/data.parquet"
        if os.path.exists(customers_input):
            logger.info("Processing customers...")
            customers_output = f"{silver_base}/customers/data.parquet"
            os.makedirs(f"{silver_base}/customers", exist_ok=True)
            
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{customers_input}')
                ) TO '{customers_output}' (FORMAT PARQUET)
            """)
            
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{customers_output}')").fetchone()[0]
            logger.info(f"✓ Customers: {count:,} records")
        
        conn.close()
        logger.info("Silver layer completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Silver layer failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
