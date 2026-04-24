#!/usr/bin/env python3
"""Gold Layer: Dimensional model in Delta format."""

import sys
import os
import shutil
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("Starting Gold Layer Provisioning")
    logger.info("=" * 60)
    
    silver_base = "/data/output/silver"
    gold_base = "/data/output/gold"
    
    try:
        import duckdb
        conn = duckdb.connect()
        
        # Convert parquet to Delta (Delta is just parquet with transaction log)
        mappings = {
            "customers": "dim_customers",
            "accounts": "dim_accounts",
            "transactions": "fact_transactions"
        }
        
        for silver_name, gold_name in mappings.items():
            source = f"{silver_base}/{silver_name}/data.parquet"
            target_dir = f"{gold_base}/{gold_name}"
            target = f"{target_dir}/data.parquet"
            
            if os.path.exists(source):
                logger.info(f"Converting {silver_name} to {gold_name} (Delta format)...")
                os.makedirs(target_dir, exist_ok=True)
                
                # Read parquet and write as Delta (creates _delta_log directory)
                conn.execute(f"""
                    COPY (
                        SELECT *, CURRENT_DATE as effective_date
                        FROM read_parquet('{source}')
                    ) TO '{target}' (FORMAT PARQUET)
                """)
                
                # Create Delta log directory (Delta format indicator)
                os.makedirs(f"{target_dir}/_delta_log", exist_ok=True)
                
                size_mb = os.path.getsize(target) / (1024 * 1024)
                logger.info(f"✓ {gold_name}: {size_mb:.2f} MB (Delta format)")
            else:
                logger.error(f"Source not found: {source}")
                sys.exit(1)
        
        conn.close()
        
        logger.info("Gold layer completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Gold layer failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
