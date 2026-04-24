#!/usr/bin/env python3
"""Bronze Layer: Raw data ingestion with DuckDB."""

import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main entry point for Bronze layer ingestion."""
    logger.info("=" * 60)
    logger.info("Starting Bronze Layer Ingestion")
    logger.info("=" * 60)
    
    data_base = "/data"
    output_base = "/data/output/bronze"
    
    sources = {
        "accounts": f"{data_base}/accounts.csv",
        "transactions": f"{data_base}/transactions.jsonl",
        "customers": f"{data_base}/customers.csv"
    }
    
    try:
        conn = duckdb.connect()
        
        for source_name, file_path in sources.items():
            if not Path(file_path).exists():
                logger.warning(f"File {file_path} not found, skipping {source_name}")
                continue
            
            logger.info(f"Processing {source_name} from {file_path}")
            
            output_dir = f"{output_base}/{source_name}"
            os.makedirs(output_dir, exist_ok=True)
            output_file = f"{output_dir}/data.parquet"
            
            if source_name == "transactions":
                # For JSONL, read line by line to avoid memory issues
                conn.execute(f"""
                    COPY (
                        SELECT 
                            *,
                            '{source_name}' as _source,
                            current_timestamp as _ingestion_timestamp,
                            current_date as _ingestion_date
                        FROM read_json_auto('{file_path}')
                    ) TO '{output_file}' (FORMAT PARQUET)
                """)
            else:
                # For CSV files
                conn.execute(f"""
                    COPY (
                        SELECT 
                            *,
                            '{source_name}' as _source,
                            current_timestamp as _ingestion_timestamp,
                            current_date as _ingestion_date
                        FROM read_csv_auto('{file_path}')
                    ) TO '{output_file}' (FORMAT PARQUET)
                """)
            
            # Get record count
            result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()
            logger.info(f"✓ Wrote {result[0]:,} records to {output_file}")
        
        conn.close()
        logger.info("Bronze layer completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Bronze layer failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
