#!/usr/bin/env python3
import sys, os, logging, duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("Starting Stage 2 Bronze Layer Ingestion")
    logger.info("=" * 60)
    
    input_base = "/data/input"
    output_base = "/data/output/bronze"
    
    conn = duckdb.connect()
    
    accounts_file = f"{input_base}/accounts.csv"
    if os.path.exists(accounts_file):
        logger.info("Processing accounts...")
        os.makedirs(f"{output_base}/accounts", exist_ok=True)
        conn.execute(f"COPY (SELECT * FROM read_csv_auto('{accounts_file}')) TO '{output_base}/accounts/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_base}/accounts/data.parquet')").fetchone()[0]
        logger.info(f"✓ Accounts: {count:,}")
    
    tx_file = f"{input_base}/transactions.jsonl"
    if os.path.exists(tx_file):
        logger.info("Processing transactions...")
        os.makedirs(f"{output_base}/transactions", exist_ok=True)
        conn.execute(f"COPY (SELECT * FROM read_json_auto('{tx_file}')) TO '{output_base}/transactions/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_base}/transactions/data.parquet')").fetchone()[0]
        logger.info(f"✓ Transactions: {count:,}")
    
    customers_file = f"{input_base}/customers.csv"
    if os.path.exists(customers_file):
        logger.info("Processing customers...")
        os.makedirs(f"{output_base}/customers", exist_ok=True)
        conn.execute(f"COPY (SELECT * FROM read_csv_auto('{customers_file}')) TO '{output_base}/customers/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_base}/customers/data.parquet')").fetchone()[0]
        logger.info(f"✓ Customers: {count:,}")
    
    conn.close()
    logger.info("Bronze layer complete")

if __name__ == "__main__":
    main()
