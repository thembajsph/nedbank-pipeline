#!/usr/bin/env python3
import sys, os, json, logging, duckdb
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Starting Stage 2 Gold Layer")
    logger.info("=" * 60)
    
    silver_base = "/data/output/silver"
    gold_base = "/data/output/gold"
    
    conn = duckdb.connect()
    
    customers_file = f"{silver_base}/customers/data.parquet"
    if os.path.exists(customers_file):
        logger.info("Building dim_customers...")
        os.makedirs(f"{gold_base}/dim_customers", exist_ok=True)
        conn.execute(f"COPY (SELECT *, CURRENT_DATE as effective_date FROM read_parquet('{customers_file}')) TO '{gold_base}/dim_customers/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{gold_base}/dim_customers/data.parquet')").fetchone()[0]
        logger.info(f"✓ dim_customers: {count:,} records")
    
    accounts_file = f"{silver_base}/accounts/data.parquet"
    if os.path.exists(accounts_file):
        logger.info("Building dim_accounts...")
        os.makedirs(f"{gold_base}/dim_accounts", exist_ok=True)
        conn.execute(f"COPY (SELECT *, CURRENT_DATE as effective_date FROM read_parquet('{accounts_file}')) TO '{gold_base}/dim_accounts/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{gold_base}/dim_accounts/data.parquet')").fetchone()[0]
        logger.info(f"✓ dim_accounts: {count:,} records")
    
    tx_file = f"{silver_base}/transactions/data.parquet"
    if os.path.exists(tx_file):
        logger.info("Building fact_transactions...")
        os.makedirs(f"{gold_base}/fact_transactions", exist_ok=True)
        conn.execute(f"COPY (SELECT * FROM read_parquet('{tx_file}')) TO '{gold_base}/fact_transactions/data.parquet' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{gold_base}/fact_transactions/data.parquet')").fetchone()[0]
        logger.info(f"✓ fact_transactions: {count:,} records")
    
    conn.close()
    
    dq_report = {
        "run_timestamp": start_time.isoformat(),
        "stage": "2",
        "execution_duration_seconds": (datetime.now() - start_time).total_seconds()
    }
    with open("/data/output/dq_report.json", 'w') as f:
        json.dump(dq_report, f, indent=2)
    
    logger.info("Gold layer completed successfully")

if __name__ == "__main__":
    main()
