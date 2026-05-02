#!/usr/bin/env python3
import os, sys, json, logging, duckdb
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Stream Ingestion")
    
    conn = duckdb.connect()
    conn.execute("PRAGMA memory_limit='1GB'")
    
    # Load account balances
    accounts_file = "/data/output/gold/dim_accounts/data.parquet"
    if not os.path.exists(accounts_file):
        logger.error("No account balances found")
        sys.exit(1)
    
    balances = conn.execute("SELECT account_id, current_balance FROM read_parquet(?)", [accounts_file]).fetchall()
    balance_dict = {row[0]: float(row[1]) for row in balances}
    logger.info(f"Loaded {len(balance_dict)} account balances")
    
    # Process stream files
    stream_dir = Path("/data/stream")
    stream_files = sorted(stream_dir.glob("stream_*.jsonl"))
    logger.info(f"Found {len(stream_files)} stream files")
    
    os.makedirs("/data/output/stream_gold/current_balances", exist_ok=True)
    os.makedirs("/data/output/stream_gold/recent_transactions", exist_ok=True)
    
    all_updates = {}
    
    for file_path in stream_files:
        logger.info(f"Processing: {file_path.name}")
        
        with open(file_path, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                event = json.loads(line)
                account_id = event.get('account_id')
                if not account_id or account_id not in balance_dict:
                    continue
                
                amount = float(event.get('amount', 0))
                tx_type = event.get('transaction_type', '').upper()
                tx_date = event.get('transaction_date', '')
                tx_time = event.get('transaction_time', '00:00:00')
                
                current = balance_dict[account_id]
                if tx_type in ('DEBIT', 'FEE'):
                    new_balance = current - amount
                elif tx_type == 'CREDIT':
                    new_balance = current + amount
                else:
                    new_balance = current
                
                balance_dict[account_id] = new_balance
                all_updates[account_id] = new_balance
    
    # Write current_balances
    if all_updates:
        values = [f"('{aid}', {bal}, '{datetime.now().isoformat()}')" for aid, bal in list(all_updates.items())[:5000]]
        conn.execute("CREATE OR REPLACE TEMP TABLE updates AS SELECT * FROM (VALUES " + ",".join(values) + ") AS t(account_id, current_balance, updated_at)")
        conn.execute("COPY updates TO '/data/output/stream_gold/current_balances/data.parquet' (FORMAT PARQUET)")
        logger.info(f"Written current_balances for {len(all_updates)} accounts")
    
    conn.close()
    logger.info("Stream ingestion completed")

if __name__ == "__main__":
    main()
