#!/usr/bin/env python3
"""Main entry point - runs batch then streaming."""

import sys
import os
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Starting Nedbank Medallion Pipeline - Stage 3")
    logger.info("=" * 60)
    
    os.makedirs("/data/output/bronze", exist_ok=True)
    os.makedirs("/data/output/silver", exist_ok=True)
    os.makedirs("/data/output/gold", exist_ok=True)
    os.makedirs("/data/output/stream_gold/current_balances", exist_ok=True)
    os.makedirs("/data/output/stream_gold/recent_transactions", exist_ok=True)
    
    # Batch pipeline
    for layer in ['ingest', 'transform', 'provision']:
        logger.info(f"\n>>> Running batch {layer}.py...")
        result = subprocess.run([sys.executable, '-m', f'pipeline.{layer}'])
        if result.returncode != 0:
            logger.error(f"Batch failed at {layer}")
            sys.exit(result.returncode)
    
    # Streaming pipeline
    logger.info("\n>>> Running stream_ingest.py...")
    result = subprocess.run([sys.executable, '-m', 'pipeline.stream_ingest'])
    if result.returncode != 0:
        logger.error("Streaming failed")
        sys.exit(result.returncode)
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"Pipeline completed in {duration:.2f} seconds")
    logger.info("=" * 60)
    sys.exit(0)

if __name__ == "__main__":
    main()
