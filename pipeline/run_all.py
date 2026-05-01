#!/usr/bin/env python3
"""Main entry point - runs all pipeline layers."""

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
    logger.info("Starting Nedbank Medallion Pipeline - Stage 2")
    logger.info("=" * 60)
    
    # Ensure output directories exist
    os.makedirs("/data/output/bronze", exist_ok=True)
    os.makedirs("/data/output/silver", exist_ok=True)
    os.makedirs("/data/output/gold", exist_ok=True)
    
    layers = ['ingest', 'transform', 'provision']
    
    for layer in layers:
        logger.info(f"\n>>> Running {layer}.py...")
        result = subprocess.run([sys.executable, '-m', f'pipeline.{layer}'])
        if result.returncode != 0:
            logger.error(f"Pipeline failed at {layer} layer")
            sys.exit(result.returncode)
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
    logger.info("=" * 60)
    sys.exit(0)

if __name__ == "__main__":
    main()
