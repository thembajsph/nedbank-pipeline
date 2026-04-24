#!/usr/bin/env python3
"""Main entry point - runs all pipeline layers."""

import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run all pipeline layers in sequence."""
    logger.info("=" * 60)
    logger.info("Starting Nedbank Medallion Pipeline")
    logger.info("=" * 60)
    
    layers = ['ingest', 'transform', 'provision']
    
    for layer in layers:
        logger.info(f"\n>>> Running {layer}.py...")
        result = subprocess.run([sys.executable, '-m', f'pipeline.{layer}'])
        if result.returncode != 0:
            logger.error(f"Pipeline failed at {layer} layer")
            sys.exit(result.returncode)
    
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully")
    logger.info("=" * 60)
    sys.exit(0)

if __name__ == "__main__":
    main()
