#!/usr/bin/env python3
import sys, os, subprocess, logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("Starting Nedbank Medallion Pipeline - Stage 2")
    logger.info("=" * 60)
    
    os.makedirs("/data/output/bronze", exist_ok=True)
    os.makedirs("/data/output/silver", exist_ok=True)
    os.makedirs("/data/output/gold", exist_ok=True)
    
    for layer in ['ingest', 'transform', 'provision']:
        logger.info(f"\n>>> Running {layer}.py...")
        result = subprocess.run([sys.executable, '-m', f'pipeline.{layer}'])
        if result.returncode != 0:
            logger.error(f"Pipeline failed at {layer}")
            sys.exit(result.returncode)
    
    logger.info("Pipeline completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()
