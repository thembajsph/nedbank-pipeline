#!/bin/bash

set -e  # Exit on error

echo "=========================================="
echo "Nedbank Pipeline Test Suite"
echo "=========================================="

# Get absolute paths
DATA_PATH=$(cd ../data && pwd)
echo "Data path: $DATA_PATH"

# Clean previous outputs
echo -e "\n[1/4] Cleaning previous outputs..."
rm -rf ../data/output/

# Test Bronze Layer
echo -e "\n[2/4] Testing Bronze Layer..."
docker run --rm \
  -v ${DATA_PATH}:/data \
  -m 2g --cpus="2" \
  nedbank-pipeline:latest \
  python -m pipeline.ingest

if [ $? -eq 0 ]; then
    echo "✅ Bronze layer passed"
else
    echo "❌ Bronze layer failed"
    exit 1
fi

# Test Silver Layer
echo -e "\n[3/4] Testing Silver Layer..."
docker run --rm \
  -v ${DATA_PATH}:/data \
  -m 2g --cpus="2" \
  nedbank-pipeline:latest \
  python -m pipeline.transform

if [ $? -eq 0 ]; then
    echo "✅ Silver layer passed"
else
    echo "❌ Silver layer failed"
    exit 1
fi

# Test Gold Layer
echo -e "\n[4/4] Testing Gold Layer..."
docker run --rm \
  -v ${DATA_PATH}:/data \
  -m 2g --cpus="2" \
  nedbank-pipeline:latest \
  python -m pipeline.provision

if [ $? -eq 0 ]; then
    echo "✅ Gold layer passed"
else
    echo "❌ Gold layer failed"
    exit 1
fi

echo -e "\n=========================================="
echo "✅ All tests passed!"
echo "=========================================="

# Show output sizes
echo -e "\nOutput sizes:"
du -sh ../data/output/bronze/ ../data/output/silver/ ../data/output/gold/

# Show record counts
echo -e "\nRecord counts:"
docker run --rm -v ${DATA_PATH}:/data nedbank-pipeline:latest python -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
builder = SparkSession.builder.appName('count')
builder = builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
builder = builder.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f'  fact_transactions: {spark.read.format(\"delta\").load(\"/data/output/gold/fact_transactions\").count():,}')
print(f'  dim_accounts: {spark.read.format(\"delta\").load(\"/data/output/gold/dim_accounts\").count():,}')
print(f'  dim_customers: {spark.read.format(\"delta\").load(\"/data/output/gold/dim_customers\").count():,}')
spark.stop()
"
