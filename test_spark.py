#!/usr/bin/env python3
"""Test Spark configuration."""
import os
import sys

# Set Spark home
os.environ['SPARK_HOME'] = '/usr/local/lib/python3.11/dist-packages/pyspark'
os.environ['PATH'] = f"{os.environ['SPARK_HOME']}/bin:{os.environ['PATH']}"

print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

try:
    from pyspark.sql import SparkSession
    print("✓ PySpark imported successfully")
    
    spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    print(f"✓ Spark session created: {spark.version}")
    
    df = spark.range(10)
    print(f"✓ Test DataFrame count: {df.count()}")
    
    spark.stop()
    print("✓ All Spark tests passed!")
    sys.exit(0)
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
