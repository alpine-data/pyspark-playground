from pathlib import Path
import shutil

import pytest
from pyspark.sql import SparkSession


#@pytest.fixture(scope='session')
def spark():
    dirpath = Path('spark-warehouse')
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)

    return SparkSession.builder \
        .master("local") \
        .appName("datavault") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
