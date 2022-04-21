import tempfile
import shutil

from behave import *
from behave.log_capture import capture

from pyspark.sql import SparkSession


@capture()
def before_feature(context, feature):
    context.working_directory = tempfile.mkdtemp()
    
    context.cdc_batch_names = []
    context.raw_vault_tables = []
    context.dates = {}
    context.batches = {}

    context.staging_base_path = f"{context.working_directory}/staging"
    context.staging_prepared_base_path = f"{context.working_directory}/staging_prepared"
    context.raw_base_path = f"{context.working_directory}/raw"

    context.spark = SparkSession.builder \
        .master("local") \
        .appName("datavault") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@capture()
def after_feature(context, feature):
    shutil.rmtree(context.working_directory)


@capture()
def before_scenario(context, scenario):
    pass


@capture()
def after_scenario(context, scenario):
    pass