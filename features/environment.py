import tempfile
import shutil

from behave import *
from behave.log_capture import capture
from behave.model import Scenario
from pyspark.sql import SparkSession


def before_all(context):
    userdata = context.config.userdata
    continue_after_failed = userdata.getbool("runner.continue_after_failed_step", False)
    Scenario.continue_after_failed_step = continue_after_failed


@capture()
def before_scenario(context, scenario):
    context.working_directory = tempfile.mkdtemp()

    context.created_metadata = False
    context.created_raw_vault = False
    
    context.cdc_batch_names = []
    context.raw_vault_tables = []
    context.dates = {}
    context.batches = {}
    context.hkeys = {}

    # create spark session
    context.spark = SparkSession.builder \
        .master("local") \
        .appName("datavault") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@capture()
def after_scenario(context, scenario):
    shutil.rmtree(context.working_directory)

    # stop spark session
    context.spark.stop()