import datetime
import json
import copy
import re
import shutil

from pathlib import Path
from typing import Dict
from functional import seq
from pyspark import SQLContext

import pyspark.sql.functions as F

from behave import given
from behave import then
from behave import when
from behave.runner import Context
from behave.model import Row

from pysparkvault.raw.DataVaultSchemaMapping import DataVaultSchemaMapping
from pysparkvault.raw.Metadata import Metadata, MetadataConfiguration
from pysparkvault.raw.RawVaultNew import RawVault, RawVaultConfiguration
from pysparkvault.raw.Schema import Column, ColumnType, Schema
from pyspark.sql.types import *
from pyspark.sql import SparkSession


@given("we have date `{date}`.")
def step_impl(context: Context, date: str) -> None:  # noqa: F811
    context.dates[date] = datetime.datetime.now()


@given("a date `{date}` which is `{delay}` {time_unit} after `{delayed_date}`.")  # noqa: F811
def step_impl(context: Context, date: str, delay: int, time_unit: str, delayed_date: str) -> None:  # noqa: F811
    if not time_unit.endswith("s"):
        time_unit = f"{time_unit}s"
    context.dates[date] = context.dates[delayed_date] + datetime.timedelta(**{time_unit: int(delay)})


@given("a date `{date}` which is the maximum date.")  # noqa: F811
def step_impl(context: Context, date) -> None:  # noqa: F811
    context.dates[date] = datetime.datetime.max


@given("we have source database schema `{schema}` and a data vault mapping `{schema_mapping}`.")
def step_impl(context: Context, schema: str, schema_mapping: str) -> None:  # noqa: F811

    # load schema from file
    with open(f"./features/{schema}") as json_file:
        context.schema = Schema(**json.load(json_file))

    # load schema mapping from file
    context.schema_mapping = DataVaultSchemaMapping.from_yaml(f"./features/{schema_mapping}")


@given("we have CDC batch `{batch_name}`.")
def step_impl(context: Context, batch_name: str) -> None:  # noqa: F811
    if not batch_name in context.cdc_batch_names:
        context.cdc_batch_names.append(batch_name)


@given("the batch contains changes for table `{table_name}`.")
def step_impl(context: Context, table_name: str) -> None:  # noqa: F811
    if not context.created_metadata:
        create_metadata(context)
        context.created_metadata = True

    table = copy.deepcopy(context.schema.get_table(table_name))
    table.columns.insert(0, Column(name="OPERATION", type=ColumnType.text))
    table.columns.insert(1, Column(name="LOAD_DATE", type=ColumnType.date))

    data = [tuple(preprocess_row(context, row).values()) for row in context.table.rows]
    schema = StructType([StructField(column.name, map_column_type_to_spark_type(column.type), True) for column in table.columns])

    df = context.spark.createDataFrame(data, schema) 
    df.write.mode('overwrite').parquet(f"{context.metadata.config.staging_location}/{context.cdc_batch_names[-1]}/{table_name}.parquet")


@when("the CDC batch `{batch_name}` is loaded at `{load_time}`.")
def step_impl(context: Context, batch_name: str, load_time: str) -> None:  # noqa: F811
    if not context.created_raw_vault:
        create_raw_vault(context)
        context.created_raw_vault = True

    context.metadata.config.staging_location = re.sub(r"/batch.*", "", context.metadata.config.staging_location)
    context.metadata.config.staging_location = f"{context.metadata.config.staging_location}/{batch_name}"
    context.metadata.config.load_date_dt = context.dates[load_time]

    for sat in context.schema_mapping.satellites:
        context.raw.initialize_satellite(sat.name)

    for hub in context.schema_mapping.hubs:
        context.raw.initialize_hub(hub.name)
        context.raw.load_hub(hub.name)

    for lnk in context.schema_mapping.links:
        context.raw.initialize_link(lnk.name)
        context.raw.load_link(lnk.name)


@when("the $__HKEY for the following line in the raw vault table `{table}` is assigned to `{variable_name}`")
def step_impl(context: Context, table: str, variable_name: str) -> None:  # noqa: F811
    df = context.spark.table(f"{context.metadata.config.raw_public_database_name}.{table}")
    row = preprocess_row(context, context.table.rows[0])

    for column in context.table.headings:
        df = df.filter(F.col(column) == row[column])

    hkey = df.select("$__HKEY").collect()[0][0]
    context.hkeys[variable_name] = hkey


@then("the raw vault table `{table}` is created.")
def step_impl(context: Context, table: str) -> None:  # noqa: F811
    sqlContext = SQLContext(context.spark.sparkContext)
    table_names_in_db = sqlContext.tableNames(context.metadata.config.raw_public_database_name)

    table_exists = table.lower() in table_names_in_db
    assert table_exists, f"The table {table} does not exist in the raw vault."


@then("we expect the raw vault table `{table}` to contain the following entries exactly once")
@then("the raw vault table `{table}` to contain the following entries exactly once")
def step_impl(context: Context, table: str) -> None:  # noqa: F811
    df = context.spark.table(f"{context.metadata.config.raw_public_database_name}.{table}")

    # if table == "SAT__EFFECTIVITY_MOVIES_DIRECTORS":
    #     df.show()

    for row in context.table.rows:
        row = preprocess_row(context, row)
        df_new = df

        for column in context.table.headings:
            if row[column] is not None:
                df_new = df_new.filter(F.col(column) == row[column])
            else:
                df_new = df_new.filter(F.col(column).isNull())

        row_count = df_new.count()
        if row_count == 0:
            assert False, f"The entry {row.values()} is not contained in the raw vault table {table}."
        elif row_count > 1:
            assert False, f"The entry {row.values()} is contained {row_count} times in the raw vault table {table}."

    context.raw_vault_tables.append(table)


@then("we expect the raw vault table `{table}` to contain exactly `{n}` entries.")
def step_impl(context: Context, table: str, n: str) -> None:  # noqa: F811
    df = context.spark.table(f"{context.metadata.config.raw_public_database_name}.{table}")
    
    row_count = df.count()
    if row_count > int(n):
        assert False, f"The raw vault table contains more ({row_count}) than {n} entries."
    elif row_count < int(n):
        assert False, f"The raw vault table contains less ({row_count}) than {n} entries."


@then("the raw vault table should contain exactly `{n}` rows with the following attributes")
def step_impl(context: Context, n: str) -> None:  # noqa: F811
    df = context.spark.table(f"{context.metadata.config.raw_public_database_name}.{context.raw_vault_tables[-1]}")

    row = context.table.rows[0]
    row = preprocess_row(context, row)
    for column in context.table.headings:
        df = df.filter(F.col(column) == row[column])

    assert df.count() == int(n), "The number of rows in the raw vault table {table} is not correct"


def preprocess_row(context: Context, row: Row) -> Dict:
    cells = list(row)
    headings = row.headings

    # map operation strings to ids
    if "OPERATION" in headings:
        idx = headings.index("OPERATION")
        cells[idx] = str(map_opertation_sting_to_id(cells[idx]))

    # map hkeys to hkey variables
    hkey_idxs = [i for i in range(len(headings)) if "HKEY" in headings[i]]
    if hkey_idxs:
        for idx in hkey_idxs:
            cells[idx] = context.hkeys[cells[idx]]

    # preprocess strings:
    #   - map date strings to datetime objects stored in context
    #   - remove quotes from strings
    #   - replace "None" String by None
    cells = seq(cells) \
        .map(lambda c: context.dates[c] if c in context.dates.keys() else c) \
        .map(lambda c: c.strip("\"") if type(c) == str else c) \
        .map(lambda c: None if c == "None" else c) \
        .to_list()

    return dict(zip(headings, cells))


def map_opertation_sting_to_id(operation: str):
    if operation == "snapshot":
        return 0
    elif operation == "delete":
        return 1
    elif operation == "create":
        return 2
    elif operation == "before_update":
        return 3
    elif operation == "update":
        return 4
    else:
        raise ValueError('Unknown CDC operation specified.')


def map_column_type_to_spark_type(type: ColumnType) -> str:
    """
    Maps a column type to a Spark column type.

    :param type: The column type.
    :retruns: The Spark column type.
    """

    if type == ColumnType.date:
        return TimestampType()
    elif type == ColumnType.datetime:
        return TimestampType()
    elif type == ColumnType.integer:
        return IntegerType()
    elif type == ColumnType.numeric:
        return LongType()
    elif type == ColumnType.text:
        return StringType()
    elif type == ColumnType.time:
        return TimestampType()
    elif type == ColumnType.varchar:
        return StringType()
    elif type == ColumnType.boolean:
        return BooleanType()
    else:
        return StringType()


def create_spark_session(context: Context):
    # build sprak session
    if context.active_spark_session:
        context.spark.stop()

    context.spark = SparkSession.builder \
        .master("local") \
        .appName("datavault") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def create_metadata(context: Context):
    metadata_config = MetadataConfiguration(context.schema, context.schema_mapping)
    context.metadata = Metadata(context.spark, metadata_config)


def create_raw_vault(context: Context):
    # clean datalake
    dirpath = Path('spark-warehouse')
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)

    # create databases for raw and curated
    context.metadata.initialize_databases()

    raw_config = RawVaultConfiguration(context.metadata, "LOAD_DATE", "OPERATION", "LAST_UPDATE")
    context.raw = RawVault(context.spark, raw_config)
