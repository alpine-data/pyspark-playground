import datetime
import json

import pyspark.sql.functions as F

from behave import given
from behave import then
from behave import when
from behave.runner import Context
from behave.model import Row

from features.DataVaultSchemaMapping import DataVaultSchemaMapping
from features.Schema import Column, ColumnType, Schema
from pyspark.sql.types import *


@given("we have date `{date}`.")
def step_impl(context: Context, date: str) -> None:  # noqa: F811
    context.dates[date] = datetime.datetime.now()


@given("a date `{date}` which is `{delay}` {time_unit} after `{delayed_date}`.")  # noqa: F811
def step_impl(context: Context, date: str, delay: int, time_unit: str, delayed_date: str) -> None:  # noqa: F811
    if not time_unit.endswith("s"):
        time_unit = f"{time_unit}s"
    context.dates[date] = context.dates[delayed_date] + datetime.timedelta(**{time_unit: int(delay)})

@given("we have source database schema `{schema}` and a data vault mapping `{schema_mapping}`.")
def step_impl(context: Context, schema: str, schema_mapping: str) -> None:  # noqa: F811

    # load schema from file
    with open(f"./features/{schema}") as json_file:
        context.schema = Schema(**json.load(json_file))

    # load schema mapping from file
    context.schema_mapping = DataVaultSchemaMapping.from_yaml(f"./features/{schema_mapping}")


@given("we have CDC batch `{batch_name}`.")
def step_impl(context: Context, batch_name: str) -> None:  # noqa: F811
    context.cdc_batch_names.append(batch_name)


@given("the batch contains changes for table `{table_name}`.")
def step_impl(context: Context, table_name: str) -> None:  # noqa: F811
    table = context.schema.get_table(table_name)
    table.columns.insert(0, Column(name="OPERATION", type=ColumnType.integer))
    table.columns.insert(1, Column(name="LOAD_DATE", type=ColumnType.date))

    data = [tuple(list(preprocess_row(context, row))) for row in context.table.rows]
    schema = StructType([StructField(column.name, StringType(), False) for column in table.columns])

    df = context.spark.createDataFrame(data, schema)
    df.write.mode('overwrite').parquet(f"{context.staging_base_path}/{context.cdc_batch_names[-1]}/{table_name}.parquet")


@when("the CDC batch `{batch_name}` is loaded at `{load_time}`.")
def step_impl(context: Context, batch_name: str, load_time: str) -> None:  # noqa: F811
    pass


@then("we expect the raw vault table `{table}` to contain the following entries exactly once")
@then("the raw vault table `{table}` to contain the following entries exactly once")
def step_impl(context: Context, table: str) -> None:  # noqa: F811
    # df = context.spark.table(f"{context.raw_base_path}.{table}")

    # for row in context.table.rows:
    #     row = preprocess_row(context, row)
    #     df_new = df
    #     for column in context.table.headings:
    #         df_new = df_new.filter(F.col(column) == row[column])

    #     row_count = df_new.count()
    #     if row_count == 0:
    #         assert False, f"The entry {list(row)} is not contained in the raw vault table {table}."
    #     elif row_count > 1:
    #         assert False, f"The entry {list(row)} is contained {row_count} times in the raw vault table {table}."

    # context.raw_vault_tables.append(table)
    pass

@then("we expect the raw vault table `{table}` to contain exactly `{n}` entries.")
def step_impl(context: Context, table: str, n: str) -> None:  # noqa: F811
    # df = context.spark.table(f"{context.raw_base_path}.{table}")
    
    # row_count = df.count()
    # if row_count > int(n):
    #     assert False, "The raw vault table contains more than {n} entries."
    # elif row_count < int(n):
    #     assert False, "The raw vault table contains less than {n} entries."
    pass


@then("the raw vault table should contain exactly `{n}` rows with the following attributes")
def step_impl(context: Context, n: str) -> None:  # noqa: F811
    # row = context.table.rows[0]
    # for column in context.table.headings:
    #     df = df.filter(F.col(column) == row[column])

    # assert df.count() == int(n), "The number of rows in the raw vault table {table} is not correct"
    pass


def preprocess_row(context: Context, row: Row) -> Row:
    cells = list(row)
    headings = row.headings

    # map operation strings to ids
    if "OPERATION" in headings:
        idx = headings.index("OPERATION")
        cells[idx] = str(map_opertation_sting_to_id(cells[idx]))

    # map date strings to datetime objects stored in context
    cells = [str(context.dates[cells[cell]]) if cell in context.dates.keys() == str else cell for cell in cells]

    # remove quotes from strings
    cells = [cell.strip("\"") if type(cell) == str else cell for cell in cells]

    return Row(headings, cells)


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
