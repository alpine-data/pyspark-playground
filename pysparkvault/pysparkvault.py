from pyspark.sql.dataframe import DataFrameStatFunctions
import pyspark.sql.functions as F

from delta.tables import *

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from typing import List, Tuple

from pyspark.sql.types import TimestampType

def __hkey(business_key_column_names: List[str]) -> Column:
    business_key_columns = list(map(lambda c: F.col(c), business_key_column_names))
    return F.md5(F.concat_ws(',', *business_key_columns))

def __hash(column_names: List[str]) -> Column:
    return __hkey(column_names)

def __load_date_to_timestamp(source_column: str = 'load_date') -> Column:
    return F.to_timestamp(F.col('load_date'), "yyyy-MM-dd'T'HH:mm:ss'Z'");

def load_hub(spark: SparkSession, hub_table_name: str, df: DataFrame, load_date_str: str, business_key_column_names: List[str], source_name: str):
    """
    
    """
    hub_column_names = ['hkey', 'load_date', 'last_seen_date', 'record_source']

    business_key_columns = list(map(lambda c: F.col(c), business_key_column_names))
    hub_columns = list(map(lambda c: F.col(c), hub_column_names))

    all_columns = hub_columns + business_key_columns
    all_column_names = hub_column_names + business_key_column_names
    map_columns = dict(list(map(lambda c: (c, f'updates.{c}'), all_column_names)))
    
    df = df \
        .withColumn('hkey', __hkey(business_key_column_names)) \
        .withColumn('load_date', F.lit(load_date_str)) \
        .withColumn('last_seen_date', __load_date_to_timestamp()) \
        .withColumn('load_date', __load_date_to_timestamp()) \
        .withColumn('record_source', F.lit(source_name)) \
        .select(*all_columns)

    DeltaTable.forName(spark, hub_table_name) \
        .alias('hub') \
        .merge(df.alias('updates'), 'hub.hkey = updates.hkey') \
        .whenMatchedUpdate(set = { 'last_seen_date': 'updates.last_seen_date' }) \
        .whenNotMatchedInsert(values = map_columns) \
        .execute()

    spark.sql(f'SELECT * FROM {hub_table_name} ORDER BY load_date').show()

def load_satellite(spark: SparkSession, sat_table_name: str, df: DataFrame, load_date_str: str, business_key_column_names: List[str]):
    """
    
    """
    sat_column_names = ['hkey', 'hdiff', 'load_date', 'load_end_date']
    attr_column_names = list(filter(lambda c: c not in business_key_column_names, df.columns))
    all_column_names = sat_column_names + attr_column_names
    all_columns = list(map(lambda c: F.col(c), all_column_names))

    map_columns = dict(list(map(lambda c: (c, f'updates.{c}'), all_column_names)))

    df = df \
        .withColumn('hkey', __hkey(business_key_column_names)) \
        .withColumn('hdiff', __hash(attr_column_names)) \
        .withColumn('load_date', F.lit(load_date_str)) \
        .withColumn('load_date', __load_date_to_timestamp()) \
        .withColumn('load_end_date', F.lit(None)) \
        .select(*all_columns)

    DeltaTable.forName(spark, sat_table_name) \
        .alias('sat') \
        .merge(df.alias('updates'), 'sat.hkey = updates.hkey and sat.hdiff = updates.hdiff') \
        .whenNotMatchedInsert(values = map_columns) \
        .execute()

    dfUpdated = spark.sql(f'''
        SELECT l.hkey, l.hdiff, r.load_date FROM {sat_table_name} AS l 
        FULL OUTER JOIN {sat_table_name} AS r ON l.hkey = r.hkey
        WHERE l.load_end_date IS NULL
        AND l.hdiff != r.hdiff
        AND l.load_date < r.load_date
    ''')

    DeltaTable.forName(spark, sat_table_name) \
        .alias('sat') \
        .merge(dfUpdated.alias('updates'), 'sat.hkey = updates.hkey and sat.hdiff = updates.hdiff') \
        .whenMatchedUpdate(set = { 'load_end_date': 'updates.load_date' }) \
        .execute()

    spark.sql(f'SELECT * FROM {sat_table_name} ORDER BY load_date').show()

def load_link(spark: SparkSession, lnk_table_name: str, hubs: List[Tuple[str, DataFrame]]):
    """
    
    """

