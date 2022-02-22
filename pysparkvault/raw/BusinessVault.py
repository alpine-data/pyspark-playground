import pyspark.sql.functions as F

from pyspark.sql import DataFrame, Column
from pyspark.sql.session import SparkSession
from typing import List, Optional, Union

from .DataVaultShared import *

class BusinessVaultConfiguration:

    def __init__(self, source_system_name: str) -> None:
        self.source_system_name = source_system_name
        self.raw_database_name = f'{self.source_system_name}__raw'.lower()

class BusinessVault:

    def __init__(self, spark: SparkSession, config: BusinessVaultConfiguration, conventions: DataVaultConventions = DataVaultConventions()) -> None:
        self.spark = spark
        self.config = config
        self.conventions = conventions

    def create_active_code_reference_table(self, ref_table_name: str, ref_active_table_name, id_column: str) -> None:
        """
        Creates an extract of a reference table only containing the current, up-to-date value for the references.
        """
        df_ref = self.spark.table(f"`{self.config.raw_database_name}`.`{ref_table_name}`")

        df_ref_left = df_ref \
            .groupBy(df_ref[id_column], df_ref[self.conventions.ref_group_column_name()]) \
            .agg(F.max(df_ref[self.conventions.load_date_column_name()]).alias(self.conventions.load_date_column_name()))

        df_ref_left \
            .join(df_ref, 
                (df_ref_left[id_column] == df_ref[id_column]) & \
                (df_ref_left[self.conventions.ref_group_column_name()] == df_ref[self.conventions.ref_group_column_name()])) \
            .drop(df_ref_left[id_column]) \
            .drop(df_ref_left[self.conventions.ref_group_column_name()]) \
            .drop(df_ref_left[self.conventions.load_date_column_name()]) \
            .write.mode('overwrite').saveAsTable(f"`{self.config.raw_database_name}`.`{ref_active_table_name}`") # TODO mw: Write to curated database.

    def initialize_database(self) -> None:
        """
        Initialize database.
        """

        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.config.raw_database_name} LOCATION '{self.config.raw_base_path}'""")

    def read_data_from_hub_sat_and_pit(self, hub_name: str, sat_name: str, pit_name: str, attributes: List[str], include_hkey: bool = False) -> DataFrame:
        df_pit = self.spark.table(f"`{self.config.raw_database_name}`.`{pit_name}`")
        df_sat = self.spark.table(f"`{self.config.raw_database_name}`.`{sat_name}`")
        df_hub = self.spark.table(f"`{self.config.raw_database_name}`.`{hub_name}`")

        hub_attributes = list(set(df_hub.columns) & set(attributes))
        sat_attributes = list(set(df_sat.columns) & set(attributes))
        hub_attributes = list([df_hub[column] for column in hub_attributes])
        sat_attributes = list([df_sat[column] for column in sat_attributes])

        if include_hkey:
            hub_attributes = hub_attributes + [df_hub[self.conventions.hkey_column_name()]]

        attribute_columns = hub_attributes + sat_attributes

        return df_pit \
            .join(df_sat, (
                (df_pit[self.conventions.hkey_column_name()] == df_sat[self.conventions.hkey_column_name()]) & \
                (df_pit[self.conventions.load_date_column_name()] == df_sat[self.conventions.load_date_column_name()]))) \
            .join(df_hub, df_hub[self.conventions.hkey_column_name()] == df_pit[self.conventions.hkey_column_name()]) \
            .select(attribute_columns + [df_pit[self.conventions.load_date_column_name()], df_pit[self.conventions.load_end_date_column_name()]]) \
            .groupBy(attribute_columns) \
            .agg(
                F.min(self.conventions.load_date_column_name()).alias(self.conventions.load_date_column_name()), 
                F.max(self.conventions.load_end_date_column_name()).alias(self.conventions.load_end_date_column_name()))

    def read_data_from_hub(self, name: str, attributes: List[str], include_hkey: bool = False) -> DataFrame:
        name = self.conventions.remove_prefix(name)

        hub_name = self.conventions.hub_name(name)
        sat_name = self.conventions.sat_name(name)
        pit_name = self.conventions.pit_name(name)

        return self.read_data_from_hub_sat_and_pit(hub_name, sat_name, pit_name, attributes, include_hkey)

    def zip_historized_dataframes(
        self, left: DataFrame, right: DataFrame, on: Union[str, List[str], Column, List[Column]], how: str = 'inner',
        left_load_date_column: Optional[Column] = None, left_load_end_date_column: Optional[Column] = None,
        right_load_date_column: Optional[Column] = None, right_load_end_date_column: Optional[Column] = None,
        load_date_column: Optional[str] = None, load_end_date_column: Optional[str] = None):

        if left_load_date_column is None:
            left_load_date_column = left[self.conventions.load_date_column_name()]

        if left_load_end_date_column is None:
            left_load_end_date_column = left[self.conventions.load_end_date_column_name()]

        if right_load_date_column is None:
            right_load_date_column = right[self.conventions.load_date_column_name()]

        if right_load_end_date_column is None:
            right_load_end_date_column = right[self.conventions.load_end_date_column_name()]

        if load_date_column is None:
            load_date_column = self.conventions.load_date_column_name()

        if load_end_date_column is None:
            load_end_date_column = self.conventions.load_end_date_column_name()

        result = left \
            .join(right, on, how=how)

        result = result \
            .filter(right_load_end_date_column.isNull() | left_load_date_column.isNull() | (right_load_end_date_column > left_load_date_column)) \
            .filter(left_load_end_date_column.isNull() | right_load_date_column.isNull() | (left_load_end_date_column > right_load_date_column)) \
            .withColumn(
                '$__LOAD_DATE__TMP', 
                F.greatest(left_load_date_column, right_load_date_column)) \
            .withColumn(
                '$__LOAD_END_DATE_TMP',
                F.least(left_load_end_date_column, right_load_end_date_column))
        
        return result \
            .drop(left_load_date_column) \
            .drop(left_load_end_date_column) \
            .drop(right_load_date_column) \
            .drop(right_load_end_date_column) \
            .withColumnRenamed('$__LOAD_DATE__TMP', load_date_column) \
            .withColumnRenamed('$__LOAD_END_DATE_TMP', load_end_date_column)

    def join_linked_hubs(
        self, 
        from_name: str, 
        to_name: str, 
        link_table_name: str,
        from_hkey_column_name: str,
        to_hkey_column_name: str,
        from_attributes: List[str],
        to_attributes: List[str],
        remove_hkeys: bool = False) -> DataFrame:

        from_df = self.read_data_from_hub(from_name, from_attributes, True)
        to_df = self.read_data_from_hub(to_name, to_attributes, True)

        return self.join_linked_dataframes(from_df, to_df, link_table_name, from_hkey_column_name, to_hkey_column_name, remove_hkeys)

    def join_linked_dataframes(
        self,
        from_df: DataFrame,
        to_df: DataFrame,
        link_table_name: str,
        lnk_from_hkey_column_name: str,
        lnk_to_hkey_column_name: str,
        from_df_hkey: Optional[Column] = None,
        to_df_hkey: Optional[Column] = None,
        from_load_date_column: Optional[Column] = None, 
        from_load_end_date_column: Optional[Column] = None,
        to_load_date_column: Optional[Column] = None, 
        to_load_end_date_column: Optional[Column] = None,
        load_date_column: Optional[str] = None, 
        load_end_date_column: Optional[str] = None) -> DataFrame:

        if from_df_hkey is None:
            from_df_hkey = from_df[self.conventions.hkey_column_name()]

        if to_df_hkey is None:
            to_df_hkey = to_df[self.conventions.hkey_column_name()]

        link_table_name = self.conventions.link_name(link_table_name)
        lnk_df = self.spark.table(f"`{self.config.raw_database_name}`.`{link_table_name}`")

        return self \
            .zip_historized_dataframes(
                lnk_df \
                    .drop(lnk_df[self.conventions.load_date_column_name()]) \
                    .join(from_df, lnk_df[lnk_from_hkey_column_name] == from_df_hkey, how="right") \
                    .drop(lnk_df[self.conventions.hkey_column_name()]) \
                    .drop(lnk_df[self.conventions.record_source_column_name()]),
                to_df, 
                lnk_df[lnk_to_hkey_column_name] == to_df_hkey, 
                how='left', 
                left_load_date_column=from_load_date_column,
                left_load_end_date_column=from_load_end_date_column,
                right_load_date_column=to_load_date_column,
                right_load_end_date_column=to_load_end_date_column,
                load_date_column=load_date_column,
                load_end_date_column=load_end_date_column) \
            .drop(lnk_from_hkey_column_name) \
            .drop(lnk_to_hkey_column_name)