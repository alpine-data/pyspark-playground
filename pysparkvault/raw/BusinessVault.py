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
            .write.mode('overwrite').saveAsTable(ref_active_table_name) # TODO mw: Write to currated database.

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
        left_load_date_column: Optional[str] = None, left_load_end_date_column: Optional[str] = None,
        right_load_date_column: Optional[str] = None, right_load_end_date_column: Optional[str] = None,
        load_date_column: Optional[str] = None, load_end_date_column: Optional[str] = None):

        if left_load_date_column is None:
            left_load_date_column = self.conventions.load_date_column_name()

        if left_load_end_date_column is None:
            left_load_end_date_column = self.conventions.load_end_date_column_name()

        if right_load_date_column is None:
            right_load_date_column = self.conventions.load_date_column_name()

        if right_load_end_date_column is None:
            right_load_end_date_column = self.conventions.load_end_date_column_name()

        if load_date_column is None:
            load_date_column = self.conventions.load_date_column_name()

        if load_end_date_column is None:
            load_end_date_column = self.conventions.load_end_date_column_name()

        left_load_date_column_tmp = f"{left_load_date_column}__LEFT"
        left_load_end_date_column_tmp = f"{left_load_end_date_column}__LEFT"

        right_load_date_column_tmp = f"{right_load_date_column}__RIGHT"
        right_load_end_date_column_tmp = f"{right_load_end_date_column}__RIGHT"

        left = left \
            .withColumnRenamed(left_load_date_column, left_load_date_column_tmp) \
            .withColumnRenamed(left_load_end_date_column, left_load_end_date_column_tmp)

        right = right \
            .withColumnRenamed(right_load_date_column, right_load_date_column_tmp) \
            .withColumnRenamed(right_load_end_date_column, right_load_end_date_column_tmp)

        result = left \
            .join(right, on, how=how)

        result = result \
            .filter(result[right_load_end_date_column_tmp] > result[left_load_date_column_tmp]) \
            .filter(result[left_load_end_date_column_tmp] > result[right_load_date_column_tmp]) \
            .withColumn(
                load_date_column, 
                F.greatest(result[left_load_date_column_tmp], result[right_load_date_column_tmp])) \
            .withColumn(
                load_end_date_column,
                F.least(result[left_load_end_date_column_tmp], result[right_load_end_date_column_tmp]))
        
        result = result \
            .drop(left_load_date_column_tmp) \
            .drop(left_load_end_date_column_tmp) \
            .drop(right_load_date_column_tmp) \
            .drop(right_load_end_date_column_tmp)

        return result

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
        from_hkey_column_name: str,
        to_hkey_column_name: str,
        remove_hkeys: bool = False) -> DataFrame:

        link_table_name = self.conventions.link_name(link_table_name)
        lnk_df = self.spark.table(f"`{self.config.raw_database_name}`.`{link_table_name}`")

        result = self \
            .zip_historized_dataframes(
                lnk_df \
                    .drop(lnk_df[self.conventions.load_date_column_name()]) \
                    .join(from_df, lnk_df[from_hkey_column_name] == from_df[self.conventions.hkey_column_name()]),
                to_df,
                lnk_df[to_hkey_column_name] == to_df[self.conventions.hkey_column_name()])

        if remove_hkeys:
            result = result \
                .drop(lnk_df[self.conventions.hkey_column_name()]) \
                .drop(lnk_df[self.conventions.record_source_column_name()]) \
                .drop(lnk_df[from_hkey_column_name]) \
                .drop(lnk_df[to_hkey_column_name]) \
                .drop(from_df[self.conventions.hkey_column_name()]) \
                .drop(to_df[self.conventions.hkey_column_name()])

        return result