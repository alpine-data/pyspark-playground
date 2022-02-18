from datetime import datetime
from json import load
import select
import pyspark.sql.functions as F

from delta.tables import *
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DataType
from pyspark.sql.session import SparkSession
from typing import List, Optional, Union

from pyspark.sql.types import StringType, StructField, StructType,TimestampType


class DataVaultFunctions:

    @staticmethod
    def hash(column_names: List[str]) -> Column:
        """
        Calculates a MD5 hash of provided columns.

        :param column_names - The columns which should be included in the hash.
        """

        columns = list(map(lambda c: F.col(c), column_names))
        return F.md5(F.concat_ws(',', *columns))

    @staticmethod
    def to_columns(column_names: List[str]) -> List[Column]:
        """"
        Convert a list of column names to DataFrame columns.

        :param column_names - The list if column names.
        """
        return list(map(lambda c: F.col(c), column_names))

    @staticmethod
    def to_timestamp(column_name: str = 'load_date', pattern: str = "yyyy-MM-dd'T'HH:mm:ss'Z'") -> Column:
        """
        Converts a date str of a format (pattern) to a timestamp column.

        :param column_name - The column which contains the date string.
        :param pattern - The (java.time) pattern of the date string.
        """

        return F.to_timestamp(F.col(column_name), pattern);


class DataVaultConventions:

    def __init__(
        self,  column_prefix = '$__', hub = 'HUB__', link = 'LNK__', ref = 'REF__', sat = 'SAT__', pit = 'PIT__',
        hkey = 'HKEY', hdiff = 'HDIFF', load_date = 'LOAD_DATE', load_end_date = 'LOAD_END_DATE', record_source = 'RECORD_SOURCE',
        ref_group = 'GROUP', deleted = 'DELETED', valid_from = 'VALID_FROM', valid_to = 'VALID_TO', cdc_operation = 'OPERATION') -> None:
        
        self.COLUMN_PREFIX = column_prefix
        self.HUB = hub
        self.LINK = link
        self.REF = ref
        self.SAT = sat
        self.PIT = pit
        self.HKEY = hkey
        self.HDIFF = hdiff
        self.LOAD_DATE = load_date
        self.LOAD_END_DATE = load_end_date
        self.RECORD_SOURCE = record_source
        self.REF_GROUP = ref_group
        self.DELETED = deleted
        self.VALID_FROM = valid_from
        self.VALID_TO = valid_to
        self.CDC_OPERATION = cdc_operation

    def cdc_operation_column_name(self) -> str:
        """
        Return the column name of the CDC operation (used in prepared staging tables).
        """
        return f'{self.COLUMN_PREFIX}{self.CDC_OPERATION}'

    def hdiff_column_name(self) -> str:
        """
        Return the column name for HDIFF column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.HDIFF}'

    def hkey_column_name(self) -> str:
        """
        Return the column name for HKEY column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.HKEY}'

    def hub_name(self, source_table_name: str) -> str:
        """
        Returns a name of a HUB table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """

        source_table_name = source_table_name.upper()

        if source_table_name.startswith(self.HUB):
            return source_table_name
        else:
            return f'{self.HUB}{source_table_name}'

    def link_name(self, name: str) -> str:
        """
        Returns a name of a LINK table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """

        name = name.upper()

        if name.startswith(self.LINK):
            return name
        else:
            return f'{self.LINK}{name}'

    def load_date_column_name(self) -> str:
        """
        Return the column name for LOAD_DATE column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.LOAD_DATE}'

    def load_end_date_column_name(self) -> str:
        """
        Return the column name for LOAD_END_DATE column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.LOAD_END_DATE}'

    def pit_name(self, name: str) -> str:
        """
        Returns a name of a PIT (point-in-time-table) table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """

        name = name.upper()

        if name.startswith(self.PIT):
            return name
        else:
            return f'{self.PIT}{name}'

    def record_source_column_name(self) -> str:
        """
        Return the column name for RECORD_SOURCE column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.RECORD_SOURCE}'

    def remove_prefix(self, name: str) -> str:
        """
        Return a table name without its prefix (e.g. 'HUB_FOO` will be transformed to 'FOO').
        """

        return name \
            .replace(self.HUB, '') \
            .replace(self.LINK, '') \
            .replace(self.REF, '') \
            .replace(self.SAT, '') \
            .replace(self.PIT, '')

    def ref_group_column_name(self) -> str:
        """
        Returns the column name for group column of shared reference tables.
        """
        return f'{self.COLUMN_PREFIX}{self.REF_GROUP}'

    def ref_name(self, name: str) -> str:
        """
        Returns a name of a REF (reference) table, base on the base name.  This method ensures, that the name is prefixed with the configured
        reference prefix. If the prefix is already present, it will not be added.
        """

        name = name.upper()

        if name.startswith(self.REF):
            return name
        else:
            return f'{self.REF}{name}'

    def sat_name(self, name: str) -> str:
        """
        Returns a name of a SAT (satellite) table, based on the base name. This method ensures, that the name is prefixed with the configured
        satellite prefix. If the prefix is already present, it will not be added.
        """

        name = name.upper()

        if name.startswith(self.SAT):
            return name
        else:
            return f'{self.SAT}{name}'

    def valid_from_column_name(self) -> str:
        """
        Return the column name for VALID_FROM column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.VALID_FROM}'

    def valid_to_column_name(self) -> str:
        """
        Return the column name for VALID_TO column including configured prefix.
        """

        return f'{self.COLUMN_PREFIX}{self.VALID_TO}'


class DataVaultConfiguration:

    def __init__(
        self, source_system_name: str, staging_base_path: str, staging_prepared_base_path: str, raw_base_path: str, 
        staging_load_date_column_name: str, staging_cdc_operation_column_name: str,
        snapshot_override_load_date_based_on_column: str) -> None:

        """
        Configuration parameters for the DataVault automation.

        :param source_system_name - The (technical) name of the source system. This name is used for naming resources. The allowed pattern is [A-Z0-9_]{1,}.
        :param staging_base_path - The base path of thse staged source files (w/o trailing slash).
        :param staging_prepared_base_path - The base path of for the temporary prepared staging tables.
        :param raw_base_path - The base path of the raw layer on the data lake.
        :param staging_load_date_column_name - The column name which should be used as a load date from the staged tables.
        :param staging_cdc_operation_column_name - The name of the column which contains the CDC operation number.
        :param snapshot_override_load_date_based_on_column - In case of a snapshot, the load date might be overriden by the value of this column.
        """
        self.source_system_name = source_system_name
        self.staging_base_path = staging_base_path
        self.staging_prepared_base_path = staging_prepared_base_path
        self.raw_base_path = raw_base_path

        self.staging_load_date_column_name = staging_load_date_column_name
        self.staging_cdc_operation_column_name = staging_cdc_operation_column_name
        self.snapshot_override_load_date_based_on_column = snapshot_override_load_date_based_on_column

        self.staging_prepared_database_name = f'{self.source_system_name}__staging_prepared'.lower()
        self.raw_database_name = f'{self.source_system_name}__raw'.lower()


class ColumnDefinition:

    def __init__(self, name: str, type: DataType, nullable: bool = False, comment: Optional[str] = None) -> None:
        """
        Value class to define a table column.
        """

        self.name = name
        self.type = type
        self.nullable = nullable
        self.comment = comment


class SatelliteDefinition:

    def __init__(self, name: str, attributes: List[str]) -> None:
        """
        A definition how a setllite is derived. Please do not use this method directly. Use DataVault#create_satellite_definition.

        :param name - The name of the satellite table in the raw vault.
        :param attributes - The name of the columns/ attributes as in source table and satellite table.
        """

        self.name = name
        self.attributes = attributes


class ColumnReference:

    def __init__(self, table: str, column: str) -> None:
        """
        Simple value class to describe a reference to a column.

        :param table - The name of the table the column belongs to.
        :üaram column - The name of the column.
        """
        self.table = table
        self.column = column


class ForeignKey:

    def __init__(self, column: str, to: ColumnReference) -> None:
        """
        Simple value class to describe a foreign key constraint.

        :param column - The name of the column which points to a foreign table/ column.
        :param to - The reference to the foreign column.
        """

        self.column = column
        self.to = to


class LinkedHubDefinition:

    def __init__(self, hkey_column_name: str, foreign_key: ForeignKey) -> None:
        """
        A value class to specify a linked hub of a Link table.

        :param hkey_column_name - The name of the column in the link table.
        :param foreign_key - The foreign key from the Links staging table to the staging table of the linked hub.
        """
        self.hkey_column_name = hkey_column_name
        self.foreign_key = foreign_key


class RawVault:
    """
    TODO jf: Explain rough process and how method

    """

    def __init__(self, spark: SparkSession, config: DataVaultConfiguration, conventions: DataVaultConventions = DataVaultConventions()) -> None:
        self.spark = spark
        self.config = config
        self.conventions = conventions

    def create_hub(self, name: str, business_key_columns: List[ColumnDefinition]) -> None:
        """
        Creates a hub table in the raw database. Does only create the table if it does not exist yet.

        :param name - The name of the hub table, usually starting with `HUB__`.
        :param business_key_columns - The columns for the hub are the keys which compose the business key. Tuple contains (name, type).
        """

        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.hkey_column_name(), StringType()), # TODO mw: Add comments to column
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
            ColumnDefinition(self.conventions.record_source_column_name(), StringType())
        ] + business_key_columns

        self.__create_external_table(self.config.raw_database_name, self.conventions.hub_name(name), columns)

    def create_link(self, name: str, column_names: List[str]) -> None: # TODO MW: Specify whether Link is History/ Transaction
        """
        Creates a link table in the raw database. Does only create the table if it does not exist yet.

        :param name - The name of the link table, usually starting with `LNK__`.
        :param column_names - The name of the columns which containg hash keys pointing to other hubs.
        """

        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.hkey_column_name(), StringType()), # TODO mw: Add comments to column
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
            ColumnDefinition(self.conventions.record_source_column_name(), StringType())
        ] + [ ColumnDefinition(column_name, StringType()) for column_name in column_names ]

        self.__create_external_table(self.config.raw_database_name, self.conventions.link_name(name), columns)

    def create_point_in_time_table_for_single_satellite(self, pit_name: str, satellite_name: str) -> None:
        """
        Creates a point-in-time-table for a single satellite by adding a calculated load end date column.

        :param satellite_name - The name of the satellite from which the PIT is derived.
        """
        sat_name = f'{self.config.raw_database_name}.{self.conventions.sat_name(satellite_name)}'
        pit_table_name = f'{self.config.raw_database_name}.{self.conventions.pit_name(pit_name)}'

        df = self.spark.table(sat_name)
        df \
            .alias('l') \
            .join(df.alias('r'), F.col('l.$__HKEY') == F.col('r.$__HKEY')) \
            .select(F.col('l.$__HKEY').alias('$__HKEY'), F.col('l.$__LOAD_DATE').alias('$__LOAD_END_DATE'))

        df = df \
            .alias('l') \
            .join(df.alias('r'), [
                    (F.col(f'l.{self.conventions.hkey_column_name()}') == F.col(f'r.{self.conventions.hkey_column_name()}')) & \
                    (F.col(f'l.{self.conventions.load_date_column_name()}') < F.col(f'r.{self.conventions.load_date_column_name()}')) \
                ], how='left') \
            .select(
                F.col(f'l.{self.conventions.hkey_column_name()}'), 
                F.col(f'l.{self.conventions.load_date_column_name()}'), 
                F.col(f'r.{self.conventions.load_date_column_name()}').alias(self.conventions.load_end_date_column_name())) \
            .groupBy(
                F.col(self.conventions.hkey_column_name()),
                F.col(self.conventions.load_date_column_name())) \
            .agg(
                F.min(self.conventions.load_end_date_column_name()).alias(self.conventions.load_end_date_column_name())
            ) \
            .withColumn(
                self.conventions.load_end_date_column_name(), 
                F \
                    .when(F.isnull(self.conventions.load_end_date_column_name()), datetime.max) \
                    .otherwise(F.col(self.conventions.load_end_date_column_name()))) \
            .write.mode('overwrite').saveAsTable(pit_table_name)
        

    def create_reference_table(self, name: str, id_column: ColumnDefinition, attribute_columns: List[ColumnDefinition]) -> None:
        """
        Creates a reference table in the raw vault. Does only create the table if it does not exist yet.

        :param name - The name of the reference table, usually starting with `REF__`.
        :param id_column - The definition of the column which is used as a key for the reference table.
        :param attribute_columns - The attributes which should be stored in the reference table.
        """

        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.hdiff_column_name(), StringType()),
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
            id_column
        ] + attribute_columns

        self.__create_external_table(self.config.raw_database_name, self.conventions.ref_name(name), columns)

    def create_code_reference_table(self, name: str, id_column: ColumnDefinition, attribute_columns: List[ColumnDefinition]) -> None:
        """
        Creates a special reference table in the raw vault. This reference table may be used for a set of reference tables which have a similar schema (code reference tables). 
        Does only create the table if it does not exist yet.

        :param name - The name of the reference table, usually starting with `REF__`.
        :param id_column - The definition of the column which is used as a key for the reference table.
        :param attribute_columns - The attributes which should be stored in the reference table.
        """

        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.ref_group_column_name(), StringType()),
            ColumnDefinition(self.conventions.hdiff_column_name(), StringType()),
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
            id_column,
        ] + attribute_columns

        self.__create_external_table(self.config.raw_database_name, self.conventions.ref_name(name), columns)

    def create_satellite(self, name: str, attribute_columns: List[ColumnDefinition]) -> None:
        """
        Creates a satellite table in the raw vault. Does only create the table if it does not exist yet.

        :param name - The name of the satellite table, usually starting with `SAT__`.
        :param attribute_columns - The attributes which should be stored in the satellite.
        """

        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.hkey_column_name(), StringType()), # TODO mw: Add comments to column
            ColumnDefinition(self.conventions.hdiff_column_name(), StringType()),
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
        ] + attribute_columns

        self.__create_external_table(self.config.raw_database_name, self.conventions.sat_name(name), columns)

    def initialize_database(self) -> None:
        """
        Initialize database.
        """

        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.config.staging_prepared_database_name} LOCATION '{self.config.staging_prepared_base_path}'""")
        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.config.raw_database_name} LOCATION '{self.config.raw_base_path}'""")

    def load_hub_from_prepared_staging_table(self, staging_table_name: str, hub_table_name: str, business_key_column_names: List[str], satellites: List[SatelliteDefinition] = []) -> None:
        """
        Loads a hub from a prepared staging table. The prepared staging table must have a HKEY calculated.

        :param staging_table_name - The name of the staging table.
        :param hub_table_name - The name of the hub table in the raw vault.
        :param business_key_column_names - The list of columns which contribute to the business key of the hub.
        :param satellites - Optional. A list of satellites which is loaded from the prepared staging table. The form of the tuple is.
        """

        hub_table_name = self.conventions.hub_name(hub_table_name)
        hub_table_name = f'{self.config.raw_database_name}.{hub_table_name}'
        stage_table_name = f'{self.config.staging_prepared_database_name}.{staging_table_name}'

        self.spark.sql(f"REFRESH TABLE {stage_table_name}") # TODO mw: Really required?
        columns = [self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), self.conventions.record_source_column_name()] + business_key_column_names
        
        hub_df = self.spark.table(hub_table_name)
        staged_df = self.spark.table(stage_table_name)

        staged_df \
            .withColumn(self.conventions.load_date_column_name(), F.current_timestamp()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .distinct() \
            .join(hub_df, hub_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()], how='left_anti') \
            .select(columns) \
            .write.mode('append').saveAsTable(hub_table_name)

        for satellite in satellites:
            self.load_satellite_from_prepared_stage_dataframe(staged_df, satellite)

    def load_link_for_linked_source_tables_from_prepared_staging_tables(
        self, 
        from_staging_table_name: str, 
        from_staging_foreign_key: ForeignKey, 
        link_table_name: str,
        from_hkey_column_name: str,
        to_hkey_column_name: str) -> None:
        """
        Loads a link for two linked source tables based on the prepared staging tables.

        :param from_staging_table_name - The name of the staging table which contains a foreign key to another entity.
        :param from_staging_foreign_key - The foreign key constraint of the source/ staging table which points to the other entity.
        :param link_table_name - The name of the link table in the raw vault.
        :param from_hkey_column_name - The name of the column pointing to the origin of the link in the link table.
        :param to_hkey_column_name - The name of the column pointing to the target of the link in the link table.
        """

        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'
        link_df = self.spark.table(link_table_name)

        staged_to_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{from_staging_foreign_key.to.table}') \
            .withColumnRenamed(self.conventions.hkey_column_name(), to_hkey_column_name) \
            .select([from_staging_foreign_key.to.column, to_hkey_column_name])

        staged_from_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{from_staging_table_name}') \
            .withColumnRenamed(self.conventions.hkey_column_name(), from_hkey_column_name) \
            .select([from_staging_foreign_key.column, from_hkey_column_name])

        joined_df = staged_from_df \
            .join(staged_to_df, staged_from_df[from_staging_foreign_key.column] == staged_to_df[from_staging_foreign_key.to.column]) \
            .select([from_hkey_column_name, to_hkey_column_name]) \
            .distinct() \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([from_hkey_column_name, to_hkey_column_name])) \
            .withColumn(self.conventions.load_date_column_name(), F.current_timestamp()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .select([self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), self.conventions.record_source_column_name(), from_hkey_column_name, to_hkey_column_name])

        joined_df.join(link_df, link_df[self.conventions.hkey_column_name()] == joined_df[self.conventions.hkey_column_name()], how='left_anti') \
            .write.mode('append').saveAsTable(link_table_name)

    
    def load_link_from_prepared_stage_table(self, staging_table_name: str, links: List[LinkedHubDefinition], link_table_name: str, satellites: List[SatelliteDefinition]) -> None:
        """
        Loads a link with data from a staging table which is a already a link table in the source.

        :param staging_table_name - The name of the staging table from which the link table is derived.
        :param links - The list of linked hubs for the link table.
        :param link_table_name - The name of the link table in the raw vault.
        :param satellites - Definitions of the satellites for the link.
        """

        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'

        columns = [self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), self.conventions.record_source_column_name()] \
            + [ link.hkey_column_name for link in links ]

        staged_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{staging_table_name}') \
            .select([ link.foreign_key.column for link in links ])
        
        for link in links:
            link_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{link.foreign_key.to.table}') \
                .withColumnRenamed(self.conventions.hkey_column_name(), link.hkey_column_name) \
                .select([link.foreign_key.to.column, link.hkey_column_name])

            staged_df = staged_df \
                .join(link_df, link_df[link.foreign_key.to.column] == staged_df[link.foreign_key.column], how='left')

        link_df = self.spark.table(link_table_name)

        staged_df = staged_df \
            .select([ link.hkey_column_name for link in links ]) \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([ link.hkey_column_name for link in links ])) \
            .withColumn(self.conventions.load_date_column_name(), F.current_timestamp()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .distinct()
        
        staged_df = staged_df \
            .join(link_df, link_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()], how='left_anti') \
            .select(columns) \
            .write.mode('append').saveAsTable(link_table_name)
        

    def load_references_from_prepared_stage_table(self, staging_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging table. 

        :param staging_table_name - The name of the table in the prepared staging area.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        columns = [id_column, self.conventions.hdiff_column_name(), self.conventions.load_date_column_name()] + attributes

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name)
        staged_df = self.spark.table(f'`{self.config.staging_prepared_database_name}`.`{staging_table_name}`')

        join_condition = [ref_df[id_column] == staged_df[id_column], \
            ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        staged_df  = staged_df \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
            .select(columns) \
            .distinct() \
            .join(ref_df, join_condition, how='left_anti') \
            .write.mode('append').saveAsTable(ref_table_name)


    def load_code_references_from_prepared_stage_table(self, staging_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging table. 

        :param staging_table_name - The name of the table in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        columns = [self.conventions.ref_group_column_name(), id_column, self.conventions.hdiff_column_name(), self.conventions.load_date_column_name()] + attributes

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name)
        staged_df = self.spark.table(f'`{self.config.staging_prepared_database_name}`.`{staging_table_name}`')

        join_condition = [ref_df[id_column] == staged_df[id_column], \
            ref_df[self.conventions.ref_group_column_name()] == staging_table_name.lower(), \
            ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        staged_df = staged_df \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
            .withColumn(self.conventions.ref_group_column_name(), F.lit(staging_table_name.lower())) \
            .select(columns) \
            .distinct() \
            .join(ref_df, join_condition, how='left_anti') \
            .write.mode('append').saveAsTable(ref_table_name)

    def load_satellite_from_prepared_stage_dataframe(self, staged_df: DataFrame, satellite: SatelliteDefinition) -> None:
        """
        Loads a satellite from a data frame which contains prepared an staged data.

        :param staged_df - The dataframe which contains the staged and prepared data for the satellite.
        :param satellite - The satellite definition.
        """

        columns = [self.conventions.hkey_column_name(), self.conventions.hdiff_column_name(), self.conventions.load_date_column_name()] \
            + [ column for column in satellite.attributes ]

        sat_table_name = f'{self.config.raw_database_name}.{satellite.name}'
        sat_df = self.spark.table(sat_table_name)

        join_condition = [sat_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()], \
            sat_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        # TODO mw: Remove  distinct() for performance reasons? Should not happen in any case.       
        staged_df \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(satellite.attributes)) \
            .select(columns) \
            .distinct() \
            .join(sat_df, join_condition, how='left_anti') \
            .write.mode('append').saveAsTable(sat_table_name)


    def stage_table(self, name: str, source: str, hkey_columns: List[str] = []) -> None: # TODO mw: Multiple HKeys, HDiffs?
        """
        Stages a source table. Additional columns will be created/ calculated and stored in the staging database. 

        :param name - The name of the table in the prepared staging area.
        :param source - The source file path, relative to staging_base_path (w/o leading slash).
        :param hkey_columns - Optional. Column names which should be used to calculate a hash key.
        """

        #
        # Load source data from Parquet file.
        #
        df = self.spark.read.load(f'{self.config.staging_base_path}/{source}', format='parquet')

        #
        # Add DataVault specific columns
        #
        df = df \
            .withColumnRenamed(self.config.staging_load_date_column_name, self.conventions.load_date_column_name()) \
            .withColumnRenamed(self.config.staging_cdc_operation_column_name, self.conventions.cdc_operation_column_name()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name))

        #
        # Update load_date in case of snapshot load (CDC Operation < 1).
        #
        if self.config.snapshot_override_load_date_based_on_column in df.columns:
            df = df.withColumn(
                self.conventions.load_date_column_name(), 
                F \
                    .when(df[self.conventions.cdc_operation_column_name()] < 1, df[self.config.snapshot_override_load_date_based_on_column]) \
                    .otherwise(df[self.conventions.load_date_column_name()]))

        if len(hkey_columns) > 0: 
            df = df.withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash(hkey_columns))

        #
        # Write staged table into staging area.
        #
        df.write.mode('overwrite').saveAsTable(f'{self.config.staging_prepared_database_name}.{name}')

    def __create_external_table(self, database: str, name: str, columns: List[ColumnDefinition]) -> None:
        """
        :param database - The name of the database where the table should be created.
        :param name - The name of the table which should be created.
        :param columns - Column definitions for the tables.
        """

        schema = StructType([ StructField(c.name, c.type, c.nullable) for c in columns ])
        df: DataFrame = self.spark.createDataFrame([], schema)
        df.write.mode('ignore').saveAsTable(f'{database}.{name}')


class BusinessVault:

    def __init__(self, spark: SparkSession, config: DataVaultConfiguration, conventions: DataVaultConventions = DataVaultConventions()) -> None:
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