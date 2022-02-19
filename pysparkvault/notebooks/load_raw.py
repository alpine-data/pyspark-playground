spark: SparkSession = None
#
#
####################################################################################################################################
####################################################################################################################################
#
#

import pyspark.sql.functions as F

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType,TimestampType
from typing import List

from pysparkvault.raw.DataVaultShared import *


class RawVaultConfiguration:

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


class RawVault:
    """
    TODO jf: Explain rough process and how method

    """

    def __init__(self, spark: SparkSession, config: RawVaultConfiguration, conventions: DataVaultConventions = DataVaultConventions()) -> None:
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


#
#
####################################################################################################################################
####################################################################################################################################
#
#

from pyspark.sql.types import *

source_system_name = 'Allegro'
staging_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/staging/allegro/2022/02/16/1200'
staging_prepared_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/staging/allegro/prepared'
raw_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/raw/allegro'

staging_load_date_column_name = 'load_date'
staging_cdc_operation_column_name = 'operation'
snapshot_override_load_date_based_on_column = 'UpdateTime'


config = RawVaultConfiguration(source_system_name, staging_base_path, staging_prepared_base_path, raw_base_path, staging_load_date_column_name, staging_cdc_operation_column_name, snapshot_override_load_date_based_on_column)
raw_vault = RawVault(spark, config)
raw_vault.initialize_database()

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Create hub tables.
#
raw_vault.create_hub('HUB__CLAIM', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__EXPOSURE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__POLICY', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__INCIDENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__COVERAGE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__RISKUNIT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__USER', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__CREDENTIAL', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__ACCOUNT', [ColumnDefinition('PublicID', StringType())])

raw_vault.create_hub('HUB__ACCOUNTASSIGNMENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__SPECIALHANDLING', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__STRATEGY', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__ACCOUNTMANAGEMENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__ACCOUNTPOLICY', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__OTHERREFERENCES', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__LIABILITYCONCEPT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__DEDUCTIBLE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__COVERAGETERMS', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__ENDORSEMENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__TERM', [ColumnDefinition('PublicID', StringType())])

raw_vault.create_hub('HUB__VEHICLESALVAGE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__TRIPITEM', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__MOBILEPROPERTYITEM', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__MOBPROPCOSTDETAILS', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__SETTLEMENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__ICDCODE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__INJURYDIAGNOSIS', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__WORKRESUMPTIONDETAILS', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__SALARYDATA', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__RELULOOKUP', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__RELUCODE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__BODILYINJURYPOINTPEL', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__TREATMENT', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__DISEASE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__PUREFINCOSTTYPE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__FINANCIALLOSSITEM', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__BIINCIDENTLOSSITEM', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__INCIDENTCESSION', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__LOSSADJUSTERORDER', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__OTHERINSURER', [ColumnDefinition('PublicID', StringType())])

raw_vault.create_hub('HUB__VEHICLE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__HANDLINGFEES', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__CATASTROPHE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__CAUSERDETAIL', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__LCTFLEETQUESTIONAIRE', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__PRODUCTCODES', [ColumnDefinition('PublicID', StringType())])

raw_vault.create_hub('HUB__NAMEDPERSON', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__EMPLOYEMENTDATA', [ColumnDefinition('PublicID', StringType())])
raw_vault.create_hub('HUB__FURTHEREMPLOYERS', [ColumnDefinition('PublicID', StringType())])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Create link tables.
#
raw_vault.create_link('LNK__CLAIM__POLICY', ['CLAIM_HKEY', 'POLICY_HKEY'])
raw_vault.create_link('LNK__EXPOSURE__CLAIM', ['EXPOSURE_HKEY', 'CLAIM_HKEY'])
raw_vault.create_link('LNK__INCIDENT__CLAIM', ['INCIDENT_HKEY', 'CLAIM_HKEY'])
raw_vault.create_link('LNK__COVERAGE__POLICY', ['COVERAGE_HKEY', 'POLICY_HKEY'])
raw_vault.create_link('LNK__COVERAGE__RISKUNIT', ['COVERAGE_HKEY', 'RISKUNIT_HKEY'])
raw_vault.create_link('LNK__RISKUNIT__POLICY', ['RISKUNIT_HKEY', 'POLICY_HKEY'])
raw_vault.create_link('LNK__USER__CREDENTIAL', ['USER_HKEY', 'CREDENTIAL_HKEY'])

raw_vault.create_link('LNK__SPECIALHANDLING__ACCOUNT', ['SPECIALHANDLING_HKEY', 'ACCOUNT_HKEY'])
raw_vault.create_link('LNK__ACCOUNTASSIGNMENT__SPECIALHANDLING', ['ACCOUNTASSIGNMENT_HKEY', 'SPECIALHANDLING_HKEY'])
raw_vault.create_link('LNK__STRATEGY__ACCOUNTASSIGNMENT', ['STRATEGY_HKEY', 'ACCOUNTASSIGNMENT_HKEY'])
raw_vault.create_link('LNK__ACCOUNTMANAGEMENT__ACCOUNT', ['ACCOUNTMANAGEMENT_HKEY', 'ACCOUNT_HKEY'])
raw_vault.create_link('LNK__ACCOUNTPOLICY__ACCOUNT', ['ACCOUNTPOLICY_HKEY', 'ACCOUNT_HKEY'])
#POLICY -> ACCOUNT
raw_vault.create_link('LNK__POLICY__ACCOUNT', ['POLICY_HKEY', 'ACCOUNT_HKEY'])

raw_vault.create_link('LNK__OTHERREFERENCES__CLAIM', ['OTHERREFERENCES_HKEY', 'CLAIM_HKEY'])
raw_vault.create_link('LNK__LIABILITYCONCEPT__CLAIM', ['LIABILITYCONCEPT_HKEY', 'CLAIM_HKEY'])
raw_vault.create_link('LNK__DEDUCTIBLE__COVERAGE', ['DEDUCTIBLE_HKEY', 'COVERAGE_HKEY'])
raw_vault.create_link('LNK__COVERAGETERMS__COVERAGE', ['COVERAGETERMS_HKEY', 'COVERAGE_HKEY'])
raw_vault.create_link('LNK__ENDORSEMENT__COVERAGE', ['ENDORSEMENT_HKEY', 'COVERAGE_HKEY'])
raw_vault.create_link('LNK__ENDORSEMENT__RISKUNIT', ['ENDORSEMENT_HKEY', 'RISKUNIT_HKEY'])
raw_vault.create_link('LNK__TERM__POLICY', ['TERM_HKEY', 'POLICY_HKEY'])
raw_vault.create_link('LNK__TERM__RISKUNIT', ['TERM_HKEY', 'RISKUNIT_HKEY'])

raw_vault.create_link('LNK__INCIDENT__VEHICLESALVAGE', ['INCIDENT_HKEY', 'VEHICLESALVAGE_HKEY'])
raw_vault.create_link('LNK__TRIPITEM__INCIDENT', ['TRIPITEM_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__MOBILEPROPERTYITEM__INCIDENT', ['MOBILEPROPERTYITEM_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__MOBPROPCOSTDETAILS__MOBILEPROPERTYITEM', ['MOBPROPCOSTDETAILS_HKEY','MOBILEPROPERTYITEM_HKEY'])
raw_vault.create_link('LNK__MOBILEPROPERTYITEM__SETTLEMENT', ['MOBILEPROPERTYITEM_HKEY', 'SETTLEMENT_HKEY'])
raw_vault.create_link('LNK__INJURYDIAGNOSIS__INCIDENT', ['INJURYDIAGNOSIS_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__INJURYDIAGNOSIS__ICDCODE', ['INJURYDIAGNOSIS_HKEY', 'ICDCODE_HKEY'])
raw_vault.create_link('LNK__RELUCODE__INCIDENT', ['RELUCODE_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__RELUCODE__RELULOOKUP', ['RELUCODE_HKEY', 'RELULOOKUP_HKEY'])
raw_vault.create_link('LNK__BODILYINJURYPOINTPEL__INCIDENT', ['BODILYINJURYPOINTPEL_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__TREATMENT__INCIDENT', ['TREATMENT_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__DISEASE__INCIDENT', ['DISEASE_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__PUREFINCOSTTYPE__INCIDENT', ['PUREFINCOSTTYPE_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__FINANCIALLOSSITEM__INCIDENT', ['FINANCIALLOSSITEM_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__BIINCIDENTLOSSITEM__INCIDENT', ['BIINCIDENTLOSSITEM_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__INCIDENTCESSION__INCIDENT', ['INCIDENTCESSION_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__LOSSADJUSTERORDER__INCIDENT', ['LOSSADJUSTERORDER_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__OTHERINSURER__INCIDENT', ['OTHERINSURER_HKEY', 'INCIDENT_HKEY'])
raw_vault.create_link('LNK__FINANCIALLOSSITEM__INCIDENTCESESSION', ['FINANCIALLOSSITEM_HKEY', 'INCIDENTCESESSION_HKEY'])

raw_vault.create_link('LNK__RISKUNIT__VEHICLE', ['RISKUNIT_HKEY', 'VEHICLE_HKEY'])
raw_vault.create_link('LNK__CLAIM__HANDLINGFEES', ['CLAIM_HKEY', 'HANDLINGFEES_HKEY'])
raw_vault.create_link('LNK__CLAIM__CATASTROPHE', ['CLAIM_HKEY', 'CATASTROPHE_HKEY'])
raw_vault.create_link('LNK__CLAIM__CAUSERDETAIL', ['CLAIM_HKEY', 'CAUSERDETAIL_HKEY'])
raw_vault.create_link('LNK__CLAIM__LCTFLEETQUESTIONAIRE', ['CLAIM_HKEY', 'LCTFLEETQUESTIONAIRE_HKEY'])
raw_vault.create_link('LNK__CLAIM__PRODUCTCODES', ['CLAIM_HKEY', 'PRODUCTCODES_HKEY'])
raw_vault.create_link('LNK__WORKRESUMPTIONDETAILS__EMPLOYMENTDATA', ['WORKRESUMPTIONDETAILS_HKEY', 'EMPLOYMENTDATA_HKEY'])
raw_vault.create_link('LNK__SALARYDATA__EMPLOYMENTDATA', ['SALARYDATA_HKEY', 'EMPLOYMENTDATA_HKEY'])
raw_vault.create_link('LNK__SALARYDATA__FURTHEREMPLOYERS', ['SALARYDATA_HKEY', 'FURTHEREMPLOYERS_HKEY'])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Create satellite tables
#
raw_vault.create_satellite('SAT__CLAIM', [
    ColumnDefinition('Alg_statusClaimProcess', IntegerType(), True), ColumnDefinition('Alg_RiskShieldStatus', IntegerType(), True), ColumnDefinition('Alg_IsScalePointJumpEligible', BooleanType(), True), ColumnDefinition('Alg_electroCasco', BooleanType(), True), ColumnDefinition('PriorLossDate', TimestampType(), True), ColumnDefinition('CountryOfLoss', IntegerType(), True), ColumnDefinition('Alg_OriginalRenteCSSNumber', StringType(), True), ColumnDefinition('Alg_IsRentCSS', BooleanType(), True), ColumnDefinition('Alg_Sanction', IntegerType(), True), ColumnDefinition('Alg_TitlisMasterView', LongType(), True), ColumnDefinition('Alg_IsSendEmail', BooleanType(), True), ColumnDefinition('Alg_MigrationSubStatus', IntegerType(), True), ColumnDefinition('Alg_IsRent87OpenSPAZCase', BooleanType(), True), ColumnDefinition('Alg_LegacyCiriardPolicyNumber', StringType(), True), ColumnDefinition('Alg_ClaimByUUID', StringType(), True), ColumnDefinition('vendorClaimIntakeSummary', LongType(), True), ColumnDefinition('Alg_ClaimViewLink', StringType(), True), ColumnDefinition('Alg_MigrationCompleteTime', TimestampType(), True), ColumnDefinition('Alg_LegacySystem', IntegerType(), True), ColumnDefinition('Alg_IsRent87', BooleanType(), True),
    ColumnDefinition('Alg_OriginalClaimCreationDate', TimestampType(), True), ColumnDefinition('Alg_MigrationStatus', IntegerType(), True), ColumnDefinition('Alg_OriginalRent87number', StringType(), True), ColumnDefinition('Alg_NVBCourtesyDate', TimestampType(), True), ColumnDefinition('Alg_IsCededCompany', BooleanType(), True), ColumnDefinition('Alg_IsNCBWarning', BooleanType(), True), ColumnDefinition('Alg_ShouldIncludeReduction', BooleanType(), True), ColumnDefinition('Alg_InsuredCharcteristics', IntegerType(), True), ColumnDefinition('Alg_DriverUnknown', BooleanType(), True), ColumnDefinition('Alg_RapportExisting', IntegerType(), True), ColumnDefinition('Alg_CreatedBySystem', IntegerType(), True), ColumnDefinition('Alg_IsMigrated', BooleanType(), True), ColumnDefinition('Alg_OriginalClaimNumber', StringType(), True), ColumnDefinition('Alg_IsMigratedExposure', BooleanType(), True), ColumnDefinition('Alg_PaymentMigrationStatus', BooleanType(), True), ColumnDefinition('Alg_SendAutoCloseLetter', BooleanType(), True), ColumnDefinition('Alg_GenInvoiceDocFlag', BooleanType(), True), ColumnDefinition('Alg_AdditionalLossCause', LongType(), True), ColumnDefinition('DoNotDestroy', BooleanType(), True), ColumnDefinition('Alg_LCTFleetQuestionaire', LongType(), True),
    ColumnDefinition('Alg_Fraud', LongType(), True), ColumnDefinition('Alg_IsScalepoint', BooleanType(), True), ColumnDefinition('Alg_RegisteredCountry', IntegerType(), True), ColumnDefinition('Alg_IsFirstAndFinal', BooleanType(), True), ColumnDefinition('IsUnverifiedCreatedFromUI', BooleanType(), True), ColumnDefinition('Alg_StopAutomaticPayment', BooleanType(), True), ColumnDefinition('Alg_MonitRecCurrency', IntegerType(), True), ColumnDefinition('Alg_MonitRecoveryAmount', DecimalType(18,2), True), ColumnDefinition('Alg_GlassType', IntegerType(), True), ColumnDefinition('Alg_IPSVehicleType', IntegerType(), True), ColumnDefinition('Alg_IPSAgeOfDriver', IntegerType(), True), ColumnDefinition('Alg_IPSUsageVehicle', IntegerType(), True), ColumnDefinition('Alg_IPSEventType', IntegerType(), True), ColumnDefinition('Alg_IPSMovementVehicle', IntegerType(), True), ColumnDefinition('Alg_IPSBlameCode', IntegerType(), True), ColumnDefinition('Alg_IPSRoadType', IntegerType(), True), ColumnDefinition('Alg_VendorSourceSystem', IntegerType(), True), ColumnDefinition('Alg_UnderInsurance', IntegerType(), True), ColumnDefinition('Alg_DeductiblesExchangeDate', TimestampType(), True), ColumnDefinition('Alg_DeductibleComments', StringType(), True),
    ColumnDefinition('Alg_HandlingFees', LongType(), True), ColumnDefinition('Alg_LossCauseDescription', StringType(), True), ColumnDefinition('Alg_NCBLastRelevantClaim', StringType(), True), ColumnDefinition('Alg_IPSDICDIL', IntegerType(), True), ColumnDefinition('Alg_NCBComment', StringType(), True), ColumnDefinition('Alg_IsQuickClaim', BooleanType(), True), ColumnDefinition('Alg_IPSContractType', BooleanType(), True), ColumnDefinition('Alg_NCBLastRelClmCloseDate', TimestampType(), True), ColumnDefinition('isPurgeReady', BooleanType(), True), ColumnDefinition('Alg_PeriodOfValidity', IntegerType(), True), ColumnDefinition('Alg_ProductCNP', IntegerType(), True), ColumnDefinition('Alg_QualificationNGF', IntegerType(), True), ColumnDefinition('Alg_CourtesyDate', TimestampType(), True), ColumnDefinition('Alg_CausingVehicleType', IntegerType(), True), ColumnDefinition('Alg_IsCededCase', BooleanType(), True), ColumnDefinition('Alg_CededDate', TimestampType(), True), ColumnDefinition('Alg_IsCourtesyClaim', BooleanType(), True), ColumnDefinition('Alg_InterclaimsGCode', IntegerType(), True), ColumnDefinition('Alg_FileDeletion', TimestampType(), True), ColumnDefinition('Alg_InterClaimsType', IntegerType(), True),
    ColumnDefinition('Alg_NGForNVBClaim', IntegerType(), True), ColumnDefinition('Alg_IsServiceAffected', BooleanType(), True), ColumnDefinition('Alg_IsApplySanctions', BooleanType(), True), ColumnDefinition('Alg_IsComprehensiveSanctions', BooleanType(), True), ColumnDefinition('Alg_ProducingCountry', StringType(), True), ColumnDefinition('Alg_IPSClaimHandlingProtocol', BooleanType(), True), ColumnDefinition('Alg_ProgramName', StringType(), True), ColumnDefinition('Alg_CnpMode', IntegerType(), True), ColumnDefinition('Alg_SwissJournalID', StringType(), True), ColumnDefinition('Alg_IsClaimNoPolicy', BooleanType(), True), ColumnDefinition('Alg_HasTrip', BooleanType(), True), ColumnDefinition('Alg_AccidentProtocol', BooleanType(), True), ColumnDefinition('Alg_HasConstruction', BooleanType(), True), ColumnDefinition('Alg_HasInjury', BooleanType(), True), ColumnDefinition('Alg_HasVehicles', BooleanType(), True), ColumnDefinition('Alg_HasMachinesTechnical', BooleanType(), True), ColumnDefinition('Alg_HasBuilding', BooleanType(), True), ColumnDefinition('Alg_HasGoodsAndMovables', BooleanType(), True), ColumnDefinition('Alg_HasPureFinancialLoss', BooleanType(), True), ColumnDefinition('Alg_HasBusinessInt', BooleanType(), True),
    ColumnDefinition('HazardousWaste', BooleanType(), True), ColumnDefinition('Alg_ClaimsMadeDate', TimestampType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('SIScore', IntegerType(), True), ColumnDefinition('Alg_IsTestingLCTASH', BooleanType(), True), ColumnDefinition('LargeLossNotificationStatus', IntegerType(), True), ColumnDefinition('AssignmentDate', TimestampType(), True), ColumnDefinition('Alg_IsPreventiveClmNotif', BooleanType(), True), ColumnDefinition('MMIdate', TimestampType(), True), ColumnDefinition('ClaimWorkCompID', LongType(), True), ColumnDefinition('Weather', IntegerType(), True), ColumnDefinition('CoverageInQuestion', BooleanType(), True), ColumnDefinition('DateCompDcsnMade', TimestampType(), True), ColumnDefinition('Alg_LastReopenedDate', TimestampType(), True), ColumnDefinition('Alg_LossLocAddressLine1', StringType(), True), ColumnDefinition('Alg_DataMismatchClaim', LongType(), True), ColumnDefinition('AgencyId', StringType(), True), ColumnDefinition('Alg_SubroPossibleCreatedByUser', LongType(), True), ColumnDefinition('ISOKnown', BooleanType(), True), ColumnDefinition('Alg_InterestInsured', StringType(), True),
    ColumnDefinition('PreviousUserID', LongType(), True), ColumnDefinition('Alg_ClaimReopenReason', IntegerType(), True), ColumnDefinition('Alg_EarlyIntervention', BooleanType(), True), ColumnDefinition('ReinsuranceReportable', BooleanType(), True), ColumnDefinition('GroupSegment', IntegerType(), True), ColumnDefinition('Alg_LiabilityOrigin', IntegerType(), True), ColumnDefinition('PolicyID', LongType(), True), ColumnDefinition('AccidentType', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('BenefitsStatusDcsn', BooleanType(), True), ColumnDefinition('Alg_CloseClaim', BooleanType(), True), ColumnDefinition('LossLocationCode', StringType(), True), ColumnDefinition('StorageBarCodeNum', StringType(), True), ColumnDefinition('StorageVolumes', StringType(), True), ColumnDefinition('LocationCodeID', LongType(), True), ColumnDefinition('Alg_TranspDocDate', TimestampType(), True), ColumnDefinition('WeatherRelated', BooleanType(), True), ColumnDefinition('Alg_IsSegmentAssigned', BooleanType(), True), ColumnDefinition('Alg_RequiredPolicyRefreshFlag', BooleanType(), True), ColumnDefinition('DateRptdToInsured', TimestampType(), True),
    ColumnDefinition('ConcurrentEmp', IntegerType(), True), ColumnDefinition('JurisdictionState', IntegerType(), True), ColumnDefinition('Alg_ResponsibleTeam', IntegerType(), True), ColumnDefinition('Alg_LossEventType', IntegerType(), True), ColumnDefinition('LOBCode', IntegerType(), True), ColumnDefinition('Alg_EstimatedLossDate', BooleanType(), True), ColumnDefinition('Alg_LossReason', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Alg_AddresUnknown', BooleanType(), True), ColumnDefinition('Alg_ConveyanceCode', IntegerType(), True), ColumnDefinition('CloseDate', TimestampType(), True), ColumnDefinition('Alg_ReturnCallIPEnsued', BooleanType(), True), ColumnDefinition('StateAckNumber', StringType(), True), ColumnDefinition('ReopenedReason', IntegerType(), True), ColumnDefinition('Alg_LosslocCanton', IntegerType(), True), ColumnDefinition('Alg_IPSLOB', StringType(), True), ColumnDefinition('Alg_IPSNumber', StringType(), True), ColumnDefinition('HospitalDays', IntegerType(), True), ColumnDefinition('PoliceDeptInfo', StringType(), True), ColumnDefinition('SIUStatus', IntegerType(), True),
    ColumnDefinition('SIEscalateSIUdate', TimestampType(), True), ColumnDefinition('Alg_OccupationalSafty', BooleanType(), True), ColumnDefinition('InjuredOnPremises', BooleanType(), True), ColumnDefinition('Alg_Elucidation', IntegerType(), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('StatuteDate', TimestampType(), True), ColumnDefinition('Alg_MainContactChoiceType', IntegerType(), True), ColumnDefinition('Alg_CoverageInQuestion', IntegerType(), True), ColumnDefinition('AssignedUserID', LongType(), True), ColumnDefinition('Strategy', IntegerType(), True), ColumnDefinition('EmployerValidityReason', StringType(), True), ColumnDefinition('InjWkrInMPN', TimestampType(), True), ColumnDefinition('InsuredPremises', BooleanType(), True), ColumnDefinition('ISOReceiveDate', TimestampType(), True), ColumnDefinition('Alg_AcuteSpecificInjuries', BooleanType(), True), ColumnDefinition('Alg_LiabilityReasonPH', StringType(), True), ColumnDefinition('WorkloadUpdated', TimestampType(), True), ColumnDefinition('EmpSentMPNNotice', TimestampType(), True), ColumnDefinition('StorageLocationState', IntegerType(), True), ColumnDefinition('Alg_CarrierName', StringType(), True),
    ColumnDefinition('SubrogationStatus', IntegerType(), True), ColumnDefinition('ComputerSecurity', BooleanType(), True), ColumnDefinition('SafetyEquipUsed', BooleanType(), True), ColumnDefinition('ClaimSource', IntegerType(), True), ColumnDefinition('SIEscalateSIU', IntegerType(), True), ColumnDefinition('Alg_CovInQnComment', StringType(), True), ColumnDefinition('LossDate', TimestampType(), True), ColumnDefinition('Alg_ManualStop', BooleanType(), True), ColumnDefinition('Alg_InvolvedObjects', IntegerType(), True), ColumnDefinition('Alg_IPSEmergingRisk', IntegerType(), True), ColumnDefinition('ClaimantRprtdDate', TimestampType(), True), ColumnDefinition('Alg_TransportTo', StringType(), True), ColumnDefinition('Alg_GNDeduction', IntegerType(), True), ColumnDefinition('Alg_ProductCode', LongType(), True), ColumnDefinition('EmpQusValidity', IntegerType(), True), ColumnDefinition('InsurerSentMPNNotice', TimestampType(), True), ColumnDefinition('Segment', IntegerType(), True), ColumnDefinition('EmploymentDataID', LongType(), True), ColumnDefinition('Alg_NextSteps', StringType(), True), ColumnDefinition('ModifiedDutyAvail', BooleanType(), True),
    ColumnDefinition('Alg_ReopenClaimNote', StringType(), True), ColumnDefinition('Alg_DiagnosisDescription', StringType(), True), ColumnDefinition('Alg_ClaimSubState', IntegerType(), True), ColumnDefinition('Alg_SendToDownstream', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Alg_SpecialisationClaim', IntegerType(), True), ColumnDefinition('Alg_CurrStatus', StringType(), True), ColumnDefinition('LossCause', IntegerType(), True), ColumnDefinition('HowReported', IntegerType(), True), ColumnDefinition('Alg_ClaimOutsidePolicyPeriod', IntegerType(), True), ColumnDefinition('ExaminationDate', TimestampType(), True), ColumnDefinition('DateEligibleForArchive', TimestampType(), True), ColumnDefinition('FlaggedReason', StringType(), True), ColumnDefinition('ISOEnabled', BooleanType(), True), ColumnDefinition('Alg_ClaimOutsidePlcyPrdComment', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('DrugsInvolved', IntegerType(), True), ColumnDefinition('StateFileNumber', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('PreviousQueueID', LongType(), True),
    ColumnDefinition('Alg_DuplicateClaim', BooleanType(), True), ColumnDefinition('DateRptdToEmployer', TimestampType(), True), ColumnDefinition('Alg_IsProfMalPracticeClaim', BooleanType(), True), ColumnDefinition('Alg_FreeMovementCase', BooleanType(), True), ColumnDefinition('AssignedByUserID', LongType(), True), ColumnDefinition('ExposureBegan', TimestampType(), True), ColumnDefinition('FlaggedDate', TimestampType(), True), ColumnDefinition('SafetyEquipProv', BooleanType(), True), ColumnDefinition('Alg_AcuteInjuriesDescription', IntegerType(), True), ColumnDefinition('Alg_DentalCare', BooleanType(), True), ColumnDefinition('PreviousGroupID', LongType(), True), ColumnDefinition('ReportedDate', TimestampType(), True), ColumnDefinition('Alg_ExpectedDateOfBirth', TimestampType(), True), ColumnDefinition('AssignmentStatus', IntegerType(), True), ColumnDefinition('Alg_IPSTypeOfLoss', IntegerType(), True), ColumnDefinition('Alg_PHKnowsDiagnosisUpdateTime', TimestampType(), True), ColumnDefinition('Alg_SubrogationPossible', BooleanType(), True), ColumnDefinition('UnattachedDocumentForFNOL_Ext', LongType(), True), ColumnDefinition('Alg_Organization', StringType(), True), ColumnDefinition('Alg_LossActivity', IntegerType(), True),
    ColumnDefinition('LossLocationID', LongType(), True), ColumnDefinition('CurrentConditions', BooleanType(), True), ColumnDefinition('Alg_TransportFrom', StringType(), True), ColumnDefinition('LockingColumn', IntegerType(), True), ColumnDefinition('ShowMedicalFirstInfo', IntegerType(), True), ColumnDefinition('LossLocationSpatialDenorm', StringType(), True), ColumnDefinition('HospitalDate', TimestampType(), True), ColumnDefinition('Alg_PHKnowsDiagnosis', BooleanType(), True), ColumnDefinition('Alg_FilteringCoverages', BooleanType(), True), ColumnDefinition('Alg_LossPlace', IntegerType(), True), ColumnDefinition('LossType', IntegerType(), True), ColumnDefinition('PurgeDate', TimestampType(), True), ColumnDefinition('Alg_Recovery', BooleanType(), True), ColumnDefinition('FurtherTreatment', BooleanType(), True), ColumnDefinition('ManifestationDate', TimestampType(), True), ColumnDefinition('Alg_BenefitExpirationDate', TimestampType(), True), ColumnDefinition('Alg_LossControlTool', BooleanType(), True), ColumnDefinition('Alg_ProfMalPracticeClm', IntegerType(), True), ColumnDefinition('Alg_FlagClaimForSuva', BooleanType(), True), ColumnDefinition('ClaimantDenormID', LongType(), True),
    ColumnDefinition('InjuredRegularJob', BooleanType(), True), ColumnDefinition('Alg_RegionForAssignment', IntegerType(), True), ColumnDefinition('CatastropheID', LongType(), True), ColumnDefinition('Alg_LossOccurenceDate', TimestampType(), True), ColumnDefinition('Alg_IsInjuredReached', BooleanType(), True), ColumnDefinition('Alg_LosslocCountry', IntegerType(), True), ColumnDefinition('Alg_AdjReserves', DecimalType(18,2), True), ColumnDefinition('ISOSendDate', TimestampType(), True), ColumnDefinition('ISOStatus', IntegerType(), True), ColumnDefinition('Alg_SegmentationTrigger', BooleanType(), True), ColumnDefinition('Alg_IsPrintClosingDocument', BooleanType(), True), ColumnDefinition('WorkloadWeight', IntegerType(), True), ColumnDefinition('ValidationLevel', IntegerType(), True), ColumnDefinition('Alg_PercentLiability', DecimalType(5,2), True), ColumnDefinition('StorageDate', TimestampType(), True), ColumnDefinition('FireDeptInfo', StringType(), True), ColumnDefinition('ReportedByType', IntegerType(), True), ColumnDefinition('Alg_BenefitExpiration', BooleanType(), True), ColumnDefinition('Alg_PolicyTypeForSearch', IntegerType(), True), ColumnDefinition('Alg_CollectiveDamage', BooleanType(), True),
    ColumnDefinition('QueueAssignGroupID', LongType(), True), ColumnDefinition('Mold', BooleanType(), True), ColumnDefinition('FirstNoticeSuit', BooleanType(), True), ColumnDefinition('State', IntegerType(), True), ColumnDefinition('Alg_Statement', IntegerType(), True), ColumnDefinition('Alg_DateOfConversation', TimestampType(), True), ColumnDefinition('PreexDisblty', BooleanType(), True), ColumnDefinition('IncidentReport', BooleanType(), True), ColumnDefinition('Alg_IPSLawsuit', IntegerType(), True), ColumnDefinition('Alg_IsFlagClaimDossierSubmtd', BooleanType(), True), ColumnDefinition('ClaimNumber', StringType(), True), ColumnDefinition('DateFormRetByEmp', TimestampType(), True), ColumnDefinition('InsuredDenormID', LongType(), True), ColumnDefinition('TreatedPatientBfr', BooleanType(), True), ColumnDefinition('Alg_SuvaFlagRandom', BooleanType(), True), ColumnDefinition('Alg_CauserDetail', LongType(), True), ColumnDefinition('PTPinMPN', BooleanType(), True), ColumnDefinition('StorageCategory', IntegerType(), True), ColumnDefinition('Alg_OrganisationId', StringType(), True), ColumnDefinition('Alg_IsUserAutoAssigned', BooleanType(), True),
    ColumnDefinition('PermissionRequired', IntegerType(), True), ColumnDefinition('Alg_LastCloseDate', TimestampType(), True), ColumnDefinition('Alg_NumberOfAttempts', IntegerType(), True), ColumnDefinition('Alg_TranspDocID', StringType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('ClosedOutcome', IntegerType(), True), ColumnDefinition('EmploymentInjury', BooleanType(), True), ColumnDefinition('AssignedQueueID', LongType(), True), ColumnDefinition('Flagged', IntegerType(), True), ColumnDefinition('SalvageStatus', IntegerType(), True), ColumnDefinition('Alg_isBulkFnolClaim', BooleanType(), True), ColumnDefinition('SupplementalWorkloadWeight', IntegerType(), True), ColumnDefinition('Alg_LosslocPostalCode', StringType(), True), ColumnDefinition('Alg_CaseID', StringType(), True), ColumnDefinition('AssignedGroupID', LongType(), True), ColumnDefinition('Alg_DeductionReason', StringType(), True), ColumnDefinition('Alg_SunnetLossTime', TimestampType(), True), ColumnDefinition('Alg_AccidentCode', StringType(), True), ColumnDefinition('Alg_PolicyDocumentsFound', BooleanType(), True), ColumnDefinition('FaultRating', IntegerType(), True),
    ColumnDefinition('DateCompDcsnDue', TimestampType(), True), ColumnDefinition('Alg_IsCatastropheNotApplicable', BooleanType(), True), ColumnDefinition('Alg_VerifiedDate', TimestampType(), True), ColumnDefinition('Alg_CloseClaimNote', StringType(), True), ColumnDefinition('StorageType', IntegerType(), True), ColumnDefinition('LocationOfTheft', IntegerType(), True), ColumnDefinition('Alg_RelapseCheck', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Progress', IntegerType(), True), ColumnDefinition('ReOpenDate', TimestampType(), True), ColumnDefinition('ReinsuranceFlaggedStatus', IntegerType(), True), ColumnDefinition('Alg_IPSCauseofEvent', IntegerType(), True), ColumnDefinition('DeathDate', TimestampType(), True), ColumnDefinition('Fault', DecimalType(4,1), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ClaimTier', IntegerType(), True), ColumnDefinition('OtherRecovStatus', IntegerType(), True), ColumnDefinition('StorageBoxNum', StringType(), True), ColumnDefinition('Alg_FaultRatingModifiedDate', TimestampType(), True), ColumnDefinition('ExposureEnded', TimestampType(), True),
    ColumnDefinition('LitigationStatus', IntegerType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('DiagnosticCnsistnt', BooleanType(), True), ColumnDefinition('Alg_Diagnosis', IntegerType(), True), ColumnDefinition('DateRptdToAgent', TimestampType(), True), ColumnDefinition('Alg_FirstPhoneCallNotNeeded', BooleanType(), True), ColumnDefinition('Alg_FirstNotificationConfirm', BooleanType(), True), ColumnDefinition('SIULifeCycleState', IntegerType(), True), ColumnDefinition('Alg_SelectedCatastrophe', StringType(), True), ColumnDefinition('Alg_EarlyInterventionDate', TimestampType(), True), ColumnDefinition('Alg_LossLocCity', StringType(), True), ColumnDefinition('Alg_PercentDeduction', DecimalType(5,2), True), ColumnDefinition('DateFormGivenToEmp', TimestampType(), True), ColumnDefinition('MainContactType', IntegerType(), True)
])

raw_vault.create_satellite('SAT__EXPOSURE', [
    ColumnDefinition('PreviousGroupID', LongType(), True), ColumnDefinition('CoverageID', LongType(), True), ColumnDefinition('IncidentLimitReached', BooleanType(), True), ColumnDefinition('Locked', BooleanType(), True), ColumnDefinition('AssignedByUserID', LongType(), True), ColumnDefinition('OtherCovgChoice', IntegerType(), True), ColumnDefinition('DiagnosticCnsistnt', BooleanType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('Alg_ExposureNumber', StringType(), True), ColumnDefinition('PIPClaimAggLimitReached', BooleanType(), True), ColumnDefinition('PreviousQueueID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Alg_LastTaxStmtRunTime', TimestampType(), True), ColumnDefinition('ExaminationDate', TimestampType(), True), ColumnDefinition('VocBenefitsID', LongType(), True), ColumnDefinition('LossCategory', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('Alg_InitialDispositionDate', TimestampType(), True), ColumnDefinition('CompBenefitsID', LongType(), True),
    ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('LifePensionBenefitsID', LongType(), True), ColumnDefinition('ReOpenDate', TimestampType(), True), ColumnDefinition('Alg_isUninsuredExposure', BooleanType(), True), ColumnDefinition('ALg_CreateMedexpReserve', BooleanType(), True), ColumnDefinition('AverageWeeklyWages', DecimalType(18,2), True), ColumnDefinition('Progress', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Segment', IntegerType(), True), ColumnDefinition('LastDayWorked', TimestampType(), True), ColumnDefinition('Alg_TypeOfSegmentation', IntegerType(), True), ColumnDefinition('DepreciatedValue', DecimalType(18,2), True), ColumnDefinition('SSBenefit', BooleanType(), True), ColumnDefinition('PPDBenefitsID', LongType(), True), ColumnDefinition('TPDBenefitsID', LongType(), True), ColumnDefinition('SecurityLevel', IntegerType(), True), ColumnDefinition('Alg_IsSelected', BooleanType(), True), ColumnDefinition('DeathBenefitsID', LongType(), True), ColumnDefinition('WCBenefit', BooleanType(), True), ColumnDefinition('AssignedGroupID', LongType(), True),
    ColumnDefinition('SupplementalWorkloadWeight', IntegerType(), True), ColumnDefinition('WCPreexDisblty', BooleanType(), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('Alg_SubjectToDeductible', BooleanType(), True), ColumnDefinition('AssignedQueueID', LongType(), True), ColumnDefinition('MetricLimitGeneration', IntegerType(), True), ColumnDefinition('StatLineID', LongType(), True), ColumnDefinition('WorkloadUpdated', TimestampType(), True), ColumnDefinition('BreakIn', BooleanType(), True), ColumnDefinition('TempLocationID', LongType(), True), ColumnDefinition('ClosedOutcome', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Alg_IsCaseDebtEnfOffice', BooleanType(), True), ColumnDefinition('ExposureTier', IntegerType(), True), ColumnDefinition('ISOReceiveDate', TimestampType(), True), ColumnDefinition('IncidentID', LongType(), True), ColumnDefinition('NewEmpDataID', LongType(), True), ColumnDefinition('Alg_LastCloseDate', TimestampType(), True), ColumnDefinition('WageBenefit', BooleanType(), True), ColumnDefinition('Strategy', IntegerType(), True),
    ColumnDefinition('AssignedUserID', LongType(), True), ColumnDefinition('Alg_IsUserAutoAssigned', BooleanType(), True), ColumnDefinition('SettleDate', TimestampType(), True), ColumnDefinition('Alg_ReopenExposureNote', StringType(), True), ColumnDefinition('Alg_ExpReopenReason', IntegerType(), True), ColumnDefinition('SSDIBenefitsID', LongType(), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('WCPreexDisbltyInfo', StringType(), True), ColumnDefinition('TreatedPatientBfr', BooleanType(), True), ColumnDefinition('PriorEmpDataID', LongType(), True), ColumnDefinition('WageStmtSent', TimestampType(), True), ColumnDefinition('Alg_IsAutoCreated', BooleanType(), True), ColumnDefinition('PIPPersonAggLimitReached', BooleanType(), True), ColumnDefinition('Alg_CloseExposureNote', StringType(), True), ColumnDefinition('State', IntegerType(), True), ColumnDefinition('HospitalDays', IntegerType(), True), ColumnDefinition('CoverageSubType', IntegerType(), True), ColumnDefinition('ClaimOrder', IntegerType(), True), ColumnDefinition('Alg_GroupSegment', IntegerType(), True), ColumnDefinition('Alg_ExposureSubState', IntegerType(), True),
    ColumnDefinition('Alg_CloseExposure', BooleanType(), True), ColumnDefinition('ReplacementValue', DecimalType(18,2), True), ColumnDefinition('ReopenedReason', IntegerType(), True), ColumnDefinition('LossParty', IntegerType(), True), ColumnDefinition('WCBenefitsID', LongType(), True), ColumnDefinition('CloseDate', TimestampType(), True), ColumnDefinition('OtherCoverageInfo', StringType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('ValidationLevel', IntegerType(), True), ColumnDefinition('WorkloadWeight', IntegerType(), True), ColumnDefinition('Alg_BeneficiaryCode', IntegerType(), True), ColumnDefinition('DaysInWeek', IntegerType(), True), ColumnDefinition('JurisdictionState', IntegerType(), True), ColumnDefinition('LostPropertyType', IntegerType(), True), ColumnDefinition('ISOStatus', IntegerType(), True), ColumnDefinition('PTDBenefitsID', LongType(), True), ColumnDefinition('ISOSendDate', TimestampType(), True), ColumnDefinition('TTDBenefitsID', LongType(), True), ColumnDefinition('SSDIEligible', BooleanType(), True), ColumnDefinition('ExposureLimitReached', BooleanType(), True),
    ColumnDefinition('ExposureType', IntegerType(), True), ColumnDefinition('PIPNonMedAggLimitReached', BooleanType(), True), ColumnDefinition('ClaimantDenormID', LongType(), True), ColumnDefinition('WageStmtRecd', TimestampType(), True), ColumnDefinition('Alg_ExpectedDeductibleHistory', DecimalType(18,2), True), ColumnDefinition('PIPESSLimitReached', BooleanType(), True), ColumnDefinition('RIAgreementGroupID', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Alg_VZ4', StringType(), True), ColumnDefinition('Alg_ExpectedDeductible', DecimalType(18,2), True), ColumnDefinition('RIGroupSetExternally', BooleanType(), True), ColumnDefinition('FurtherTreatment', BooleanType(), True), ColumnDefinition('PIPDeathBenefitsID', LongType(), True), ColumnDefinition('Alg_SUBBRC', StringType(), True), ColumnDefinition('RSBenefitsID', LongType(), True), ColumnDefinition('Alg_InitialReserveCreation', BooleanType(), True), ColumnDefinition('PreviousUserID', LongType(), True), ColumnDefinition('ISOKnown', BooleanType(), True), ColumnDefinition('Alg_LastReopenedDate', TimestampType(), True), ColumnDefinition('ClaimantType', IntegerType(), True),
    ColumnDefinition('DisBenefitsID', LongType(), True), ColumnDefinition('HospitalDate', TimestampType(), True), ColumnDefinition('PrimaryCoverage', IntegerType(), True), ColumnDefinition('PIPVocBenefitsID', LongType(), True), ColumnDefinition('Alg_ManualSegmentation', BooleanType(), True), ColumnDefinition('Alg_WaitingPeriod', IntegerType(), True), ColumnDefinition('SettleMethod', IntegerType(), True), ColumnDefinition('CurrentConditions', BooleanType(), True), ColumnDefinition('Alg_VAC', StringType(), True), ColumnDefinition('OtherCoverage', BooleanType(), True), ColumnDefinition('ContactPermitted', BooleanType(), True), ColumnDefinition('AssignmentDate', TimestampType(), True), ColumnDefinition('AssignmentStatus', IntegerType(), True), ColumnDefinition('Alg_LifeCertDocDeliveryMethod', StringType(), True), ColumnDefinition('Alg_LifeCertReminderSentDate', TimestampType(), True), ColumnDefinition('Alg_IsInflationChangeDocSent', BooleanType(), True), ColumnDefinition('Alg_PensionTaxStmtRecipient', LongType(), True), ColumnDefinition('Alg_LifeCertDocSentDate', TimestampType(), True), ColumnDefinition('Alg_ConfOfEdDocSentDate', TimestampType(), True), ColumnDefinition('Alg_InflationChangeRecipient', LongType(), True),
    ColumnDefinition('Alg_EduInstitution', StringType(), True), ColumnDefinition('Alg_IsLifeCertConfReceived', BooleanType(), True), ColumnDefinition('Alg_IsLifeCertReminderSent', BooleanType(), True), ColumnDefinition('Alg_PensionTaxDocSentDate', TimestampType(), True), ColumnDefinition('Alg_IsLifeCertDocSent', BooleanType(), True), ColumnDefinition('Alg_InflationChangeDocSentDate', TimestampType(), True), ColumnDefinition('Alg_IsConfOfEdDocSent', BooleanType(), True), ColumnDefinition('Alg_MaxSalChangeDocSentDate', TimestampType(), True), ColumnDefinition('Alg_IsPensionTaxDocSent', BooleanType(), True), ColumnDefinition('Alg_ExpGraduationDate', TimestampType(), True), ColumnDefinition('Alg_LifeCertConfReceivedDate', TimestampType(), True), ColumnDefinition('Alg_IsMaxSalChangeDocSent', BooleanType(), True), ColumnDefinition('Alg_ConfOfEdRecipient', LongType(), True), ColumnDefinition('Alg_LifeCertDocRecipient', LongType(), True), ColumnDefinition('Alg_MaxSalChangeRecipient', LongType(), True), ColumnDefinition('Alg_LifeCertReminderRecipient', LongType(), True), ColumnDefinition('Alg_PensionCase', BooleanType(), True), ColumnDefinition('Alg_scalepointID', StringType(), True), ColumnDefinition('Alg_PensionType', IntegerType(), True), ColumnDefinition('Alg_ExchangeDate', TimestampType(), True),
    ColumnDefinition('Alg_DedExchangeRate', DecimalType(8,4), True), ColumnDefinition('Alg_DeductibleAmount', DecimalType(18,2), True), ColumnDefinition('Alg_CurrencyDeductible', IntegerType(), True), ColumnDefinition('Alg_DWHKontrahentNr', IntegerType(), True), ColumnDefinition('Alg_IsAutomaticPaid', BooleanType(), True), ColumnDefinition('Alg_Fraud', LongType(), True), ColumnDefinition('Alg_CreatedBySystem', IntegerType(), True), ColumnDefinition('Alg_AutoPaymentAutoClosed', BooleanType(), True), ColumnDefinition('Alg_IsMigratedExposure', BooleanType(), True), ColumnDefinition('Alg_ScalepointTimestamp', TimestampType(), True), ColumnDefinition('Alg_BousMalusChargePolicy', BooleanType(), True), ColumnDefinition('Alg_BonusMalusChargePolicy', BooleanType(), True), ColumnDefinition('Alg_MigrationPensionID', StringType(), True), ColumnDefinition('Alg_CiriardMigrationCoverID', StringType(), True), ColumnDefinition('Alg_MigPensBlockAutoPayUntil', TimestampType(), True), ColumnDefinition('Alg_IsMigPensMultipleRecipient', BooleanType(), True), ColumnDefinition('Alg_IsPensMultipleRecipient', BooleanType(), True), ColumnDefinition('Alg_PensionInitialReserve', DecimalType(18,2), True), ColumnDefinition('Alg_PaidIndemnityExp', DecimalType(18,2), True), ColumnDefinition('Alg_RemainingDedExp', DecimalType(18,2), True),
    ColumnDefinition('Alg_AlreadyAppliedDedExp', DecimalType(18,2), True), ColumnDefinition('Alg_PaidExpenseExp', DecimalType(18,2), True), ColumnDefinition('Alg_ScalepointCalledByQueue', BooleanType(), True)
])

raw_vault.create_satellite('SAT__POLICY', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('PolicySuffix', StringType(), True), ColumnDefinition('Alg_PolicyVersion', StringType(), True), ColumnDefinition('AccountID', LongType(), True), ColumnDefinition('Alg_LanguageCode', IntegerType(), True), ColumnDefinition('Alg_CancellationReason', IntegerType(), True), ColumnDefinition('TotalVehicles', IntegerType(), True), ColumnDefinition('OtherInsInfo', StringType(), True), ColumnDefinition('Alg_TransportNumber', IntegerType(), True), ColumnDefinition('InsuredSICCode', StringType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Alg_ExternalRUReference', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('AssignedRisk', BooleanType(), True), ColumnDefinition('FinancialInterests', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ReturnToWorkPrgm', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Alg_BalanceOfOpenInvCurrency', IntegerType(), True),
    ColumnDefinition('Retired', LongType(), True), ColumnDefinition('ValidationLevel', IntegerType(), True), ColumnDefinition('CoverageForm', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Alg_BalanceOfOpenInvAmount', DecimalType(18,2), True), ColumnDefinition('Alg_PolicyProductCode', StringType(), True), ColumnDefinition('Alg_LastPolicyRefresh', TimestampType(), True), ColumnDefinition('CancellationDate', TimestampType(), True), ColumnDefinition('OrigEffectiveDate', TimestampType(), True), ColumnDefinition('Alg_InsuredCompanyReference', StringType(), True), ColumnDefinition('Alg_Note', StringType(), True), ColumnDefinition('AccountNumber', StringType(), True), ColumnDefinition('Alg_GeneralConditionofInsuranc', StringType(), True), ColumnDefinition('ForeignCoverage', BooleanType(), True), ColumnDefinition('Notes', StringType(), True), ColumnDefinition('Participation', DecimalType(4,1), True), ColumnDefinition('WCOtherStates', StringType(), True), ColumnDefinition('Verified', BooleanType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('OtherInsurance', BooleanType(), True),
    ColumnDefinition('Alg_CoInsurance', IntegerType(), True), ColumnDefinition('PolicySource', IntegerType(), True), ColumnDefinition('WCStates', StringType(), True), ColumnDefinition('RetroactiveDate', TimestampType(), True), ColumnDefinition('Alg_InsBySplAgreemnt', BooleanType(), True), ColumnDefinition('EffectiveDate', TimestampType(), True), ColumnDefinition('ReportingDate', TimestampType(), True), ColumnDefinition('ExpirationDate', TimestampType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Alg_LastPolicyChangeDate', TimestampType(), True), ColumnDefinition('PolicyType', IntegerType(), True), ColumnDefinition('UnderwritingCo', IntegerType(), True), ColumnDefinition('Alg_CustomerSegment', StringType(), True), ColumnDefinition('Status', IntegerType(), True), ColumnDefinition('Alg_ContactRole', StringType(), True), ColumnDefinition('Alg_AgencyCode', StringType(), True), ColumnDefinition('TotalProperties', IntegerType(), True), ColumnDefinition('PolicyRatingPlan', IntegerType(), True), ColumnDefinition('CustomerServiceTier', IntegerType(), True), ColumnDefinition('PolicyNumber', StringType(), True),
    ColumnDefinition('UnderwritingGroup', IntegerType(), True), ColumnDefinition('PolicySystemPeriodID', LongType(), True), ColumnDefinition('ProducerCode', StringType(), True), ColumnDefinition('Alg_ConditionIssueDate', TimestampType(), True), ColumnDefinition('Alg_PolicyEndDate', TimestampType(), True), ColumnDefinition('Alg_SalePoint', StringType(), True), ColumnDefinition('Alg_AffinityID', StringType(), True), ColumnDefinition('Alg_IssueVersionDate', TimestampType(), True), ColumnDefinition('Alg_MovementEndDate', TimestampType(), True), ColumnDefinition('Alg_MovementStartDate', TimestampType(), True), ColumnDefinition('Alg_AffinityName', StringType(), True), ColumnDefinition('Alg_ClaimFlow', IntegerType(), True), ColumnDefinition('Alg_PolicySystem', IntegerType(), True), ColumnDefinition('Alg_ContactSystem', IntegerType(), True), ColumnDefinition('Alg_PolicySystemId', StringType(), True), ColumnDefinition('Alg_CommercialCode', StringType(), True), ColumnDefinition('Alg_PolicySourceSystem', IntegerType(), True), ColumnDefinition('Alg_MovementReason', IntegerType(), True), ColumnDefinition('Alg_SettlementRating', IntegerType(), True), ColumnDefinition('Alg_FrameworkContractNumber', StringType(), True),
    ColumnDefinition('Alg_MovementType', IntegerType(), True), ColumnDefinition('Alg_ProductDescription', StringType(), True), ColumnDefinition('Alg_TechnicalProductCode', IntegerType(), True), ColumnDefinition('Alg_InCompletePolicy', BooleanType(), True), ColumnDefinition('Alg_BrokerType', IntegerType(), True), ColumnDefinition('Alg_ConditionIssue', StringType(), True), ColumnDefinition('Alg_AffBusinessSector', StringType(), True)
])

raw_vault.create_satellite('SAT__INCIDENT', [
    ColumnDefinition('Whichestimationconsidered', IntegerType(), True), ColumnDefinition('InsuredRevenueCurrency', IntegerType(), True), ColumnDefinition('EstimateCost_Sec', DecimalType(18,2), True), ColumnDefinition('RecovInd', IntegerType(), True), ColumnDefinition('LocationInd', BooleanType(), True), ColumnDefinition('TotalLossPoints', IntegerType(), True), ColumnDefinition('MachineInstallationName', IntegerType(), True), ColumnDefinition('NumStories', IntegerType(), True), ColumnDefinition('AirbagsMissing', BooleanType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('VehicleTitleRecvd', IntegerType(), True), ColumnDefinition('CarrierCompensated', BooleanType(), True), ColumnDefinition('ExtWallMat', IntegerType(), True), ColumnDefinition('Reportavailable', BooleanType(), True), ColumnDefinition('FireBurnDash', BooleanType(), True), ColumnDefinition('GeneralInjuryType', IntegerType(), True), ColumnDefinition('FireProtectionAvailable', BooleanType(), True), ColumnDefinition('RecovClassType', IntegerType(), True), ColumnDefinition('Alg_IsOwnerRetainVehicle', BooleanType(), True), ColumnDefinition('CreateUserID', LongType(), True),
    ColumnDefinition('DriverRelation', IntegerType(), True), ColumnDefinition('FloorOwner', BooleanType(), True), ColumnDefinition('RentalAgency', StringType(), True), ColumnDefinition('SalvageProceeds', DecimalType(18,2), True), ColumnDefinition('DebrisRemovalInd', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('LossArea', IntegerType(), True), ColumnDefinition('VehicleTitleReqd', IntegerType(), True), ColumnDefinition('AssessmentTargetCloseDate', TimestampType(), True), ColumnDefinition('FencesDamaged', BooleanType(), True), ColumnDefinition('BodyShopSelected', BooleanType(), True), ColumnDefinition('LossDesc', StringType(), True), ColumnDefinition('VehicleParked', BooleanType(), True), ColumnDefinition('FireBurnWindshield', BooleanType(), True), ColumnDefinition('VehicleDriveable', BooleanType(), True), ColumnDefinition('EquipmentFailure', BooleanType(), True), ColumnDefinition('NumberOfPeopleOnPolicy', IntegerType(), True), ColumnDefinition('ReturnToWorkValid', BooleanType(), True), ColumnDefinition('FloodSaltWater', BooleanType(), True), ColumnDefinition('TripRUID', LongType(), True),
    ColumnDefinition('Reason', StringType(), True), ColumnDefinition('Alg_Hospitalization', BooleanType(), True), ColumnDefinition('Machine', LongType(), True), ColumnDefinition('Speed', IntegerType(), True), ColumnDefinition('BIIncidentNotes', StringType(), True), ColumnDefinition('AssessmentName', StringType(), True), ColumnDefinition('CitationIssued', IntegerType(), True), ColumnDefinition('SalvagePrep', DecimalType(18,2), True), ColumnDefinition('SalvageTow', DecimalType(18,2), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('Amount', DecimalType(18,2), True), ColumnDefinition('Impairment', DecimalType(4,1), True), ColumnDefinition('InsuredGrossProfit', DecimalType(18,2), True), ColumnDefinition('MovePermission', BooleanType(), True), ColumnDefinition('WaterLevelSeats', BooleanType(), True), ColumnDefinition('YearBuilt', TimestampType(), True), ColumnDefinition('Mileage100K', BooleanType(), True), ColumnDefinition('OperatingMode', IntegerType(), True), ColumnDefinition('VehicleType', IntegerType(), True), ColumnDefinition('TotalLoss', BooleanType(), True),
    ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Currency_Sec_Est', IntegerType(), True), ColumnDefinition('Operatinghours', IntegerType(), True), ColumnDefinition('TotalLoss_Machine', BooleanType(), True), ColumnDefinition('InsuredGrossProfitCurrency', IntegerType(), True), ColumnDefinition('Alg_MedicalControllable', IntegerType(), True), ColumnDefinition('StorageFclty', StringType(), True), ColumnDefinition('RecovState', IntegerType(), True), ColumnDefinition('BaggageType', IntegerType(), True), ColumnDefinition('AirbagsDeployed', BooleanType(), True), ColumnDefinition('MinorOnPolicy', IntegerType(), True), ColumnDefinition('DriverRelToOwner', IntegerType(), True), ColumnDefinition('OdomRead', IntegerType(), True), ColumnDefinition('Extrication', BooleanType(), True), ColumnDefinition('ParcelNumber', StringType(), True), ColumnDefinition('IsFNOLCapturing', BooleanType(), True), ColumnDefinition('SalvageNet', DecimalType(18,2), True), ColumnDefinition('ReturnToModWorkActual', BooleanType(), True), ColumnDefinition('Leasing', BooleanType(), True), ColumnDefinition('IncludeContentLineItems', BooleanType(), True),
    ColumnDefinition('LocationAddress', LongType(), True), ColumnDefinition('ConsClassType', StringType(), True), ColumnDefinition('IncludeLineItems', BooleanType(), True), ColumnDefinition('RecovDate', TimestampType(), True), ColumnDefinition('HitAndRun', BooleanType(), True), ColumnDefinition('CarrierCompensatedAmount', DecimalType(18,2), True), ColumnDefinition('ClassType', IntegerType(), True), ColumnDefinition('ReturnToModWorkDate', TimestampType(), True), ColumnDefinition('AntiThftInd', BooleanType(), True), ColumnDefinition('StorageFeeAmt', DecimalType(18,2), True), ColumnDefinition('CurrencyDamage', IntegerType(), True), ColumnDefinition('IsFullBI', BooleanType(), True), ColumnDefinition('PercentageDrivenByMinor', IntegerType(), True), ColumnDefinition('MedicalTreatmentType', IntegerType(), True), ColumnDefinition('Notes', StringType(), True), ColumnDefinition('RentalRequired', BooleanType(), True), ColumnDefinition('InsuredValue', IntegerType(), True), ColumnDefinition('FixedAmount', DecimalType(18,2), True), ColumnDefinition('BaggageMissingFrom', TimestampType(), True), ColumnDefinition('RecovCondType', IntegerType(), True),
    ColumnDefinition('VehiclePolStatus', IntegerType(), True), ColumnDefinition('VehicleAge5Years', BooleanType(), True), ColumnDefinition('RentalReserveNo', StringType(), True), ColumnDefinition('YearsInHome', IntegerType(), True), ColumnDefinition('ConstructionName', StringType(), True), ColumnDefinition('VehicleUseReason', IntegerType(), True), ColumnDefinition('EstRepairCost', DecimalType(18,2), True), ColumnDefinition('VehicleLocation', StringType(), True), ColumnDefinition('BaggageRecoveredOn', TimestampType(), True), ColumnDefinition('InsuredIncreasedCOWCurrency', IntegerType(), True), ColumnDefinition('TypeOfRoof', IntegerType(), True), ColumnDefinition('RecoveryLocationID', LongType(), True), ColumnDefinition('Appraisal', BooleanType(), True), ColumnDefinition('ReturnToWorkDate', TimestampType(), True), ColumnDefinition('LocationMachine', IntegerType(), True), ColumnDefinition('Maintenancecontractexist', BooleanType(), True), ColumnDefinition('SalvageStorage', DecimalType(18,2), True), ColumnDefinition('SalvageCompany', StringType(), True), ColumnDefinition('IsFNOLUnattachedData', BooleanType(), True), ColumnDefinition('AssessmentType', IntegerType(), True),
    ColumnDefinition('Severity', IntegerType(), True), ColumnDefinition('OwnerRetainingSalvage', BooleanType(), True), ColumnDefinition('Category', IntegerType(), True), ColumnDefinition('DateVehicleRecovered', TimestampType(), True), ColumnDefinition('FireProtDetails', StringType(), True), ColumnDefinition('SprinkRetServ', IntegerType(), True), ColumnDefinition('DateSalvageAssigned', TimestampType(), True), ColumnDefinition('OccupancyType', IntegerType(), True), ColumnDefinition('LossOccured', IntegerType(), True), ColumnDefinition('SplitDamage', BooleanType(), True), ColumnDefinition('AlarmType', IntegerType(), True), ColumnDefinition('LossofUse', BooleanType(), True), ColumnDefinition('ClaimIncident', BooleanType(), True), ColumnDefinition('StartDate', TimestampType(), True), ColumnDefinition('SalvageTitle', DecimalType(18,2), True), ColumnDefinition('Alg_UVGGArticle99', BooleanType(), True), ColumnDefinition('InspectionRequired', BooleanType(), True), ColumnDefinition('InsuranceType', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('VehLockInd', BooleanType(), True),
    ColumnDefinition('ID', LongType(), True), ColumnDefinition('DescOther', StringType(), True), ColumnDefinition('InternalUserID', LongType(), True), ColumnDefinition('AssessmentCloseDate', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('MaterialsDamaged', StringType(), True), ColumnDefinition('EstimatesReceived', IntegerType(), True), ColumnDefinition('EstDamageType', IntegerType(), True), ColumnDefinition('Collision', BooleanType(), True), ColumnDefinition('Lastmaintenance_service', TimestampType(), True), ColumnDefinition('RentalBeginDate', TimestampType(), True), ColumnDefinition('EMSInd', BooleanType(), True), ColumnDefinition('AssessmentStatus', IntegerType(), True), ColumnDefinition('IsSeasonalBusiness', BooleanType(), True), ColumnDefinition('SecondEstimaterequired', BooleanType(), True), ColumnDefinition('BuildingValue', DecimalType(18,2), True), ColumnDefinition('BuildingName', StringType(), True), ColumnDefinition('AlreadyRepaired', BooleanType(), True), ColumnDefinition('VehicleID', LongType(), True), ColumnDefinition('Alg_IsLeasing', BooleanType(), True),
    ColumnDefinition('LossEstimate', DecimalType(18,2), True), ColumnDefinition('StorageAccrInd', IntegerType(), True), ColumnDefinition('PropertySize', IntegerType(), True), ColumnDefinition('HazardInvolved', IntegerType(), True), ColumnDefinition('AmbulanceUsed', BooleanType(), True), ColumnDefinition('WhenToView', StringType(), True), ColumnDefinition('TotalLiabilityCurrency', IntegerType(), True), ColumnDefinition('AssessmentComment', StringType(), True), ColumnDefinition('IsFixedAmount', BooleanType(), True), ColumnDefinition('VehicleAge10Years', BooleanType(), True), ColumnDefinition('RentalEndDate', TimestampType(), True), ColumnDefinition('MealsDays', IntegerType(), True), ColumnDefinition('RelatedTripRUID', LongType(), True), ColumnDefinition('Alg_EmploymentDataID', LongType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('RepWhereDisInd', BooleanType(), True), ColumnDefinition('PhantomVehicle', BooleanType(), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('DisabledDueToAccident', IntegerType(), True), ColumnDefinition('TotalCosts', DecimalType(18,2), True),
    ColumnDefinition('FireBurnEngine', BooleanType(), True), ColumnDefinition('Alg_ReRunRELK', BooleanType(), True), ColumnDefinition('ComponentsMissing', BooleanType(), True), ColumnDefinition('Alg_ReRunRELU', BooleanType(), True), ColumnDefinition('DateVehicleSold', TimestampType(), True), ColumnDefinition('TotalCostsCurrency', IntegerType(), True), ColumnDefinition('Alg_VehicleSalvageID', LongType(), True), ColumnDefinition('VehTowedInd', BooleanType(), True), ColumnDefinition('CollisionPoint', IntegerType(), True), ColumnDefinition('MealsPeople', IntegerType(), True), ColumnDefinition('ExtDamagetxt', StringType(), True), ColumnDefinition('NumSprinkler', IntegerType(), True), ColumnDefinition('SprinklerType', IntegerType(), True), ColumnDefinition('YearOfConstruction', TimestampType(), True), ColumnDefinition('AffdvCmplInd', IntegerType(), True), ColumnDefinition('LostWages', BooleanType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('DelayOnly', BooleanType(), True), ColumnDefinition('PropertyID', LongType(), True), ColumnDefinition('InteriorMissing', BooleanType(), True),
    ColumnDefinition('Alg_RecoveryCountry', IntegerType(), True), ColumnDefinition('VehicleSubmerged', BooleanType(), True), ColumnDefinition('InsuredAmount', DecimalType(18,2), True), ColumnDefinition('TrafficViolation', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('ReturnToWorkActual', BooleanType(), True), ColumnDefinition('EstimatedDuration', IntegerType(), True), ColumnDefinition('Alg_VehicleLicenseType', IntegerType(), True), ColumnDefinition('InsuredAmountCurrency', IntegerType(), True), ColumnDefinition('InsuredIncreasedCOW', DecimalType(18,2), True), ColumnDefinition('VehicleDirection', IntegerType(), True), ColumnDefinition('AppraisalFirstAppointment', TimestampType(), True), ColumnDefinition('VehicleOperable', BooleanType(), True), ColumnDefinition('RentalDailyRate', DecimalType(18,2), True), ColumnDefinition('IsCivilProceedings', BooleanType(), True), ColumnDefinition('EstimationOfBuilding', BooleanType(), True), ColumnDefinition('InformationCivilProceedings', StringType(), True), ColumnDefinition('FinancialNotes', StringType(), True), ColumnDefinition('LotNumber', StringType(), True), ColumnDefinition('VehStolenInd', BooleanType(), True),
    ColumnDefinition('WaterLevelDash', BooleanType(), True), ColumnDefinition('EstRepairTime', StringType(), True), ColumnDefinition('MoldInvolved', IntegerType(), True), ColumnDefinition('DetailedInjuryType', IntegerType(), True), ColumnDefinition('TotalLiabilityAmount', DecimalType(18,2), True), ColumnDefinition('DamagedAreaSize', IntegerType(), True), ColumnDefinition('NumSprinkOper', IntegerType(), True), ColumnDefinition('PropertyDesc', StringType(), True), ColumnDefinition('VehicleACV', DecimalType(18,2), True), ColumnDefinition('ActionsTaken', StringType(), True), ColumnDefinition('Alg_TotalSalary', DecimalType(18,2), True), ColumnDefinition('OwnersPermission', BooleanType(), True), ColumnDefinition('MealsRate', DecimalType(18,2), True), ColumnDefinition('InsuredRevenue', DecimalType(18,2), True), ColumnDefinition('ReturnToModWorkValid', BooleanType(), True), ColumnDefinition('VehCondType', IntegerType(), True), ColumnDefinition('VehicleLossParty', IntegerType(), True), ColumnDefinition('RoofMaterial', IntegerType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('VehicleRollOver', BooleanType(), True),
    ColumnDefinition('TripCategory', IntegerType(), True), ColumnDefinition('YearOfBuilt', TimestampType(), True), ColumnDefinition('TypeofBuilding', IntegerType(), True), ColumnDefinition('OwnerType', IntegerType(), True), ColumnDefinition('Alg_Object', IntegerType(), True), ColumnDefinition('MultiInsurance', BooleanType(), True), ColumnDefinition('TransportType', IntegerType(), True), ColumnDefinition('ProvisionalSumInsured', BooleanType(), True), ColumnDefinition('WallSafe', BooleanType(), True), ColumnDefinition('ChipNumber', StringType(), True), ColumnDefinition('MechanicalDevice', BooleanType(), True), ColumnDefinition('FireAlarm', BooleanType(), True), ColumnDefinition('RiskUnitID', LongType(), True), ColumnDefinition('FireSecurityDevice', BooleanType(), True), ColumnDefinition('FireSecurity', BooleanType(), True), ColumnDefinition('AlarmSystem', BooleanType(), True), ColumnDefinition('Alg_IsReplacementVehicle', BooleanType(), True), ColumnDefinition('LossParty', IntegerType(), True), ColumnDefinition('BIExchangeDate', TimestampType(), True), ColumnDefinition('TC_ExchangeDate', TimestampType(), True),
    ColumnDefinition('TL_ExchangeDate', TimestampType(), True), ColumnDefinition('Alg_InjuredParty', IntegerType(), True), ColumnDefinition('Alg_IsZurRepairNetwork', BooleanType(), True), ColumnDefinition('Alg_RepairReason', IntegerType(), True), ColumnDefinition('Alg_RepairVendorID', LongType(), True), ColumnDefinition('Alg_ConcernedPerson', IntegerType(), True), ColumnDefinition('Alg_VendorDetails', LongType(), True), ColumnDefinition('Alg_LossEstRepBy', IntegerType(), True), ColumnDefinition('Alg_IndexNumber', IntegerType(), True), ColumnDefinition('LossEstimateCurrency', IntegerType(), True), ColumnDefinition('Alg_IsAutomativ', BooleanType(), True), ColumnDefinition('Alg_Fraud', LongType(), True), ColumnDefinition('Alg_IsZurPartnerNetwork', IntegerType(), True), ColumnDefinition('VignetteNumber', StringType(), True), ColumnDefinition('Alg_CreatedBySystem', IntegerType(), True), ColumnDefinition('Alg_LossPartyType', IntegerType(), True), ColumnDefinition('FreeDescription', StringType(), True), ColumnDefinition('DescriptionOfRisk', IntegerType(), True), ColumnDefinition('Alg_DocumentReceiverID', StringType(), True), ColumnDefinition('Alg_Garage', LongType(), True),
    ColumnDefinition('MachinesDescription', StringType(), True), ColumnDefinition('Alg_objectListComplete', BooleanType(), True), ColumnDefinition('Alg_SourceClaimNumber', StringType(), True), ColumnDefinition('Alg_itemizationCaseRef', StringType(), True), ColumnDefinition('Alg_isSameAsReporter', BooleanType(), True)
])

raw_vault.create_satellite('SAT__COVERAGE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Notes', StringType(), True), ColumnDefinition('Alg_GDOProductNumber', StringType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('PolicyID', LongType(), True), ColumnDefinition('ReplaceAggLimit', DecimalType(18,2), True), ColumnDefinition('State', IntegerType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('ExposureLimit', DecimalType(18,2), True), ColumnDefinition('EffectiveDate', TimestampType(), True), ColumnDefinition('Deductible', DecimalType(18,2), True), ColumnDefinition('CoverageBasis', IntegerType(), True), ColumnDefinition('ClaimAggLimit', DecimalType(18,2), True), ColumnDefinition('Alg_AdditionalInformationTitle', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('ExpirationDate', TimestampType(), True), ColumnDefinition('Alg_AddtionalInformationDetail', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True),
    ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('IncidentLimit', DecimalType(18,2), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Coinsurance', DecimalType(4,1), True), ColumnDefinition('RiskUnitID', LongType(), True), ColumnDefinition('PersonAggLimit', DecimalType(18,2), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('LimitsIndicator', IntegerType(), True), ColumnDefinition('NonmedAggLimit', DecimalType(18,2), True), ColumnDefinition('Type', IntegerType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('Alg_VZ4', StringType(), True), ColumnDefinition('Alg_VOG', StringType(), True), ColumnDefinition('Alg_VA', StringType(), True), ColumnDefinition('Alg_SGF', StringType(), True), ColumnDefinition('Alg_VG', StringType(), True), ColumnDefinition('Alg_SGS', StringType(), True), ColumnDefinition('Alg_grossPremium', DecimalType(18,2), True), ColumnDefinition('Alg_PolicyCoverOption', StringType(), True),
    ColumnDefinition('Alg_PolicyCover', StringType(), True), ColumnDefinition('Alg_EPTNumberForCnp', StringType(), True), ColumnDefinition('Alg_TechnicalCoverCode', StringType(), True), ColumnDefinition('Alg_TariffPosition', StringType(), True), ColumnDefinition('Alg_TechnicalPremium', DecimalType(18,2), True)
])

raw_vault.create_satellite('SAT__RISKUNIT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Alg_RUSerialNumber', IntegerType(), True), ColumnDefinition('EndDate', TimestampType(), True), ColumnDefinition('GroupName', StringType(), True), ColumnDefinition('Alg_RUCodeID', StringType(), True), ColumnDefinition('ConstructionYear', StringType(), True), ColumnDefinition('StartDate', TimestampType(), True), ColumnDefinition('TechnicalObjectValue', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('OtherRiskType', IntegerType(), True), ColumnDefinition('Alg_ExternalRUReference', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Selected', BooleanType(), True), ColumnDefinition('Alg_RUCodeValue', StringType(), True), ColumnDefinition('Alg_RUCodeCode', StringType(), True), ColumnDefinition('Alg_RUCodeLabel', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Alg_RUClassification', StringType(), True),
    ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('TripDescription', StringType(), True), ColumnDefinition('Alg_TechnicalObject', LongType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('VehicleLocationID', LongType(), True), ColumnDefinition('serialNumber', IntegerType(), True), ColumnDefinition('ClassCodeID', LongType(), True), ColumnDefinition('PolicyLocationID', LongType(), True), ColumnDefinition('Alg_TransportReference', StringType(), True), ColumnDefinition('VehicleID', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('RUNumber', IntegerType(), True), ColumnDefinition('PolicyID', LongType(), True), ColumnDefinition('BuildingID', LongType(), True), ColumnDefinition('Alg_ParcelNumber', StringType(), True), ColumnDefinition('GeographicalRegion', IntegerType(), True), ColumnDefinition('Alg_TransportDepartureCity', StringType(), True), ColumnDefinition('Alg_Selected', BooleanType(), True), ColumnDefinition('Alg_TransportArrivalCity', StringType(), True), ColumnDefinition('Alg_AddtionalInformationDetail', StringType(), True),
    ColumnDefinition('Alg_WayOfTransport', StringType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Alg_AddtionalInformationTitle', StringType(), True), ColumnDefinition('Alg_TechnicalObjectSubRU', LongType(), True), ColumnDefinition('Alg_BuildingSubRU', LongType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('ObjectSerialNumber', StringType(), True), ColumnDefinition('MechanicalSecurityDevice', BooleanType(), True), ColumnDefinition('AnimalName', StringType(), True), ColumnDefinition('Equipment', IntegerType(), True), ColumnDefinition('IndexYear', IntegerType(), True), ColumnDefinition('UnderInsuranceInEventOfClaim', BooleanType(), True), ColumnDefinition('ModelPlanesUp30Kg', BooleanType(), True), ColumnDefinition('ChipNumber', StringType(), True), ColumnDefinition('NumberOfDogs', IntegerType(), True), ColumnDefinition('NumberOfRooms', DecimalType(5,2), True), ColumnDefinition('HeatingSystem', IntegerType(), True), ColumnDefinition('ConstructionRoof', IntegerType(), True), ColumnDefinition('Model', StringType(), True),
    ColumnDefinition('IndexPoints', BooleanType(), True), ColumnDefinition('PropertyOwnerType', BooleanType(), True), ColumnDefinition('BuiltYear', TimestampType(), True), ColumnDefinition('AlarmSystem', IntegerType(), True), ColumnDefinition('AnimalSpecies', IntegerType(), True), ColumnDefinition('Alg_Object', IntegerType(), True), ColumnDefinition('FireSecurityDevice', IntegerType(), True), ColumnDefinition('Manufacturer', StringType(), True), ColumnDefinition('BuildingUse', IntegerType(), True), ColumnDefinition('Alg_Concerns', StringType(), True), ColumnDefinition('Alg_SubtypeObject', IntegerType(), True), ColumnDefinition('WallSafe', BooleanType(), True), ColumnDefinition('NumberOfAdults', IntegerType(), True), ColumnDefinition('NumberOfChildren', IntegerType(), True), ColumnDefinition('OwnerType', IntegerType(), True), ColumnDefinition('BuildingType', IntegerType(), True), ColumnDefinition('Breed', StringType(), True), ColumnDefinition('DogsCoverRequired', BooleanType(), True), ColumnDefinition('AdditionalCoInsuredPersons', BooleanType(), True), ColumnDefinition('Riskinspection', BooleanType(), True),
    ColumnDefinition('Cessionnaire', BooleanType(), True), ColumnDefinition('Alg_InsuredObjectSequence', StringType(), True), ColumnDefinition('RiskReport', BooleanType(), True), ColumnDefinition('InventoryList', BooleanType(), True), ColumnDefinition('Alg_MiscDriver', BooleanType(), True), ColumnDefinition('Alg_HelpPointPlus', BooleanType(), True), ColumnDefinition('Alg_Transmission', IntegerType(), True), ColumnDefinition('Alg_AdditionalCoInsuredPersons', BooleanType(), True), ColumnDefinition('Alg_AppraisalDate', TimestampType(), True), ColumnDefinition('Alg_MaximumCompensation', DecimalType(18,2), True), ColumnDefinition('ConstructionMethod', IntegerType(), True), ColumnDefinition('Alg_KeyType', StringType(), True), ColumnDefinition('Alg_LimitOfIndemnity', DecimalType(18,2), True), ColumnDefinition('NumberWildAnimals', IntegerType(), True), ColumnDefinition('NumberOfFlats', IntegerType(), True), ColumnDefinition('Alg_LeasedVehicle', BooleanType(), True), ColumnDefinition('InsoulationGlass', BooleanType(), True), ColumnDefinition('Appraisal', IntegerType(), True), ColumnDefinition('Alg_MaximumVehicleAgeInYears', IntegerType(), True), ColumnDefinition('FreeDescription', StringType(), True),
    ColumnDefinition('DescriptionOfRisk', IntegerType(), True), ColumnDefinition('Alg_InsuredPersonalCircle', IntegerType(), True), ColumnDefinition('EffectiveSumInsured', DecimalType(18,2), True), ColumnDefinition('SumInsured', DecimalType(18,2), True), ColumnDefinition('MoreThanTheGivenNumberOfRooms', BooleanType(), True), ColumnDefinition('DateOfLastEstimate', TimestampType(), True), ColumnDefinition('RiskLocationType', IntegerType(), True), ColumnDefinition('CustomerItemId', IntegerType(), True), ColumnDefinition('InsuredItemDescription', StringType(), True), ColumnDefinition('Alg_DWHRUSequenceForCnp', StringType(), True), ColumnDefinition('Alg_PurchasePriceProtection', BooleanType(), True), ColumnDefinition('Alg_NumberOfRisks', IntegerType(), True), ColumnDefinition('NumberOfTravelDays', IntegerType(), True), ColumnDefinition('Alg_SumInsuredPerRisk', DecimalType(18,2), True), ColumnDefinition('NumberOfPeople', IntegerType(), True), ColumnDefinition('InsuredPersons', StringType(), True), ColumnDefinition('NumberOfVehicles', IntegerType(), True), ColumnDefinition('Alg_Lfz_Werk_Nummer', StringType(), True), ColumnDefinition('Alg_Lfz_Anz_Triebwerke', StringType(), True), ColumnDefinition('Alg_Lfz_Art', StringType(), True),
    ColumnDefinition('Alg_Lfz_Sitze_Pax', StringType(), True), ColumnDefinition('Alg_Lfz_Werk_Nr', StringType(), True), ColumnDefinition('Alf_Lfz_Typ', StringType(), True), ColumnDefinition('Alg_Lfz_Baujahr', StringType(), True), ColumnDefinition('Alg_Lfz_Immatr', StringType(), True), ColumnDefinition('Alg_Lfz_MTOM', StringType(), True), ColumnDefinition('Alg_Lfz_Spez', StringType(), True), ColumnDefinition('Alg_Lfz_Sitze_Crew', StringType(), True), ColumnDefinition('Alg_Flottennummer', StringType(), True), ColumnDefinition('Alg_Lfz_Hersteller', StringType(), True), ColumnDefinition('Alg_Lfz_Typ', StringType(), True), ColumnDefinition('Alg_PurchasePriceProtDate', TimestampType(), True), ColumnDefinition('Alg_PurchasePriceProtPrice', DecimalType(18,2), True), ColumnDefinition('Alg_PurchasePriceProtType', BooleanType(), True), ColumnDefinition('Alg_RentShareVehicleProtection', BooleanType(), True), ColumnDefinition('Alg_CyberSafeShopAndSurf', BooleanType(), True), ColumnDefinition('Alg_HomeAssistance', BooleanType(), True)
])

raw_vault.create_satellite('SAT__USER', [
    ColumnDefinition('GAD_ID', StringType(), True), ColumnDefinition('Alg_MigrationLOB', StringType(), True), ColumnDefinition('ObfuscatedInternal', BooleanType(), True), ColumnDefinition('Alg_SelectedLOB', IntegerType(), True), ColumnDefinition('Alg_SelectedLossType', IntegerType(), True), ColumnDefinition('Alg_FiliationCode', StringType(), True), ColumnDefinition('JobTitle', StringType(), True), ColumnDefinition('ContactID', LongType(), True), ColumnDefinition('TimeZone', IntegerType(), True), ColumnDefinition('DefaultCountry', IntegerType(), True), ColumnDefinition('SystemUserType', IntegerType(), True), ColumnDefinition('isTechnical', BooleanType(), True), ColumnDefinition('CredentialID', LongType(), True), ColumnDefinition('Alg_LastLogin', TimestampType(), True), ColumnDefinition('QuickClaim', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Alg_StartDate', TimestampType(), True), ColumnDefinition('PolicyType', IntegerType(), True), ColumnDefinition('senderDefault', IntegerType(), True), ColumnDefinition('ValidationLevel', IntegerType(), True),
    ColumnDefinition('DefaultPhoneCountry', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('NewlyAssignedActivities', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Alg_EndDate', TimestampType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('AuthorityProfileID', LongType(), True), ColumnDefinition('LossType', IntegerType(), True), ColumnDefinition('Alg_IndividualSignature', BooleanType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Locale', IntegerType(), True), ColumnDefinition('ExperienceLevel', IntegerType(), True), ColumnDefinition('Language', IntegerType(), True), ColumnDefinition('ExternalUser', BooleanType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Department', StringType(), True), ColumnDefinition('Alg_PrinterId', StringType(), True), ColumnDefinition('Alg_VacationStatus', IntegerType(), True), ColumnDefinition('VacationStatus', IntegerType(), True), ColumnDefinition('OrganizationID', LongType(), True),
    ColumnDefinition('Alg_AcademicTitle', StringType(), True), ColumnDefinition('SessionTimeoutSecs', IntegerType(), True), ColumnDefinition('SpatialPointDenorm', StringType(), True), ColumnDefinition('UserSettingsID', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('OffsetStatsUpdateTime', TimestampType(), True), ColumnDefinition('LoadCommandID', LongType(), True)
])

raw_vault.create_satellite('SAT__CREDENTIAL', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('FailedAttempts', IntegerType(), True), ColumnDefinition('Active', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('FailedTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('UserNameDenorm', StringType(), True), ColumnDefinition('UserName', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('LockDate', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Password', StringType(), True), ColumnDefinition('Alg_DeactivatedDate', TimestampType(), True), ColumnDefinition('ObfuscatedInternal', BooleanType(), True)
])

raw_vault.create_satellite('SAT__ACCOUNT', [
    ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('AccountHolderID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Alg_AccountHolder', StringType(), True), ColumnDefinition('AccountNumber', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True)
])

raw_vault.create_satellite('SAT__ACCOUNTASSIGNMENT', [
    ColumnDefinition('GroupHack', LongType(), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('UserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('AccountSpecialHandlingID', LongType(), True), ColumnDefinition('UserHack', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Strategy', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('GroupID', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('PolicyNumber', StringType(), True), ColumnDefinition('LossType', IntegerType(), True)
])

raw_vault.create_satellite('SAT__SPECIALHANDLING', [
    ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('AccountID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('CustomerServiceTier', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True)
])

raw_vault.create_satellite('SAT__STRATEGY', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('StrategyType', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('GroupStrategy', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('alg_AccountAssignment', LongType(), True), ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__ACCOUNTMANAGEMENT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('IsConfirmationLetter', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('AccountID', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Responsibility', StringType(), True), ColumnDefinition('PaymentAddress', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('ECommunication', StringType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('IsSettlementLetter', BooleanType(), True), ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__ACCOUNTPOLICY', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('AccountID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('ProductDescription', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('PolicyNumber', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('Alg_ClaimCorrespondenceAddr', BooleanType(), True), ColumnDefinition('CollectiveDamagePolicy', BooleanType(), True), ColumnDefinition('IsBulkInvoiceApplicable', BooleanType(), True), ColumnDefinition('Alg_LossType', IntegerType(), True)
])

raw_vault.create_satellite('SAT__OTHERREFERENCES', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ExposureID', LongType(), True), ColumnDefinition('ReferenceType', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('PrimaryReference', BooleanType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('ReferenceNumber', StringType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('ServiceRequestID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('MatterID', LongType(), True), ColumnDefinition('ClaimContactID', LongType(), True)
])

raw_vault.create_satellite('SAT__LIABILITYCONCEPT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('LiabilityPercentage', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('AffectedRole', StringType(), True), ColumnDefinition('ResponsibleRole', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('LiabilityReason', StringType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('AffectedNameID', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('ResponsibleNameID', LongType(), True), ColumnDefinition('ResponsibleInsurerID', LongType(), True), ColumnDefinition('AffectedInsurerID', LongType(), True)
])

raw_vault.create_satellite('SAT__DEDUCTIBLE', [
    ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('CoverageID', LongType(), True), ColumnDefinition('Alg_DeductibleAmountFixed', DecimalType(18,2), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Paid', BooleanType(), True), ColumnDefinition('Alg_DeductibleMax', DecimalType(18,2), True), ColumnDefinition('EditReason', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Overridden', BooleanType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Waived', BooleanType(), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('Amount', DecimalType(18,2), True), ColumnDefinition('Alg_DeductibleMin', DecimalType(18,2), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Alg_Percentage', IntegerType(), True), ColumnDefinition('Alg_Notes', StringType(), True),
    ColumnDefinition('Alg_ApplicableDeductible', DecimalType(18,2), True), ColumnDefinition('Alg_SpecialTerm', DecimalType(18,2), True), ColumnDefinition('Alg_SanitatedDedNewDriver', DecimalType(18,2), True), ColumnDefinition('Alg_DeductibleNewDriver', DecimalType(18,2), True)
])

raw_vault.create_satellite('SAT__COVERAGETERMS', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('ModelRestriction', IntegerType(), True), ColumnDefinition('CoverageID', LongType(), True), ColumnDefinition('NumericValue', DecimalType(20,4), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Alg_Label', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('CovTermOrder', IntegerType(), True), ColumnDefinition('ModelAggregation', IntegerType(), True), ColumnDefinition('CovTermPattern', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('FinancialAmount', DecimalType(18,2), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('DateFormat', StringType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Alg_TermType', IntegerType(), True), ColumnDefinition('Code', StringType(), True), ColumnDefinition('Text', StringType(), True),
    ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Units', IntegerType(), True), ColumnDefinition('Alg_FinancialCurrency', IntegerType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('DateString', StringType(), True)
])

raw_vault.create_satellite('SAT__ENDORSEMENT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Coverage', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('isNotAsociatedWithClaim', BooleanType(), True), ColumnDefinition('FormNumber', StringType(), True), ColumnDefinition('PolicyID', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('RiskUnit', LongType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('EffectiveDate', TimestampType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('ExpirationDate', TimestampType(), True), ColumnDefinition('Alg_UserVariables', BooleanType(), True),
    ColumnDefinition('DescPerslLine', StringType(), True)
])

raw_vault.create_satellite('SAT__TERM', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('NumericValue', DecimalType(20,4), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('PolicyID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('FinancialCurrency', IntegerType(), True), ColumnDefinition('CovTermPattern', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('FinancialAmount', DecimalType(18,2), True), ColumnDefinition('TermOrder', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('DateFormat', StringType(), True), ColumnDefinition('TermType', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Text', StringType(), True), ColumnDefinition('Code', StringType(), True), ColumnDefinition('RiskUnitID', LongType(), True), ColumnDefinition('Units', IntegerType(), True),
    ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Label', StringType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('DateString', StringType(), True)
])

raw_vault.create_satellite('SAT__VEHICLESALVAGE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('SellDate', TimestampType(), True), ColumnDefinition('SellingPriceCHF', DecimalType(18,2), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('BuyingVAT', DecimalType(18,2), True), ColumnDefinition('VehicleFirstReg', TimestampType(), True), ColumnDefinition('TaxationPrinciple', IntegerType(), True), ColumnDefinition('BuyingPriceCHF', DecimalType(18,2), True), ColumnDefinition('PreTaxPrinciple', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Country', IntegerType(), True), ColumnDefinition('ClaimID', LongType(), True), ColumnDefinition('ExchangeBuyingAmount', DecimalType(18,2), True), ColumnDefinition('KeyType', StringType(), True), ColumnDefinition('SellingVAT', DecimalType(18,2), True), ColumnDefinition('SellingPriceCHFCurrency', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Vin', StringType(), True), ColumnDefinition('SellingCurrency', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True),
    ColumnDefinition('ExchangeSellingAmount', DecimalType(18,2), True), ColumnDefinition('BuyingPriceCHFCurrency', IntegerType(), True), ColumnDefinition('VehicleType', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('BuyingPrice', DecimalType(18,2), True), ColumnDefinition('BuyingCurrency', IntegerType(), True), ColumnDefinition('Make', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('BuyDate', TimestampType(), True), ColumnDefinition('LicensePlate', StringType(), True), ColumnDefinition('SellingPrice', DecimalType(18,2), True), ColumnDefinition('Type', StringType(), True), ColumnDefinition('SellingVATRatePct', StringType(), True), ColumnDefinition('BuyingVATRatePct', StringType(), True), ColumnDefinition('VehicleObjectType', IntegerType(), True)
])

raw_vault.create_satellite('SAT__TRIPITEM', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Assessment', IntegerType(), True), ColumnDefinition('BookingFrom', StringType(), True), ColumnDefinition('CostType', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('TripAccommodation', LongType(), True), ColumnDefinition('BookingPlace', StringType(), True), ColumnDefinition('DepartureDateOrTime', TimestampType(), True), ColumnDefinition('BookingType', IntegerType(), True), ColumnDefinition('BookingTo', StringType(), True), ColumnDefinition('AgentCurrency', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ArrivalDateOrTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('BookingNumber', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('ReasonForDenial', StringType(), True),
    ColumnDefinition('Fees', DecimalType(18,2), True), ColumnDefinition('TripSegment', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('AgentFees', DecimalType(18,2), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('TripIncident', LongType(), True), ColumnDefinition('FeesCurrency', IntegerType(), True), ColumnDefinition('TI_Agentfee_ExchangeDate', TimestampType(), True), ColumnDefinition('TI_Fee_ExchangeDate', TimestampType(), True), ColumnDefinition('IsItemPaid', BooleanType(), True), ColumnDefinition('TransLineItem', LongType(), True)
])

raw_vault.create_satellite('SAT__MOBILEPROPERTYITEM', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('OriginalPurchaseDate', TimestampType(), True), ColumnDefinition('EffectiveCostCurrency', IntegerType(), True), ColumnDefinition('EffectivCosts', DecimalType(18,2), True), ColumnDefinition('ItemDescription', StringType(), True), ColumnDefinition('IsDeath', BooleanType(), True), ColumnDefinition('Quantity', IntegerType(), True), ColumnDefinition('Alg_IsItemPaid', BooleanType(), True), ColumnDefinition('ReplacementComment', StringType(), True), ColumnDefinition('MonDemandedAmount', DecimalType(18,2), True), ColumnDefinition('SecondEstimateRepairman', LongType(), True), ColumnDefinition('CompensationType', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('RepairEstimateReceived', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('IsCession', BooleanType(), True), ColumnDefinition('RepairEstimatedCost', DecimalType(18,2), True), ColumnDefinition('OriginalCost', DecimalType(18,2), True), ColumnDefinition('RepairCurrency', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True),
    ColumnDefinition('ReplacementCurrency', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('IsThirdPartyDamaged', BooleanType(), True), ColumnDefinition('Repairman', LongType(), True), ColumnDefinition('SecondEstimateCurrency', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('OriginalCostCurrency', IntegerType(), True), ColumnDefinition('Alg_CategoryDamagedObject', IntegerType(), True), ColumnDefinition('Service', IntegerType(), True), ColumnDefinition('DamageItemType', IntegerType(), True), ColumnDefinition('IsSelected', BooleanType(), True), ColumnDefinition('IsEffCostUpdatedManually', BooleanType(), True), ColumnDefinition('MonDemandedCurrency', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Alg_TransLineItem', LongType(), True), ColumnDefinition('LimitCurrency', IntegerType(), True), ColumnDefinition('AlG_MobilePropertyIncident', LongType(), True), ColumnDefinition('ItemType', IntegerType(), True), ColumnDefinition('OriginalReceiptExisting', IntegerType(), True),
    ColumnDefinition('LimitAmount', DecimalType(8,2), True), ColumnDefinition('IsInsuredDamaged', BooleanType(), True), ColumnDefinition('IsDeductPretax', BooleanType(), True), ColumnDefinition('IsAlreadyRepaired', BooleanType(), True), ColumnDefinition('Comment', StringType(), True), ColumnDefinition('ReplacementCost', DecimalType(18,2), True), ColumnDefinition('OriginalCurrency', IntegerType(), True), ColumnDefinition('IsFixedAmount', BooleanType(), True), ColumnDefinition('EstimationConsidered', IntegerType(), True), ColumnDefinition('IsSecondEstimateRequired', BooleanType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('NoOfItems', DecimalType(5,2), True), ColumnDefinition('AgeOfItems', IntegerType(), True), ColumnDefinition('ReplacementAmount', DecimalType(18,2), True), ColumnDefinition('IsPaid', BooleanType(), True), ColumnDefinition('OriginalCostAmount', DecimalType(18,2), True), ColumnDefinition('SecondEstimateRepairCost', DecimalType(18,2), True), ColumnDefinition('DamageQuantity', IntegerType(), True), ColumnDefinition('IsTotalLoss', BooleanType(), True), ColumnDefinition('LastKnownRestoration', TimestampType(), True),
    ColumnDefinition('PathwayAge', IntegerType(), True), ColumnDefinition('PathwaysMaterial', IntegerType(), True), ColumnDefinition('AgeofPathway', IntegerType(), True), ColumnDefinition('PathwayType', IntegerType(), True), ColumnDefinition('DelayOnly', BooleanType(), True), ColumnDefinition('BaggageMissingFrom', TimestampType(), True), ColumnDefinition('BaggageRecoveredOn', TimestampType(), True), ColumnDefinition('EC_ExchangeDate', TimestampType(), True), ColumnDefinition('OC_ExchangeDate', TimestampType(), True), ColumnDefinition('Alg_ExternalID', StringType(), True), ColumnDefinition('DepreciationCurrency', IntegerType(), True), ColumnDefinition('DepreciationAmount', DecimalType(18,2), True), ColumnDefinition('ExternalID', StringType(), True), ColumnDefinition('ScalepointID', StringType(), True), ColumnDefinition('Settlement', LongType(), True), ColumnDefinition('DamageDescription', StringType(), True), ColumnDefinition('IsReductions', BooleanType(), True), ColumnDefinition('IsExclusions', BooleanType(), True), ColumnDefinition('IsCoverage', BooleanType(), True), ColumnDefinition('Place_City', StringType(), True),
    ColumnDefinition('Date', TimestampType(), True), ColumnDefinition('Participant', StringType(), True), ColumnDefinition('IsDeductible', BooleanType(), True), ColumnDefinition('DeductibleAmountFixed', DecimalType(18,2), True), ColumnDefinition('ReductionsDesc', StringType(), True), ColumnDefinition('ExclusionsDesc', StringType(), True), ColumnDefinition('CoverageDesc', StringType(), True)
])

raw_vault.create_satellite('SAT__MOBPROPCOSTDETAILS', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('IsRepairCost', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('IsOriginalCost', BooleanType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('IsAmortization', BooleanType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('MobilePropertyItem', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('IsReplacementCost', BooleanType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__SETTLEMENT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Type', IntegerType(), True), ColumnDefinition('ReplacementAmountCase', DecimalType(18,2), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('SubtotalAmount', DecimalType(18,2), True)
])

raw_vault.create_satellite('SAT__ICDCODE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('ExpiryDate', TimestampType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('Chronic', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('AvailabilityDate', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Code', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('BodySystem', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('CodeDesc', StringType(), True)
])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

raw_vault.create_satellite('SAT__INJURYDIAGNOSIS', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Alg_DiagnosisType', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('DateEnded', TimestampType(), True), ColumnDefinition('Alg_Median', IntegerType(), True), ColumnDefinition('CriticalPoint', IntegerType(), True), ColumnDefinition('DateStarted', TimestampType(), True), ColumnDefinition('Compensable', BooleanType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('IsPrimary', BooleanType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('Alg_TypeOfAccident', IntegerType(), True), ColumnDefinition('InjuryIncidentID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('TurningPoint', IntegerType(), True), ColumnDefinition('Alg_TypeOfIllness', IntegerType(), True), ColumnDefinition('Alg_IncapacityToWork', StringType(), True),
    ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('ICDCode', LongType(), True), ColumnDefinition('ContactID', LongType(), True)
])

raw_vault.create_satellite('SAT__WORKRESUMPTIONDETAILS', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('SuspendedFrom', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Alg_EmploymentID', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('ITW_Percent', IntegerType(), True), ColumnDefinition('WorkOfResumption', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ResumptionDate', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('TransLineItem', LongType(), True), ColumnDefinition('TypeOfDailyAllowance', IntegerType(), True), ColumnDefinition('Amount', DecimalType(18,2), True), ColumnDefinition('Alg_daSelected', BooleanType(), True), ColumnDefinition('Comment', StringType(), True), ColumnDefinition('InjuryIncident', LongType(), True),
    ColumnDefinition('Document', LongType(), True)
])

raw_vault.create_satellite('SAT__SALARYDATA', [
    ColumnDefinition('CHFSalarySum', DecimalType(18,2), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CHFSalaryAmount', DecimalType(18,2), True), ColumnDefinition('IsDailyRateOverridden', BooleanType(), True), ColumnDefinition('SalaryType', IntegerType(), True), ColumnDefinition('EmploymentID', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Alg_EmploymentID', LongType(), True), ColumnDefinition('CurrencyType', IntegerType(), True), ColumnDefinition('SalaryUnit', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Alg_FurtherEmployerID', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('OverrideHourlyRate', DecimalType(18,2), True), ColumnDefinition('isMainEmployer', BooleanType(), True), ColumnDefinition('SalaryAmount', DecimalType(18,2), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('SalaryPercentage', DecimalType(5,2), True), ColumnDefinition('validFrom', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True),
    ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('OverrideDailyRate', DecimalType(18,2), True), ColumnDefinition('IsHourlyRateOverridden', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('EvalCalculationSetID', LongType(), True), ColumnDefinition('IsMigrated', BooleanType(), True)
])

raw_vault.create_satellite('SAT__RELULOOKUP', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Effective_Date', TimestampType(), True), ColumnDefinition('Mental_Prog_G100_Orig', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Kat_We', StringType(), True), ColumnDefinition('Max_Treat_Duration', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Max_Treat_Duration_Num', IntegerType(), True), ColumnDefinition('SurgicalTreatment', StringType(), True), ColumnDefinition('Key_HG', StringType(), True), ColumnDefinition('ReLU_Price_Version', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('Physical_Prog_K050_Orig', IntegerType(), True), ColumnDefinition('InitalRELUCode', BooleanType(), True), ColumnDefinition('Kat_CH', IntegerType(), True), ColumnDefinition('DetailedInjury', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('ReluCode', StringType(), True),
    ColumnDefinition('ReLU_HK_Total', DecimalType(18,2), True), ColumnDefinition('ConservativeTreatment', StringType(), True), ColumnDefinition('InjuryLocation', StringType(), True), ColumnDefinition('Information', StringType(), True), ColumnDefinition('Mental_Prog_G050_Orig', IntegerType(), True), ColumnDefinition('Key_We', StringType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('InjuryType', StringType(), True), ColumnDefinition('Physiotherapy', StringType(), True), ColumnDefinition('Key_UG', StringType(), True), ColumnDefinition('TreatmentType', StringType(), True), ColumnDefinition('GrossInjury', StringType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('Submission_CM', StringType(), True), ColumnDefinition('Physical_Prog_K100_Orig', IntegerType(), True)
])

raw_vault.create_satellite('SAT__RELUCODE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('InjuryIncidentID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Activity', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('RELULookupID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__BODILYINJURYPOINTPEL', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('injuryIncidentID', LongType(), True), ColumnDefinition('injurySeverity', IntegerType(), True), ColumnDefinition('sunetFlag', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('bodilyInjuryComment', StringType(), True), ColumnDefinition('injuryPoint', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('specificBodyParts', IntegerType(), True), ColumnDefinition('bodilyInjuryTypeOfInjury', IntegerType(), True)
])

raw_vault.create_satellite('SAT__TREATMENT', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('LeavingDate', TimestampType(), True), ColumnDefinition('DoctorAdviceDate', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('InjuryIncident', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('TreatmentFinished', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Address', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Treatment', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Comment', StringType(), True), ColumnDefinition('Alg_DocHospContact', LongType(), True)
])

raw_vault.create_satellite('SAT__DISEASE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('injuryIncidentID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('DiseasePattern', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('DiseaseSeverity', IntegerType(), True), ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__PUREFINCOSTTYPE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('LossTypeCurrency', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('LossTypeAmount', DecimalType(18,2), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Alg_FinLossIncident', LongType(), True), ColumnDefinition('Alg_PurFinLossType', IntegerType(), True), ColumnDefinition('ExchangeDate', TimestampType(), True)
])

raw_vault.create_satellite('SAT__FINANCIALLOSSITEM', [
    ColumnDefinition('InformationCriminalprocess', StringType(), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('FunctionFrom', TimestampType(), True), ColumnDefinition('FactsAndCircumstances', StringType(), True), ColumnDefinition('InabilityToPay', TimestampType(), True), ColumnDefinition('EndDate', TimestampType(), True), ColumnDefinition('IsResponsible', BooleanType(), True), ColumnDefinition('LiabilityInPercentage', DecimalType(5,2), True), ColumnDefinition('SuretyBondSum', DecimalType(18,2), True), ColumnDefinition('StartDate', TimestampType(), True), ColumnDefinition('Currency', IntegerType(), True), ColumnDefinition('IsFirstDemand', BooleanType(), True), ColumnDefinition('FunctionType', IntegerType(), True), ColumnDefinition('AccusedPerson', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Alg_FinancialLossIncident', LongType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('ITProvider', LongType(), True), ColumnDefinition('SuretyBondSumCurrency', IntegerType(), True), ColumnDefinition('Contact', StringType(), True),
    ColumnDefinition('Cessionary', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('FunctionDescription', StringType(), True), ColumnDefinition('LiabilityAmount', DecimalType(18,2), True), ColumnDefinition('ValidFrom', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Causer', LongType(), True), ColumnDefinition('LiabilityAmtCurrency', IntegerType(), True), ColumnDefinition('obligator', LongType(), True), ColumnDefinition('IsPolicyHolderObligee', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('CancellationDate', TimestampType(), True), ColumnDefinition('Obligee', LongType(), True), ColumnDefinition('CivilLawer', LongType(), True), ColumnDefinition('ClientDebtor', LongType(), True), ColumnDefinition('Notes', StringType(), True), ColumnDefinition('IsCriminalProcess', BooleanType(), True), ColumnDefinition('GuaranteeSum', DecimalType(18,2), True), ColumnDefinition('Reason', StringType(), True),
    ColumnDefinition('IsSelected', BooleanType(), True), ColumnDefinition('Involved', LongType(), True), ColumnDefinition('IsJointAndSeveralLiability', BooleanType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('CriminalLawer', LongType(), True), ColumnDefinition('IsHoldsVoting', BooleanType(), True), ColumnDefinition('ProtectedDefault', TimestampType(), True), ColumnDefinition('FunctionTo', TimestampType(), True), ColumnDefinition('IsBackBond', BooleanType(), True), ColumnDefinition('CounterSurety', LongType(), True), ColumnDefinition('IsCivilProceedings', BooleanType(), True), ColumnDefinition('InformationCivilProceedings', StringType(), True), ColumnDefinition('RepresentedBy', LongType(), True), ColumnDefinition('PositionInformation', StringType(), True), ColumnDefinition('IsClaimsPaymentCessionary', BooleanType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Circumstances', StringType(), True), ColumnDefinition('Accusation', StringType(), True), ColumnDefinition('ValidTo', TimestampType(), True), ColumnDefinition('Subtype', IntegerType(), True),
    ColumnDefinition('LiabilityCause', StringType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('CI_ExchangeDate', TimestampType(), True), ColumnDefinition('WI_ExchangeDate', TimestampType(), True), ColumnDefinition('SB_ExchangeDate', TimestampType(), True), ColumnDefinition('LL_ExchangeRate', TimestampType(), True)
])

raw_vault.create_satellite('SAT__BIINCIDENTLOSSITEM', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('VariableCosts', DecimalType(18,2), True), ColumnDefinition('FixCosts', DecimalType(18,2), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Days', IntegerType(), True), ColumnDefinition('BusinessInterruptionIncident', LongType(), True), ColumnDefinition('DateFrom', TimestampType(), True), ColumnDefinition('VariableCostsCurrency', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Revenue', DecimalType(18,2), True), ColumnDefinition('BIPercent', IntegerType(), True), ColumnDefinition('Comment', StringType(), True), ColumnDefinition('FixCostsCurrency', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('IRGPCurrency', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('DateTo', TimestampType(), True), ColumnDefinition('Retired', LongType(), True),
    ColumnDefinition('IRGPAmount', DecimalType(18,2), True), ColumnDefinition('RevenueCurrency', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('BI_ExchangeDate', TimestampType(), True), ColumnDefinition('FC_ExchangeDate', TimestampType(), True), ColumnDefinition('VC_ExchangeDate', TimestampType(), True)
])

raw_vault.create_satellite('SAT__INCIDENTCESSION', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Cessionary', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('PureCreditInsurance', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Incident', LongType(), True), ColumnDefinition('ClaimContactRoleID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Comment', StringType(), True)
])
raw_vault.create_satellite('SAT__LOSSADJUSTERORDER', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CustomerContact', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('AdditionalInformation', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('InspectionLocation', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('LossAdjusterType', IntegerType(), True), ColumnDefinition('ReassignClaim', BooleanType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('RelatedIncident', LongType(), True)
])
raw_vault.create_satellite('SAT__OTHERINSURER', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('InjuryIncident', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('Type', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Benefits', BooleanType(), True)
])

raw_vault.create_satellite('SAT__VEHICLE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('OffRoadStyle', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('LoanMonthlyPayment', DecimalType(18,2), True), ColumnDefinition('LoanPayoffAmount', DecimalType(18,2), True), ColumnDefinition('Manufacturer', IntegerType(), True), ColumnDefinition('State', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('Vin', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('Color', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Model', StringType(), True), ColumnDefinition('Make', StringType(), True), ColumnDefinition('Alg_YearDate', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Alg_VehicleAddrLocation', StringType(), True),
    ColumnDefinition('Loan', BooleanType(), True), ColumnDefinition('Style', IntegerType(), True), ColumnDefinition('Alg_LicensePlateType', IntegerType(), True), ColumnDefinition('LoanMonthsRemaining', IntegerType(), True), ColumnDefinition('Alg_Country', IntegerType(), True), ColumnDefinition('LicensePlate', StringType(), True), ColumnDefinition('BoatType', IntegerType(), True), ColumnDefinition('PolicySystemId', StringType(), True), ColumnDefinition('Year', IntegerType(), True), ColumnDefinition('SerialNumber', StringType(), True), ColumnDefinition('Alg_CatalogPrice', DecimalType(18,2), True), ColumnDefinition('Alg_GrossWeight', IntegerType(), True), ColumnDefinition('Alg_ValidityOfDriversLicense', IntegerType(), True), ColumnDefinition('Alg_FuelType', IntegerType(), True), ColumnDefinition('Alg_FirstRegistrationDate', TimestampType(), True), ColumnDefinition('Alg_VehicleModel', IntegerType(), True), ColumnDefinition('Alg_VehicleObjectType', IntegerType(), True), ColumnDefinition('Alg_VehicleType', IntegerType(), True), ColumnDefinition('Alg_NumberOfSeats', IntegerType(), True), ColumnDefinition('Alg_SpecialVehicle', IntegerType(), True),
    ColumnDefinition('Alg_AccessoriesPrice', DecimalType(18,2), True), ColumnDefinition('Alg_EngineCubicCapacity', IntegerType(), True), ColumnDefinition('Alg_EmptyWeight', IntegerType(), True), ColumnDefinition('Alg_Use', IntegerType(), True), ColumnDefinition('Alg_EnginePowerInDINPS', IntegerType(), True), ColumnDefinition('Alg_Type', StringType(), True), ColumnDefinition('Alg_EurotaxKey', StringType(), True), ColumnDefinition('Alg_KeyType', StringType(), True), ColumnDefinition('Alg_FuelConsumption', DecimalType(5,2), True), ColumnDefinition('Alg_ThousandsKmsDrivenPerYear', DecimalType(8,2), True)
])

raw_vault.create_satellite('SAT__HANDLINGFEES', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('NetFeesNGF', DecimalType(18,2), True), ColumnDefinition('ChangedCurrency', IntegerType(), True), ColumnDefinition('AlreadyAppliedNGF', DecimalType(18,2), True), ColumnDefinition('IsNormalFeesChngd', BooleanType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('TotalPaidNVB', DecimalType(18,2), True), ColumnDefinition('IsSpecialFeesChngd', BooleanType(), True), ColumnDefinition('AppliedFeesSpecial', DecimalType(18,2), True), ColumnDefinition('NetFeesNVB', DecimalType(18,2), True), ColumnDefinition('ExchangeDate', TimestampType(), True), ColumnDefinition('AppliedFeesNormal', DecimalType(18,2), True), ColumnDefinition('RateFeeSpecial', BooleanType(), True), ColumnDefinition('CalculatedRecovery', DecimalType(18,2), True), ColumnDefinition('alreadyAppliedNVB', DecimalType(18,2), True), ColumnDefinition('Recovery', BooleanType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('VatFees', DecimalType(18,2), True), ColumnDefinition('IsRecoveryFeesChngd', BooleanType(), True), ColumnDefinition('SpecialMaxFee', BooleanType(), True),
    ColumnDefinition('ID', LongType(), True), ColumnDefinition('CalculatedNVB', DecimalType(18,2), True), ColumnDefinition('SpentHours', DecimalType(4,2), True), ColumnDefinition('Ceded', BooleanType(), True), ColumnDefinition('IsNetFeesChgd', BooleanType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('VATrateNGF', DecimalType(4,2), True), ColumnDefinition('CalculatedNormal', DecimalType(18,2), True), ColumnDefinition('IsAppFeesChgd', BooleanType(), True), ColumnDefinition('IsNetFeesChngd', BooleanType(), True), ColumnDefinition('TotalAppliedFees', DecimalType(18,2), True), ColumnDefinition('Normal', BooleanType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('SpecialCase', BooleanType(), True), ColumnDefinition('SpecialMinFee', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('AppliedFeesNVB', DecimalType(18,2), True), ColumnDefinition('VATrateNVB', DecimalType(4,2), True), ColumnDefinition('AppliedFeesCeded', DecimalType(18,2), True),
    ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('TotalFeesSum', DecimalType(18,2), True), ColumnDefinition('AppliedFeesRecovery', DecimalType(18,2), True), ColumnDefinition('IsAppliedFee', BooleanType(), True), ColumnDefinition('IsCededApplied', BooleanType(), True), ColumnDefinition('AppliedFeesCededNVB', DecimalType(18,2), True), ColumnDefinition('TotalAppliedFees_NVB', DecimalType(18,2), True), ColumnDefinition('TotalAppliedFees_NGF', DecimalType(18,2), True), ColumnDefinition('DefaultCurrency', IntegerType(), True)
])

raw_vault.create_satellite('SAT__CATASTROPHE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CatastropheValidFrom', TimestampType(), True), ColumnDefinition('ScheduleBatch', BooleanType(), True), ColumnDefinition('Active', BooleanType(), True), ColumnDefinition('TopLeftLatitude', DecimalType(7,5), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Name', StringType(), True), ColumnDefinition('PCSCatastropheNumber', StringType(), True), ColumnDefinition('TopLeftLongitude', DecimalType(8,5), True), ColumnDefinition('CatastropheValidTo', TimestampType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('PolicyEffectiveDate', TimestampType(), True), ColumnDefinition('Alg_CatastropheZoneType', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('PolicyRetrievalCompletionTime', TimestampType(), True), ColumnDefinition('CatastropheNumber', StringType(), True), ColumnDefinition('PolicyRetrievalSetTime', TimestampType(), True),
    ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Alg_Country', IntegerType(), True), ColumnDefinition('Comments', StringType(), True), ColumnDefinition('AssignedUserID', LongType(), True), ColumnDefinition('Type', IntegerType(), True), ColumnDefinition('Description', StringType(), True), ColumnDefinition('BottomRightLatitude', DecimalType(7,5), True), ColumnDefinition('BottomRightLongitude', DecimalType(8,5), True), ColumnDefinition('Alg_CoordinatorGroup', LongType(), True), ColumnDefinition('Alg_CoordinatorUser', LongType(), True), ColumnDefinition('Alg_IsMultipleCountry', BooleanType(), True)
])
raw_vault.create_satellite('SAT__CAUSERDETAIL', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('CauserInformation', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('CauserName', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('VehicularCategory', IntegerType(), True), ColumnDefinition('Subtype', IntegerType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('ExaminationDate', TimestampType(), True)
])

raw_vault.create_satellite('SAT__LCTFLEETQUESTIONAIRE', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('DriverStateFleet', IntegerType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('LossCauseFleet', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('DriverLiabilityFleet', IntegerType(), True), ColumnDefinition('EmpRelationshipFleet', IntegerType(), True), ColumnDefinition('LoadLossCauseFleet', IntegerType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('MovementVehicleFleet', IntegerType(), True), ColumnDefinition('RoadConditionFleet', IntegerType(), True), ColumnDefinition('RoadTypeFleet', IntegerType(), True), ColumnDefinition('VehicleUsage', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('LossEventFleet', IntegerType(), True), ColumnDefinition('VehicleDefectsFleet', IntegerType(), True), ColumnDefinition('DriverBehaviourFleet', IntegerType(), True), ColumnDefinition('LoadConditionFleet', IntegerType(), True),
    ColumnDefinition('ID', LongType(), True)
])

raw_vault.create_satellite('SAT__PRODUCTCODES', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('ProductCodeLabelEN', StringType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('ProductCodeLabelIT', StringType(), True), ColumnDefinition('Priority', IntegerType(), True), ColumnDefinition('ProductCodeLabelFR', StringType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('DocumentTextDE', StringType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('DocumentTextEN', StringType(), True), ColumnDefinition('DocumentTextIT', StringType(), True), ColumnDefinition('DocumentTextFR', StringType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ProductCodeLabelDE', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('ProductCode', StringType(), True)
])

raw_vault.create_satellite('SAT__NAMEDPERSON', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('Gender', IntegerType(), True), ColumnDefinition('NamedEmployeeReference', StringType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Salary', DecimalType(18,2), True), ColumnDefinition('Name', StringType(), True), ColumnDefinition('NamedEmployeeRestrictions', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('ProfessionalStatus', StringType(), True), ColumnDefinition('GroupID', LongType(), True), ColumnDefinition('DateOfBirth', TimestampType(), True), ColumnDefinition('Selected', BooleanType(), True), ColumnDefinition('SalaryCurrency', IntegerType(), True), ColumnDefinition('Surname', StringType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('EntryDate', TimestampType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('TypeofPerson', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True),
    ColumnDefinition('Retired', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('serialNumber', IntegerType(), True)
])

raw_vault.create_satellite('SAT__EMPLOYEMENTDATA', [
    ColumnDefinition('WageAmount', DecimalType(18,2), True), ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('OvertimeRate', DecimalType(18,2), True), ColumnDefinition('Alg_WorkResumed', BooleanType(), True), ColumnDefinition('Alg_LastWorkingTime', TimestampType(), True), ColumnDefinition('NumDaysWorked', DecimalType(2,1), True), ColumnDefinition('SSBenefitsAmnt', DecimalType(18,2), True), ColumnDefinition('WagePaymentCont', BooleanType(), True), ColumnDefinition('Alg_ProfessionalPosition', IntegerType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('WageAmountPostInjury', DecimalType(18,2), True), ColumnDefinition('InjuryStartTime', TimestampType(), True), ColumnDefinition('ID', LongType(), True), ColumnDefinition('SSBenefits', BooleanType(), True), ColumnDefinition('HireState', IntegerType(), True), ColumnDefinition('FirstPhoneCalldone', BooleanType(), True), ColumnDefinition('Alg_WorkAssignment', IntegerType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('HireDate', TimestampType(), True), ColumnDefinition('BeanVersion', IntegerType(), True),
    ColumnDefinition('ScndInjryFndAmnt', DecimalType(18,2), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('Alg_FurtherEmployment', BooleanType(), True), ColumnDefinition('FirstPhoneCallunknown', BooleanType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('Alg_WorkSuspendedFrom', TimestampType(), True), ColumnDefinition('Alg_WorkPlace', IntegerType(), True), ColumnDefinition('LastWorkedDate', TimestampType(), True), ColumnDefinition('ScndInjryFnd', BooleanType(), True), ColumnDefinition('LastYearIncome', DecimalType(18,2), True), ColumnDefinition('DepartmentCode', StringType(), True), ColumnDefinition('ClassCodeID', LongType(), True), ColumnDefinition('Alg_InjuryDescription', StringType(), True), ColumnDefinition('Alg_SpecialCase', IntegerType(), True), ColumnDefinition('Alg_DurationWorkIncapacity', IntegerType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('Occupation', StringType(), True), ColumnDefinition('Alg_EmployeeID', IntegerType(), True), ColumnDefinition('Alg_DegreeOfEmployment', IntegerType(), True), ColumnDefinition('Alg_ReasonForAbsenceDenorm', StringType(), True),
    ColumnDefinition('PaidFull', BooleanType(), True), ColumnDefinition('Alg_ReasonForAbsence', StringType(), True), ColumnDefinition('EmploymentStatus', IntegerType(), True), ColumnDefinition('ScndInjryFndDate', TimestampType(), True), ColumnDefinition('IncapacityToWorkInDays', IntegerType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('PayPeriod', IntegerType(), True), ColumnDefinition('Alg_LimitedOrTerminatedDate', TimestampType(), True), ColumnDefinition('Alg_IncapacityToWork', BooleanType(), True), ColumnDefinition('Alg_Employment', IntegerType(), True), ColumnDefinition('Alg_DateOfEmployment', TimestampType(), True), ColumnDefinition('NumHoursWorked', DecimalType(3,1), True), ColumnDefinition('SelfEmployed', BooleanType(), True), ColumnDefinition('DaysWorkedWeek', StringType(), True), ColumnDefinition('Alg_StdWorkingHrsPerWeek', DecimalType(5,2), True), ColumnDefinition('Alg_HrsWorkedPerWeek', DecimalType(5,2), True), ColumnDefinition('Alg_NonEmployed', IntegerType(), True)
])
raw_vault.create_satellite('SAT__FURTHEREMPLOYERS', [
    ColumnDefinition('LoadCommandID', LongType(), True), ColumnDefinition('CreateUserID', LongType(), True), ColumnDefinition('BeanVersion', IntegerType(), True), ColumnDefinition('LastWorkingTime', TimestampType(), True), ColumnDefinition('ArchivePartition', LongType(), True), ColumnDefinition('Retired', LongType(), True), ColumnDefinition('CreateTime', TimestampType(), True), ColumnDefinition('ResponsiblePerson', StringType(), True), ColumnDefinition('AccidentInsurance', LongType(), True), ColumnDefinition('UpdateUserID', LongType(), True), ColumnDefinition('DegreeOfEmployment', IntegerType(), True), ColumnDefinition('incident', LongType(), True), ColumnDefinition('UpdateTime', TimestampType(), True), ColumnDefinition('WorkHrsPerWeek', IntegerType(), True), ColumnDefinition('ID', LongType(), True)
])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Stage tables.
#
raw_vault.stage_table('cc_claim', 'cc_claim.parquet', ['PublicID'])
raw_vault.stage_table('cc_exposure', 'cc_exposure.parquet', ['PublicID'])
raw_vault.stage_table('cc_policy', 'cc_policy.parquet', ['PublicID'])
raw_vault.stage_table('cc_incident', 'cc_incident.parquet', ['PublicID'])
raw_vault.stage_table('cc_coverage', 'cc_coverage.parquet', ['PublicID'])
raw_vault.stage_table('cc_riskunit', 'cc_riskunit.parquet', ['PublicID'])
raw_vault.stage_table('cc_user', 'cc_user.parquet', ['PublicID'])
raw_vault.stage_table('cc_credential', 'cc_credential.parquet', ['PublicID'])
raw_vault.stage_table('cc_account', 'cc_account.parquet', ['PublicID'])

raw_vault.stage_table('ccx_alg_accountassignment', 'ccx_alg_accountassignment.parquet', ['PublicID'])
raw_vault.stage_table('cc_specialhandling', 'cc_specialhandling.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_strategy', 'ccx_alg_strategy.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_accountmanagement', 'ccx_alg_accountmanagement.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_accountpolicy', 'ccx_alg_accountpolicy.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_otherreferences', 'ccx_alg_otherreferences.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_liabilityconcept', 'ccx_alg_liabilityconcept.parquet', ['PublicID'])
raw_vault.stage_table('cc_deductible', 'cc_deductible.parquet', ['PublicID'])
raw_vault.stage_table('cc_coverageterms', 'cc_coverageterms.parquet', ['PublicID'])
raw_vault.stage_table('cc_endorsement', 'cc_endorsement.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_term', 'ccx_alg_term.parquet', ['PublicID'])

raw_vault.stage_table('ccx_alg_vehiclesalvage', 'ccx_alg_vehiclesalvage.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_tripitem', 'ccx_alg_tripitem.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_mobilepropertyitem', 'ccx_alg_mobilepropertyitem.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_mobpropcostdetails', 'ccx_alg_mobpropcostdetails.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_settlement', 'ccx_alg_settlement.parquet', ['PublicID'])
raw_vault.stage_table('cc_injurydiagnosis', 'cc_injurydiagnosis.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_workresumptiondetails', 'ccx_alg_workresumptiondetails.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_salarydata', 'ccx_alg_salarydata.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_RELUlookup', 'ccx_alg_RELUlookup.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_RELUcode', 'ccx_alg_RELUcode.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_bodilyInjuryPoint_pel', 'ccx_alg_bodilyInjuryPoint_pel.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_treatment', 'ccx_alg_treatment.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_purefincosttype', 'ccx_alg_purefincosttype.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_financiallossitem', 'ccx_alg_financiallossitem.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_biincidentlossitem', 'ccx_alg_biincidentlossitem.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_incidentcession', 'ccx_alg_incidentcession.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_lossadjusterorder', 'ccx_alg_lossadjusterorder.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_otherinsurer', 'ccx_alg_otherinsurer.parquet', ['PublicID'])

raw_vault.stage_table('cc_vehicle', 'cc_vehicle.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_handlingfees', 'ccx_alg_handlingfees.parquet', ['PublicID'])
raw_vault.stage_table('cc_catastrophe', 'cc_catastrophe.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_causerdetail', 'ccx_alg_causerdetail.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_lctfleetquestionaire', 'ccx_alg_lctfleetquestionaire.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_productcodes', 'ccx_Alg_productcodes.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_namedperson', 'ccx_alg_namedperson.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_employementdata', 'ccx_alg_employementdata.parquet', ['PublicID'])
raw_vault.stage_table('ccx_alg_furtheremployers', 'ccx_alg_furtheremployers.parquet', ['PublicID'])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Load Hubs
#

raw_vault.load_hub_from_prepared_staging_table(
    'cc_claim', 'HUB__CLAIM', ['PublicID'],
    [
        SatelliteDefinition('SAT__CLAIM', [
            'Alg_statusClaimProcess', 'Alg_RiskShieldStatus', 'Alg_IsScalePointJumpEligible', 'Alg_electroCasco', 'PriorLossDate', 'CountryOfLoss', 'Alg_OriginalRenteCSSNumber', 'Alg_IsRentCSS', 'Alg_Sanction', 'Alg_TitlisMasterView', 'Alg_IsSendEmail', 'Alg_MigrationSubStatus', 'Alg_IsRent87OpenSPAZCase', 'Alg_LegacyCiriardPolicyNumber', 'Alg_ClaimByUUID', 'vendorClaimIntakeSummary', 'Alg_ClaimViewLink', 'Alg_MigrationCompleteTime', 'Alg_LegacySystem', 'Alg_IsRent87',
            'Alg_OriginalClaimCreationDate', 'Alg_MigrationStatus', 'Alg_OriginalRent87number', 'Alg_NVBCourtesyDate', 'Alg_IsCededCompany', 'Alg_IsNCBWarning', 'Alg_ShouldIncludeReduction', 'Alg_InsuredCharcteristics', 'Alg_DriverUnknown', 'Alg_RapportExisting', 'Alg_CreatedBySystem', 'Alg_IsMigrated', 'Alg_OriginalClaimNumber', 'Alg_IsMigratedExposure', 'Alg_PaymentMigrationStatus', 'Alg_SendAutoCloseLetter', 'Alg_GenInvoiceDocFlag', 'Alg_AdditionalLossCause', 'DoNotDestroy', 'Alg_LCTFleetQuestionaire',
            'Alg_Fraud', 'Alg_IsScalepoint', 'Alg_RegisteredCountry', 'Alg_IsFirstAndFinal', 'IsUnverifiedCreatedFromUI', 'Alg_StopAutomaticPayment', 'Alg_MonitRecCurrency', 'Alg_MonitRecoveryAmount', 'Alg_GlassType', 'Alg_IPSVehicleType', 'Alg_IPSAgeOfDriver', 'Alg_IPSUsageVehicle', 'Alg_IPSEventType', 'Alg_IPSMovementVehicle', 'Alg_IPSBlameCode', 'Alg_IPSRoadType', 'Alg_VendorSourceSystem', 'Alg_UnderInsurance', 'Alg_DeductiblesExchangeDate', 'Alg_DeductibleComments',
            'Alg_HandlingFees', 'Alg_LossCauseDescription', 'Alg_NCBLastRelevantClaim', 'Alg_IPSDICDIL', 'Alg_NCBComment', 'Alg_IsQuickClaim', 'Alg_IPSContractType', 'Alg_NCBLastRelClmCloseDate', 'isPurgeReady', 'Alg_PeriodOfValidity', 'Alg_ProductCNP', 'Alg_QualificationNGF', 'Alg_CourtesyDate', 'Alg_CausingVehicleType', 'Alg_IsCededCase', 'Alg_CededDate', 'Alg_IsCourtesyClaim', 'Alg_InterclaimsGCode', 'Alg_FileDeletion', 'Alg_InterClaimsType',
            'Alg_NGForNVBClaim', 'Alg_IsServiceAffected', 'Alg_IsApplySanctions', 'Alg_IsComprehensiveSanctions', 'Alg_ProducingCountry', 'Alg_IPSClaimHandlingProtocol', 'Alg_ProgramName', 'Alg_CnpMode', 'Alg_SwissJournalID', 'Alg_IsClaimNoPolicy', 'Alg_HasTrip', 'Alg_AccidentProtocol', 'Alg_HasConstruction', 'Alg_HasInjury', 'Alg_HasVehicles', 'Alg_HasMachinesTechnical', 'Alg_HasBuilding', 'Alg_HasGoodsAndMovables', 'Alg_HasPureFinancialLoss', 'Alg_HasBusinessInt',
            'HazardousWaste', 'Alg_ClaimsMadeDate', 'Description', 'SIScore', 'Alg_IsTestingLCTASH', 'LargeLossNotificationStatus', 'AssignmentDate', 'Alg_IsPreventiveClmNotif', 'MMIdate', 'ClaimWorkCompID', 'Weather', 'CoverageInQuestion', 'DateCompDcsnMade', 'Alg_LastReopenedDate', 'Alg_LossLocAddressLine1', 'Alg_DataMismatchClaim', 'AgencyId', 'Alg_SubroPossibleCreatedByUser', 'ISOKnown', 'Alg_InterestInsured',
            'PreviousUserID', 'Alg_ClaimReopenReason', 'Alg_EarlyIntervention', 'ReinsuranceReportable', 'GroupSegment', 'Alg_LiabilityOrigin', 'PolicyID', 'AccidentType', 'CreateTime', 'BenefitsStatusDcsn', 'Alg_CloseClaim', 'LossLocationCode', 'StorageBarCodeNum', 'StorageVolumes', 'LocationCodeID', 'Alg_TranspDocDate', 'WeatherRelated', 'Alg_IsSegmentAssigned', 'Alg_RequiredPolicyRefreshFlag', 'DateRptdToInsured',
            'ConcurrentEmp', 'JurisdictionState', 'Alg_ResponsibleTeam', 'Alg_LossEventType', 'LOBCode', 'Alg_EstimatedLossDate', 'Alg_LossReason', 'Retired', 'Alg_AddresUnknown', 'Alg_ConveyanceCode', 'CloseDate', 'Alg_ReturnCallIPEnsued', 'StateAckNumber', 'ReopenedReason', 'Alg_LosslocCanton', 'Alg_IPSLOB', 'Alg_IPSNumber', 'HospitalDays', 'PoliceDeptInfo', 'SIUStatus',
            'SIEscalateSIUdate', 'Alg_OccupationalSafty', 'InjuredOnPremises', 'Alg_Elucidation', 'LoadCommandID', 'StatuteDate', 'Alg_MainContactChoiceType', 'Alg_CoverageInQuestion', 'AssignedUserID', 'Strategy', 'EmployerValidityReason', 'InjWkrInMPN', 'InsuredPremises', 'ISOReceiveDate', 'Alg_AcuteSpecificInjuries', 'Alg_LiabilityReasonPH', 'WorkloadUpdated', 'EmpSentMPNNotice', 'StorageLocationState', 'Alg_CarrierName',
            'SubrogationStatus', 'ComputerSecurity', 'SafetyEquipUsed', 'ClaimSource', 'SIEscalateSIU', 'Alg_CovInQnComment', 'LossDate', 'Alg_ManualStop', 'Alg_InvolvedObjects', 'Alg_IPSEmergingRisk', 'ClaimantRprtdDate', 'Alg_TransportTo', 'Alg_GNDeduction', 'Alg_ProductCode', 'EmpQusValidity', 'InsurerSentMPNNotice', 'Segment', 'EmploymentDataID', 'Alg_NextSteps', 'ModifiedDutyAvail',
            'Alg_ReopenClaimNote', 'Alg_DiagnosisDescription', 'Alg_ClaimSubState', 'Alg_SendToDownstream', 'BeanVersion', 'Alg_SpecialisationClaim', 'Alg_CurrStatus', 'LossCause', 'HowReported', 'Alg_ClaimOutsidePolicyPeriod', 'ExaminationDate', 'DateEligibleForArchive', 'FlaggedReason', 'ISOEnabled', 'Alg_ClaimOutsidePlcyPrdComment', 'ID', 'DrugsInvolved', 'StateFileNumber', 'UpdateTime', 'PreviousQueueID',
            'Alg_DuplicateClaim', 'DateRptdToEmployer', 'Alg_IsProfMalPracticeClaim', 'Alg_FreeMovementCase', 'AssignedByUserID', 'ExposureBegan', 'FlaggedDate', 'SafetyEquipProv', 'Alg_AcuteInjuriesDescription', 'Alg_DentalCare', 'PreviousGroupID', 'ReportedDate', 'Alg_ExpectedDateOfBirth', 'AssignmentStatus', 'Alg_IPSTypeOfLoss', 'Alg_PHKnowsDiagnosisUpdateTime', 'Alg_SubrogationPossible', 'UnattachedDocumentForFNOL_Ext', 'Alg_Organization', 'Alg_LossActivity',
            'LossLocationID', 'CurrentConditions', 'Alg_TransportFrom', 'LockingColumn', 'ShowMedicalFirstInfo', 'LossLocationSpatialDenorm', 'HospitalDate', 'Alg_PHKnowsDiagnosis', 'Alg_FilteringCoverages', 'Alg_LossPlace', 'LossType', 'PurgeDate', 'Alg_Recovery', 'FurtherTreatment', 'ManifestationDate', 'Alg_BenefitExpirationDate', 'Alg_LossControlTool', 'Alg_ProfMalPracticeClm', 'Alg_FlagClaimForSuva', 'ClaimantDenormID',
            'InjuredRegularJob', 'Alg_RegionForAssignment', 'CatastropheID', 'Alg_LossOccurenceDate', 'Alg_IsInjuredReached', 'Alg_LosslocCountry', 'Alg_AdjReserves', 'ISOSendDate', 'ISOStatus', 'Alg_SegmentationTrigger', 'Alg_IsPrintClosingDocument', 'WorkloadWeight', 'ValidationLevel', 'Alg_PercentLiability', 'StorageDate', 'FireDeptInfo', 'ReportedByType', 'Alg_BenefitExpiration', 'Alg_PolicyTypeForSearch', 'Alg_CollectiveDamage',
            'QueueAssignGroupID', 'Mold', 'FirstNoticeSuit', 'State', 'Alg_Statement', 'Alg_DateOfConversation', 'PreexDisblty', 'IncidentReport', 'Alg_IPSLawsuit', 'Alg_IsFlagClaimDossierSubmtd', 'ClaimNumber', 'DateFormRetByEmp', 'InsuredDenormID', 'TreatedPatientBfr', 'Alg_SuvaFlagRandom', 'Alg_CauserDetail', 'PTPinMPN', 'StorageCategory', 'Alg_OrganisationId', 'Alg_IsUserAutoAssigned',
            'PermissionRequired', 'Alg_LastCloseDate', 'Alg_NumberOfAttempts', 'Alg_TranspDocID', 'ArchivePartition', 'ClosedOutcome', 'EmploymentInjury', 'AssignedQueueID', 'Flagged', 'SalvageStatus', 'Alg_isBulkFnolClaim', 'SupplementalWorkloadWeight', 'Alg_LosslocPostalCode', 'Alg_CaseID', 'AssignedGroupID', 'Alg_DeductionReason', 'Alg_SunnetLossTime', 'Alg_AccidentCode', 'Alg_PolicyDocumentsFound', 'FaultRating',
            'DateCompDcsnDue', 'Alg_IsCatastropheNotApplicable', 'Alg_VerifiedDate', 'Alg_CloseClaimNote', 'StorageType', 'LocationOfTheft', 'Alg_RelapseCheck', 'UpdateUserID', 'Progress', 'ReOpenDate', 'ReinsuranceFlaggedStatus', 'Alg_IPSCauseofEvent', 'DeathDate', 'Fault', 'CreateUserID', 'ClaimTier', 'OtherRecovStatus', 'StorageBoxNum', 'Alg_FaultRatingModifiedDate', 'ExposureEnded',
            'LitigationStatus', 'Currency', 'DiagnosticCnsistnt', 'Alg_Diagnosis', 'DateRptdToAgent', 'Alg_FirstPhoneCallNotNeeded', 'Alg_FirstNotificationConfirm', 'SIULifeCycleState', 'Alg_SelectedCatastrophe', 'Alg_EarlyInterventionDate', 'Alg_LossLocCity', 'Alg_PercentDeduction', 'DateFormGivenToEmp', 'MainContactType'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_exposure', 'HUB__EXPOSURE', ['PublicID'],
    [
        SatelliteDefinition('SAT__EXPOSURE', [
            'PreviousGroupID', 'CoverageID', 'IncidentLimitReached', 'Locked', 'AssignedByUserID', 'OtherCovgChoice', 'DiagnosticCnsistnt', 'Currency', 'Alg_ExposureNumber', 'PIPClaimAggLimitReached', 'PreviousQueueID', 'UpdateTime', 'ID', 'Alg_LastTaxStmtRunTime', 'ExaminationDate', 'VocBenefitsID', 'LossCategory', 'CreateUserID', 'Alg_InitialDispositionDate', 'CompBenefitsID',
            'BeanVersion', 'LifePensionBenefitsID', 'ReOpenDate', 'Alg_isUninsuredExposure', 'ALg_CreateMedexpReserve', 'AverageWeeklyWages', 'Progress', 'UpdateUserID', 'Segment', 'LastDayWorked', 'Alg_TypeOfSegmentation', 'DepreciatedValue', 'SSBenefit', 'PPDBenefitsID', 'TPDBenefitsID', 'SecurityLevel', 'Alg_IsSelected', 'DeathBenefitsID', 'WCBenefit', 'AssignedGroupID',
            'SupplementalWorkloadWeight', 'WCPreexDisblty', 'ClaimID', 'Alg_SubjectToDeductible', 'AssignedQueueID', 'MetricLimitGeneration', 'StatLineID', 'WorkloadUpdated', 'BreakIn', 'TempLocationID', 'ClosedOutcome', 'ArchivePartition', 'Alg_IsCaseDebtEnfOffice', 'ExposureTier', 'ISOReceiveDate', 'IncidentID', 'NewEmpDataID', 'Alg_LastCloseDate', 'WageBenefit', 'Strategy',
            'AssignedUserID', 'Alg_IsUserAutoAssigned', 'SettleDate', 'Alg_ReopenExposureNote', 'Alg_ExpReopenReason', 'SSDIBenefitsID', 'LoadCommandID', 'WCPreexDisbltyInfo', 'TreatedPatientBfr', 'PriorEmpDataID', 'WageStmtSent', 'Alg_IsAutoCreated', 'PIPPersonAggLimitReached', 'Alg_CloseExposureNote', 'State', 'HospitalDays', 'CoverageSubType', 'ClaimOrder', 'Alg_GroupSegment', 'Alg_ExposureSubState',
            'Alg_CloseExposure', 'ReplacementValue', 'ReopenedReason', 'LossParty', 'WCBenefitsID', 'CloseDate', 'OtherCoverageInfo', 'Retired', 'ValidationLevel', 'WorkloadWeight', 'Alg_BeneficiaryCode', 'DaysInWeek', 'JurisdictionState', 'LostPropertyType', 'ISOStatus', 'PTDBenefitsID', 'ISOSendDate', 'TTDBenefitsID', 'SSDIEligible', 'ExposureLimitReached',
            'ExposureType', 'PIPNonMedAggLimitReached', 'ClaimantDenormID', 'WageStmtRecd', 'Alg_ExpectedDeductibleHistory', 'PIPESSLimitReached', 'RIAgreementGroupID', 'CreateTime', 'Alg_VZ4', 'Alg_ExpectedDeductible', 'RIGroupSetExternally', 'FurtherTreatment', 'PIPDeathBenefitsID', 'Alg_SUBBRC', 'RSBenefitsID', 'Alg_InitialReserveCreation', 'PreviousUserID', 'ISOKnown', 'Alg_LastReopenedDate', 'ClaimantType',
            'DisBenefitsID', 'HospitalDate', 'PrimaryCoverage', 'PIPVocBenefitsID', 'Alg_ManualSegmentation', 'Alg_WaitingPeriod', 'SettleMethod', 'CurrentConditions', 'Alg_VAC', 'OtherCoverage', 'ContactPermitted', 'AssignmentDate', 'AssignmentStatus', 'Alg_LifeCertDocDeliveryMethod', 'Alg_LifeCertReminderSentDate', 'Alg_IsInflationChangeDocSent', 'Alg_PensionTaxStmtRecipient', 'Alg_LifeCertDocSentDate', 'Alg_ConfOfEdDocSentDate', 'Alg_InflationChangeRecipient',
            'Alg_EduInstitution', 'Alg_IsLifeCertConfReceived', 'Alg_IsLifeCertReminderSent', 'Alg_PensionTaxDocSentDate', 'Alg_IsLifeCertDocSent', 'Alg_InflationChangeDocSentDate', 'Alg_IsConfOfEdDocSent', 'Alg_MaxSalChangeDocSentDate', 'Alg_IsPensionTaxDocSent', 'Alg_ExpGraduationDate', 'Alg_LifeCertConfReceivedDate', 'Alg_IsMaxSalChangeDocSent', 'Alg_ConfOfEdRecipient', 'Alg_LifeCertDocRecipient', 'Alg_MaxSalChangeRecipient', 'Alg_LifeCertReminderRecipient', 'Alg_PensionCase', 'Alg_scalepointID', 'Alg_PensionType', 'Alg_ExchangeDate',
            'Alg_DedExchangeRate', 'Alg_DeductibleAmount', 'Alg_CurrencyDeductible', 'Alg_DWHKontrahentNr', 'Alg_IsAutomaticPaid', 'Alg_Fraud', 'Alg_CreatedBySystem', 'Alg_AutoPaymentAutoClosed', 'Alg_IsMigratedExposure', 'Alg_ScalepointTimestamp', 'Alg_BousMalusChargePolicy', 'Alg_BonusMalusChargePolicy', 'Alg_MigrationPensionID', 'Alg_CiriardMigrationCoverID', 'Alg_MigPensBlockAutoPayUntil', 'Alg_IsMigPensMultipleRecipient', 'Alg_IsPensMultipleRecipient', 'Alg_PensionInitialReserve', 'Alg_PaidIndemnityExp', 'Alg_RemainingDedExp',
            'Alg_AlreadyAppliedDedExp', 'Alg_PaidExpenseExp', 'Alg_ScalepointCalledByQueue'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_policy', 'HUB__POLICY', ['PublicID'],
    [
        SatelliteDefinition('SAT__POLICY', [
            'LoadCommandID', 'PolicySuffix', 'Alg_PolicyVersion', 'AccountID', 'Alg_LanguageCode', 'Alg_CancellationReason', 'TotalVehicles', 'OtherInsInfo', 'Alg_TransportNumber', 'InsuredSICCode', 'Currency', 'UpdateTime', 'Alg_ExternalRUReference', 'ID', 'AssignedRisk', 'FinancialInterests', 'CreateUserID', 'ReturnToWorkPrgm', 'BeanVersion', 'Alg_BalanceOfOpenInvCurrency',
            'Retired', 'ValidationLevel', 'CoverageForm', 'UpdateUserID', 'Alg_BalanceOfOpenInvAmount', 'Alg_PolicyProductCode', 'Alg_LastPolicyRefresh', 'CancellationDate', 'OrigEffectiveDate', 'Alg_InsuredCompanyReference', 'Alg_Note', 'AccountNumber', 'Alg_GeneralConditionofInsuranc', 'ForeignCoverage', 'Notes', 'Participation', 'WCOtherStates', 'Verified', 'CreateTime', 'OtherInsurance',
            'Alg_CoInsurance', 'PolicySource', 'WCStates', 'RetroactiveDate', 'Alg_InsBySplAgreemnt', 'EffectiveDate', 'ReportingDate', 'ExpirationDate', 'ArchivePartition', 'Alg_LastPolicyChangeDate', 'PolicyType', 'UnderwritingCo', 'Alg_CustomerSegment', 'Status', 'Alg_ContactRole', 'Alg_AgencyCode', 'TotalProperties', 'PolicyRatingPlan', 'CustomerServiceTier', 'PolicyNumber',
            'UnderwritingGroup', 'PolicySystemPeriodID', 'ProducerCode', 'Alg_ConditionIssueDate', 'Alg_PolicyEndDate', 'Alg_SalePoint', 'Alg_AffinityID', 'Alg_IssueVersionDate', 'Alg_MovementEndDate', 'Alg_MovementStartDate', 'Alg_AffinityName', 'Alg_ClaimFlow', 'Alg_PolicySystem', 'Alg_ContactSystem', 'Alg_PolicySystemId', 'Alg_CommercialCode', 'Alg_PolicySourceSystem', 'Alg_MovementReason', 'Alg_SettlementRating', 'Alg_FrameworkContractNumber',
            'Alg_MovementType', 'Alg_ProductDescription', 'Alg_TechnicalProductCode', 'Alg_InCompletePolicy', 'Alg_BrokerType', 'Alg_ConditionIssue', 'Alg_AffBusinessSector'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_incident', 'HUB__INCIDENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__INCIDENT', [
            'Whichestimationconsidered', 'InsuredRevenueCurrency', 'EstimateCost_Sec', 'RecovInd', 'LocationInd', 'TotalLossPoints', 'MachineInstallationName', 'NumStories', 'AirbagsMissing', 'Currency', 'VehicleTitleRecvd', 'CarrierCompensated', 'ExtWallMat', 'Reportavailable', 'FireBurnDash', 'GeneralInjuryType', 'FireProtectionAvailable', 'RecovClassType', 'Alg_IsOwnerRetainVehicle', 'CreateUserID',
            'DriverRelation', 'FloorOwner', 'RentalAgency', 'SalvageProceeds', 'DebrisRemovalInd', 'UpdateUserID', 'LossArea', 'VehicleTitleReqd', 'AssessmentTargetCloseDate', 'FencesDamaged', 'BodyShopSelected', 'LossDesc', 'VehicleParked', 'FireBurnWindshield', 'VehicleDriveable', 'EquipmentFailure', 'NumberOfPeopleOnPolicy', 'ReturnToWorkValid', 'FloodSaltWater', 'TripRUID',
            'Reason', 'Alg_Hospitalization', 'Machine', 'Speed', 'BIIncidentNotes', 'AssessmentName', 'CitationIssued', 'SalvagePrep', 'SalvageTow', 'ClaimID', 'Amount', 'Impairment', 'InsuredGrossProfit', 'MovePermission', 'WaterLevelSeats', 'YearBuilt', 'Mileage100K', 'OperatingMode', 'VehicleType', 'TotalLoss',
            'ArchivePartition', 'Currency_Sec_Est', 'Operatinghours', 'TotalLoss_Machine', 'InsuredGrossProfitCurrency', 'Alg_MedicalControllable', 'StorageFclty', 'RecovState', 'BaggageType', 'AirbagsDeployed', 'MinorOnPolicy', 'DriverRelToOwner', 'OdomRead', 'Extrication', 'ParcelNumber', 'IsFNOLCapturing', 'SalvageNet', 'ReturnToModWorkActual', 'Leasing', 'IncludeContentLineItems',
            'LocationAddress', 'ConsClassType', 'IncludeLineItems', 'RecovDate', 'HitAndRun', 'CarrierCompensatedAmount', 'ClassType', 'ReturnToModWorkDate', 'AntiThftInd', 'StorageFeeAmt', 'CurrencyDamage', 'IsFullBI', 'PercentageDrivenByMinor', 'MedicalTreatmentType', 'Notes', 'RentalRequired', 'InsuredValue', 'FixedAmount', 'BaggageMissingFrom', 'RecovCondType',
            'VehiclePolStatus', 'VehicleAge5Years', 'RentalReserveNo', 'YearsInHome', 'ConstructionName', 'VehicleUseReason', 'EstRepairCost', 'VehicleLocation', 'BaggageRecoveredOn', 'InsuredIncreasedCOWCurrency', 'TypeOfRoof', 'RecoveryLocationID', 'Appraisal', 'ReturnToWorkDate', 'LocationMachine', 'Maintenancecontractexist', 'SalvageStorage', 'SalvageCompany', 'IsFNOLUnattachedData', 'AssessmentType',
            'Severity', 'OwnerRetainingSalvage', 'Category', 'DateVehicleRecovered', 'FireProtDetails', 'SprinkRetServ', 'DateSalvageAssigned', 'OccupancyType', 'LossOccured', 'SplitDamage', 'AlarmType', 'LossofUse', 'ClaimIncident', 'StartDate', 'SalvageTitle', 'Alg_UVGGArticle99', 'InspectionRequired', 'InsuranceType', 'UpdateTime', 'VehLockInd',
            'ID', 'DescOther', 'InternalUserID', 'AssessmentCloseDate', 'BeanVersion', 'MaterialsDamaged', 'EstimatesReceived', 'EstDamageType', 'Collision', 'Lastmaintenance_service', 'RentalBeginDate', 'EMSInd', 'AssessmentStatus', 'IsSeasonalBusiness', 'SecondEstimaterequired', 'BuildingValue', 'BuildingName', 'AlreadyRepaired', 'VehicleID', 'Alg_IsLeasing',
            'LossEstimate', 'StorageAccrInd', 'PropertySize', 'HazardInvolved', 'AmbulanceUsed', 'WhenToView', 'TotalLiabilityCurrency', 'AssessmentComment', 'IsFixedAmount', 'VehicleAge10Years', 'RentalEndDate', 'MealsDays', 'RelatedTripRUID', 'Alg_EmploymentDataID', 'Subtype', 'RepWhereDisInd', 'PhantomVehicle', 'LoadCommandID', 'DisabledDueToAccident', 'TotalCosts',
            'FireBurnEngine', 'Alg_ReRunRELK', 'ComponentsMissing', 'Alg_ReRunRELU', 'DateVehicleSold', 'TotalCostsCurrency', 'Alg_VehicleSalvageID', 'VehTowedInd', 'CollisionPoint', 'MealsPeople', 'ExtDamagetxt', 'NumSprinkler', 'SprinklerType', 'YearOfConstruction', 'AffdvCmplInd', 'LostWages', 'Retired', 'DelayOnly', 'PropertyID', 'InteriorMissing',
            'Alg_RecoveryCountry', 'VehicleSubmerged', 'InsuredAmount', 'TrafficViolation', 'CreateTime', 'ReturnToWorkActual', 'EstimatedDuration', 'Alg_VehicleLicenseType', 'InsuredAmountCurrency', 'InsuredIncreasedCOW', 'VehicleDirection', 'AppraisalFirstAppointment', 'VehicleOperable', 'RentalDailyRate', 'IsCivilProceedings', 'EstimationOfBuilding', 'InformationCivilProceedings', 'FinancialNotes', 'LotNumber', 'VehStolenInd',
            'WaterLevelDash', 'EstRepairTime', 'MoldInvolved', 'DetailedInjuryType', 'TotalLiabilityAmount', 'DamagedAreaSize', 'NumSprinkOper', 'PropertyDesc', 'VehicleACV', 'ActionsTaken', 'Alg_TotalSalary', 'OwnersPermission', 'MealsRate', 'InsuredRevenue', 'ReturnToModWorkValid', 'VehCondType', 'VehicleLossParty', 'RoofMaterial', 'Description', 'VehicleRollOver',
            'TripCategory', 'YearOfBuilt', 'TypeofBuilding', 'OwnerType', 'Alg_Object', 'MultiInsurance', 'TransportType', 'ProvisionalSumInsured', 'WallSafe', 'ChipNumber', 'MechanicalDevice', 'FireAlarm', 'RiskUnitID', 'FireSecurityDevice', 'FireSecurity', 'AlarmSystem', 'Alg_IsReplacementVehicle', 'LossParty', 'BIExchangeDate', 'TC_ExchangeDate',
            'TL_ExchangeDate', 'Alg_InjuredParty', 'Alg_IsZurRepairNetwork', 'Alg_RepairReason', 'Alg_RepairVendorID', 'Alg_ConcernedPerson', 'Alg_VendorDetails', 'Alg_LossEstRepBy', 'Alg_IndexNumber', 'LossEstimateCurrency', 'Alg_IsAutomativ', 'Alg_Fraud', 'Alg_IsZurPartnerNetwork', 'VignetteNumber', 'Alg_CreatedBySystem', 'Alg_LossPartyType', 'FreeDescription', 'DescriptionOfRisk', 'Alg_DocumentReceiverID', 'Alg_Garage',
            'MachinesDescription', 'Alg_objectListComplete', 'Alg_SourceClaimNumber', 'Alg_itemizationCaseRef', 'Alg_isSameAsReporter'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_coverage', 'HUB__COVERAGE', ['PublicID'],
    [
        SatelliteDefinition('SAT__COVERAGE', [
            'LoadCommandID', 'Notes', 'Alg_GDOProductNumber', 'CreateTime', 'PolicyID', 'ReplaceAggLimit', 'State', 'Currency', 'ExposureLimit', 'EffectiveDate', 'Deductible', 'CoverageBasis', 'ClaimAggLimit', 'Alg_AdditionalInformationTitle', 'UpdateTime', 'ID', 'ExpirationDate', 'Alg_AddtionalInformationDetail', 'CreateUserID', 'ArchivePartition',
            'BeanVersion', 'IncidentLimit', 'Retired', 'Coinsurance', 'RiskUnitID', 'PersonAggLimit', 'UpdateUserID', 'LimitsIndicator', 'NonmedAggLimit', 'Type', 'Subtype', 'PolicySystemId', 'Alg_VZ4', 'Alg_VOG', 'Alg_VA', 'Alg_SGF', 'Alg_VG', 'Alg_SGS', 'Alg_grossPremium', 'Alg_PolicyCoverOption',
            'Alg_PolicyCover', 'Alg_EPTNumberForCnp', 'Alg_TechnicalCoverCode', 'Alg_TariffPosition', 'Alg_TechnicalPremium'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_riskunit', 'HUB__RISKUNIT', ['PublicID'],
    [
        SatelliteDefinition('SAT__RISKUNIT', [
            'LoadCommandID', 'Alg_RUSerialNumber', 'EndDate', 'GroupName', 'Alg_RUCodeID', 'ConstructionYear', 'StartDate', 'TechnicalObjectValue', 'UpdateTime', 'OtherRiskType', 'Alg_ExternalRUReference', 'ID', 'Selected', 'Alg_RUCodeValue', 'Alg_RUCodeCode', 'Alg_RUCodeLabel', 'CreateUserID', 'BeanVersion', 'Retired', 'Alg_RUClassification',
            'UpdateUserID', 'TripDescription', 'Alg_TechnicalObject', 'PolicySystemId', 'VehicleLocationID', 'serialNumber', 'ClassCodeID', 'PolicyLocationID', 'Alg_TransportReference', 'VehicleID', 'CreateTime', 'RUNumber', 'PolicyID', 'BuildingID', 'Alg_ParcelNumber', 'GeographicalRegion', 'Alg_TransportDepartureCity', 'Alg_Selected', 'Alg_TransportArrivalCity', 'Alg_AddtionalInformationDetail',
            'Alg_WayOfTransport', 'ArchivePartition', 'Alg_AddtionalInformationTitle', 'Alg_TechnicalObjectSubRU', 'Alg_BuildingSubRU', 'Subtype', 'Description', 'ObjectSerialNumber', 'MechanicalSecurityDevice', 'AnimalName', 'Equipment', 'IndexYear', 'UnderInsuranceInEventOfClaim', 'ModelPlanesUp30Kg', 'ChipNumber', 'NumberOfDogs', 'NumberOfRooms', 'HeatingSystem', 'ConstructionRoof', 'Model',
            'IndexPoints', 'PropertyOwnerType', 'BuiltYear', 'AlarmSystem', 'AnimalSpecies', 'Alg_Object', 'FireSecurityDevice', 'Manufacturer', 'BuildingUse', 'Alg_Concerns', 'Alg_SubtypeObject', 'WallSafe', 'NumberOfAdults', 'NumberOfChildren', 'OwnerType', 'BuildingType', 'Breed', 'DogsCoverRequired', 'AdditionalCoInsuredPersons', 'Riskinspection',
            'Cessionnaire', 'Alg_InsuredObjectSequence', 'RiskReport', 'InventoryList', 'Alg_MiscDriver', 'Alg_HelpPointPlus', 'Alg_Transmission', 'Alg_AdditionalCoInsuredPersons', 'Alg_AppraisalDate', 'Alg_MaximumCompensation', 'ConstructionMethod', 'Alg_KeyType', 'Alg_LimitOfIndemnity', 'NumberWildAnimals', 'NumberOfFlats', 'Alg_LeasedVehicle', 'InsoulationGlass', 'Appraisal', 'Alg_MaximumVehicleAgeInYears', 'FreeDescription',
            'DescriptionOfRisk', 'Alg_InsuredPersonalCircle', 'EffectiveSumInsured', 'SumInsured', 'MoreThanTheGivenNumberOfRooms', 'DateOfLastEstimate', 'RiskLocationType', 'CustomerItemId', 'InsuredItemDescription', 'Alg_DWHRUSequenceForCnp', 'Alg_PurchasePriceProtection', 'Alg_NumberOfRisks', 'NumberOfTravelDays', 'Alg_SumInsuredPerRisk', 'NumberOfPeople', 'InsuredPersons', 'NumberOfVehicles', 'Alg_Lfz_Werk_Nummer', 'Alg_Lfz_Anz_Triebwerke', 'Alg_Lfz_Art',
            'Alg_Lfz_Sitze_Pax', 'Alg_Lfz_Werk_Nr', 'Alf_Lfz_Typ', 'Alg_Lfz_Baujahr', 'Alg_Lfz_Immatr', 'Alg_Lfz_MTOM', 'Alg_Lfz_Spez', 'Alg_Lfz_Sitze_Crew', 'Alg_Flottennummer', 'Alg_Lfz_Hersteller', 'Alg_Lfz_Typ', 'Alg_PurchasePriceProtDate', 'Alg_PurchasePriceProtPrice', 'Alg_PurchasePriceProtType', 'Alg_RentShareVehicleProtection', 'Alg_CyberSafeShopAndSurf', 'Alg_HomeAssistance'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_user', 'HUB__USER', ['PublicID'],
    [
        SatelliteDefinition('SAT__USER', [
            'GAD_ID', 'Alg_MigrationLOB', 'ObfuscatedInternal', 'Alg_SelectedLOB', 'Alg_SelectedLossType', 'Alg_FiliationCode', 'JobTitle', 'ContactID', 'TimeZone', 'DefaultCountry', 'SystemUserType', 'isTechnical', 'CredentialID', 'Alg_LastLogin', 'QuickClaim', 'UpdateUserID', 'Alg_StartDate', 'PolicyType', 'senderDefault', 'ValidationLevel',
            'DefaultPhoneCountry', 'Retired', 'NewlyAssignedActivities', 'BeanVersion', 'Alg_EndDate', 'CreateUserID', 'AuthorityProfileID', 'LossType', 'Alg_IndividualSignature', 'ID', 'Locale', 'ExperienceLevel', 'Language', 'ExternalUser', 'UpdateTime', 'Department', 'Alg_PrinterId', 'Alg_VacationStatus', 'VacationStatus', 'OrganizationID',
            'Alg_AcademicTitle', 'SessionTimeoutSecs', 'SpatialPointDenorm', 'UserSettingsID', 'CreateTime', 'OffsetStatsUpdateTime', 'LoadCommandID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_credential', 'HUB__CREDENTIAL', ['PublicID'],
    [
        SatelliteDefinition('SAT__CREDENTIAL', [
            'LoadCommandID', 'CreateUserID', 'FailedAttempts', 'Active', 'BeanVersion', 'Retired', 'CreateTime', 'FailedTime', 'UpdateUserID', 'UserNameDenorm', 'UserName', 'UpdateTime', 'LockDate', 'ID', 'Password', 'Alg_DeactivatedDate', 'ObfuscatedInternal'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_account', 'HUB__ACCOUNT', ['PublicID'],
    [
        SatelliteDefinition('SAT__ACCOUNT', [
            'CreateUserID', 'AccountHolderID', 'UpdateTime', 'Alg_AccountHolder', 'AccountNumber', 'BeanVersion', 'CreateTime', 'Retired', 'ID', 'UpdateUserID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_accountassignment', 'HUB__ACCOUNTASSIGNMENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__ACCOUNTASSIGNMENT', [
            'GroupHack', 'LoadCommandID', 'CreateUserID', 'UserID', 'BeanVersion', 'Retired', 'CreateTime', 'AccountSpecialHandlingID', 'UserHack', 'UpdateUserID', 'Strategy', 'UpdateTime', 'GroupID', 'ID', 'PolicyNumber', 'LossType'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_specialhandling', 'HUB__SPECIALHANDLING', ['PublicID'],
    [
        SatelliteDefinition('SAT__SPECIALHANDLING', [
            'CreateUserID', 'UpdateTime', 'AccountID', 'BeanVersion', 'CustomerServiceTier', 'CreateTime', 'Retired', 'Subtype', 'ID', 'UpdateUserID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_strategy', 'HUB__STRATEGY', ['PublicID'],
    [
        SatelliteDefinition('SAT__STRATEGY', [
            'LoadCommandID', 'CreateUserID', 'BeanVersion', 'Retired', 'CreateTime', 'StrategyType', 'UpdateUserID', 'GroupStrategy', 'UpdateTime', 'alg_AccountAssignment', 'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_accountmanagement', 'HUB__ACCOUNTMANAGEMENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__ACCOUNTMANAGEMENT', [
            'LoadCommandID', 'CreateUserID', 'IsConfirmationLetter', 'BeanVersion', 'AccountID', 'Retired', 'CreateTime', 'Responsibility', 'PaymentAddress', 'UpdateUserID', 'ECommunication', 'Comments', 'UpdateTime', 'IsSettlementLetter', 'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_accountpolicy', 'HUB__ACCOUNTPOLICY', ['PublicID'],
    [
        SatelliteDefinition('SAT__ACCOUNTPOLICY', [
            'LoadCommandID', 'CreateUserID', 'AccountID', 'BeanVersion', 'Retired', 'CreateTime', 'ProductDescription', 'UpdateUserID', 'UpdateTime', 'PolicyNumber', 'ID', 'Description', 'Alg_ClaimCorrespondenceAddr', 'CollectiveDamagePolicy', 'IsBulkInvoiceApplicable', 'Alg_LossType'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_otherreferences', 'HUB__OTHERREFERENCES', ['PublicID'],
    [
        SatelliteDefinition('SAT__OTHERREFERENCES', [
            'LoadCommandID', 'CreateUserID', 'ExposureID', 'ReferenceType', 'BeanVersion', 'ArchivePartition', 'CreateTime', 'PrimaryReference', 'Retired', 'UpdateUserID', 'ReferenceNumber', 'Comments', 'ServiceRequestID', 'UpdateTime', 'ClaimID', 'ID', 'MatterID', 'ClaimContactID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_liabilityconcept', 'HUB__LIABILITYCONCEPT', ['PublicID'],
    [
        SatelliteDefinition('SAT__LIABILITYCONCEPT', [
            'LoadCommandID', 'CreateUserID', 'LiabilityPercentage', 'BeanVersion', 'ArchivePartition', 'CreateTime', 'Retired', 'AffectedRole', 'ResponsibleRole', 'UpdateUserID', 'LiabilityReason', 'Comments', 'UpdateTime', 'ClaimID', 'AffectedNameID', 'ID', 'ResponsibleNameID', 'ResponsibleInsurerID', 'AffectedInsurerID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_deductible', 'HUB__DEDUCTIBLE', ['PublicID'],
    [
        SatelliteDefinition('SAT__DEDUCTIBLE', [
            'CreateUserID', 'CoverageID', 'Alg_DeductibleAmountFixed', 'BeanVersion', 'ArchivePartition', 'CreateTime', 'Retired', 'Paid', 'Alg_DeductibleMax', 'EditReason', 'UpdateUserID', 'Overridden', 'UpdateTime', 'Waived', 'ClaimID', 'Amount', 'Alg_DeductibleMin', 'ID', 'Alg_Percentage', 'Alg_Notes',
            'Alg_ApplicableDeductible', 'Alg_SpecialTerm', 'Alg_SanitatedDedNewDriver', 'Alg_DeductibleNewDriver'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_coverageterms', 'HUB__COVERAGETERMS', ['PublicID'],
    [
        SatelliteDefinition('SAT__COVERAGETERMS', [
            'LoadCommandID', 'ModelRestriction', 'CoverageID', 'NumericValue', 'CreateTime', 'Alg_Label', 'UpdateTime', 'CovTermOrder', 'ModelAggregation', 'CovTermPattern', 'ID', 'FinancialAmount', 'CreateUserID', 'DateFormat', 'ArchivePartition', 'BeanVersion', 'Retired', 'Alg_TermType', 'Code', 'Text',
            'UpdateUserID', 'Units', 'Alg_FinancialCurrency', 'Subtype', 'Description', 'PolicySystemId', 'DateString'
        ])
    ])


raw_vault.load_hub_from_prepared_staging_table(
    'cc_endorsement', 'HUB__ENDORSEMENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__ENDORSEMENT', [
            'LoadCommandID', 'CreateUserID', 'BeanVersion', 'Coverage', 'ArchivePartition', 'CreateTime', 'Retired', 'isNotAsociatedWithClaim', 'FormNumber', 'PolicyID', 'UpdateUserID', 'RiskUnit', 'Comments', 'EffectiveDate', 'UpdateTime', 'ID', 'Description', 'PolicySystemId', 'ExpirationDate', 'Alg_UserVariables',
            'DescPerslLine'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_term', 'HUB__TERM', ['PublicID'],
    [
        SatelliteDefinition('SAT__TERM', [
            'LoadCommandID', 'NumericValue', 'CreateTime', 'PolicyID', 'UpdateTime', 'FinancialCurrency', 'CovTermPattern', 'ID', 'FinancialAmount', 'TermOrder', 'CreateUserID', 'DateFormat', 'TermType', 'ArchivePartition', 'BeanVersion', 'Retired', 'Text', 'Code', 'RiskUnitID', 'Units',
            'UpdateUserID', 'Label', 'Subtype', 'PolicySystemId', 'Description', 'DateString'
        ])
    ])


raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_vehiclesalvage', 'HUB__VEHICLESALVAGE', ['PublicID'],
    [
        SatelliteDefinition('SAT__VEHICLESALVAGE', [
            'LoadCommandID', 'SellDate', 'SellingPriceCHF', 'CreateTime', 'BuyingVAT', 'VehicleFirstReg', 'TaxationPrinciple', 'BuyingPriceCHF', 'PreTaxPrinciple', 'UpdateTime', 'Country', 'ClaimID', 'ExchangeBuyingAmount', 'KeyType', 'SellingVAT', 'SellingPriceCHFCurrency', 'ID', 'Vin', 'SellingCurrency', 'CreateUserID',
            'ExchangeSellingAmount', 'BuyingPriceCHFCurrency', 'VehicleType', 'BeanVersion', 'ArchivePartition', 'Retired', 'BuyingPrice', 'BuyingCurrency', 'Make', 'UpdateUserID', 'BuyDate', 'LicensePlate', 'SellingPrice', 'Type', 'SellingVATRatePct', 'BuyingVATRatePct', 'VehicleObjectType'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_tripitem', 'HUB__TRIPITEM', ['PublicID'],
    [
        SatelliteDefinition('SAT__TRIPITEM', [
            'LoadCommandID', 'Assessment', 'BookingFrom', 'CostType', 'CreateTime', 'TripAccommodation', 'BookingPlace', 'DepartureDateOrTime', 'BookingType', 'BookingTo', 'AgentCurrency', 'UpdateTime', 'ArrivalDateOrTime', 'ID', 'BookingNumber', 'CreateUserID', 'BeanVersion', 'ArchivePartition', 'Retired', 'ReasonForDenial',
            'Fees', 'TripSegment', 'UpdateUserID', 'AgentFees', 'Comments', 'TripIncident', 'FeesCurrency', 'TI_Agentfee_ExchangeDate', 'TI_Fee_ExchangeDate', 'IsItemPaid', 'TransLineItem'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_mobilepropertyitem', 'HUB__MOBILEPROPERTYITEM', ['PublicID'],
    [
        SatelliteDefinition('SAT__MOBILEPROPERTYITEM', [
            'LoadCommandID', 'OriginalPurchaseDate', 'EffectiveCostCurrency', 'EffectivCosts', 'ItemDescription', 'IsDeath', 'Quantity', 'Alg_IsItemPaid', 'ReplacementComment', 'MonDemandedAmount', 'SecondEstimateRepairman', 'CompensationType', 'UpdateTime', 'RepairEstimateReceived', 'ID', 'IsCession', 'RepairEstimatedCost', 'OriginalCost', 'RepairCurrency', 'CreateUserID',
            'ReplacementCurrency', 'BeanVersion', 'Retired', 'IsThirdPartyDamaged', 'Repairman', 'SecondEstimateCurrency', 'UpdateUserID', 'OriginalCostCurrency', 'Alg_CategoryDamagedObject', 'Service', 'DamageItemType', 'IsSelected', 'IsEffCostUpdatedManually', 'MonDemandedCurrency', 'CreateTime', 'Alg_TransLineItem', 'LimitCurrency', 'AlG_MobilePropertyIncident', 'ItemType', 'OriginalReceiptExisting',
            'LimitAmount', 'IsInsuredDamaged', 'IsDeductPretax', 'IsAlreadyRepaired', 'Comment', 'ReplacementCost', 'OriginalCurrency', 'IsFixedAmount', 'EstimationConsidered', 'IsSecondEstimateRequired', 'ArchivePartition', 'NoOfItems', 'AgeOfItems', 'ReplacementAmount', 'IsPaid', 'OriginalCostAmount', 'SecondEstimateRepairCost', 'DamageQuantity', 'IsTotalLoss', 'LastKnownRestoration',
            'PathwayAge', 'PathwaysMaterial', 'AgeofPathway', 'PathwayType', 'DelayOnly', 'BaggageMissingFrom', 'BaggageRecoveredOn', 'EC_ExchangeDate', 'OC_ExchangeDate', 'Alg_ExternalID', 'DepreciationCurrency', 'DepreciationAmount', 'ExternalID', 'ScalepointID', 'Settlement', 'DamageDescription', 'IsReductions', 'IsExclusions', 'IsCoverage', 'Place_City',
            'Date', 'Participant', 'IsDeductible', 'DeductibleAmountFixed', 'ReductionsDesc', 'ExclusionsDesc', 'CoverageDesc'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_mobpropcostdetails', 'HUB__MOBPROPCOSTDETAILS', ['PublicID'],
    [
        SatelliteDefinition('SAT__MOBPROPCOSTDETAILS', [
            'LoadCommandID', 'CreateUserID', 'IsRepairCost', 'BeanVersion', 'IsOriginalCost', 'ArchivePartition', 'IsAmortization', 'Retired', 'CreateTime', 'MobilePropertyItem', 'UpdateUserID', 'IsReplacementCost', 'UpdateTime', 'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_settlement', 'HUB__SETTLEMENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__SETTLEMENT', [
            'LoadCommandID', 'CreateUserID', 'BeanVersion', 'Retired', 'CreateTime', 'UpdateUserID', 'UpdateTime', 'Type', 'ReplacementAmountCase', 'ID', 'SubtotalAmount'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_icdcode', 'HUB__ICDCODE', ['PublicID'],
    [
        SatelliteDefinition('SAT__ICDCODE', [
            'LoadCommandID', 'ExpiryDate', 'CreateUserID', 'Chronic', 'BeanVersion', 'AvailabilityDate', 'Retired', 'CreateTime', 'Code', 'UpdateUserID', 'BodySystem', 'UpdateTime', 'ID', 'CodeDesc'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_injurydiagnosis', 'HUB__INJURYDIAGNOSIS', ['PublicID'],
    [
        SatelliteDefinition('SAT__INJURYDIAGNOSIS', [
            'LoadCommandID', 'Alg_DiagnosisType', 'CreateTime', 'DateEnded', 'Alg_Median', 'CriticalPoint', 'DateStarted', 'Compensable', 'UpdateTime', 'IsPrimary', 'ID', 'CreateUserID', 'Alg_TypeOfAccident', 'InjuryIncidentID', 'ArchivePartition', 'BeanVersion', 'Retired', 'TurningPoint', 'Alg_TypeOfIllness', 'Alg_IncapacityToWork',
            'UpdateUserID', 'Comments', 'ICDCode', 'ContactID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_workresumptiondetails', 'HUB__WORKRESUMPTIONDETAILS', ['PublicID'],
    [
        SatelliteDefinition('SAT__WORKRESUMPTIONDETAILS', [
            'LoadCommandID', 'CreateUserID', 'SuspendedFrom', 'BeanVersion', 'ArchivePartition', 'Retired', 'CreateTime', 'Alg_EmploymentID', 'UpdateUserID', 'ITW_Percent', 'WorkOfResumption', 'UpdateTime', 'ResumptionDate', 'ID', 'TransLineItem', 'TypeOfDailyAllowance', 'Amount', 'Alg_daSelected', 'Comment', 'InjuryIncident',
            'Document'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_salarydata', 'HUB__SALARYDATA', ['PublicID'],
    [
        SatelliteDefinition('SAT__SALARYDATA', [
            'CHFSalarySum', 'LoadCommandID', 'CHFSalaryAmount', 'IsDailyRateOverridden', 'SalaryType', 'EmploymentID', 'CreateTime', 'Alg_EmploymentID', 'CurrencyType', 'SalaryUnit', 'UpdateTime', 'Alg_FurtherEmployerID', 'ID', 'OverrideHourlyRate', 'isMainEmployer', 'SalaryAmount', 'CreateUserID', 'SalaryPercentage', 'validFrom', 'BeanVersion',
            'ArchivePartition', 'Retired', 'OverrideDailyRate', 'IsHourlyRateOverridden', 'UpdateUserID', 'EvalCalculationSetID', 'IsMigrated'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_RELUlookup', 'HUB__RELULOOKUP', ['PublicID'],
    [
        SatelliteDefinition('SAT__RELULOOKUP', [
            'LoadCommandID', 'Effective_Date', 'Mental_Prog_G100_Orig', 'CreateTime', 'Kat_We', 'Max_Treat_Duration', 'UpdateTime', 'ID', 'Max_Treat_Duration_Num', 'SurgicalTreatment', 'Key_HG', 'ReLU_Price_Version', 'CreateUserID', 'Physical_Prog_K050_Orig', 'InitalRELUCode', 'Kat_CH', 'DetailedInjury', 'BeanVersion', 'Retired', 'ReluCode',
            'ReLU_HK_Total', 'ConservativeTreatment', 'InjuryLocation', 'Information', 'Mental_Prog_G050_Orig', 'Key_We', 'UpdateUserID', 'InjuryType', 'Physiotherapy', 'Key_UG', 'TreatmentType', 'GrossInjury', 'Description', 'Submission_CM', 'Physical_Prog_K100_Orig'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_RELUcode', 'HUB__RELUCODE', ['PublicID'],
    [
        SatelliteDefinition('SAT__RELUCODE', [
            'LoadCommandID', 'CreateUserID', 'InjuryIncidentID', 'ArchivePartition', 'BeanVersion', 'Retired', 'CreateTime', 'Activity', 'UpdateUserID', 'RELULookupID', 'UpdateTime', 'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_bodilyInjuryPoint_pel', 'HUB__BODILYINJURYPOINTPEL', ['PublicID'],
    [
        SatelliteDefinition('SAT__BODILYINJURYPOINTPEL', [
            'LoadCommandID', 'injuryIncidentID', 'injurySeverity', 'sunetFlag', 'BeanVersion', 'ArchivePartition', 'bodilyInjuryComment', 'injuryPoint', 'ID', 'specificBodyParts', 'bodilyInjuryTypeOfInjury'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_treatment', 'HUB__TREATMENT', ['PublicID'],
    [
        SatelliteDefinition('SAT__TREATMENT', [
            'LoadCommandID', 'CreateUserID', 'LeavingDate', 'DoctorAdviceDate', 'BeanVersion', 'ArchivePartition', 'InjuryIncident', 'Retired', 'CreateTime', 'TreatmentFinished', 'UpdateUserID', 'Address', 'UpdateTime', 'Treatment', 'ID', 'Comment', 'Alg_DocHospContact'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_disease', 'HUB__DISEASE', ['PublicID'],
    [
        SatelliteDefinition('SAT__DISEASE', [
            'LoadCommandID', 'CreateUserID', 'injuryIncidentID', 'ArchivePartition', 'BeanVersion', 'Retired', 'CreateTime', 'UpdateUserID', 'Comments', 'DiseasePattern', 'UpdateTime', 'DiseaseSeverity', 'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_purefincosttype', 'HUB__PUREFINCOSTTYPE', ['PublicID'],
    [
        SatelliteDefinition('SAT__PUREFINCOSTTYPE', [
            'LoadCommandID', 'CreateUserID', 'ArchivePartition', 'BeanVersion', 'Retired', 'CreateTime', 'LossTypeCurrency', 'UpdateUserID', 'LossTypeAmount', 'UpdateTime', 'ID', 'Alg_FinLossIncident', 'Alg_PurFinLossType', 'ExchangeDate'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_financiallossitem', 'HUB__FINANCIALLOSSITEM', ['PublicID'],
    [
        SatelliteDefinition('SAT__FINANCIALLOSSITEM', [
            'InformationCriminalprocess', 'LoadCommandID', 'FunctionFrom', 'FactsAndCircumstances', 'InabilityToPay', 'EndDate', 'IsResponsible', 'LiabilityInPercentage', 'SuretyBondSum', 'StartDate', 'Currency', 'IsFirstDemand', 'FunctionType', 'AccusedPerson', 'UpdateTime', 'Alg_FinancialLossIncident', 'ID', 'ITProvider', 'SuretyBondSumCurrency', 'Contact',
            'Cessionary', 'CreateUserID', 'FunctionDescription', 'LiabilityAmount', 'ValidFrom', 'BeanVersion', 'Retired', 'Causer', 'LiabilityAmtCurrency', 'obligator', 'IsPolicyHolderObligee', 'UpdateUserID', 'CancellationDate', 'Obligee', 'CivilLawer', 'ClientDebtor', 'Notes', 'IsCriminalProcess', 'GuaranteeSum', 'Reason',
            'IsSelected', 'Involved', 'IsJointAndSeveralLiability', 'CreateTime', 'CriminalLawer', 'IsHoldsVoting', 'ProtectedDefault', 'FunctionTo', 'IsBackBond', 'CounterSurety', 'IsCivilProceedings', 'InformationCivilProceedings', 'RepresentedBy', 'PositionInformation', 'IsClaimsPaymentCessionary', 'ArchivePartition', 'Circumstances', 'Accusation', 'ValidTo', 'Subtype',
            'LiabilityCause', 'Description', 'CI_ExchangeDate', 'WI_ExchangeDate', 'SB_ExchangeDate', 'LL_ExchangeRate'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_biincidentlossitem', 'HUB__BIINCIDENTLOSSITEM', ['PublicID'],
    [
        SatelliteDefinition('SAT__BIINCIDENTLOSSITEM', [
            'LoadCommandID', 'CreateTime', 'VariableCosts', 'FixCosts', 'UpdateTime', 'Days', 'BusinessInterruptionIncident', 'DateFrom', 'VariableCostsCurrency', 'ID', 'Revenue', 'BIPercent', 'Comment', 'FixCostsCurrency', 'CreateUserID', 'IRGPCurrency', 'ArchivePartition', 'BeanVersion', 'DateTo', 'Retired',
            'IRGPAmount', 'RevenueCurrency', 'UpdateUserID', 'Subtype', 'BI_ExchangeDate', 'FC_ExchangeDate', 'VC_ExchangeDate'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_incidentcession', 'HUB__INCIDENTCESSION', ['PublicID'],
    [
        SatelliteDefinition('SAT__INCIDENTCESSION', [
            'LoadCommandID', 'Cessionary', 'CreateUserID', 'BeanVersion', 'ArchivePartition', 'PureCreditInsurance', 'Retired', 'CreateTime', 'UpdateUserID', 'Incident', 'ClaimContactRoleID', 'UpdateTime', 'ID', 'Comment'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_lossadjusterorder', 'HUB__LOSSADJUSTERORDER', ['PublicID'],
    [
        SatelliteDefinition('SAT__LOSSADJUSTERORDER', [
            'LoadCommandID', 'CustomerContact', 'CreateUserID', 'AdditionalInformation', 'BeanVersion', 'ArchivePartition', 'InspectionLocation', 'Retired', 'CreateTime', 'UpdateUserID', 'UpdateTime', 'LossAdjusterType', 'ReassignClaim', 'ID', 'RelatedIncident'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_otherinsurer', 'HUB__OTHERINSURER', ['PublicID'],
    [
        SatelliteDefinition('SAT__OTHERINSURER', [
            'LoadCommandID', 'CreateUserID', 'ArchivePartition', 'BeanVersion', 'InjuryIncident', 'Retired', 'CreateTime', 'UpdateUserID', 'Comments', 'UpdateTime', 'Type', 'ID', 'Benefits'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'cc_vehicle', 'HUB__VEHICLE', ['PublicID'],
    [
        SatelliteDefinition('SAT__VEHICLE', [
            'LoadCommandID', 'OffRoadStyle', 'CreateTime', 'LoanMonthlyPayment', 'LoanPayoffAmount', 'Manufacturer', 'State', 'UpdateTime', 'ID', 'Vin', 'CreateUserID', 'Color', 'BeanVersion', 'ArchivePartition', 'Retired', 'Model', 'Make', 'Alg_YearDate', 'UpdateUserID', 'Alg_VehicleAddrLocation',
            'Loan', 'Style', 'Alg_LicensePlateType', 'LoanMonthsRemaining', 'Alg_Country', 'LicensePlate', 'BoatType', 'PolicySystemId', 'Year', 'SerialNumber', 'Alg_CatalogPrice', 'Alg_GrossWeight', 'Alg_ValidityOfDriversLicense', 'Alg_FuelType', 'Alg_FirstRegistrationDate', 'Alg_VehicleModel', 'Alg_VehicleObjectType', 'Alg_VehicleType', 'Alg_NumberOfSeats', 'Alg_SpecialVehicle',
            'Alg_AccessoriesPrice', 'Alg_EngineCubicCapacity', 'Alg_EmptyWeight', 'Alg_Use', 'Alg_EnginePowerInDINPS', 'Alg_Type', 'Alg_EurotaxKey', 'Alg_KeyType', 'Alg_FuelConsumption', 'Alg_ThousandsKmsDrivenPerYear'
        ])
    ])
raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_handlingfees', 'HUB__HANDLINGFEES', ['PublicID'],
    [
        SatelliteDefinition('SAT__HANDLINGFEES', [
            'LoadCommandID', 'NetFeesNGF', 'ChangedCurrency', 'AlreadyAppliedNGF', 'IsNormalFeesChngd', 'CreateTime', 'TotalPaidNVB', 'IsSpecialFeesChngd', 'AppliedFeesSpecial', 'NetFeesNVB', 'ExchangeDate', 'AppliedFeesNormal', 'RateFeeSpecial', 'CalculatedRecovery', 'alreadyAppliedNVB', 'Recovery', 'UpdateTime', 'VatFees', 'IsRecoveryFeesChngd', 'SpecialMaxFee',
            'ID', 'CalculatedNVB', 'SpentHours', 'Ceded', 'IsNetFeesChgd', 'CreateUserID', 'VATrateNGF', 'CalculatedNormal', 'IsAppFeesChgd', 'IsNetFeesChngd', 'TotalAppliedFees', 'Normal', 'BeanVersion', 'Retired', 'SpecialCase', 'SpecialMinFee', 'UpdateUserID', 'AppliedFeesNVB', 'VATrateNVB', 'AppliedFeesCeded',
            'Subtype', 'TotalFeesSum', 'AppliedFeesRecovery', 'IsAppliedFee', 'IsCededApplied', 'AppliedFeesCededNVB', 'TotalAppliedFees_NVB', 'TotalAppliedFees_NGF', 'DefaultCurrency'
        ])
    ])


raw_vault.load_hub_from_prepared_staging_table(
    'cc_catastrophe', 'HUB__CATASTROPHE', ['PublicID'],
    [
        SatelliteDefinition('SAT__CATASTROPHE', [
            'LoadCommandID', 'CatastropheValidFrom', 'ScheduleBatch', 'Active', 'TopLeftLatitude', 'CreateTime', 'Name', 'PCSCatastropheNumber', 'TopLeftLongitude', 'CatastropheValidTo', 'UpdateTime', 'ID', 'CreateUserID', 'PolicyEffectiveDate', 'Alg_CatastropheZoneType', 'BeanVersion', 'Retired', 'PolicyRetrievalCompletionTime', 'CatastropheNumber', 'PolicyRetrievalSetTime',
            'UpdateUserID', 'Alg_Country', 'Comments', 'AssignedUserID', 'Type', 'Description', 'BottomRightLatitude', 'BottomRightLongitude', 'Alg_CoordinatorGroup', 'Alg_CoordinatorUser', 'Alg_IsMultipleCountry'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_causerdetail', 'HUB__CAUSERDETAIL', ['PublicID'],
    [
        SatelliteDefinition('SAT__CAUSERDETAIL', [
            'LoadCommandID', 'CreateUserID', 'CauserInformation', 'BeanVersion', 'Retired', 'CreateTime', 'UpdateUserID', 'CauserName', 'UpdateTime', 'VehicularCategory', 'Subtype', 'ID', 'ExaminationDate'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_lctfleetquestionaire', 'HUB__LCTFLEETQUESTIONAIRE', ['PublicID'],
    [
        SatelliteDefinition('SAT__LCTFLEETQUESTIONAIRE', [
            'LoadCommandID', 'CreateUserID', 'DriverStateFleet', 'BeanVersion', 'LossCauseFleet', 'CreateTime', 'Retired', 'DriverLiabilityFleet', 'EmpRelationshipFleet', 'LoadLossCauseFleet', 'UpdateUserID', 'MovementVehicleFleet', 'RoadConditionFleet', 'RoadTypeFleet', 'VehicleUsage', 'UpdateTime', 'LossEventFleet', 'VehicleDefectsFleet', 'DriverBehaviourFleet', 'LoadConditionFleet',
            'ID'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_Alg_productcodes', 'HUB__PRODUCTCODES', ['PublicID'],
    [
        SatelliteDefinition('SAT__PRODUCTCODES', [
            'LoadCommandID', 'ProductCodeLabelEN', 'CreateUserID', 'ProductCodeLabelIT', 'Priority', 'ProductCodeLabelFR', 'BeanVersion', 'CreateTime', 'DocumentTextDE', 'Retired', 'UpdateUserID', 'DocumentTextEN', 'DocumentTextIT', 'DocumentTextFR', 'UpdateTime', 'ProductCodeLabelDE', 'ID', 'ProductCode'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_namedperson', 'HUB__NAMEDPERSON', ['PublicID'],
    [
        SatelliteDefinition('SAT__NAMEDPERSON', [
            'LoadCommandID', 'Gender', 'NamedEmployeeReference', 'CreateTime', 'Salary', 'Name', 'NamedEmployeeRestrictions', 'UpdateTime', 'ProfessionalStatus', 'GroupID', 'DateOfBirth', 'Selected', 'SalaryCurrency', 'Surname', 'ID', 'EntryDate', 'CreateUserID', 'TypeofPerson', 'ArchivePartition', 'BeanVersion',
            'Retired', 'UpdateUserID', 'serialNumber'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_employementdata', 'HUB__EMPLOYEMENTDATA', ['PublicID'],
    [
        SatelliteDefinition('SAT__EMPLOYEMENTDATA', [
            'WageAmount', 'LoadCommandID', 'OvertimeRate', 'Alg_WorkResumed', 'Alg_LastWorkingTime', 'NumDaysWorked', 'SSBenefitsAmnt', 'WagePaymentCont', 'Alg_ProfessionalPosition', 'UpdateTime', 'WageAmountPostInjury', 'InjuryStartTime', 'ID', 'SSBenefits', 'HireState', 'FirstPhoneCalldone', 'Alg_WorkAssignment', 'CreateUserID', 'HireDate', 'BeanVersion',
            'ScndInjryFndAmnt', 'Retired', 'Alg_FurtherEmployment', 'FirstPhoneCallunknown', 'UpdateUserID', 'Alg_WorkSuspendedFrom', 'Alg_WorkPlace', 'LastWorkedDate', 'ScndInjryFnd', 'LastYearIncome', 'DepartmentCode', 'ClassCodeID', 'Alg_InjuryDescription', 'Alg_SpecialCase', 'Alg_DurationWorkIncapacity', 'CreateTime', 'Occupation', 'Alg_EmployeeID', 'Alg_DegreeOfEmployment', 'Alg_ReasonForAbsenceDenorm',
            'PaidFull', 'Alg_ReasonForAbsence', 'EmploymentStatus', 'ScndInjryFndDate', 'IncapacityToWorkInDays', 'ArchivePartition', 'PayPeriod', 'Alg_LimitedOrTerminatedDate', 'Alg_IncapacityToWork', 'Alg_Employment', 'Alg_DateOfEmployment', 'NumHoursWorked', 'SelfEmployed', 'DaysWorkedWeek', 'Alg_StdWorkingHrsPerWeek', 'Alg_HrsWorkedPerWeek', 'Alg_NonEmployed'
        ])
    ])

raw_vault.load_hub_from_prepared_staging_table(
    'ccx_alg_furtheremployers', 'HUB__FURTHEREMPLOYERS', ['PublicID'],
    [
        SatelliteDefinition('SAT__FURTHEREMPLOYERS', [
            'LoadCommandID', 'CreateUserID', 'BeanVersion', 'LastWorkingTime', 'ArchivePartition', 'Retired', 'CreateTime', 'ResponsiblePerson', 'AccidentInsurance', 'UpdateUserID', 'DegreeOfEmployment', 'incident', 'UpdateTime', 'WorkHrsPerWeek', 'ID'
        ])
    ])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Load Links
#
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('PolicyID', ColumnReference('cc_policy', 'ID')), 'LNK__CLAIM__POLICY', 'CLAIM_HKEY', 'POLICY_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_exposure', ForeignKey('ClaimID', ColumnReference('cc_claim', 'ID')), 'LNK__EXPOSURE__CLAIM', 'EXPOSURE_HKEY', 'CLAIM_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_incident', ForeignKey('ClaimID', ColumnReference('cc_claim', 'ID')), 'LNK__INCIDENT__CLAIM', 'INCIDENT_HKEY', 'CLAIM_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_coverage', ForeignKey('PolicyID', ColumnReference('cc_policy', 'ID')), 'LNK__COVERAGE__POLICY', 'COVERAGE_HKEY', 'POLICY_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_coverage', ForeignKey('RiskUnitID', ColumnReference('cc_riskunit', 'ID')), 'LNK__COVERAGE__RISKUNIT', 'COVERAGE_HKEY', 'RISKUNIT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_riskunit', ForeignKey('PolicyID', ColumnReference('cc_policy', 'ID')), 'LNK__RISKUNIT__POLICY', 'RISKUNIT_HKEY', 'POLICY_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_user', ForeignKey('CredentialID', ColumnReference('cc_credential', 'ID')), 'LNK__USER__CREDENTIAL', 'USER_HKEY', 'CREDENTIAL_HKEY')

raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_specialhandling', ForeignKey('AccountID', ColumnReference('cc_account', 'ID')), 'LNK__SPECIALHANDLING__ACCOUNT', 'SPECIALHANDLING_HKEY', 'ACCOUNT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_accountassignment', ForeignKey('AccountSpecialHandlingID', ColumnReference('cc_specialhandling', 'ID')), 'LNK__ACCOUNTASSIGNMENT__SPECIALHANDLING', 'ACCOUNTASSIGNMENT_HKEY', 'SPECIALHANDLING_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_strategy', ForeignKey('alg_AccountAssignment', ColumnReference('ccx_alg_accountassignment', 'AccountSpecialHandlingID')), 'LNK__STRATEGY__ACCOUNTASSIGNMENT', 'STRATEGY_HKEY', 'ACCOUNTASSIGNMENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_accountmanagement', ForeignKey('AccountID', ColumnReference('cc_account', 'ID')), 'LNK__ACCOUNTMANAGEMENT__ACCOUNT', 'ACCOUNTMANAGEMENT_HKEY', 'ACCOUNT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_accountpolicy', ForeignKey('AccountID', ColumnReference('cc_account', 'ID')), 'LNK__ACCOUNTPOLICY__ACCOUNT', 'ACCOUNTPOLICY_HKEY', 'ACCOUNT_HKEY')

# policy to account
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_policy', ForeignKey('AccountID', ColumnReference('cc_account', 'ID')), 'LNK__POLICY__ACCOUNT', 'POLICY_HKEY', 'ACCOUNT_HKEY')

raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_otherreferences', ForeignKey('ClaimID', ColumnReference('cc_claim', 'ID')), 'LNK__OTHERREFERENCES__CLAIM', 'OTHERREFERENCES_HKEY', 'CLAIM_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_liabilityconcept', ForeignKey('ClaimID', ColumnReference('cc_claim', 'ID')), 'LNK__LIABILITYCONCEPT__CLAIM', 'LIABILITYCONCEPT_HKEY', 'CLAIM_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_deductible', ForeignKey('CoverageID', ColumnReference('cc_coverage', 'ID')), 'LNK__DEDUCTIBLE__COVERAGE', 'DEDUCTIBLE_HKEY', 'COVERAGE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_coverageterms', ForeignKey('CoverageID', ColumnReference('cc_coverage', 'ID')), 'LNK__COVERAGETERMS__COVERAGE', 'COVERAGETERMS_HKEY', 'COVERAGE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_endorsement', ForeignKey('Coverage', ColumnReference('cc_coverage', 'ID')), 'LNK__ENDORSEMENT__COVERAGE', 'ENDORSEMENT_HKEY', 'COVERAGE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_endorsement', ForeignKey('RiskUnit', ColumnReference('cc_riskunit', 'ID')), 'LNK__ENDORSEMENT__RISKUNIT', 'ENDORSEMENT_HKEY', 'RISKUNIT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_term', ForeignKey('PolicyID', ColumnReference('cc_policy', 'ID')), 'LNK__TERM__POLICY', 'TERM_HKEY', 'POLICY_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_term', ForeignKey('RiskUnitID', ColumnReference('cc_riskunit', 'ID')), 'LNK__TERM__RISKUNIT', 'TERM_HKEY', 'RISKUNIT_HKEY')

raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_incident', ForeignKey('Alg_VehicleSalvageID', ColumnReference('ccx_alg_vehiclesalvage', 'ID')), 'LNK__INCIDENT__VEHICLESALVAGE', 'INCIDENT_HKEY', 'VEHICLESALVAGE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_tripitem', ForeignKey('TripIncident', ColumnReference('cc_incident', 'ID')), 'LNK__TRIPITEM__INCIDENT', 'TRIPITEM_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_mobilepropertyitem', ForeignKey('AlG_MobilePropertyIncident', ColumnReference('cc_incident', 'ID')), 'LNK__MOBILEPROPERTYITEM__INCIDENT', 'MOBILEPROPERTYITEM_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_mobpropcostdetails', ForeignKey('MobilePropertyItem', ColumnReference('ccx_alg_mobilepropertyitem', 'ID')), 'LNK__MOBPROPCOSTDETAILS__MOBILEPROPERTYITEM', 'MOBPROPCOSTDETAILS_HKEY','MOBILEPROPERTYITEM_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_mobilepropertyitem', ForeignKey('Settlement', ColumnReference('ccx_alg_settlement', 'ID')), 'LNK__MOBILEPROPERTYITEM__SETTLEMENT', 'MOBILEPROPERTYITEM_HKEY', 'SETTLEMENT_HKEY')

raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_injurydiagnosis', ForeignKey('ICDCode', ColumnReference('cc_icdcode', 'ID')), 'LNK__INJURYDIAGNOSIS__ICDCODE', 'INJURYDIAGNOSIS_HKEY', 'ICDCODE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_injurydiagnosis', ForeignKey('InjuryIncidentID', ColumnReference('cc_incident', 'ID')), 'LNK__INJURYDIAGNOSIS__INCIDENT', 'INJURYDIAGNOSIS_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_RELUcode', ForeignKey('InjuryIncidentID', ColumnReference('cc_incident', 'ID')),'LNK__RELUCODE__INCIDENT', 'RELUCODE_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_RELUcode', ForeignKey('RELULookupID', ColumnReference('ccx_alg_RELUlookup', 'ID')),'LNK__RELUCODE__RELULOOKUP', 'RELUCODE_HKEY', 'RELULOOKUP_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_bodilyInjuryPoint_pel', ForeignKey('injuryIncidentID', ColumnReference('cc_incident', 'ID')),'LNK__BODILYINJURYPOINTPEL__INCIDENT', 'BODILYINJURYPOINTPEL_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_treatment', ForeignKey('InjuryIncident', ColumnReference('cc_incident', 'ID')),'LNK__TREATMENT__INCIDENT', 'TREATMENT_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_disease', ForeignKey('injuryIncidentID', ColumnReference('cc_incident', 'ID')),'LNK__DISEASE__INCIDENT', 'DISEASE_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_purefincosttype', ForeignKey('Alg_FinLossIncident', ColumnReference('cc_incident', 'ID')),'LNK__PUREFINCOSTTYPE__INCIDENT', 'PUREFINCOSTTYPE_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_financiallossitem', ForeignKey('Alg_FinancialLossIncident', ColumnReference('cc_incident', 'ID')),'LNK__FINANCIALLOSSITEM__INCIDENT', 'FINANCIALLOSSITEM_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_biincidentlossitem', ForeignKey('BusinessInterruptionIncident', ColumnReference('cc_incident', 'ID')),'LNK__BIINCIDENTLOSSITEM__INCIDENT', 'BIINCIDENTLOSSITEM_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_incidentcession', ForeignKey('Incident', ColumnReference('cc_incident', 'ID')),'LNK__INCIDENTCESSION__INCIDENT', 'INCIDENTCESSION_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_lossadjusterorder', ForeignKey('RelatedIncident', ColumnReference('cc_incident', 'ID')),'LNK__LOSSADJUSTERORDER__INCIDENT', 'LOSSADJUSTERORDER_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_otherinsurer', ForeignKey('InjuryIncident', ColumnReference('cc_incident', 'ID')),'LNK__OTHERINSURER__INCIDENT', 'OTHERINSURER_HKEY', 'INCIDENT_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_financiallossitem', ForeignKey('Cessionary', ColumnReference('ccx_alg_incidentcession', 'ID')),'LNK__FINANCIALLOSSITEM__INCIDENTCESESSION', 'FINANCIALLOSSITEM_HKEY', 'INCIDENTCESESSION_HKEY')

raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_riskunit', ForeignKey('VehicleID', ColumnReference('cc_vehicle', 'ID')), 'LNK__RISKUNIT__VEHICLE', 'RISKUNIT_HKEY', 'VEHICLE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('Alg_HandlingFees', ColumnReference('ccx_alg_handlingfees', 'ID')),'LNK__CLAIM__HANDLINGFEES', 'CLAIM_HKEY', 'HANDLINGFEES_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('CatastropheID', ColumnReference('cc_catastrophe', 'ID')),'LNK__CLAIM__CATASTROPHE', 'CLAIM_HKEY', 'CATASTROPHE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('Alg_CauserDetail', ColumnReference('ccx_alg_causerdetail', 'ID')),'LNK__CLAIM__CAUSERDETAIL', 'CLAIM_HKEY', 'CAUSERDETAIL_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('Alg_LCTFleetQuestionaire', ColumnReference('ccx_alg_lctfleetquestionaire', 'ID')),'LNK__CLAIM__LCTFLEETQUESTIONAIRE', 'CLAIM_HKEY', 'LCTFLEETQUESTIONAIRE_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('Alg_ProductCode', ColumnReference('ccx_Alg_productcodes', 'ID')),'LNK__CLAIM__PRODUCTCODES', 'CLAIM_HKEY', 'PRODUCTCODES_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_workresumptiondetails', ForeignKey('Alg_EmploymentID', ColumnReference('ccx_alg_employementdata', 'ID')),'LNK__WORKRESUMPTIONDETAILS__EMPLOYMENTDATA', 'WORKRESUMPTIONDETAILS_HKEY', 'EMPLOYMENTDATA_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_salarydata', ForeignKey('Alg_EmploymentID', ColumnReference('ccx_alg_employementdata', 'ID')),'LNK__SALARYDATA__EMPLOYMENTDATA', 'SALARYDATA_HKEY', 'EMPLOYMENTDATA_HKEY')
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('ccx_alg_salarydata', ForeignKey('Alg_FurtherEmployerID', ColumnReference('ccx_alg_furtheremployers', 'ID')),'LNK__SALARYDATA__FURTHEREMPLOYERS', 'SALARYDATA_HKEY', 'FURTHEREMPLOYERS_HKEY')

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# Update Point-in-time-Tables
#

raw_vault.create_point_in_time_table_for_single_satellite('PIT__CLAIM', 'SAT__CLAIM')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__COVERAGE', 'SAT__COVERAGE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__EXPOSURE', 'SAT__EXPOSURE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__INCIDENT', 'SAT__INCIDENT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__POLICY', 'SAT__POLICY')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__RISKUNIT', 'SAT__RISKUNIT')

raw_vault.create_point_in_time_table_for_single_satellite('PIT__USER', 'SAT__USER')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__CREDENTIAL', 'SAT__CREDENTIAL')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__ACCOUNT', 'SAT__ACCOUNT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__ACCOUNTASSIGNMENT', 'SAT__ACCOUNTASSIGNMENT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__SPECIALHANDLING', 'SAT__SPECIALHANDLING')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__STRATEGY', 'SAT__STRATEGY')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__ACCOUNTPOLICY', 'SAT__ACCOUNTPOLICY')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__OTHERREFERENCES', 'SAT__OTHERREFERENCES')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__LIABILITYCONCEPT', 'SAT__LIABILITYCONCEPT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__DEDUCTIBLE', 'SAT__DEDUCTIBLE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__COVERAGETERMS', 'SAT__COVERAGETERMS')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__ENDORSEMENT', 'SAT__ENDORSEMENT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__TERM', 'SAT__TERM')

raw_vault.create_point_in_time_table_for_single_satellite('PIT__VEHICLESALVAGE', 'SAT__VEHICLESALVAGE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__TRIPITEM', 'SAT__TRIPITEM')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__MOBILEPROPERTYITEM', 'SAT__MOBILEPROPERTYITEM')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__MOBPROPCOSTDETAILS', 'SAT__MOBPROPCOSTDETAILS')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__ICDCODE', 'SAT__ICDCODE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__INJURYDIAGNOSIS', 'SAT__INJURYDIAGNOSIS')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__WORKRESUMPTIONDETAILS', 'SAT__WORKRESUMPTIONDETAILS')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__SALARYDATA', 'SAT__SALARYDATA')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__RELULOOKUP', 'SAT__RELULOOKUP')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__RELUCODE', 'SAT__RELUCODE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__BODILYINJURYPOINTPEL', 'SAT__BODILYINJURYPOINTPEL')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__TREATMENT', 'SAT__TREATMENT')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__DISEASE', 'SAT__DISEASE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__PUREFINCOSTTYPE', 'SAT__PUREFINCOSTTYPE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__FINANCIALLOSSITEM', 'SAT__FINANCIALLOSSITEM')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__BIINCIDENTLOSSITEM', 'SAT__BIINCIDENTLOSSITEM')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__INCIDENTCESSION', 'SAT__INCIDENTCESSION')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__LOSSADJUSTERORDER', 'SAT__LOSSADJUSTERORDER')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__OTHERINSURER', 'SAT__OTHERINSURER')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__VEHICLE', 'SAT__VEHICLE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__VEHICLE', 'SAT__VEHICLE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__HANDLINGFEES', 'SAT__HANDLINGFEES')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__CATASTROPHE', 'SAT__CATASTROPHE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__CAUSERDETAIL', 'SAT__CAUSERDETAIL')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__LCTFLEETQUESTIONAIRE', 'SAT__LCTFLEETQUESTIONAIRE')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__PRODUCTCODES', 'SAT__PRODUCTCODES')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__NAMEDPERSON', 'SAT__NAMEDPERSON')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__EMPLOYEMENTDATA', 'SAT__EMPLOYEMENTDATA')
raw_vault.create_point_in_time_table_for_single_satellite('PIT__FURTHEREMPLOYERS', 'SAT__FURTHEREMPLOYERS')

#
#
####################################################################################################################################
####################################################################################################################################
#
#

raw_vault.create_code_reference_table('REF__TYPELIST', ColumnDefinition('ID', IntegerType(), True), [
    ColumnDefinition('S_Alg_RM', IntegerType(), True), ColumnDefinition('NAME', StringType(), True), ColumnDefinition('S_Alg_PL', IntegerType(), True), ColumnDefinition('S_de', IntegerType(), True), ColumnDefinition('S_Alg_NL', IntegerType(), True), ColumnDefinition('S_Alg_PT', IntegerType(), True), ColumnDefinition('S_fr', IntegerType(), True), ColumnDefinition('L_it', StringType(), True), ColumnDefinition('L_en_US', StringType(), True), ColumnDefinition('PRIORITY', IntegerType(), True), ColumnDefinition('L_Alg_ES', StringType(), True), ColumnDefinition('TYPECODE', StringType(), True), ColumnDefinition('S_en_US', IntegerType(), True), ColumnDefinition('RETIRED', BooleanType(), True), ColumnDefinition('L_Alg_RM', StringType(), True), ColumnDefinition('L_Alg_PL', StringType(), True), ColumnDefinition('L_de', StringType(), True), ColumnDefinition('L_Alg_NL', StringType(), True), ColumnDefinition('S_it', IntegerType(), True), ColumnDefinition('L_Alg_PT', StringType(), True),
    ColumnDefinition('DESCRIPTION', StringType(), True), ColumnDefinition('S_Alg_ES', IntegerType(), True), ColumnDefinition('L_fr', StringType(), True)
])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

raw_vault.stage_table('cctl_alg_acutespecificinjuries', 'cctl_alg_acutespecificinjuries.parquet', [])
raw_vault.stage_table('cctl_alg_ageofitems', 'cctl_alg_ageofitems.parquet', [])
raw_vault.stage_table('cctl_alg_alarmsystem', 'cctl_alg_alarmsystem.parquet', [])
raw_vault.stage_table('cctl_alg_animalspecies', 'cctl_alg_animalspecies.parquet', [])
raw_vault.stage_table('cctl_alg_biincidentlossitem', 'cctl_alg_biincidentlossitem.parquet', [])
raw_vault.stage_table('cctl_alg_bipoints_pel', 'cctl_alg_bipoints_pel.parquet', [])
raw_vault.stage_table('cctl_alg_bitypeofsalary', 'cctl_alg_bitypeofsalary.parquet', [])
raw_vault.stage_table('cctl_alg_biunit', 'cctl_alg_biunit.parquet', [])
raw_vault.stage_table('cctl_alg_brokertype', 'cctl_alg_brokertype.parquet', [])
raw_vault.stage_table('cctl_alg_buildingtype', 'cctl_alg_buildingtype.parquet', [])
raw_vault.stage_table('cctl_alg_buildinguse', 'cctl_alg_buildinguse.parquet', [])
raw_vault.stage_table('cctl_alg_cancellationreason', 'cctl_alg_cancellationreason.parquet', [])
raw_vault.stage_table('cctl_alg_categorydamagedobject', 'cctl_alg_categorydamagedobject.parquet', [])
raw_vault.stage_table('cctl_alg_causer', 'cctl_alg_causer.parquet', [])
raw_vault.stage_table('cctl_alg_causingvehicletype', 'cctl_alg_causingvehicletype.parquet', [])
raw_vault.stage_table('cctl_alg_claimflow', 'cctl_alg_claimflow.parquet', [])
raw_vault.stage_table('cctl_alg_claimsgroupsegment', 'cctl_alg_claimsgroupsegment.parquet', [])
raw_vault.stage_table('cctl_alg_claimsubstate', 'cctl_alg_claimsubstate.parquet', [])
raw_vault.stage_table('cctl_alg_cnpmode', 'cctl_alg_cnpmode.parquet', [])
raw_vault.stage_table('cctl_alg_constructionmethod', 'cctl_alg_constructionmethod.parquet', [])
raw_vault.stage_table('cctl_alg_conveyancecode', 'cctl_alg_conveyancecode.parquet', [])
raw_vault.stage_table('cctl_alg_costtype', 'cctl_alg_costtype.parquet', [])
raw_vault.stage_table('cctl_alg_createdbysystem', 'cctl_alg_createdbysystem.parquet', [])
raw_vault.stage_table('cctl_alg_diagnosis', 'cctl_alg_diagnosis.parquet', [])
raw_vault.stage_table('cctl_alg_diagnosistype', 'cctl_alg_diagnosistype.parquet', [])
raw_vault.stage_table('cctl_alg_dicdilindicator', 'cctl_alg_dicdilindicator.parquet', [])
raw_vault.stage_table('cctl_alg_diseasepattern', 'cctl_alg_diseasepattern.parquet', [])
raw_vault.stage_table('cctl_alg_driverbehaviourfleet', 'cctl_alg_driverbehaviourfleet.parquet', [])
raw_vault.stage_table('cctl_alg_driverliabilityfleet', 'cctl_alg_driverliabilityfleet.parquet', [])
raw_vault.stage_table('cctl_alg_driverstatefleet', 'cctl_alg_driverstatefleet.parquet', [])
raw_vault.stage_table('cctl_alg_employeeconditions', 'cctl_alg_employeeconditions.parquet', [])
raw_vault.stage_table('cctl_alg_employeepersontype', 'cctl_alg_employeepersontype.parquet', [])
raw_vault.stage_table('cctl_alg_emprelationshipfleet', 'cctl_alg_emprelationshipfleet.parquet', [])
raw_vault.stage_table('cctl_alg_equipment', 'cctl_alg_equipment.parquet', [])
raw_vault.stage_table('cctl_alg_estimdurofbi', 'cctl_alg_estimdurofbi.parquet', [])
raw_vault.stage_table('cctl_alg_exposuresubstate', 'cctl_alg_exposuresubstate.parquet', [])
raw_vault.stage_table('cctl_alg_financiallossitem', 'cctl_alg_financiallossitem.parquet', [])
raw_vault.stage_table('cctl_alg_freedescription', 'cctl_alg_freedescription.parquet', [])
raw_vault.stage_table('cctl_alg_fueltype', 'cctl_alg_fueltype.parquet', [])
raw_vault.stage_table('cctl_alg_function', 'cctl_alg_function.parquet', [])
raw_vault.stage_table('cctl_alg_glasstype', 'cctl_alg_glasstype.parquet', [])
raw_vault.stage_table('cctl_alg_gndeduction', 'cctl_alg_gndeduction.parquet', [])
raw_vault.stage_table('cctl_alg_handlingfees', 'cctl_alg_handlingfees.parquet', [])
raw_vault.stage_table('cctl_alg_heatingsystem', 'cctl_alg_heatingsystem.parquet', [])
raw_vault.stage_table('cctl_alg_illnesstype', 'cctl_alg_illnesstype.parquet', [])
raw_vault.stage_table('cctl_alg_injuredparty', 'cctl_alg_injuredparty.parquet', [])
raw_vault.stage_table('cctl_alg_insurancetype', 'cctl_alg_insurancetype.parquet', [])
raw_vault.stage_table('cctl_alg_insuredcharcteristics', 'cctl_alg_insuredcharcteristics.parquet', [])
raw_vault.stage_table('cctl_alg_insuredpersonalcircle', 'cctl_alg_insuredpersonalcircle.parquet', [])
raw_vault.stage_table('cctl_alg_insuredvalue', 'cctl_alg_insuredvalue.parquet', [])
raw_vault.stage_table('cctl_alg_interclaimsgcode', 'cctl_alg_interclaimsgcode.parquet', [])
raw_vault.stage_table('cctl_alg_interclaimstype', 'cctl_alg_interclaimstype.parquet', [])
raw_vault.stage_table('cctl_alg_involvedobjects', 'cctl_alg_involvedobjects.parquet', [])
raw_vault.stage_table('cctl_alg_ipsblamecode', 'cctl_alg_ipsblamecode.parquet', [])
raw_vault.stage_table('cctl_alg_ipscauseofevent', 'cctl_alg_ipscauseofevent.parquet', [])
raw_vault.stage_table('cctl_alg_ipsemergingrisk', 'cctl_alg_ipsemergingrisk.parquet', [])
raw_vault.stage_table('cctl_alg_ipseventtype', 'cctl_alg_ipseventtype.parquet', [])
raw_vault.stage_table('cctl_alg_ipslawsuit', 'cctl_alg_ipslawsuit.parquet', [])
raw_vault.stage_table('cctl_alg_ipsmovementvehicle', 'cctl_alg_ipsmovementvehicle.parquet', [])
raw_vault.stage_table('cctl_alg_ipsroadtype', 'cctl_alg_ipsroadtype.parquet', [])
raw_vault.stage_table('cctl_alg_ipstypeofloss', 'cctl_alg_ipstypeofloss.parquet', [])
raw_vault.stage_table('cctl_alg_ipsusagevehicle', 'cctl_alg_ipsusagevehicle.parquet', [])
raw_vault.stage_table('cctl_alg_ipsvehicletype', 'cctl_alg_ipsvehicletype.parquet', [])
raw_vault.stage_table('cctl_alg_iszurpartnernetwork', 'cctl_alg_iszurpartnernetwork.parquet', [])
raw_vault.stage_table('cctl_alg_lctash_elucidation', 'cctl_alg_lctash_elucidation.parquet', [])
raw_vault.stage_table('cctl_alg_lctash_origliability', 'cctl_alg_lctash_origliability.parquet', [])
raw_vault.stage_table('cctl_alg_lctash_profmalprcclm', 'cctl_alg_lctash_profmalprcclm.parquet', [])
raw_vault.stage_table('cctl_alg_lctash_specclaim', 'cctl_alg_lctash_specclaim.parquet', [])
raw_vault.stage_table('cctl_alg_legacysystem', 'cctl_alg_legacysystem.parquet', [])
raw_vault.stage_table('cctl_alg_licenseplatetype', 'cctl_alg_licenseplatetype.parquet', [])
raw_vault.stage_table('cctl_alg_loadconditionfleet', 'cctl_alg_loadconditionfleet.parquet', [])
raw_vault.stage_table('cctl_alg_loadlosscausefleet', 'cctl_alg_loadlosscausefleet.parquet', [])
raw_vault.stage_table('cctl_alg_locationmachine', 'cctl_alg_locationmachine.parquet', [])
raw_vault.stage_table('cctl_alg_lossactivity', 'cctl_alg_lossactivity.parquet', [])
raw_vault.stage_table('cctl_alg_lossadjustertype', 'cctl_alg_lossadjustertype.parquet', [])
raw_vault.stage_table('cctl_alg_losscausefleet', 'cctl_alg_losscausefleet.parquet', [])
raw_vault.stage_table('cctl_alg_lossestrepby', 'cctl_alg_lossestrepby.parquet', [])
raw_vault.stage_table('cctl_alg_losseventfleet', 'cctl_alg_losseventfleet.parquet', [])
raw_vault.stage_table('cctl_alg_losseventtype', 'cctl_alg_losseventtype.parquet', [])
raw_vault.stage_table('cctl_alg_lossplace', 'cctl_alg_lossplace.parquet', [])
raw_vault.stage_table('cctl_alg_machtechinstallname', 'cctl_alg_machtechinstallname.parquet', [])
raw_vault.stage_table('cctl_alg_migrationstatus', 'cctl_alg_migrationstatus.parquet', [])
raw_vault.stage_table('cctl_alg_movementreason', 'cctl_alg_movementreason.parquet', [])
raw_vault.stage_table('cctl_alg_movementtype', 'cctl_alg_movementtype.parquet', [])
raw_vault.stage_table('cctl_alg_movementvehiclefleet', 'cctl_alg_movementvehiclefleet.parquet', [])
raw_vault.stage_table('cctl_alg_ngfnvbclaim', 'cctl_alg_ngfnvbclaim.parquet', [])
raw_vault.stage_table('cctl_alg_numberofattempts', 'cctl_alg_numberofattempts.parquet', [])
raw_vault.stage_table('cctl_alg_object', 'cctl_alg_object.parquet', [])
raw_vault.stage_table('cctl_alg_operatingmode', 'cctl_alg_operatingmode.parquet', [])
raw_vault.stage_table('cctl_alg_otherinsurertype', 'cctl_alg_otherinsurertype.parquet', [])
raw_vault.stage_table('cctl_alg_pensiontype', 'cctl_alg_pensiontype.parquet', [])
raw_vault.stage_table('cctl_alg_periodofvalidity', 'cctl_alg_periodofvalidity.parquet', [])
raw_vault.stage_table('cctl_alg_policysourcesystem', 'cctl_alg_policysourcesystem.parquet', [])
raw_vault.stage_table('cctl_alg_policysystem', 'cctl_alg_policysystem.parquet', [])
raw_vault.stage_table('cctl_alg_pretaxprinciple', 'cctl_alg_pretaxprinciple.parquet', [])
raw_vault.stage_table('cctl_alg_productcnp', 'cctl_alg_productcnp.parquet', [])
raw_vault.stage_table('cctl_alg_purfinlosscategory', 'cctl_alg_purfinlosscategory.parquet', [])
raw_vault.stage_table('cctl_alg_purfinlosstype', 'cctl_alg_purfinlosstype.parquet', [])
raw_vault.stage_table('cctl_alg_qualificationngf', 'cctl_alg_qualificationngf.parquet', [])
raw_vault.stage_table('cctl_alg_receiptexisting', 'cctl_alg_receiptexisting.parquet', [])
raw_vault.stage_table('cctl_alg_referencetype', 'cctl_alg_referencetype.parquet', [])
raw_vault.stage_table('cctl_alg_reluactivity', 'cctl_alg_reluactivity.parquet', [])
raw_vault.stage_table('cctl_alg_repairreason', 'cctl_alg_repairreason.parquet', [])
raw_vault.stage_table('cctl_alg_risklocationtype', 'cctl_alg_risklocationtype.parquet', [])
raw_vault.stage_table('cctl_alg_roadconditionfleet', 'cctl_alg_roadconditionfleet.parquet', [])
raw_vault.stage_table('cctl_alg_roadtypefleet', 'cctl_alg_roadtypefleet.parquet', [])
raw_vault.stage_table('cctl_alg_sanction', 'cctl_alg_sanction.parquet', [])
raw_vault.stage_table('cctl_alg_segmentationtype', 'cctl_alg_segmentationtype.parquet', [])
raw_vault.stage_table('cctl_alg_senderdefault', 'cctl_alg_senderdefault.parquet', [])
raw_vault.stage_table('cctl_alg_specialvehicle', 'cctl_alg_specialvehicle.parquet', [])
raw_vault.stage_table('cctl_alg_specificbodyparts', 'cctl_alg_specificbodyparts.parquet', [])
raw_vault.stage_table('cctl_alg_spsettlementtype', 'cctl_alg_spsettlementtype.parquet', [])
raw_vault.stage_table('cctl_alg_statement', 'cctl_alg_statement.parquet', [])
raw_vault.stage_table('cctl_alg_strategytype', 'cctl_alg_strategytype.parquet', [])
raw_vault.stage_table('cctl_alg_subtypeobject', 'cctl_alg_subtypeobject.parquet', [])
raw_vault.stage_table('cctl_alg_taxationprinciple', 'cctl_alg_taxationprinciple.parquet', [])
raw_vault.stage_table('cctl_alg_technicalproductcode', 'cctl_alg_technicalproductcode.parquet', [])
raw_vault.stage_table('cctl_alg_term', 'cctl_alg_term.parquet', [])
raw_vault.stage_table('cctl_alg_termtype', 'cctl_alg_termtype.parquet', [])
raw_vault.stage_table('cctl_alg_treatmenttype', 'cctl_alg_treatmenttype.parquet', [])
raw_vault.stage_table('cctl_alg_tripcategory', 'cctl_alg_tripcategory.parquet', [])
raw_vault.stage_table('cctl_alg_tripcategoryreason', 'cctl_alg_tripcategoryreason.parquet', [])
raw_vault.stage_table('cctl_alg_typeofaccident', 'cctl_alg_typeofaccident.parquet', [])
raw_vault.stage_table('cctl_alg_typeofcompensation', 'cctl_alg_typeofcompensation.parquet', [])
raw_vault.stage_table('cctl_alg_typeofdailyallowance', 'cctl_alg_typeofdailyallowance.parquet', [])
raw_vault.stage_table('cctl_alg_typeofinjury', 'cctl_alg_typeofinjury.parquet', [])
raw_vault.stage_table('cctl_alg_typeofroof', 'cctl_alg_typeofroof.parquet', [])
raw_vault.stage_table('cctl_alg_use', 'cctl_alg_use.parquet', [])
raw_vault.stage_table('cctl_alg_validitydriverlicense', 'cctl_alg_validitydriverlicense.parquet', [])
raw_vault.stage_table('cctl_alg_valuationsi', 'cctl_alg_valuationsi.parquet', [])
raw_vault.stage_table('cctl_alg_vehicledefectsfleet', 'cctl_alg_vehicledefectsfleet.parquet', [])
raw_vault.stage_table('cctl_alg_vehiclemodel', 'cctl_alg_vehiclemodel.parquet', [])
raw_vault.stage_table('cctl_alg_vehicleobjecttype', 'cctl_alg_vehicleobjecttype.parquet', [])
raw_vault.stage_table('cctl_alg_vehicletype', 'cctl_alg_vehicletype.parquet', [])
raw_vault.stage_table('cctl_alg_vehicleusage', 'cctl_alg_vehicleusage.parquet', [])
raw_vault.stage_table('cctl_alg_vehicularcategory', 'cctl_alg_vehicularcategory.parquet', [])
raw_vault.stage_table('cctl_alg_vendorsourcesystem', 'cctl_alg_vendorsourcesystem.parquet', [])
raw_vault.stage_table('cctl_alg_yesnoapproved', 'cctl_alg_yesnoapproved.parquet', [])
raw_vault.stage_table('cctl_algp2_pathwaysmaterial', 'cctl_algp2_pathwaysmaterial.parquet', [])
raw_vault.stage_table('cctl_algp2_pathwaytype', 'cctl_algp2_pathwaytype.parquet', [])
raw_vault.stage_table('cctl_assessmentaction', 'cctl_assessmentaction.parquet', [])
raw_vault.stage_table('cctl_assignmentstatus', 'cctl_assignmentstatus.parquet', [])
raw_vault.stage_table('cctl_claimclosedoutcometype', 'cctl_claimclosedoutcometype.parquet', [])
raw_vault.stage_table('cctl_claimreopenedreason', 'cctl_claimreopenedreason.parquet', [])
raw_vault.stage_table('cctl_claimsecuritytype', 'cctl_claimsecuritytype.parquet', [])
raw_vault.stage_table('cctl_claimsegment', 'cctl_claimsegment.parquet', [])
raw_vault.stage_table('cctl_claimsource', 'cctl_claimsource.parquet', [])
raw_vault.stage_table('cctl_claimstate', 'cctl_claimstate.parquet', [])
raw_vault.stage_table('cctl_claimstrategy', 'cctl_claimstrategy.parquet', [])
raw_vault.stage_table('cctl_contentlineitemcategory', 'cctl_contentlineitemcategory.parquet', [])
raw_vault.stage_table('cctl_country', 'cctl_country.parquet', [])
raw_vault.stage_table('cctl_coverage', 'cctl_coverage.parquet', [])
raw_vault.stage_table('cctl_coveragetype', 'cctl_coveragetype.parquet', [])
raw_vault.stage_table('cctl_covterm', 'cctl_covterm.parquet', [])
raw_vault.stage_table('cctl_covtermmodelval', 'cctl_covtermmodelval.parquet', [])
raw_vault.stage_table('cctl_covtermpattern', 'cctl_covtermpattern.parquet', [])
raw_vault.stage_table('cctl_currency', 'cctl_currency.parquet', [])
raw_vault.stage_table('cctl_exposureclosedoutcometype', 'cctl_exposureclosedoutcometype.parquet', [])
raw_vault.stage_table('cctl_exposurereopenedreason', 'cctl_exposurereopenedreason.parquet', [])
raw_vault.stage_table('cctl_exposurestate', 'cctl_exposurestate.parquet', [])
raw_vault.stage_table('cctl_exposuretier', 'cctl_exposuretier.parquet', [])
raw_vault.stage_table('cctl_exposuretype', 'cctl_exposuretype.parquet', [])
raw_vault.stage_table('cctl_faultrating', 'cctl_faultrating.parquet', [])
raw_vault.stage_table('cctl_flaggedtype', 'cctl_flaggedtype.parquet', [])
raw_vault.stage_table('cctl_gendertype', 'cctl_gendertype.parquet', [])
raw_vault.stage_table('cctl_howreportedtype', 'cctl_howreportedtype.parquet', [])
raw_vault.stage_table('cctl_icdbodysystem', 'cctl_icdbodysystem.parquet', [])
raw_vault.stage_table('cctl_incident', 'cctl_incident.parquet', [])
raw_vault.stage_table('cctl_languagetype', 'cctl_languagetype.parquet', [])
raw_vault.stage_table('cctl_litigationstatus', 'cctl_litigationstatus.parquet', [])
raw_vault.stage_table('cctl_lobcode', 'cctl_lobcode.parquet', [])
raw_vault.stage_table('cctl_localetype', 'cctl_localetype.parquet', [])
raw_vault.stage_table('cctl_losscause', 'cctl_losscause.parquet', [])
raw_vault.stage_table('cctl_losspartytype', 'cctl_losspartytype.parquet', [])
raw_vault.stage_table('cctl_losstype', 'cctl_losstype.parquet', [])
raw_vault.stage_table('cctl_ownertype', 'cctl_ownertype.parquet', [])
raw_vault.stage_table('cctl_personrelationtype', 'cctl_personrelationtype.parquet', [])
raw_vault.stage_table('cctl_phonecountrycode', 'cctl_phonecountrycode.parquet', [])
raw_vault.stage_table('cctl_policystatus', 'cctl_policystatus.parquet', [])
raw_vault.stage_table('cctl_policytype', 'cctl_policytype.parquet', [])
raw_vault.stage_table('cctl_riskunit', 'cctl_riskunit.parquet', [])
raw_vault.stage_table('cctl_severitytype', 'cctl_severitytype.parquet', [])
raw_vault.stage_table('cctl_specialhandling', 'cctl_specialhandling.parquet', [])
raw_vault.stage_table('cctl_state', 'cctl_state.parquet', [])
raw_vault.stage_table('cctl_subrogationstatus', 'cctl_subrogationstatus.parquet', [])
raw_vault.stage_table('cctl_systemusertype', 'cctl_systemusertype.parquet', [])
raw_vault.stage_table('cctl_transporttype', 'cctl_transporttype.parquet', [])
raw_vault.stage_table('cctl_userexperiencetype', 'cctl_userexperiencetype.parquet', [])
raw_vault.stage_table('cctl_vacationstatustype', 'cctl_vacationstatustype.parquet', [])
raw_vault.stage_table('cctl_validationlevel', 'cctl_validationlevel.parquet', [])
raw_vault.stage_table('cctl_vehiclestyle', 'cctl_vehiclestyle.parquet', [])
raw_vault.stage_table('cctl_yesno', 'cctl_yesno.parquet', [])
raw_vault.stage_table('cctl_zonetype', 'cctl_zonetype.parquet', [])

#
#
####################################################################################################################################
####################################################################################################################################
#
#

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_acutespecificinjuries', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ageofitems', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_alarmsystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_animalspecies', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_biincidentlossitem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_bipoints_pel', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_bitypeofsalary', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_biunit', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_brokertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_buildingtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_buildinguse', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_cancellationreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_categorydamagedobject', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_causer', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_causingvehicletype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_claimflow', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_claimsgroupsegment', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_claimsubstate', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_cnpmode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_constructionmethod', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_conveyancecode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_costtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_createdbysystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_diagnosis', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_diagnosistype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_dicdilindicator', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_diseasepattern', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_driverbehaviourfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_driverliabilityfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_driverstatefleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_employeeconditions', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_employeepersontype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_emprelationshipfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_equipment', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_estimdurofbi', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_exposuresubstate', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_financiallossitem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_freedescription', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_fueltype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_function', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_glasstype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_gndeduction', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_handlingfees', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_heatingsystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_illnesstype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_injuredparty', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_insurancetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_insuredcharcteristics', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_insuredpersonalcircle', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_insuredvalue', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_interclaimsgcode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_interclaimstype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_involvedobjects', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsblamecode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipscauseofevent', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsemergingrisk', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipseventtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipslawsuit', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsmovementvehicle', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsroadtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipstypeofloss', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsusagevehicle', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ipsvehicletype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_iszurpartnernetwork', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lctash_elucidation', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lctash_origliability', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lctash_profmalprcclm', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lctash_specclaim', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_legacysystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_licenseplatetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_loadconditionfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_loadlosscausefleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_locationmachine', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lossactivity', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lossadjustertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_losscausefleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lossestrepby', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_losseventfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_losseventtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_lossplace', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_machtechinstallname', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_migrationstatus', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_movementreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_movementtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_movementvehiclefleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_ngfnvbclaim', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_numberofattempts', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_object', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_operatingmode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_otherinsurertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_pensiontype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_periodofvalidity', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_policysourcesystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_policysystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_pretaxprinciple', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_productcnp', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_purfinlosscategory', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_purfinlosstype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_qualificationngf', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_receiptexisting', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_referencetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_reluactivity', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_repairreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_risklocationtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_roadconditionfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_roadtypefleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_sanction', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_segmentationtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_senderdefault', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_specialvehicle', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_specificbodyparts', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_spsettlementtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_statement', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_strategytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_subtypeobject', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_taxationprinciple', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_technicalproductcode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_term', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_termtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_treatmenttype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_tripcategory', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_tripcategoryreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_typeofaccident', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_typeofcompensation', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_typeofdailyallowance', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_typeofinjury', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_typeofroof', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_use', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_validitydriverlicense', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_valuationsi', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehicledefectsfleet', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehiclemodel', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehicleobjecttype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehicletype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehicleusage', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vehicularcategory', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_vendorsourcesystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_alg_yesnoapproved', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_algp2_pathwaysmaterial', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_algp2_pathwaytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_assessmentaction', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_assignmentstatus', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimclosedoutcometype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimreopenedreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimsecuritytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimsegment', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimsource', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimstate', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_claimstrategy', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_contentlineitemcategory', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_country', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_coverage', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_coveragetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_covterm', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_covtermmodelval', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_covtermpattern', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_currency', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_exposureclosedoutcometype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_exposurereopenedreason', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_exposurestate', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_exposuretier', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_exposuretype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_faultrating', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_flaggedtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_gendertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_howreportedtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_icdbodysystem', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_incident', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_languagetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_litigationstatus', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_lobcode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_localetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_losscause', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_losspartytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_losstype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_ownertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_personrelationtype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_phonecountrycode', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_policystatus', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_policytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_riskunit', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_severitytype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_specialhandling', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_state', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_subrogationstatus', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_systemusertype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_transporttype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_userexperiencetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_vacationstatustype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_validationlevel', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_vehiclestyle', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_yesno', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])

raw_vault.load_code_references_from_prepared_stage_table('cctl_zonetype', 'REF__TYPELIST', 'ID', [
        'S_Alg_RM', 'NAME', 'S_Alg_PL', 'S_de', 'S_Alg_NL', 'S_Alg_PT', 'S_fr', 'L_it', 'L_en_US', 'PRIORITY', 'L_Alg_ES', 'TYPECODE', 'S_en_US', 'RETIRED', 'L_Alg_RM', 'L_Alg_PL', 'L_de', 'L_Alg_NL', 'S_it', 'L_Alg_PT',
        'DESCRIPTION', 'S_Alg_ES', 'L_fr'
    ])