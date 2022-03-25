import pyspark.sql.functions as F

from typing import List

from delta.tables import *
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, DataType
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType,TimestampType

from .DataVaultShared import *


class RawVaultConfiguration:

    def __init__(
        self, source_system_name: str, 
        staging_base_path: str, 
        staging_prepared_base_path: str, 
        raw_base_path: str, 
        staging_load_date_column_name: str, 
        staging_cdc_operation_column_name: str,
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

        self.create_effectivity_satellite(self.conventions.sat_effectivity_name(self.conventions.remove_prefix(name)))

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

        self.create_effectivity_satellite(self.conventions.sat_effectivity_name(self.conventions.remove_prefix(name)))

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

    def create_effectivity_satellite(self, name: str) -> None:
        """
        Creates an effectivity satellite table in the raw database. This satellite contains information about whether an instance is deleted or not. 
        Does only create the table if it does not exist yet.

        :param name - The name of the satellite table, usually starting with `SAT__`.
        """
        columns: List[ColumnDefinition] = [
            ColumnDefinition(self.conventions.hkey_column_name(), StringType()), # TODO mw: Add comments to column
            ColumnDefinition(self.conventions.hdiff_column_name(), StringType()),
            ColumnDefinition(self.conventions.load_date_column_name(), TimestampType()),
            ColumnDefinition(self.conventions.deleted_column_name(), BooleanType())
        ]

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
        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(hub_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'
        hub_table_name = self.conventions.hub_name(hub_table_name)
        hub_table_name = f'{self.config.raw_database_name}.{hub_table_name}'
        stage_table_name = f'{self.config.staging_prepared_database_name}.{staging_table_name}'

        self.spark.sql(f"REFRESH TABLE {stage_table_name}") # TODO mw: Really required?
        
        hub_df = self.spark.table(hub_table_name)
        staged_df = self.spark.table(stage_table_name)

        staged_df = staged_df \
            .withColumn(self.conventions.cdc_load_date_column_name(), staged_df[self.conventions.load_date_column_name()]) \
            .withColumn(self.conventions.load_date_column_name(), F.current_timestamp()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \

        self.load_effectivity_satellite_from_prepared_stage_dataframe(staged_df, sat_effectivity_table_name)

        for satellite in satellites:
            self.load_satellite_from_prepared_stage_dataframe(staged_df, satellite)

        columns = [
            self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), 
            self.conventions.record_source_column_name()
        ] + business_key_column_names

        join_condition = hub_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()]
        staged_df \
            .join(hub_df, join_condition, how='left_anti') \
            .select(columns) \
            .distinct() \
            .write.mode('append').saveAsTable(hub_table_name)

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
        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(link_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'
        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'
        hub_table_name = self.conventions.hub_name(from_staging_foreign_key.to.table)
        hub_table_name = f'{self.config.raw_database_name}.{hub_table_name}'
        hub_table_name = self.conventions.remove_source_prefix(hub_table_name)
        sat_table_name = self.conventions.sat_name(from_staging_foreign_key.to.table)
        sat_table_name = f'{self.config.raw_database_name}.{sat_table_name}'
        sat_table_name = self.conventions.remove_source_prefix(sat_table_name)
        
        link_df = self.spark.table(link_table_name)
        hub_df = self.spark.table(hub_table_name)
        sat_df = self.spark.table(sat_table_name) \
            .select([self.conventions.hkey_column_name(), from_staging_foreign_key.to.column])

        join_condition = hub_df[self.conventions.hkey_column_name()] == sat_df[self.conventions.hkey_column_name()]
        staged_to_df = hub_df \
            .join(sat_df, join_condition, how="left") \
            .drop(sat_df[self.conventions.hkey_column_name()]) \
            .withColumnRenamed(self.conventions.hkey_column_name(), to_hkey_column_name) \
            .select([from_staging_foreign_key.to.column, to_hkey_column_name]) \
            .distinct()

        columns = [
            from_staging_foreign_key.column, from_hkey_column_name, 
            self.conventions.cdc_operation_column_name(), self.conventions.load_date_column_name()
        ]

        staged_from_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{from_staging_table_name}') \
            .withColumnRenamed(self.conventions.hkey_column_name(), from_hkey_column_name) \
            .select(columns)

        columns = [
            self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), 
            self.conventions.record_source_column_name(), from_hkey_column_name, to_hkey_column_name
        ]

        join_condition = staged_from_df[from_staging_foreign_key.column] == staged_to_df[from_staging_foreign_key.to.column]
        current_timestamp = F.current_timestamp()
        joined_df = staged_from_df \
            .join(staged_to_df, join_condition) \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([from_hkey_column_name, to_hkey_column_name])) \
            .withColumn(self.conventions.load_date_column_name(), current_timestamp) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .select(columns) \
            .distinct()

        # TODO jb: check referencing issue after writing to database
        # delete_link_df.show()
        join_condition = link_df[self.conventions.hkey_column_name()] == joined_df[self.conventions.hkey_column_name()]
        joined_df \
            .join(link_df, join_condition, how='left_anti') \
            .select(columns) \
            .write.mode('append').saveAsTable(link_table_name)

        update_df = staged_from_df \
            .filter(staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.UPDATE)

        before_update_df = staged_from_df \
            .filter(staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.BEFORE_UPDATE)

        join_condition = [update_df[from_hkey_column_name] == before_update_df[from_hkey_column_name]]
        # covers the following cases:
        #   1.  BEFORE_UPDATE reference == NOT NULL & UPDATE reference == NULL
        #       -> set delete = True prev. reference
        #   2.  BEFORE_UPDATE reference == NULL & UPDATE reference == NOT NULL
        #       -> set delete = False current reference
        #   3.  BEFORE_UPDATE reference == NOT NULL & UPDATE reference == NOT NULL & BEFORE_UPDATE reference != UPDATE reference
        #       -> set delete = False current reference
        #       -> set delete = True prev. reference
        #   4.  SNAPSHOT | CREATE
        #       -> set delete = False current reference
        joined_df = update_df.alias('l') \
            .join(before_update_df.alias('r'), join_condition) \
            .filter((F.col(f'l.{from_staging_foreign_key.column}').isNull()) & (F.col(f'r.{from_staging_foreign_key.column}').isNotNull())) \
            .select("r.*") \
            .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.DELETE)) \
            .union(
                update_df.alias('l') \
                    .join(before_update_df.alias('r'), join_condition) \
                    .filter((F.col(f'l.{from_staging_foreign_key.column}').isNotNull()) & (F.col(f'r.{from_staging_foreign_key.column}').isNull())) \
                    .select("l.*") \
                    .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.CREATE))
            ) \
            .union(
                update_df.alias('l') \
                    .join(before_update_df.alias('r'), join_condition) \
                    .filter(
                        (F.col(f'l.{from_staging_foreign_key.column}').isNotNull()) & 
                        (F.col(f'r.{from_staging_foreign_key.column}').isNotNull()) &
                        (F.col(f'l.{from_staging_foreign_key.column}') != F.col(f'r.{from_staging_foreign_key.column}'))
                    ) \
                    .select("l.*") \
                    .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.CREATE))
            ) \
            .union(
                update_df.alias('l') \
                    .join(before_update_df.alias('r'), join_condition) \
                    .filter(
                        (F.col(f'l.{from_staging_foreign_key.column}').isNotNull()) & 
                        (F.col(f'r.{from_staging_foreign_key.column}').isNotNull()) &
                        (F.col(f'l.{from_staging_foreign_key.column}') != F.col(f'r.{from_staging_foreign_key.column}'))
                    ) \
                    .select("r.*") \
                    .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.DELETE))
            ) \
            .union(
                staged_from_df \
                    .filter(
                        (staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.CREATE) |
                        (staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.SNAPSHOT)
                    )
            )

        columns = [
            self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), 
            self.conventions.record_source_column_name(), self.conventions.cdc_operation_column_name(), 
            self.conventions.cdc_load_date_column_name(), from_hkey_column_name, to_hkey_column_name
        ]

        join_condition = joined_df[from_staging_foreign_key.column] == staged_to_df[from_staging_foreign_key.to.column]
        joined_df = joined_df \
            .join(staged_to_df, join_condition, how="left") \
            .withColumnRenamed(self.conventions.load_date_column_name(), self.conventions.cdc_load_date_column_name()) \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([from_hkey_column_name, to_hkey_column_name])) \
            .withColumn(self.conventions.load_date_column_name(), current_timestamp) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .select(columns) \
            .distinct()

        self.load_effectivity_satellite_from_prepared_stage_dataframe(joined_df, sat_effectivity_table_name)
    
    def load_link_from_prepared_stage_table(self, staging_table_name: str, links: List[LinkedHubDefinition], link_table_name: str, satellites: List[SatelliteDefinition]) -> None:
        """
        Loads a link with data from a staging table which is a already a link table in the source.
        TODO jb: add hint regarding hub and sat

        :param staging_table_name - The name of the staging table from which the link table is derived.
        :param links - The list of linked hubs for the link table.
        :param link_table_name - The name of the link table in the raw vault.
        :param satellites - Definitions of the satellites for the link.
        """
        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(link_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'
        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'

        staged_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{staging_table_name}')
        link_df = self.spark.table(link_table_name)
        
        for link in links:
            hub_table_name = f'{self.config.raw_database_name}.{self.conventions.remove_source_prefix(self.conventions.hub_name(link.name))}'
            hub_df = self.spark.table(hub_table_name)
            sat_table_name = f'{self.config.raw_database_name}.{self.conventions.remove_source_prefix(self.conventions.sat_name(link.name))}'
            sat_df = self.spark.table(sat_table_name) \
                .select([self.conventions.hkey_column_name(), link.foreign_key.to.column])

            join_condition = hub_df[self.conventions.hkey_column_name()] == sat_df[self.conventions.hkey_column_name()]
            joined_df = hub_df \
                .join(sat_df, join_condition, how="left") \
                .drop(sat_df[self.conventions.hkey_column_name()]) \
                .withColumnRenamed(self.conventions.hkey_column_name(), link.hkey_column_name) \
                .select([link.foreign_key.to.column, link.hkey_column_name]) \
                .distinct()

            join_condition = joined_df[link.foreign_key.to.column] == staged_df[link.foreign_key.column]
            staged_df = staged_df \
                .join(joined_df, join_condition, how='left')

        staged_df = staged_df \
            .withColumnRenamed(self.conventions.load_date_column_name(), self.conventions.cdc_load_date_column_name()) \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([ link.hkey_column_name for link in links ])) \
            .withColumn(self.conventions.load_date_column_name(), F.current_timestamp()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .distinct() \

        self.load_effectivity_satellite_from_prepared_stage_dataframe(staged_df, sat_effectivity_table_name)
        
        columns = [
            self.conventions.hkey_column_name(), self.conventions.load_date_column_name(), 
            self.conventions.record_source_column_name()
        ] + [ link.hkey_column_name for link in links ]

        join_condition = link_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()]
        staged_df = staged_df \
            .join(link_df, join_condition, how='left_anti') \
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
        sat_table_name = f'{self.config.raw_database_name}.{satellite.name}'
        sat_df = self.spark.table(sat_table_name)

        allowed_cdc_operations = [
            self.conventions.CDC_OPS.CREATE, self.conventions.CDC_OPS.UPDATE, 
            self.conventions.CDC_OPS.SNAPSHOT
        ]

        columns = [
            self.conventions.hkey_column_name(), self.conventions.hdiff_column_name(), 
            self.conventions.load_date_column_name()
        ] + [ column for column in satellite.attributes ]

        # TODO mw: Remove  distinct() for performance reasons? Should not happen in any case.
        staged_df = staged_df \
            .filter(staged_df[self.conventions.cdc_operation_column_name()].isin(allowed_cdc_operations)) \
            .withColumn(self.conventions.load_date_column_name(), staged_df[self.conventions.cdc_load_date_column_name()]) \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(satellite.attributes)) \
            .select(columns) \
            .distinct() \

        join_condition = [sat_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()], \
            sat_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        staged_df \
            .join(sat_df, join_condition, how='left_anti') \
            .write.mode('append').saveAsTable(sat_table_name)

    def load_effectivity_satellite_from_prepared_stage_dataframe(self, staged_df: DataFrame, sat_effectivity_table_name: str) -> None:
        """
        Loads an effectivity satellite from a data frame which contains prepared an staged data.

        :param staged_df - The dataframe which contains the staged and prepared data for the satellite.
        :param satellite - The satellite definition.
        """
        sat_effectivity_df = self.spark.table(sat_effectivity_table_name)
        
        allowed_cdc_operations = [
            self.conventions.CDC_OPS.CREATE, self.conventions.CDC_OPS.DELETE, 
            self.conventions.CDC_OPS.SNAPSHOT
        ]

        columns = [
            self.conventions.hkey_column_name(), self.conventions.hdiff_column_name(),
            self.conventions.load_date_column_name(), self.conventions.deleted_column_name()
        ]

        deleted_column = F.when(F.col(self.conventions.cdc_operation_column_name()) == 1, True).otherwise(False)
        staged_df = staged_df \
            .filter(staged_df[self.conventions.cdc_operation_column_name()].isin(allowed_cdc_operations)) \
            .withColumn(self.conventions.load_date_column_name(), staged_df[self.conventions.cdc_load_date_column_name()]) \
            .withColumn(self.conventions.deleted_column_name(), deleted_column) \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash([self.conventions.deleted_column_name()])) \
            .select(columns) \
            .distinct()

        join_condition = [
            sat_effectivity_df[self.conventions.hkey_column_name()] == staged_df[self.conventions.hkey_column_name()], \
            sat_effectivity_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]
        ]

        staged_df \
            .join(sat_effectivity_df, join_condition, how='left_anti') \
            .write.mode('append').saveAsTable(sat_effectivity_table_name)

    def stage_table(self, name: str, source: str, hkey_columns: List[str] = []) -> None: # TODO mw: Multiple HKeys, HDiffs?
        """
        Stages a source table. Additional columns will be created/ calculated and stored in the staging database. 

        :param name - The name of the table in the prepared staging area.
        :param source - The source file path, relative to staging_base_path (w/o leading slash).
        :param hkey_columns - Optional. Column names which should be used to calculate a hash key.
        """
        # load source data from Parquet file.
        df = self.spark.read.load(f'{self.config.staging_base_path}/{source}', format='parquet')

        # add DataVault specific columns
        df = df \
            .withColumnRenamed(self.config.staging_load_date_column_name, self.conventions.load_date_column_name()) \
            .withColumnRenamed(self.config.staging_cdc_operation_column_name, self.conventions.cdc_operation_column_name()) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name))

        # update load_date in case of snapshot load (CDC Operation < 1).
        if self.config.snapshot_override_load_date_based_on_column in df.columns:
            df = df.withColumn(
                self.conventions.load_date_column_name(), 
                F \
                    .when(df[self.conventions.cdc_operation_column_name()] < 1, df[self.config.snapshot_override_load_date_based_on_column]) \
                    .otherwise(df[self.conventions.load_date_column_name()]))

        if len(hkey_columns) > 0: 
            df = df.withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash(hkey_columns))

        # write staged table into staging area.
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
