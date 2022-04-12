import pyspark.sql.functions as F

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

from .DataVaultShared import *


class RawVaultConfiguration:

    def __init__(
        self, source_system_name: str, 
        staging_base_path: str, 
        staging_prepared_base_path: str, 
        raw_base_path: str, 
        staging_load_date_column_name: str, 
        staging_cdc_operation_column_name: str,
        snapshot_override_load_date_based_on_column: str,
        optimize_partitioning: bool = True,
        partition_size: int = 5) -> None:
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

        self.optimize_partitioning = optimize_partitioning
        self.partition_size = partition_size

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

        if self.config.optimize_partitioning:
            bucket_columns = [self.conventions.hkey_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.hub_name(name), columns, bucket_columns)
        else:
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

        if self.config.optimize_partitioning:
            bucket_columns = [self.conventions.hkey_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.link_name(name), columns, bucket_columns)
        else:
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

        if self.config.optimize_partitioning:
            bucket_columns = [id_column.name, self.conventions.load_date_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.ref_name(name), columns, bucket_columns)
        else:
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

        if self.config.optimize_partitioning:
            bucket_columns = [self.conventions.ref_group_column_name(), id_column.name, self.conventions.load_date_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.ref_name(name), columns, bucket_columns)
        else:
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

        if self.config.optimize_partitioning:
            bucket_columns = [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.sat_name(name), columns, bucket_columns)
        else:
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

        if self.config.optimize_partitioning:
            bucket_columns = [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()]
            self.__create_external_table(self.config.raw_database_name, self.conventions.sat_name(name), columns, bucket_columns)
        else:
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

        stage_table_name = f'{self.config.staging_prepared_database_name}.{staging_table_name}'
        staged_df = self.spark.table(stage_table_name)
        self.load_hub(staged_df, hub_table_name, business_key_column_names, satellites)

    def load_hub_from_source_table(self, source_table_name: str, hub_table_name: str, business_key_column_names: List[str], satellites: List[SatelliteDefinition] = []) -> None:
        """
        Loads a hub from a source table. The prepared staging table must have a HKEY calculated.

        :param source_table_name - The name of the source table.
        :param hub_table_name - The name of the hub table in the raw vault.
        :param business_key_column_names - The list of columns which contribute to the business key of the hub.
        :param satellites - Optional. A list of satellites which is loaded from the prepared staging table. The form of the tuple is.
        """
        
        staged_df = self.stage_table_df(f"{source_table_name}.parquet", business_key_column_names)
        self.load_hub(staged_df, hub_table_name, business_key_column_names, satellites)
    
    def load_hub(self, staged_df: DataFrame, hub_table_name: str, business_key_column_names: List[str], satellites: List[SatelliteDefinition] = []) -> None:
        """
        Loads a hub from a staged DataFrame.

        :param staged_df - The DataFrame containing the staged data.
        :param hub_table_name - The name of the hub table in the raw vault.
        :param business_key_column_names - The list of columns which contribute to the business key of the hub.
        :param satellites - Optional. A list of satellites which is loaded from the prepared staging table. The form of the tuple is.
        """

        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(hub_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'
        hub_table_name = self.conventions.hub_name(hub_table_name)
        hub_table_name = f'{self.config.raw_database_name}.{hub_table_name}'
        
        hub_df = self.spark.table(hub_table_name)
        
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
            
        if self.config.optimize_partitioning:
            staged_df \
                .join(hub_df, join_condition, how='left_anti') \
                .select(columns) \
                .distinct() \
                .write \
                .bucketBy(self.config.partition_size, self.conventions.hkey_column_name()) \
                .mode('append') \
                .saveAsTable(hub_table_name)
        else:
            staged_df \
                .join(hub_df, join_condition, how='left_anti') \
                .select(hub_df.columns) \
                .write \
                .mode('append') \
                .saveAsTable(hub_table_name)

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

        staged_from_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{from_staging_table_name}')
        self.load_link(staged_from_df, from_staging_foreign_key, link_table_name, from_hkey_column_name, to_hkey_column_name)

    def load_link_for_linked_source_tables_from_source_tables(
        self, 
        from_source_table_name: str,
        from_source_foreign_key: ForeignKey, 
        link_table_name: str,
        from_hkey_column_name: str,
        to_hkey_column_name: str,
        staging_business_key_columns: List[str]) -> None:
        
        """
        Loads a link for two linked source tables based on the prepared staging tables.

        :param from_staging_table_name - The name of the source table which contains a foreign key to another entity.
        :param from_staging_foreign_key - The foreign key constraint of the source/ staging table which points to the other entity.
        :param link_table_name - The name of the link table in the raw vault.
        :param from_hkey_column_name - The name of the column pointing to the origin of the link in the link table.
        :param to_hkey_column_name - The name of the column pointing to the target of the link in the link table.
        :param staging_business_key_columns - The list of columns which contribute to the business key of the hub.
        """

        staged_from_df = self.stage_table_df(f"{from_source_table_name}.parquet", staging_business_key_columns)
        self.load_link(staged_from_df, from_source_foreign_key, link_table_name, from_hkey_column_name, to_hkey_column_name)

    def load_link(
        self, 
        staged_from_df: DataFrame,
        from_foreign_key: ForeignKey, 
        link_table_name: str,
        from_hkey_column_name: str,
        to_hkey_column_name: str) -> None:
        
        """
        Loads a link for two linked source tables based on the staging DataFrame.

        :param staged_from_df - The Dataframe containing the staged data.
        :param from_staging_foreign_key - The foreign key constraint of the source/ staging table which points to the other entity.
        :param link_table_name - The name of the link table in the raw vault.
        :param from_hkey_column_name - The name of the column pointing to the origin of the link in the link table.
        :param to_hkey_column_name - The name of the column pointing to the target of the link in the link table.
        """

        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(link_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'

        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'

        hub_table_name = self.conventions.hub_name(from_foreign_key.to.table)
        hub_table_name = f'{self.config.raw_database_name}.{hub_table_name}'
        hub_table_name = self.conventions.remove_source_prefix(hub_table_name)

        sat_table_name = self.conventions.sat_name(from_foreign_key.to.table)
        sat_table_name = f'{self.config.raw_database_name}.{sat_table_name}'
        sat_table_name = self.conventions.remove_source_prefix(sat_table_name)
        
        link_df = self.spark.table(link_table_name)

        # CHANGE THIS HERE!
        # Tests fail because from_staging_foreign_key.to.column may not be in SAT but in HUB
        sat_df = self.spark.table(sat_table_name)
        hub_df = self.spark.table(hub_table_name) \
            .withColumnRenamed(self.conventions.hkey_column_name(), to_hkey_column_name)

        assert from_foreign_key.to.column in sat_df.columns or from_foreign_key.to.column in hub_df.columns, f"The column {from_foreign_key.to.column} needs to exist in {sat_table_name} or {hub_table_name}."
        if from_foreign_key.to.column in sat_df.columns:
            sat_df = sat_df \
                .groupBy(self.conventions.hkey_column_name(), from_foreign_key.to.column) \
                .agg(F.max(self.conventions.load_date_column_name()))

            hub_df = hub_df \
                .join(sat_df, hub_df[to_hkey_column_name] == sat_df[self.conventions.hkey_column_name()])

        hub_df = hub_df.select([to_hkey_column_name, from_foreign_key.to.column])

        columns = [
            from_foreign_key.column, 
            from_hkey_column_name, 
            self.conventions.cdc_operation_column_name(), 
            self.conventions.load_date_column_name()
        ]

        staged_from_df = staged_from_df \
            .withColumnRenamed(self.conventions.hkey_column_name(), from_hkey_column_name) \
            .select(columns) \
            .cache()

        columns = [
            self.conventions.hkey_column_name(), 
            self.conventions.load_date_column_name(), 
            self.conventions.record_source_column_name(), 
            from_hkey_column_name, to_hkey_column_name
        ]

        join_condition = staged_from_df[from_foreign_key.column] == hub_df[from_foreign_key.to.column]
        current_timestamp = F.current_timestamp()
        joined_df = staged_from_df \
            .join(hub_df, join_condition) \
            .withColumn(self.conventions.hkey_column_name(), DataVaultFunctions.hash([from_hkey_column_name, to_hkey_column_name])) \
            .withColumn(self.conventions.load_date_column_name(), current_timestamp) \
            .withColumn(self.conventions.record_source_column_name(), F.lit(self.config.source_system_name)) \
            .select(columns) \
            .distinct()

        # TODO jb: check referencing issue after writing to database
        # delete_link_df.show()
        join_condition = link_df[self.conventions.hkey_column_name()] == joined_df[self.conventions.hkey_column_name()]

        if self.config.optimize_partitioning:
            joined_df \
                .join(link_df, join_condition, how='left_anti') \
                .select(columns) \
                .write \
                .bucketBy(self.config.partition_size, self.conventions.hkey_column_name()) \
                .mode('append').saveAsTable(link_table_name)
        else:
            joined_df.join(link_df, join_condition, how='left_anti') \
                .select(link_df.columns) \
                .write.mode('append').saveAsTable(link_table_name)

        update_df = staged_from_df \
            .filter(staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.UPDATE) \
            .alias('l') \

        before_update_df = staged_from_df \
            .filter(staged_from_df[self.conventions.cdc_operation_column_name()] == self.conventions.CDC_OPS.BEFORE_UPDATE) \
            .alias('r') \

        if self.config.optimize_partitioning:
            update_df = update_df.repartition(self.config.partition_size, from_hkey_column_name)
            before_update_df = before_update_df.repartition(self.config.partition_size, from_hkey_column_name)

        update_df.cache()
        before_update_df.cache()

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
        joined_df = update_df \
            .join(before_update_df, join_condition) \
            .filter((F.col(f'l.{from_foreign_key.column}').isNull()) & (F.col(f'r.{from_foreign_key.column}').isNotNull())) \
            .select("r.*") \
            .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.DELETE)) \
            .union(
                update_df \
                    .join(before_update_df, join_condition) \
                    .filter((F.col(f'l.{from_foreign_key.column}').isNotNull()) & (F.col(f'r.{from_foreign_key.column}').isNull())) \
                    .select("l.*") \
                    .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.CREATE))
            ) \
            .union(
                update_df \
                    .join(before_update_df, join_condition) \
                    .filter(
                        (F.col(f'l.{from_foreign_key.column}').isNotNull()) & 
                        (F.col(f'r.{from_foreign_key.column}').isNotNull()) &
                        (F.col(f'l.{from_foreign_key.column}') != F.col(f'r.{from_foreign_key.column}'))
                    ) \
                    .select("l.*") \
                    .withColumn(self.conventions.cdc_operation_column_name(), F.lit(self.conventions.CDC_OPS.CREATE))
            ) \
            .union(
                update_df \
                    .join(before_update_df, join_condition) \
                    .filter(
                        (F.col(f'l.{from_foreign_key.column}').isNotNull()) & 
                        (F.col(f'r.{from_foreign_key.column}').isNotNull()) &
                        (F.col(f'l.{from_foreign_key.column}') != F.col(f'r.{from_foreign_key.column}'))
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

        # TODO jb: join may be removed (performance optimization)
        join_condition = joined_df[from_foreign_key.column] == hub_df[from_foreign_key.to.column]
        joined_df = joined_df \
            .join(hub_df, join_condition, how="left") \
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

        :param staging_table_name - The name of the staging table from which the link table is derived.
        :param links - The list of linked hubs for the link table.
        :param link_table_name - The name of the link table in the raw vault.
        :param satellites - Definitions of the satellites for the link.
        """

        staged_df = self.spark.table(f'{self.config.staging_prepared_database_name}.{staging_table_name}')
        self.load_multilink(staged_df, links, link_table_name, satellites)

    def load_link_from_source_table(self, source_table_name: str, links: List[LinkedHubDefinition], link_table_name: str, satellites: List[SatelliteDefinition]) -> None:
        """
        Loads a link with data from a source table which is a already a link table in the source.

        :param source_table_name - The name of the source table from which the link table is derived.
        :param links - The list of linked hubs for the link table.
        :param link_table_name - The name of the link table in the raw vault.
        :param satellites - Definitions of the satellites for the link.
        """

        staged_df = self.stage_table_df(f"{source_table_name}.parquet", [link.foreign_key.column for link in links])
        self.load_multilink(staged_df, links, link_table_name, satellites)

    def load_multilink(self, staged_df: DataFrame, links: List[LinkedHubDefinition], link_table_name: str, satellites: List[SatelliteDefinition]) -> None:
        """
        Loads a link with data from a staging DataFrame which is a already a link table in the source.

        :param staged_df - The Dataframe containing the staged data.
        :param links - The list of linked hubs for the link table.
        :param link_table_name - The name of the link table in the raw vault.
        :param satellites - Definitions of the satellites for the link.
        """

        sat_effectivity_table_name = self.conventions.sat_effectivity_name(self.conventions.remove_prefix(link_table_name))
        sat_effectivity_table_name = f'{self.config.raw_database_name}.{sat_effectivity_table_name}'
        
        link_table_name = self.conventions.link_name(link_table_name)
        link_table_name = f'{self.config.raw_database_name}.{link_table_name}'

        link_df = self.spark.table(link_table_name)
        
        for link in links:
            hub_table_name = f'{self.config.raw_database_name}.{self.conventions.remove_source_prefix(self.conventions.hub_name(link.name))}'
            hub_df = self.spark.table(hub_table_name) \
                .withColumnRenamed(self.conventions.hkey_column_name(), link.hkey_column_name) \
                .select([link.foreign_key.to.column, link.hkey_column_name])

            join_condition = hub_df[link.foreign_key.to.column] == staged_df[link.foreign_key.column]
            staged_df = staged_df \
                .join(hub_df, join_condition, how='left') \
                .drop(hub_df[link.foreign_key.to.column]) \

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

        if self.config.optimize_partitioning:
            staged_df \
                .join(link_df, join_condition, how='left_anti') \
                .select(columns) \
                .write \
                .bucketBy(self.config.partition_size, self.conventions.hkey_column_name()) \
                .mode('append').saveAsTable(link_table_name)
        else:
            staged_df \
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

        staged_df = self.spark.table(f'`{self.config.staging_prepared_database_name}`.`{staging_table_name}`')
        self.load_references(staged_df, reference_table_name, id_column, attributes)


    def load_references_from_source_table(self, source_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a source table. 

        :param source_table_name - The name of the source table.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        staged_df = self.stage_table_df(f"{source_table_name}.parquet")
        self.load_references(staged_df, reference_table_name, id_column, attributes)

    def load_references(self, staged_df: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging DataFrame. 

        :param staged_df - The DataFrame containing the staged data.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """
        columns = [id_column, self.conventions.hdiff_column_name(), self.conventions.load_date_column_name()] + attributes

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name)

        join_condition = [ref_df[id_column] == staged_df[id_column], \
            ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        staged_df = staged_df \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
            .select(columns) \
            .distinct()

        if self.config.optimize_partitioning:
            staged_df = staged_df.repartition(self.config.partition_size, [id_column, self.conventions.load_date_column_name()])
            staged_df.join(ref_df, join_condition, how='left_anti') \
                .write \
                .bucketBy(self.config.partition_size, [id_column, self.conventions.load_date_column_name()]) \
                .mode('append').saveAsTable(ref_table_name)
        else:
            staged_df.join(ref_df, join_condition, how='left_anti') \
                .write.mode('append').saveAsTable(ref_table_name)

    def load_code_references_from_prepared_stage_table(self, staging_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging table. 

        :param staging_table_name - The name of the table in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """
        
        staged_df = self.spark.table(f'`{self.config.staging_prepared_database_name}`.`{staging_table_name}`')
        self.load_code_references(staged_df, staging_table_name, reference_table_name, id_column, attributes)


    def load_code_references_from_source_table(self, source_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging table. 

        :param staging_table_name - The name of the table in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        staged_df = self.stage_table_df(f"{source_table_name}.parquet")
        self.load_code_references(staged_df, source_table_name, reference_table_name, id_column, attributes)

    def load_code_references(self, staged_df: DataFrame, staging_table_name: str, reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a reference table from a staging DataFrame.

        :param staged_df - The DataFrame containing the staged data.
        :param staging_table_name - The name of the table in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        columns = [self.conventions.ref_group_column_name(), id_column, self.conventions.hdiff_column_name(), self.conventions.load_date_column_name()] + attributes

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name)

        join_condition = [ref_df[id_column] == staged_df[id_column], \
            ref_df[self.conventions.ref_group_column_name()] == staged_df[self.conventions.ref_group_column_name()], \
            ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

        staged_df = staged_df \
            .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
            .withColumn(self.conventions.ref_group_column_name(), F.lit(staging_table_name.lower())) \
            .select(columns) \
            .distinct()

        if self.config.optimize_partitioning:
            staged_df = staged_df.repartition(self.config.partition_size, 
                [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()])
            staged_df.join(ref_df, join_condition, how='left_anti') \
                .write \
                .bucketBy(self.config.partition_size, [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()]) \
                .mode('append') \
                .saveAsTable(ref_table_name)
        else:
            staged_df.join(ref_df, join_condition, how='left_anti') \
                .write.mode('append').saveAsTable(ref_table_name)
    
    def load_code_references_from_multiple_prepared_stage_tables(self, staging_table_names: List[str], reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a code reference table from multiple staging tables. 

        :param staging_table_names - The List of names of the tables in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name) \
            .repartition(len(staging_table_names), self.conventions.ref_group_column_name()) \
            .cache()

        new_ref_df = self.spark.createDataFrame([], ref_df.schema)

        for staging_table_name in staging_table_names:
            staged_df = self.spark.table(f'`{self.config.staging_prepared_database_name}`.`{staging_table_name}`') \
                .withColumn(self.conventions.ref_group_column_name(), F.lit(staging_table_name.lower())) \
                .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
                .select(new_ref_df.columns) \
                .dropDuplicates([id_column, self.conventions.ref_group_column_name(), self.conventions.load_date_column_name()])

            join_condition = [ref_df[id_column] == staged_df[id_column], \
                ref_df[self.conventions.ref_group_column_name()] == staged_df[self.conventions.ref_group_column_name()], \
                ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

            if self.config.optimize_partitioning:
                staged_df = staged_df.repartition(self.config.partition_size, 
                    [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()])
            staged_df = staged_df.join(ref_df, join_condition, how='left_anti')
            new_ref_df = new_ref_df.union(staged_df)

        if self.config.optimize_partitioning:
            new_ref_df \
                .write \
                .bucketBy(self.config.partition_size, [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()]) \
                .mode('append') \
                .saveAsTable(ref_table_name)
        else:
            new_ref_df.write.mode('append').saveAsTable(ref_table_name)

    def load_code_references_from_multiple_source_tables(self, source_table_names: List[str], reference_table_name: str, id_column: str, attributes: List[str]) -> None:
        """
        Loads a code reference table from multiple staging tables. 

        :param staging_table_names - The List of names of the tables in the prepared staging area. The staging table name will be used as group name.
        :param reference_table_name - The name of the REF-table in the raw vault.
        :param id_column - The name of the column holding the id of the reference.
        :param attributes - The list of attributes which are stored in the reference table.
        """

        reference_table_name = self.conventions.ref_name(reference_table_name)
        ref_table_name = f'{self.config.raw_database_name}.{reference_table_name}'
        
        ref_df = self.spark.table(ref_table_name) \
            .repartition(len(source_table_names), self.conventions.ref_group_column_name()) \
            .cache()

        new_ref_df = self.spark.createDataFrame([], ref_df.schema)

        for source_table_name in source_table_names:
            staged_df = self.stage_table_df(f"{source_table_name}.parquet") \
                .withColumn(self.conventions.ref_group_column_name(), F.lit(source_table_name.lower())) \
                .withColumn(self.conventions.hdiff_column_name(), DataVaultFunctions.hash(attributes)) \
                .select(new_ref_df.columns) \
                .dropDuplicates([id_column, self.conventions.ref_group_column_name(), self.conventions.load_date_column_name()])

            join_condition = [ref_df[id_column] == staged_df[id_column], \
                ref_df[self.conventions.ref_group_column_name()] == staged_df[self.conventions.ref_group_column_name()], \
                ref_df[self.conventions.load_date_column_name()] == staged_df[self.conventions.load_date_column_name()]]

            if self.config.optimize_partitioning:
                staged_df = staged_df.repartition(self.config.partition_size, 
                    [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()])
            staged_df = staged_df.join(ref_df, join_condition, how='left_anti')
            new_ref_df = new_ref_df.union(staged_df)

        if self.config.optimize_partitioning:
            new_ref_df \
                .write \
                .bucketBy(self.config.partition_size, [self.conventions.ref_group_column_name(), id_column, self.conventions.load_date_column_name()]) \
                .mode('append') \
                .saveAsTable(ref_table_name)
        else:
            new_ref_df.write.mode('append').saveAsTable(ref_table_name)

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

        if self.config.optimize_partitioning:
            staged_df = staged_df.repartition(self.config.partition_size, [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()])
            staged_df \
                .join(sat_df, join_condition, how='left_anti') \
                .write \
                .bucketBy(self.config.partition_size, [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()]) \
                .mode('append').saveAsTable(sat_table_name)
        else:
            staged_df \
                .join(sat_df, join_condition, how='left_anti') \
                .write \
                .mode('append') \
                .saveAsTable(sat_table_name)

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

        if self.config.optimize_partitioning:
            staged_df = staged_df.repartition(self.config.partition_size, [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()])
            staged_df \
                .join(sat_effectivity_df, join_condition, how='left_anti') \
                .write \
                .bucketBy(self.config.partition_size, [self.conventions.hkey_column_name(), self.conventions.load_date_column_name()]) \
                .mode('append').saveAsTable(sat_effectivity_table_name)
        else:
            staged_df \
                .join(sat_effectivity_df, join_condition, how='left_anti') \
                .write \
                .mode('append').saveAsTable(sat_effectivity_table_name)

    def stage_table(self, name: str, source: str, hkey_columns: List[str] = []) -> None: # TODO mw: Multiple HKeys, HDiffs?
        """
        Stages a source table. Additional columns will be created/ calculated and stored in the staging database.

        :param name - The name of the table in the prepared staging area.
        :param source - The source file path, relative to staging_base_path (w/o leading slash).
        :param hkey_columns - Optional. Column names which should be used to calculate a hash key.
        """

        df = self.stage_table_df(source, hkey_columns)

        # write staged table into staging area.
        if self.config.optimize_partitioning and self.conventions.hkey_column_name() in df.columns:
            df \
                .write \
                .bucketBy(self.config.partition_size, self.conventions.hkey_column_name()) \
                .mode('overwrite') \
                .saveAsTable(f'{self.config.staging_prepared_database_name}.{name}')
        else:
            df.write.mode('overwrite').saveAsTable(f'{self.config.staging_prepared_database_name}.{name}')

    def stage_table_df(self, source: str, hkey_columns: List[str] = []) -> DataFrame:
        """
        Stages a source table. Additional columns will be created/ calculated and stored in the staging database.

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

        return df

    def __create_external_table(self, database: str, name: str, columns: List[ColumnDefinition], bucket_columns: List[str] = []) -> None:
        """
        :param database - The name of the database where the table should be created.
        :param name - The name of the table which should be created.
        :param columns - Column definitions for the tables.
        """
        schema = StructType([ StructField(c.name, c.type, c.nullable) for c in columns ])
        df: DataFrame = self.spark.createDataFrame([], schema)

        if self.config.optimize_partitioning and bucket_columns:
            df \
                .write \
                .bucketBy(self.config.partition_size, bucket_columns) \
                .mode('ignore') \
                .saveAsTable(f'{database}.{name}')
        else:
            df.write.mode('ignore').saveAsTable(f'{database}.{name}')
