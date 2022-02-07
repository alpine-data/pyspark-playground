import pyspark.sql.functions as F
import textwrap

from delta.tables import *
from pyspark.sql import DataFrame, Column
from pyspark.sql.session import SparkSession
from typing import List, Optional, Tuple


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


class LoadRaw:

    # The column prefix is added to data vault specific column names in the staging table.
    DV_COLUMN_PREFIX = 'dv__'

    # constants for common column names
    HKEY = 'hkey'
    HDIFF = 'hdiff'
    LAST_SEEN_DATE = 'last_seen_date'
    LOAD_DATE = 'load_date'
    LOAD_END_DATE = 'load_end_date'
    RECORD_SOURCE = 'record_source'

    # constants for spark datatypes
    STRING_TYPE = "STRING"
    TIMESTAMP_TYPE = "TIMESTAMP"


    def __init__(
        self, spark: SparkSession, load_date: str, 
        source_system_name: str, 
        source_system_short_name: str,
        source_base_path: str, 
        staging_database_path: str,
        raw_database_path: str,
        verbose: bool = False) -> None:
        """
        :param spark - The spark session to use for running transformations.
        :param load_date - The load date/time which is used during the load.
        :param source_system_name - The name of the source system.
        :param source_system_short_name - An abbreviation of the name, used for naming resources. MUST be snake case.

        :param source_base_path - The base path for source files on the data lake (w/o trailing slash).
        :param staging_database_path - The location of the staging database on the data lake.
        :param raw_database_path - The location of the raw database on the data lake.

        :param verbose - If True, result tables will be printed to output.
        """

        self.spark = spark
        self.load_date = load_date
        self.source_system_name = source_system_name
        self.source_system_short_name = source_system_short_name

        self.source_base_path = source_base_path
        self.staging_database_path = staging_database_path
        self.raw_database_path = raw_database_path

        self.verbose = verbose

        #
        # Calculated values
        #
        self.staging_database_name = f'{self.source_system_short_name}__staging'
        self.raw_database_name = f'{self.source_system_short_name}__raw'

    def create_hub(self, name: str, business_key_columns: List[Tuple[str, str]]) -> None:
        """
        Creates a hub table in the raw database. Does only create the table if it does not exist yet.

        :param name - The name of the hub table, usually starting with `HUB__`.
        :param business_key_columns - The columns for the hub are the keys which compose the business key. Tuple contains (name, type).
        """

        columns: List[Tuple[str, str, bool]] = [
            (self.HKEY, self.STRING_TYPE, False),
            (self.LOAD_DATE, self.TIMESTAMP_TYPE, False),
            (self.LAST_SEEN_DATE, self.TIMESTAMP_TYPE, False),
            (self.RECORD_SOURCE, self.STRING_TYPE, False)
        ]

        for col in business_key_columns:
            columns.append((col[0], col[1], True))

        self.__create_table(name, columns)

    def create_link(self, name: str, foreign_hash_key_columns: List[str]) -> None:
        """
        Creates a link table in the raw database. Does only create the table if it does not exist yet.

        :param name - The name of the link table, usually starting with `LNK__`.
        :param foreign_hash_key_columns - The name of the hash key columns pointing to other hubs.
        """

        columns: List[Tuple[str, str, bool]] = [
            (self.HKEY, self.STRING_TYPE, False),
            (self.LOAD_DATE, self.TIMESTAMP_TYPE, False),
            (self.LAST_SEEN_DATE, self.TIMESTAMP_TYPE, False),
            (self.RECORD_SOURCE, self.STRING_TYPE, False)
        ]

        for col in foreign_hash_key_columns:
            columns.append((col, self.STRING_TYPE, True))

        self.__create_table(name, columns)

    def create_satellite(self, name: str, attribute_columns: List[Tuple[str, str]]) -> None:
        """
        Creates a satellite table in the raw database. Does only create the table if it does not exist yet.

        :param name - The name of the satellite table, usually starting with `SAT__`.
        :param attribute_columns - The attributes which should be stored in the satellite.
            :0 The name of the column
            :1 The Spark SQL type of the column.
        """
        columns: List[Tuple[str, str, bool]] = [
            (self.HKEY, self.STRING_TYPE, False),
            (self.HDIFF, self.STRING_TYPE, False),
            (self.LOAD_DATE, self.TIMESTAMP_TYPE, False),
            (self.LOAD_END_DATE, self.TIMESTAMP_TYPE, True)
        ]

        for col in attribute_columns: 
            columns.append((col[0], col[1], True))

        self.__create_table(name, columns)

    def initialize_database(self) -> None:
        """
        Initialize database.
        """

        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.staging_database_name} LOCATION '{self.staging_database_path}'""")
        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.raw_database_name} LOCATION '{self.raw_database_path}'""")

    def load_hub_from_staging_table(self, staging: str, hub: str, business_key_column_names: List[str], satellites: List[Tuple[str, List[str]]]) -> None:
        """
        Loads a hub from a staging table. The staging table must have a HKEY calculated.

        :param staging - The name of the staging table.
        :param hub - The name of the hub table in the raw vault.
        :param business_key_column_names - The list of columns which contribute to the business key of the hub.
        :param satellites - Optional. A list of satellites which is loaded from the link. The form of the tuple is:
            :0 - The name of the satellite table to load.
            :1 - The list of attributes/ columns which should be stored in the satellite.
        """

        #
        # Select relevant columns from staging table.
        #
        common_column_names = [self.HKEY, self.LOAD_DATE, self.LAST_SEEN_DATE, self.RECORD_SOURCE]
        common_column_names_with_prefix = list(map(lambda s: f'{self.DV_COLUMN_PREFIX}{s}', common_column_names))
        all_column_names = common_column_names_with_prefix + business_key_column_names
        all_columns = DataVaultFunctions.to_columns(all_column_names)

        df = self.spark.sql(f'SELECT * FROM {self.staging_database_name}.{staging}').select(all_columns)
        
        for column in common_column_names:
            df = df.withColumnRenamed(f'{self.DV_COLUMN_PREFIX}{column}', column)

        #
        # Merge new data with existing data in Hub.
        #
        self.__update_hub(hub, df)

        #
        # Load satellites.
        #
        for sat in satellites:
            self.load_satellite_from_staging_table(staging, sat[0], sat[1])

    def load_link_from_link_staging_table(self, staging: str, staging_to: List[Tuple[str, str, str, str]], link: str, satellites: List[Tuple[str, List[str]]]) -> None:
        """
        Loads a link with data from a staging table which is a already a link table in the source.

        :param staging - The name of the staging table which contains two or more foreign keys.
        :param staging_to - Information about the linked tables/ hubs.
            :0 The name of the linked staging table
            :1 The name of the FK column in the source staging table
            :2 The name of the FK column in the linked staging table
            :3 The name of the hkey column in the link table
        :param link - The name of the link table to load.
        :param satellites - Optional. A list of satellites which is loaded from the link. The form of the tuple is:
            :0 - The name of the satellite table to load.
            :1 - The list of attributes/ columns which should be stored in the satellite.
        """

        staging_to: List[Tuple[int, Tuple[str, str, str, str]]] = list(enumerate(staging_to))
        hkey_selectors = list(map(lambda t: f't{t[0]}.{self.DV_COLUMN_PREFIX}{self.HKEY} AS {t[1][3]}', staging_to))
        hkey_columns = list(map(lambda t: t[1][3], staging_to))
        
        attr_selectors: List[str] = []
        for sat in satellites:
            for attr in sat[1]:
                attr_selectors.append(f'f.{attr} AS {attr}')
        
        selectors = hkey_selectors + attr_selectors
        selectors = ', '.join(selectors)

        joins = list(map(lambda t: f'JOIN {self.staging_database_name}.{t[1][0]} AS t{t[0]} ON f.{t[1][1]} = t{t[0]}.{t[1][2]}', staging_to))
        joins = ' '.join(joins)

        query = f'SELECT DISTINCT {selectors} FROM {self.staging_database_name}.{staging} AS f {joins}'
        self.load_link_from_query(query, link, hkey_columns, satellites)

    def load_link_from_linked_staging_tables(self, staging_from: str, staging_to: str, fk_column_from: str, fk_column_to: str, hkey_from: str, hkey_to: str, link: str) -> None:
        """
        Loads a link with data from two directly linked tables. 

        :param staging_from - The staging table name for the first table.
        :param staging_to - The staging table name for the second table.
        :param fk_column_from - The column of the foreign key in the first staging table.
        :param fk_column_to - The column in the second table where the foreign key of the first table points to.
        :param hkey_from - The name of the first hash key column in the link table.
        :param hkey_to - The name of the second hash key column in the table.
        :param link - The name of the link table to load.
        :param 
        """

        query = textwrap.dedent(f"""
            SELECT DISTINCT f.{self.DV_COLUMN_PREFIX}{self.HKEY} AS {hkey_from}, t.{self.DV_COLUMN_PREFIX}{self.HKEY} as {hkey_to}
            FROM {self.staging_database_name}.{staging_from} AS f
            JOIN {self.staging_database_name}.{staging_to} AS t ON f.{fk_column_from} = t.{fk_column_to}
            """.strip())

        self.load_link_from_query(query, link, [hkey_from, hkey_to], [])

    def load_link_from_query(self, query: str, link: str, hkey_columns: List[str], satellites: List[Tuple[str, List[str]]]):
        """
        Loads link data from a query which joins data from staging. The function calculates the HKEY for the link based on `hkey_columns`.
        Additionally it also loads the satellites of the link.

        :param query - The Spark SQL query to fetch/ join the data.
        :param link - The name of the link table to load.
        :param hkey_columns - The list of columns which are included in the link, usually they have the form `{hub_name}__hkey`.
        :param satellites - Optional. A list of satellites which is loaded from the link. The form of the tuple is:
            :0 - The name of the satellite table to load.
            :1 - The list of attributes/ columns which should be stored in the satellite.
        """

        df = self.spark.sql(query)
        self.load_link_from_df(df, link, hkey_columns, satellites)
            
    def load_link_from_df(self, df: DataFrame, link: str, hkey_columns: List[str], satellites: List[Tuple[str, List[str]]]):
        """
        Loads link data from a dataframe with already joined data from staging. The function calculates the HKEY for the link based on `hkey_columns`.
        Additionally it also loads the satellites of the link.

        :param df - The dataframe containing the joined data.
        :param link - The name of the link table to load.
        :param hkey_columns - The list of columns which are included in the link, usually they have the form `{hub_name}__hkey`.
        :param satellites - Optional. A list of satellites which is loaded from the link. The form of the tuple is:
            :0 - The name of the satellite table to load.
            :1 - The list of attributes/ columns which should be stored in the satellite.
        """

        #
        # Load link table
        #
        common_column_names = [self.HKEY, self.LOAD_DATE, self.LAST_SEEN_DATE, self.RECORD_SOURCE]
        all_column_names = common_column_names + hkey_columns
        all_columns = DataVaultFunctions.to_columns(all_column_names)

        df = df \
            .withColumn(self.HKEY, DataVaultFunctions.hash(hkey_columns)) \
            .withColumn(f'{self.LOAD_DATE}', F.lit(self.load_date)) \
            .withColumn(f'{self.LAST_SEEN_DATE}', DataVaultFunctions.to_timestamp()) \
            .withColumn(f'{self.LOAD_DATE}', DataVaultFunctions.to_timestamp()) \
            .withColumn(f'{self.RECORD_SOURCE}', F.lit(self.source_system_name)) \
            .select(all_columns)

        self.__update_link(link, df)

        #
        # Load satellites.
        #
        for sat in satellites:
            self.load_satellite_from_df(df, sat[0], sat[1])

    def load_satellite_from_staging_table(self, staging: str, sat: str, attribute_column_names: List[str]):
        """
        Loads a satellite from a staging table. The staging table must have a HKEY calculated.

        :param staging - The name of the staging table.
        :param sat - The name of the satellite table.
        :param attribute_column_names - The list of columns which should be included in the satellite.
        """

        query = f'SELECT * FROM {self.staging_database_name}.{staging}'
        self.load_satellite_from_query(query, sat, attribute_column_names)

    def load_satellite_from_linked_staging_table(self, staging_root: Tuple[str, str], staging_attributes: Tuple[str, str], sat: str, attribute_column_names: List[str]):
        """
        Loads a satellite from two tables. One containing the entity incl. the hash key (staging_root) and the other table contains the
        satellites attributes (staging_attributes).

        :param staging_root - A staging table name which contains the HKey for the satellite.
            :0 The name of the staging table
            :1 The foreign column name
        :param staging_attributes - A staging table which contains the attributes of the sattelite.
            :0 The name of the staging table
            :1 The name of the column name which contains the FK to the staging_root table
        :param sat - The name of the satellite table.
        :param attribute_column_names - The list of columns which should be included in the satellite.
        """


        selectors = [f'r.{self.DV_COLUMN_PREFIX}{self.HKEY}', f'r.{self.DV_COLUMN_PREFIX}{self.LOAD_DATE}']
        selectors = selectors + list(map(lambda c: f'a.{c}', attribute_column_names))
        selectors = ', '.join(selectors)

        query = f"""
            SELECT {selectors} FROM {self.staging_database_name}.{staging_attributes[0]} AS a JOIN {self.staging_database_name}.{staging_root[0]} AS r ON a.{staging_attributes[1]} = r.{staging_root[1]}
            """.strip();

        self.load_satellite_from_query(query, sat, attribute_column_names)

    def load_satellite_from_query(self, query: str, sat: str, attribute_column_names: List[str], hkey_column_names: Optional[List[str]] = None):
        """
        Loads a satellite from a data framed which is loaded with the query.

        :param query - The query to fetch the source date of the satellite.
        :param sat - The name of the satellite table.
        :param attribute_column_names - The attributes which should be inserted in the satellite.
        :param hkey_column_names - Optional. If provided, an HKEY will be calculated based on these columns.
        """

        df = self.spark.sql(query)
        self.load_satellite_from_df(df, sat, attribute_column_names, hkey_column_names)

    def load_satellite_from_df(self, df: DataFrame, sat: str, attribute_column_names: List[str], hkey_column_names: Optional[List[str]] = None):
        """
        Loads a satellite from a data framed which is loaded with the query.

        :param df - The data frame which is acts as input for the satellite.
        :param sat - The name of the satellite table.
        :param attribute_column_names - The attributes which should be inserted in the satellite.
        :param hkey_column_names - Optional. If provided, an HKEY will be calculated based on these columns.
        """

        #
        # Select relevant columns from staging table.
        #
        common_column_names = [self.HKEY, self.HDIFF, self.LOAD_DATE, self.LOAD_END_DATE]
        all_column_names = common_column_names + attribute_column_names
        all_columns = DataVaultFunctions.to_columns(all_column_names)

        for column in [self.HKEY, self.LOAD_DATE]:
            df = df.withColumnRenamed(f'{self.DV_COLUMN_PREFIX}{column}', column)

        if hkey_column_names is not None:
            df = df.withColumn(self.HKEY, DataVaultFunctions.hash(hkey_column_names))

        df = df \
            .withColumn(self.HDIFF, DataVaultFunctions.hash(attribute_column_names)) \
            .withColumn(self.LOAD_END_DATE, F.lit(None)) \
            .select(all_columns)

        #
        # Merge new data with existing data in Satellite.
        #
        self.__update_satellite(sat, df)

    def stage_table(self, name: str, source: str, hkey_columns: List[str] = []) -> None:
        """
        Stages a source table. Additional columns will be created/ calculated and stored in the staging database. 

        :param name - The name of the staged table.
        :param source - The source file path, relative to source_base_path (w/o leading slash).
        :param hkey_columns - Optional. Column names which should be used to calculate a hash key.
        """

        #
        # Load source data from Parquet file.
        #
        df = self.spark.read.load(f'{self.source_base_path}/{source}', format='parquet')

        #
        # Add DataVault specific columns
        #
        df = df \
            .withColumn(f'{self.DV_COLUMN_PREFIX}{self.LOAD_DATE}', F.lit(self.load_date)) \
            .withColumn(f'{self.DV_COLUMN_PREFIX}{self.LAST_SEEN_DATE}', DataVaultFunctions.to_timestamp(f'{self.DV_COLUMN_PREFIX}{self.LOAD_DATE}')) \
            .withColumn(f'{self.DV_COLUMN_PREFIX}{self.LOAD_DATE}', DataVaultFunctions.to_timestamp(f'{self.DV_COLUMN_PREFIX}{self.LOAD_DATE}')) \
            .withColumn(f'{self.DV_COLUMN_PREFIX}{self.RECORD_SOURCE}', F.lit(self.source_system_name))

        if len(hkey_columns) > 0: df = df.withColumn(f'{self.DV_COLUMN_PREFIX}{self.HKEY}', DataVaultFunctions.hash(hkey_columns))

        #
        # Write staged table into staging area.
        #
        df.write.mode('overwrite').saveAsTable(f'{self.staging_database_name}.{name}')

    def __create_table(self, name: str, columns: List[Tuple[str, str, bool]]):
        """
        Helper function to create a Delta Table in raw vault (if it does not exist yet).

        :param name - The name of the table.
        :param columns - The columns of the table (name, spark type, nullable).
        """

        cmd = DeltaTable.createIfNotExists(self.spark).tableName(f'{self.raw_database_name}.{name}')

        for col in columns:
            cmd = cmd.addColumn(col[0], col[1], nullable=col[2])

        cmd.execute()

    def __update_hub(self, name: str, df: DataFrame) -> None:
        """
        Update HUB with new data loaded from staging.

        :param name - The name of the hub table in the raw database.
        :param data - The prepared data. The DataFrame schema must match the Hub table schema.
        """

        table = f'{self.raw_database_name}.{name}'
        map_columns = dict(list(map(lambda c: (c, f'updates.{c}'), df.columns)))

        DeltaTable.forName(self.spark, table) \
            .alias('hub') \
            .merge(df.alias('updates'), f'hub.{self.HKEY} = updates.{self.HKEY}') \
            .whenMatchedUpdate(set = { self.LAST_SEEN_DATE: f'updates.{self.LAST_SEEN_DATE}' }) \
            .whenNotMatchedInsert(values = map_columns) \
            .execute()

        if self.verbose:
            print(textwrap.dedent(f"""
                #
                # Updated `{table}`
                #
                """).strip())

            self.spark.sql(f'SELECT * FROM {table} ORDER BY {self.LOAD_DATE}').show()

    def __update_link(self, name: str, df: DataFrame) -> None:
        """
        Update Link with new data loaded from staging.

        :param name - The name of the link table in the raw database.
        :param data - The perpared data. The DataFrame schema must match the Link table schema.
        """

        table = f'{self.raw_database_name}.{name}'
        map_columns = dict(list(map(lambda c: (c, f'updates.{c}'), df.columns)))

        DeltaTable.forName(self.spark, table) \
            .alias('hub') \
            .merge(df.alias('updates'), f'hub.{self.HKEY} = updates.{self.HKEY}') \
            .whenMatchedUpdate(set = { self.LAST_SEEN_DATE: f'updates.{self.LAST_SEEN_DATE}' }) \
            .whenNotMatchedInsert(values = map_columns) \
            .execute()

        if self.verbose:
            print(textwrap.dedent(f"""
                #
                # Updated `{table}`
                #
                """).strip())

            self.spark.sql(f'SELECT * FROM {table} ORDER BY {self.LOAD_DATE}').show()


    def __update_satellite(self, name: str, df: DataFrame) -> None:
        """
        Update Satellite with new data loaded from staging.

        :param name - The name of the satellite table in the raw database.
        :param data - The prepared data. The DataFrame schema must match the Satellite table schema.
        """

        table = f'{self.raw_database_name}.{name}'
        map_columns = dict(list(map(lambda c: (c, f'updates.{c}'), df.columns)))

        DeltaTable.forName(self.spark, table) \
            .alias('sat') \
            .merge(df.alias('updates'), f'sat.{self.HKEY} = updates.{self.HKEY} and sat.{self.HDIFF} = updates.{self.HDIFF}') \
            .whenNotMatchedInsert(values = map_columns) \
            .execute()

        dfUpdated = self.spark.sql(f'''
            SELECT l.{self.HKEY}, l.{self.HDIFF}, r.{self.LOAD_DATE} FROM {table} AS l 
            FULL OUTER JOIN {table} AS r ON l.{self.HKEY} = r.{self.HKEY}
            WHERE l.{self.LOAD_END_DATE} IS NULL
            AND l.{self.HDIFF} != r.{self.HDIFF}
            AND l.{self.LOAD_DATE} < r.{self.LOAD_DATE}
        ''')

        DeltaTable.forName(self.spark, table) \
            .alias('sat') \
            .merge(dfUpdated.alias('updates'), f'sat.{self.HKEY} = updates.{self.HKEY} and sat.{self.HDIFF} = updates.{self.HDIFF}') \
            .whenMatchedUpdate(set = { 'load_end_date': 'updates.load_date' }) \
            .execute()

        if self.verbose:
            print(textwrap.dedent(f"""
                #
                # Updated `{table}`
                #
                """).strip())

            self.spark.sql(f'SELECT * FROM {table} ORDER BY {self.LOAD_DATE}').show()
