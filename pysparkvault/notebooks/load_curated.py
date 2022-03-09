spark: SparkSession = None

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# The following cell is acopy from raw.DataVaultShared - Please only make changes there! And copy paste changes here.
#

import pyspark.sql.functions as F

from pyspark.sql import Column, dataframe
from pyspark.sql.types import DataType
from typing import List, Optional

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
        """
        Common conventions including prefixes, naming, etc. for the Data Vault.
        """
        
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

#
#
####################################################################################################################################
####################################################################################################################################
#
#

#
# The following is a copy from raw.BusinessVault. Please make changes only there and copy paste here.
#

import pyspark.sql.functions as F

from pyspark.sql import DataFrame, Column
from pyspark.sql.session import SparkSession
from typing import List, Optional, Union


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

        # TODO mw: Aggregation won't work for not overlapping.

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

#
#
####################################################################################################################################
####################################################################################################################################
#
#

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from typing import Optional, Dict

class FieldDefinition:

    def __init__(
        self, from_table: str, from_field_name: str, to_field_name: Optional[str] = None, 
        is_typelist: bool = False, typelist_table_name: Optional[str] = None,
        foreign_key: bool = False, foreign_key_to_table_name: Optional[str] = None) -> None:

        self.from_table = from_table
        self.from_field_name = from_field_name

        if to_field_name is not None:
            self.to_field_name = to_field_name
        else:
            self.to_field_name = from_field_name

        self.is_typelist = is_typelist
        self.typelist_table_name = typelist_table_name
        self.foreign_key = foreign_key
        self.foreign_key_to_table_name = foreign_key_to_table_name


class TypelistsConfiguration:

    def __init__(
        self, typelists: DataFrame, id_column: str = 'ID', 
        typecode_column: str = 'typecode', name_column: str = 'name', de_column: str = 'L_de') -> None:

        self.typelists = typelists
        self.id_column = id_column
        self.typecode_column = typecode_column
        self.name_column = name_column
        self.de_column = de_column


class CuratedConfiguration(BusinessVaultConfiguration):

    def __init__(self, source_system_name: str, base_path: str) -> None:
        BusinessVaultConfiguration.__init__(self, source_system_name)
        self.base_path = base_path
        self.curated_database_name = f'{self.source_system_name}__curated'.lower()


class Curated:

    def __init__(
        self, spark: SparkSession, config: CuratedConfiguration, 
        business_vault: BusinessVault, typelists: TypelistsConfiguration,
        conventions: DataVaultConventions = DataVaultConventions()) -> None:
        self.spark = spark

        self.business_vault = business_vault
        self.config = config
        self.conventions = conventions
        self.typelists = typelists

    def filter_retired(self, df: DataFrame) -> DataFrame:
        #
        # TODO mw: Should we check for dangling pointers/ related tables? Then this is much more complex!
        # e.g. in that case we could filter retired also while replacing with PublicID ...
        # But also we should remove all "child" entities of the one which is retired - this we cannot do automatically.
        # In that case we need to cascade through child entities. 
        #
        # Another option is to handle the retired field exactly similar like the deleted attribute - Currently under implementation by Jan.
        #

        #
        # Other question: Did I understood it right: We should only exclude the rows which have been retired at the moment of initial load? In that case
        # the story is again quite different ;) 
        #

        if 'retired' in df.columns:
            return df.filter(df['retired'] != 0)
        else:
            return df


    def get_entity_name_from_source_table_name(self, s: str) -> str:
        return s \
            .replace('cc_', '') \
            .replace('cctl_', '') \
            .replace('ccx_', '') \
            .replace('alg_', '') \
            .upper()

    
    def initialize_database(self) -> None:
        spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.config.curated_database_name} LOCATION '{self.config.base_path}'""")


    def join_user_information(self, df: DataFrame, column: Optional[str] = None) -> DataFrame:
        if column is None:
            user_id_columns = list(filter(lambda c: c.endswith('UserID'), df.columns))

            result = df
            for c in user_id_columns:
                result = self.join_user_information(result, c)

            return result
        else:
            columns = df.columns
            column_index = columns.index(column)

            columns = columns[0:column_index] + [f'{column}', f'{column}_UserName'] + columns[column_index + 1:]

            #
            # TODO mw: Store in temp table for performance reasons.
            #
            df_user = self.business_vault.read_data_from_hub('USER', ['ID', 'PublicID'], True)
            df_credential = self.business_vault.read_data_from_hub('CREDENTIAL', ['UserName'], True)
            df_user = self.business_vault.join_linked_dataframes(df_user, df_credential, 'LNK__USER__CREDENTIAL', 'USER_HKEY', 'CREDENTIAL_HKEY')

            df_user = df_user \
                .groupBy('ID') \
                .agg(F.max('$__LOAD_DATE').alias('LD')) \
                .alias('l') \
                .join(df_user.alias('r'), (F.col('l.ID') == F.col('r.ID')) & (F.col('l.LD') == F.col('r.`$__LOAD_DATE`'))) \
                .select('l.ID', 'r.PublicID', 'r.UserName')

            return df \
                .join(
                    df_user \
                        .withColumnRenamed('UserName', f'{column}_UserName') \
                        .withColumnRenamed('PublicID', f'{column}') \
                        .alias('user'), 
                    df[column] == df_user['ID'],
                    how='left') \
                .drop(df[column]) \
                .select(columns)

    def join_typelist(self, df: DataFrame, typelist_reference_column: str, typelist_name: str):
        en_column = f'{typelist_reference_column}_en'
        de_column = f'{typelist_reference_column}_de'

        tl = self.typelists.typelists \
            .select(self.conventions.ref_group_column_name(), self.typelists.id_column, self.typelists.typecode_column, self.typelists.name_column, self.typelists.de_column) \
            .withColumnRenamed(self.typelists.typecode_column, typelist_reference_column) \
            .withColumnRenamed(self.typelists.name_column, en_column) \
            .withColumnRenamed(self.typelists.de_column, de_column)

        reference_column_index = df.columns.index(typelist_reference_column)
        df_columns = [df[column] for column in df.columns]
        selected_columns = df_columns[:reference_column_index] + [tl[typelist_reference_column], tl[en_column], tl[de_column]] + df_columns[(reference_column_index+1):]

        return df \
            .join(
                tl, 
                (df[typelist_reference_column] == tl[self.typelists.id_column]) & \
                (tl[self.conventions.ref_group_column_name()] == typelist_name),
                 how='left') \
            .select(selected_columns)

    def map_to_curated(self, fields: List[FieldDefinition]) -> DataFrame:
        """
        
        """
        root_table_name = fields[0].from_table
        source_table_names = { f.from_table: self.get_entity_name_from_source_table_name(f.from_table) for f in fields }
        source_dataframes: Dict[str, DataFrame] = {}


        for source_table in source_table_names.keys():
            attributes = list([ field.from_field_name for field in fields if field.from_table is source_table ])
            source_dataframes[source_table] = self.business_vault.read_data_from_hub(source_table_names[source_table], attributes, True)

        source_dataframes[root_table_name] = source_dataframes[root_table_name] #\
            #.filter(source_dataframes[root_table_name]['$__HKEY'] == '1bd90a4afba705dcaca2df03a7b1b717')

        #
        # Rename Columns
        #
        for field in fields:
            if field.to_field_name is not field.from_field_name:
                source_dataframes[field.from_table] = source_dataframes[field.from_table] \
                    .withColumnRenamed(field.from_field_name, field.to_field_name)

        #
        # Resolve Public IDs
        #
        for field in fields:
            if field.foreign_key:
                linked_entity = self.get_entity_name_from_source_table_name(field.foreign_key_to_table_name)
                linked_hub = self.business_vault.read_data_from_hub(linked_entity, ['PublicID'], True)

                lnk_name = f'LNK__{source_table_names[field.from_table]}__{linked_entity}'
                lnk_from_hkey = f"{source_table_names[field.from_table]}_HKEY"
                lnk_to_hkey = f"{linked_entity}_HKEY"

                source_dataframes[field.from_table] = self.replace_id_with_public_id(
                    source_dataframes[field.from_table], field.to_field_name, linked_hub, lnk_name,
                    lnk_from_hkey, lnk_to_hkey, 
                    source_dataframes[field.from_table][self.conventions.hkey_column_name()],
                    linked_hub[self.conventions.hkey_column_name()])

        #
        # Join all from different source tables.
        #
        result: DataFrame = source_dataframes[root_table_name]

        for source_table in source_table_names.keys():
            if source_table is not root_table_name:
                link_name = f'LNK__{source_table_names[root_table_name]}__{source_table_names[source_table]}'

                result = self.business_vault \
                    .join_linked_dataframes(
                        result, source_dataframes[source_table], link_name, 
                        lnk_from_hkey_column_name=f"{source_table_names[root_table_name]}_HKEY",
                        lnk_to_hkey_column_name=f"{source_table_names[source_table]}_HKEY",
                        from_df_hkey=source_dataframes[root_table_name][self.conventions.hkey_column_name()],
                        to_df_hkey=source_dataframes[source_table][self.conventions.hkey_column_name()],
                        from_load_date_column=result[self.conventions.load_date_column_name()],
                        from_load_end_date_column=result[self.conventions.load_end_date_column_name()],
                        to_load_date_column=source_dataframes[source_table][self.conventions.load_date_column_name()],
                        to_load_end_date_column=source_dataframes[source_table][self.conventions.load_end_date_column_name()],
                        load_date_column=self.conventions.load_date_column_name(),
                        load_end_date_column=self.conventions.load_end_date_column_name()) \
                    .drop(source_dataframes[source_table][self.conventions.hkey_column_name()])

        #
        # Select all columns
        #
        columns = list([source_dataframes[f.from_table][f.to_field_name] for f in fields])
        result = result.select(*columns)

        #
        # Resolve Typelists
        #
        for field in fields:
            if field.is_typelist:
                result = self.join_typelist(result, field.to_field_name, field.typelist_table_name)

        #
        # Replace UserID columns
        #
        result = self.join_user_information(result)

        return result

    def replace_id_with_public_id(
        self, 
        from_df: DataFrame, 
        column_name: str, 
        to_df: DataFrame,
        lnk_table_name: str, 
        lnk_from_hkey_column_name: str,
        lnk_to_hkey_column_name: str,
        from_df_hkey: Optional[Column],
        to_df_hkey: Optional[Column]) -> DataFrame:
        """
        Replaces a technical ID with the entities Public ID.

        :param df - The data frame containing the data which should be updated.
        :param column_name - The column within df which holds the ID to the other entity.
        :param lnk_table_name - The name of the link table which contains the relations between the two entities.
        :param 
        """

        if from_df_hkey is None:
            from_df_hkey = from_df[self.conventions.hkey_column_name()]

        if to_df_hkey is None:
            to_df_hkey = to_df[self.conventions.hkey_column_name()]

        df_lnk = self.spark.table(f'{self.config.raw_database_name}.{lnk_table_name}')

        df_lnk = df_lnk \
            .join(to_df, df_lnk[lnk_to_hkey_column_name] == to_df_hkey, how='left') \
            .withColumnRenamed('PublicID', column_name) \
            .drop(df_lnk[self.conventions.hkey_column_name()]) \
            .drop(df_lnk[self.conventions.record_source_column_name()]) \
            .select(df_lnk[lnk_from_hkey_column_name], F.col(column_name)) \
            .distinct()

        reference_column_index = from_df.columns.index(column_name)
        df_columns = [from_df[column] for column in from_df.columns]
        selected_columns = df_columns[:reference_column_index] + [df_lnk[column_name]] + df_columns[(reference_column_index+1):]

        return from_df \
            .join(df_lnk, from_df_hkey == df_lnk[lnk_from_hkey_column_name], how='left') \
            .select(selected_columns)


#
#
####################################################################################################################################
####################################################################################################################################
#
#

source_system_name = 'Allegro'
base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/Allegro/currated_test'


config = CuratedConfiguration(source_system_name, base_path)
business_vault = BusinessVault(spark, config)

typelists_table_name = 'REF__TYPELIST'
typelists_active_table_name = 'REF__TYPELIST__ACTIVE'
business_vault.create_active_code_reference_table(typelists_table_name, typelists_active_table_name, 'ID')

df_typelists = spark.table(f'{config.raw_database_name}.{typelists_active_table_name}')
typelists_config = TypelistsConfiguration(df_typelists)
curated_vault = Curated(spark, config, business_vault, typelists_config)
curated_vault.initialize_database()

#
#
####################################################################################################################################
####################################################################################################################################
#
#

df = curated_vault.map_to_curated([
    FieldDefinition('cc_claim', 'PublicID'),
    FieldDefinition('cc_claim', 'UpdateUserID'),
    FieldDefinition('cc_claim', 'Alg_AccidentCode'),
    FieldDefinition('cc_claim', 'Alg_AccidentProtocol'),
    FieldDefinition('cc_claim', 'Alg_AcuteInjuriesDescription', is_typelist=True, typelist_table_name='cctl_alg_acutespecificinjuries'),
    FieldDefinition('cc_claim', 'Alg_CauserDetail', foreign_key=True, foreign_key_to_table_name='ccx_alg_causerdetail'),
    FieldDefinition('cc_claim', 'Alg_HandlingFees', foreign_key=True, foreign_key_to_table_name='ccx_alg_handlingfees'),
    FieldDefinition('ccx_alg_causerdetail', 'CauserName', to_field_name='Alg_CauserDetail_CauserName', is_typelist=True, typelist_table_name='cctl_alg_causer'),
    FieldDefinition('ccx_alg_causerdetail', 'CauserInformation', to_field_name='Alg_CauserDetail_CauserInformation'),
    FieldDefinition('ccx_alg_causerdetail', 'ExaminationDate', to_field_name='Alg_CauserDetail_ExaminationDate'),
    FieldDefinition('ccx_alg_causerdetail', 'VehicularCategory', to_field_name='Alg_CauserDetail_VehicularCategory', is_typelist=True, typelist_table_name='cctl_alg_vehicularcategory'),
    FieldDefinition('ccx_alg_lctfleetquestionaire', 'LossEventFleet', to_field_name='Alg_LCTFleetQuestionaire_LossEventFleet', is_typelist=True, typelist_table_name='cctl_alg_losseventfleet')
])

df.write.mode('overwrite').saveAsTable(f'`{config.curated_database_name}`.`claim`')
