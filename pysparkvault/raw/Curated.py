from traceback import print_tb
from typing import Optional, Dict

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from .BusinessVault import *
from .DataVaultShared import *


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
        """
        Filters rows from given DataFrame based on the retired column.

        :param df - The DataFrame to be filtered.
        """

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


    def get_entity_name_from_source_table_name(self, source_table_name: str) -> str:
        """
        Returns the name of an entity based on the source table name.

        :param source_table_name - The name of the source table.
        """

        return source_table_name \
            .replace('cc_', '') \
            .replace('cctl_', '') \
            .replace('ccx_', '') \
            .replace('alg_', '') \
            .upper()

    
    def initialize_database(self) -> None:
        """
        Initializes the database if not already exiting.
        """

        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.config.curated_database_name} LOCATION '{self.config.base_path}'""")


    def join_user_information(self, df: DataFrame, column: Optional[str] = None) -> DataFrame:
        """
        Joins a given DataFrame with user tables.

        :param df - The DataFrame to be joined with user tables.
        :param column - The UserID column.
        """

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
        """
        Joins a given DataFrame with a typelist.

        :param df - The DataFrame to be joined with the typelist.
        :param typelist_reference_column - The column in the DataFrame that references the typelist.
        :param typelist_name - The name of the typelist.
        """

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
        Maps defined FieldDefinitions to a curated data view.

        :param fields - The list of FieldDefinitions to be mapped.
        """

        root_table_name = fields[0].from_table
        source_table_names = { f.from_table: self.get_entity_name_from_source_table_name(f.from_table) for f in fields }
        source_dataframes: Dict[str, DataFrame] = {}

        for source_table in source_table_names.keys():
            attributes = list([ field.from_field_name for field in fields if field.from_table is source_table ])
            source_dataframes[source_table] = self.business_vault.read_data_from_hub(source_table_names[source_table], attributes, True)

        # rename Columns
        for field in fields:
            if field.to_field_name is not field.from_field_name:
                source_dataframes[field.from_table] = source_dataframes[field.from_table] \
                    .withColumnRenamed(field.from_field_name, field.to_field_name)

        # resolve Public IDs
        for field in fields:
            if field.foreign_key:
                linked_entity = self.get_entity_name_from_source_table_name(field.foreign_key_to_table_name)
                linked_hub = self.conventions.hub_name(linked_entity)
                linked_hub = self.business_vault.read_data_from_hub(source_table_names[source_table], ['PublicID'], True)

                # jb: who defines this naming convention?
                lnk_name = f'LNK__{source_table_names[field.from_table]}__{linked_entity}'
                lnk_from_hkey = f"{source_table_names[field.from_table]}_HKEY"
                lnk_to_hkey = f"{linked_entity}_HKEY"

                source_dataframes[field.from_table] = self.replace_id_with_public_id(
                    source_dataframes[field.from_table], field.to_field_name, linked_hub, lnk_name,
                    lnk_from_hkey, lnk_to_hkey, 
                    source_dataframes[field.from_table][self.conventions.hkey_column_name()],
                    linked_hub[self.conventions.hkey_column_name()])

        # join all from different source tables.
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

        # select all columns
        # columns = list([source_dataframes[f.from_table][f.to_field_name] for f in fields])
        # result = result.select(*columns)

        # resolve Typelists
        for field in fields:
            if field.is_typelist:
                result = self.join_typelist(result, field.to_field_name, field.typelist_table_name)

        # replace UserID columns
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

        :param from_df - The DataFrame containing the data which should be updated.
        :param column_name - The column within the DataFrame that holds the ID to the other entity.
        :param to_df - The DataFrame of the linked entity.
        :param lnk_table_name - The name of the link table which contains the relations between the two entities.
        :param lnk_from_hkey_column_name - The name of the origin HKEY column in the link table.
        :param lnk_to_hkey_column_name - The name of the target HKEY column in the link table.
        :param from_df_hkey - The HKEY column of the origin DataFrame.
        :param to_df_hkey - The HKEY column of the target DataFrame.
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
