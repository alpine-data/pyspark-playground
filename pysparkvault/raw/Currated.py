from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from .DataVaultShared import *
from .BusinessVault import *

from typing import Optional

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


class Currated:

    def __init__(
        self, spark: SparkSession, config: BusinessVaultConfiguration, conventions: DataVaultConventions, 
        business_vault: BusinessVault, typelists: TypelistsConfiguration) -> None:
        self.spark = spark

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
            df_user = self.business_vault.join_linked_dataframes(df_user, df_credential, 'LNK__USER__CREDENTIAL', 'USER_HKEY', 'CREDENTIAL_HKEY', True)

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

    def map_to_curated(fields: List[FieldDefinition]) -> DataFrame:
        """
        Hallo Freunde.
        """

        

    def replace_id_with_public_id(self, df: DataFrame, column_name: str, lnk_table_name: str, from_column_name: str, to_column_name: str, to_hub_table_name: str) -> DataFrame:
        df_lnk = self.spark.table(f'{self.config.raw_database_name}.{lnk_table_name}')
        df_to_hub = self.spark.table(f'{self.config.raw_database_name}.{to_hub_table_name}')

        df_lnk = df_lnk \
            .join(df_to_hub, df_lnk[to_column_name] == df_to_hub[self.conventions.hkey_column_name()], how='left') \
            .withColumnRenamed(df_to_hub['PublicID'], column_name)

        reference_column_index = df.columns.index(column_name)
        df_columns = [df[column] for column in df.columns]
        selected_columns = df_columns[:reference_column_index] + [df_lnk[to_column_name]] + df_columns[(reference_column_index+1):]

        return df \
            .join(df_lnk, df[self.conventions.hkey_column_name()] == df_lnk[from_column_name], how='left') \
            .select(selected_columns)
