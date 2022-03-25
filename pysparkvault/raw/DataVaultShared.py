import pyspark.sql.functions as F

from typing import List, Optional

from pyspark.sql import Column
from pyspark.sql.types import DataType


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
    def to_timestamp(column_name: str='load_date', pattern: str="yyyy-MM-dd'T'HH:mm:ss'Z'") -> Column:
        """
        Converts a date str of a format (pattern) to a timestamp column.

        :param column_name - The column which contains the date string.
        :param pattern - The (java.time) pattern of the date string.
        """
        return F.to_timestamp(F.col(column_name), pattern)


class CDCOperations:

    def __init__(self, snapshot = 0, delete = 1, create = 2, before_update = 3, update = 4):
        self.SNAPSHOT = snapshot
        self.DELETE = delete
        self.CREATE = create
        self.BEFORE_UPDATE = before_update
        self.UPDATE = update


class DataVaultConventions:

    def __init__(
        self,  
        column_prefix='$__', 
        hub='HUB__', 
        link='LNK__',
        ref='REF__',
        sat='SAT__',
        pit='PIT__',
        effectivity='EFFECTIVTY_',
        hkey='HKEY',
        hdiff='HDIFF',
        load_date='LOAD_DATE', 
        load_end_date='LOAD_END_DATE',
        cdc_load_date = 'CDC_LOAD_DATE',
        record_source='RECORD_SOURCE',
        ref_group='GROUP',
        deleted='DELETED',
        valid_from='VALID_FROM',
        valid_to='VALID_TO',
        cdc_operation='OPERATION',
        cdc_ops = CDCOperations()) -> None:
        """
        Common conventions including prefixes, naming, etc. for the Data Vault.
        """

        self.COLUMN_PREFIX = column_prefix
        self.HUB = hub
        self.LINK = link
        self.REF = ref
        self.SAT = sat
        self.PIT = pit
        self.EFFECTIVTY = effectivity
        self.HKEY = hkey
        self.HDIFF = hdiff
        self.LOAD_DATE = load_date
        self.LOAD_END_DATE = load_end_date
        self.CDC_LOAD_DATE = cdc_load_date
        self.RECORD_SOURCE = record_source
        self.REF_GROUP = ref_group
        self.DELETED = deleted
        self.VALID_FROM = valid_from
        self.VALID_TO = valid_to
        self.CDC_OPERATION = cdc_operation
        self.CDC_OPS = cdc_ops

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

    def record_source_column_name(self) -> str:
        """
        Return the column name for RECORD_SOURCE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.RECORD_SOURCE}'

    def ref_group_column_name(self) -> str:
        """
        Returns the column name for group column of shared reference tables.
        """
        return f'{self.COLUMN_PREFIX}{self.REF_GROUP}'

    def deleted_column_name(self) -> str:
        """
        Return the column name for DELETED column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.DELETED}'

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

    def cdc_load_date_column_name(self) -> str:
        """
        Return the column name for CDC_LOAD_DATE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.CDC_LOAD_DATE}'

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

    def sat_effectivity_name(self, name: str) -> str:
        """
        Returns a name of a effectivity SAT (satellite) table, based on the base name. This method ensures, that the name is prefixed with the configured
        satellite prefix. If the prefix is already present, it will not be added.
        """
        name = name.upper()

        if name.startswith(f'{self.SAT}{self.EFFECTIVTY}'):
            return name
        else:
            return f'{self.SAT}{self.EFFECTIVTY}{name}'

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

    def remove_source_prefix(name: str):
        """
        Return a table name without its source prefix.
        """
        return name \
            .replace("CC_", "") \
            .replace("CCX_", "") \
            .replace("ALG_", "")

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

    def __init__(self, name: str, hkey_column_name: str, foreign_key: ForeignKey) -> None:
        """
        A value class to specify a linked hub of a Link table.

        :param name - The base name of the hub.
        :param hkey_column_name - The name of the column in the link table.
        :param foreign_key - The foreign key from the Links staging table to the staging table of the linked hub.
        """
        self.name = name
        self.hkey_column_name = hkey_column_name
        self.foreign_key = foreign_key
