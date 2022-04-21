import json
import click

from enum import Enum
from functional import seq
from pydantic import BaseModel
from sqlalchemy import create_engine, text, inspect, types
from tabulate import tabulate
from typing import List, Optional

from sqlalchemy.dialects.mssql import BIT, BIGINT

class ColumnType(str, Enum):
    """
    Supported column types.
    """

    date = 'date'
    datetime = 'datetime'
    integer = 'integer'
    numeric = 'numeric'
    text = 'text'
    time = 'time'
    timestamp = 'timestamp'
    varchar = 'varchar'
    geography = 'geography'
    boolean = 'boolean'
    long = 'long'


class ColumnReference(BaseModel):
    """
    A wrapper class to specify a referenc to a column of another table.
    """
    table: str
    column: str

class ForeignKey(BaseModel):
    """
    A class to describe a foreign key constraint.
    """
    column: str
    foreignColumn: ColumnReference

class Column(BaseModel):
    """
    A model class to describe a column of a database.
    """

    name: str
    type: ColumnType

    def __str__(self) -> str:
        return f'Column({self.name}, {self.type})'

    def __repr__(self) -> str:
        return self.__str__()

    def __getitem__(self, item):
        return getattr(self, item)

class Table(BaseModel):
    """
    A model class to describe a relational database table.
    """

    name: str
    columns: List[Column]
    
    primaryKey: Optional[List[str]]
    foreignKeys: List[ForeignKey]

    def column(self, column_name: str) -> Column:
        result: Column = seq(self.columns) \
            .filter(lambda c: c.name == column_name) \
            .list()
        if len(result) == 0:
            raise Exception(f'Column {column_name} does not exist in table. Table is:\n\n{self}')
        
        return result[0]

    def get_column_constraints(self, column: str) -> str:
        """
        Returns a short string representation of column constraints of given column.
        """
        s = []

        if self.primaryKey is not None and column in self.primaryKey:
            s.append('PK')

        for fk in self.foreignKeys:
            if column == fk.column:
                s.append(f'FK ({fk.foreignColumn.table}.{fk.foreignColumn.column})')

        return ', '.join(s)

    def get_dependent_tables(self) -> List[str]:
        """
        Returns a list of tables to which this table has dependencies.
        """

        return seq(self.foreignKeys).map(lambda fk: fk.foreignColumn.table)

    def __str__(self) -> str:
        """
        String representation of the column.
        """
        result = self.name + '\n'
        result += ('-' * len(self.name)) + '\n'

        cols = seq(self.columns).map(lambda c: [c.name, c.type, self.get_column_constraints(c.name)])
        result += tabulate(cols, headers=["COLUMN", "TYPE", "CONSTRAINTS"], tablefmt="plain")

        return result

    def __repr__(self) -> str:
        """
        String representation of the column.
        """
        return self.__str__()

class Schema(BaseModel):
    name: str
    tables: List[Table]

    @staticmethod 
    def fromTables(name: str, tables: List[Table]) -> 'Schema':
        all_tables: List[Table] = tables.copy()
        sorted_tables: List[Table] = []

        while len(all_tables) > 0:
            for idx in range(len(all_tables) - 1, -1, -1):
                table = all_tables[idx]
                all_dependencies_defined = True

                for dependency in table.get_dependent_tables():
                    if dependency not in seq(sorted_tables).map(lambda t: t.name):
                        all_dependencies_defined = False

                if all_dependencies_defined:
                    sorted_tables.append(all_tables.pop(idx))

        return Schema(name = name, tables = sorted_tables)

    @staticmethod
    def fromDatabaseConnection(connection_string: str, schema_name: str) -> 'Schema':
        def map_column_type(type) -> ColumnType:
            if isinstance(type, types.INTEGER):
                return ColumnType.integer
            elif isinstance(type, types.NUMERIC):
                return ColumnType.numeric
            elif isinstance(type, types.VARCHAR):
                return ColumnType.varchar
            elif isinstance(type, types.TIMESTAMP):
                return ColumnType.timestamp
            elif isinstance(type, types.NullType):
                return ColumnType.geography
            elif isinstance(type, BIT):
                return ColumnType.boolean
            elif isinstance(type, BIGINT):
                return ColumnType.long
            else:
                return ColumnType.text


        engine = create_engine(connection_string, echo=False, future=True)
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()

        if not schema_name in schemas:
            raise Exception(f'The schema `{schema_name}` does not exist in the database.')

        tables = []

        for schema in schemas:

            if(schema != schema_name):
                continue

            bar_length = len(inspector.get_table_names(schema=schema))

            with click.progressbar(label="Ingesting "+schema, length=bar_length) as bar:
                for table_name in inspector.get_table_names(schema=schema):

                    columns = []
                    foreignKeys = []
                    primaryKey = inspector.get_pk_constraint(table_name, schema=schema)['constrained_columns']

                    if len(primaryKey) == 0:
                        primaryKey = None

                    for column in inspector.get_columns(table_name, schema=schema):
                        columns.append(Column(name=column['name'], type=map_column_type(column['type'])))

                    for fk in inspector.get_foreign_keys(table_name, schema=schema):
                        for fkc in seq(fk['constrained_columns']).zip(fk['referred_columns']):
                            foreignKeys.append(ForeignKey(column=fkc[0], foreignColumn=ColumnReference(table=fk['referred_table'], column=fkc[1])))

                    tbl = Table(name=table_name, columns=columns, primaryKey=primaryKey, foreignKeys=foreignKeys)
                    tables.append(tbl)
                    bar.update(1)

        return Schema.fromTables(name = schema_name, tables = tables)

    def sorted(self) -> "Schema":
        return Schema.fromTables(name=self.name, tables=self.tables)

    def rm_tables(self, remaining_tables : List[str] = None, removing_tables : List[str] = None) -> 'Schema':
        """
        method to remove specific tables or keep specific tables. This method does not check for constraints and can
        lead to error prone schemas
        """
        if remaining_tables:
            new_tables = List[Table]
            for table in self.tables:
                if table.name in remaining_tables:
                    new_tables.append(table)
            self.tables = new_tables
        if removing_tables:
            for table in self.tables:
                if table.name in removing_tables:
                    self.tables.remove(table)
        return self

    def get_table(self, table_name: str) -> Optional[Table]:
        return next(iter([t for t in self.tables if t.name == table_name]), None)

    def get_filtered_tables(self, table_names: List[str]) -> List[Table]:
        return [d for d in self.tables if d.name in table_names]

    def get_tables_by_datatype(self, datatype: ColumnType):
        tables = []

        for table in self.tables:
            for column in table.columns:
                if column.type == datatype:
                    tables.append(table)
                    break

        return tables


    def __str__(self) -> str:
        return '\n\n'.join(seq(self.tables).map(lambda t: t.__str__()))

    def __repr__(self) -> str:
        return self.__str__()

