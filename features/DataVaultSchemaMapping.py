import os
import collections
import yaml
import pydantic

from typing import Dict, List, Optional, Set
from enum import Enum
from functional import seq
from pydantic import BaseModel

class SafeLineLoader(yaml.loader.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=True)
        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping

class RawVaultEntity(Enum):
    HUB = "Hub"
    LINK = "Link" # A link which connects two hubs.
    MULTILINK = "Multilink" # A link which connects more than two hubs.
    SATELLITE = "Satellite"
    REFERENCE_TABLE = "Reference Table"


class ForeignKey(BaseModel):
    """
    Wrapper which describes a foreign key relationship in the source system.
    """

    #
    # The name of the table in the source system where the foreign key points to.
    #
    table: str

    #
    # The name of the column in the referenced source table.
    #
    column: str

    def json(self) -> dict:
        return {
            "table": self.table,
            "column": self.column
        }

    @staticmethod
    def from_json(d: dict) -> 'ForeignKey':
        return ForeignKey(**d)


class LinkToDefinition(BaseModel):
    """
    Wrapper class describing a hub participating in a link.
    """

    #
    # The name of the column in the source table where the link is derived from.
    # In the source system, this is the column which defines the foreign key constraint.
    #
    source_column: str

    #
    # The foreign key constraint which is defined on the source table of the link.
    #
    source_foreign_key: ForeignKey

    #
    # The name of the link tables column in the raw vault.
    #
    raw_column: str

    def json(self) -> dict:
        return {
            "source_column": self.source_column,
            "source_foreign_key": self.source_foreign_key.json(),
            "raw_column": self.raw_column
        }

    def from_json(d: dict) -> 'LinkToDefinition':
        custom = {
            "source_foreign_key": ForeignKey.from_json(d["source_foreign_key"])
        }

        return LinkToDefinition(**{**d, **custom})


class Satellite(BaseModel):
    """
    Model class describing the mapping of a satellite to its source tables in the source system.

    A satellite may specify `included_attributes` and `excluded_attributes`, but not both. If none is specified
    the satellite takes all attributes from the source table.

    A satellite must either belong to a `hub` or a `link` may be specified. Thus, exactly one of these fields must be set.
    """

    #
    # The name of the satellite in the raw vault.
    #
    name: str

    #
    # Indicator whether this satellite contains private (sensitive) information.
    #
    private: bool = False

    #
    # The name of the table in the source system from which this satellite is derived.
    #
    source_table: str

    #
    # A list of attributes which should be included in the satellite.
    #
    included_attributes: List[str] = []

    #
    # A list of attributes which should be excluded from the satellite.
    #
    excluded_attributes: List[str] = []

    #
    # The name of the hub this satellite belongs to.
    #
    hub: Optional[str]

    #
    # The name of the link this satellite belongs to.
    #
    link: Optional[str]

    @pydantic.validator('name')
    def check_name(cls, v: str):
        assert v.isidentifier(), '`name` is not allowed to contain special characters.'
        return v

    @pydantic.root_validator
    def check_either_hub_or_link(cls, values: dict) -> dict:
        """
        This method checks that either hub or link are set.

        See https://pydantic-docs.helpmanual.io/usage/validators/ for details on Pydantic validators.
        """

        hub, link = values.get('hub'), values.get('link')

        if hub is not None and link is not None:
            name = values.get('name')
            raise ValueError(f'Either `hub` or `link` can be specified by satellite `{name}`, not both.')
        elif hub is None and link is None:
            name = values.get('name')
            raise ValueError(f'Either `hub` or `link` must be specified by satellite `{name}.`')

        return values

    @pydantic.root_validator
    def check_either_included_or_excluded(cls, values: dict) -> dict:
        """
        This method checks that either included_attributes or excluded_attributes is defined, or both
        values are empty.
        """

        included, excluded = values.get('included_attributes'), values.get('excluded_attributes')

        # TODO: Not sure whether the values are already initialized with defaults or not (empty list vs None)
        # Currently we assume, the defaults are set in values. Thus we'll get empty lists.

        if len(included) > 0 and len(excluded) > 0:
            name = values.get('name')
            raise ValueError(
                f'Either `included_attributes` or `excluded_attributes` can be specified by satellite `{name}`, not both.')
        return values

    def get_selected_attributes(self, available_attributes: List[str]) -> List[str]:
        """
        Returns the selected attributes based on all available attributes and included / excluded attributes.

        :param available_attributes: The List of available attributes.
        :returns: The List of all selected attributes
        """
        if self.included_attributes:
            selected_attributes: List[str] = seq(available_attributes) \
                .filter(lambda c: c in self.included_attributes) \
                .list()
        else:
            selected_attributes: List[str] = seq(available_attributes) \
                .filter(lambda c: c not in self.excluded_attributes) \
                .list()

        return selected_attributes

    def get_reference_name(self) -> str:
        """
        Returns the name of the given reference (hub or link).
        """
        
        if self.hub:
            return self.hub
        else:
            return self.link

    def json(self) -> dict:
        output = {
            "name": self.name,
            "private": self.private,
            "source_table": self.source_table
        }

        if self.hub is not None:
            output["hub"] = self.hub
        else:
            output["link"] = self.link

        if self.included_attributes:
            output["included_attributes"] = self.included_attributes
        elif self.excluded_attributes:
            output["excluded_attributes"] = self.included_attributes

        return output

    def from_json(d: dict) -> 'Satellite':
        return Satellite(**d)


class Hub(BaseModel):
    """
    Model class describing the mapping of a hub to its source tables in the source system.
    """

    #
    # The name of the hub in the raw vault.
    #
    name: str

    #
    # The name of the table in the source system where the hub is derived from.
    #
    source_table: str

    #
    # A list of fields of the source table which compose the business key.
    # This list must not be empty.
    #
    business_keys: List[str]

    #
    # Indicator whether the Hub should be considered as private.
    # That should be the case if the set of business keys contains a private attribute.
    #
    private: bool = False

    @pydantic.validator('name')
    def check_name(cls, v: str):
        assert v.isidentifier(), '`name` is not allowed to contain special characters.'
        return v

    @pydantic.validator('business_keys')
    def check_names_not_empty(cls, v: List[str]):
        assert v is not None and len(v) > 0, '`business_keys` is not allowed to be empty.'
        return v

    def json(self) -> dict:
        return {
            "name": self.name,
            "source_table": self.source_table,
            "business_keys": self.business_keys
        }

    @staticmethod
    def from_json(d: dict) -> 'Hub':
        return Hub(**d)


class Link(BaseModel):
    """
    Model class describing the mapping of a link to its source tables in the source system.
    """

    #
    # The name of the link table in the raw vault.
    #
    name: str

    #
    # The name of the table where this link is derived from.
    #
    source_table: str

    #
    # The name of the link tables column in the raw vault.
    # Used if only one "to" relation is available (optional)
    #
    raw_column: Optional[str] = None

    #
    # Describes the columns of the link table and how they are derived from the source system.
    # This list may not be empty.
    #
    to: List[LinkToDefinition]

    @pydantic.validator('name')
    def check_name(cls, v: str) -> str:
        assert v.isidentifier(), '`name` is not allowed to contain special characters.'
        return v

    @pydantic.validator('to')
    def check_to_definitions_not_empty(cls, v: List[LinkToDefinition]) -> List[LinkToDefinition]:
        assert v is not None and len(v) > 0, '`to` is not allowed to be empty.'
        return v

    @pydantic.root_validator
    def check_raw_column_only_standard_link(cls, values: dict) -> dict:
        if values["raw_column"]:
            assert len(values["to"]) == 1, "raw_column may only be specified for hub-to-hub links"

        return values

    def is_based_on_link_table(self) -> bool:
        """
        Determines whether a link is based on a link table in the source system or not.

        :returns: True if link is based on a link table in the source system, else False.
        """

        if not self.raw_column:
            return True
        else:
            return False

    def get_raw_columns(self) -> List[str]:
        """
        Returns all raw_columns of a link.
        """

        raw_columns = []
        if not self.is_based_on_link_table():
            raw_columns.append(self.raw_column)
        for to in self.to:
            raw_columns.append(to.raw_column)
        return raw_columns

    def get_source_columns(self) -> List[str]:
        """
        Returns all source_columns of a link.
        """

        return [to.source_column for to in self.to]
            
    def json(self) -> dict:
        output = {
            "name": self.name,
            "source_table": self.source_table,
            "to": [to_table.json() for to_table in self.to]
        }
        
        if self.raw_column:
            output["raw_column"] = self.raw_column
        
        return output

    @staticmethod
    def from_json(d: dict) -> 'Link':
        custom = {
            "to": [LinkToDefinition.from_json(to) for to in d["to"]]
        }

        return Link(**{**d, **custom})


class ReferenceTable(BaseModel):
    """
    Model class describing the mapping of a reference table to its source tables in the source system.

    If a reference table definition contains more than one source tables, it is expected that all these tables
    have a common schema. In this case, the reference tables can be merged into one reference table in the raw vault.
    """

    #
    # The name of the reference table in the raw vault.
    #
    name: str

    #
    # The tables in the source system from which the reference table is derived.
    #
    source_tables: List[str]

    #
    # The name of the column in the source table(s) which contains the unique id for each entry.
    # This is typically the column where other table's foreign key constraints point to.
    #
    source_id_column: str

    @pydantic.validator('source_tables')
    def check_source_tables_not_empty(cls, v: List[str]):
        assert v is not None and len(v) > 0, '`source_tables` is not allowed to be empty.'
        return v

    def json(self) -> dict:
        return {
            "name": self.name,
            "source_tables": self.source_tables,
            "source_id_column": self.source_id_column
        }

    def is_code_reference_table(self) -> bool:
        """
        Returns True if the reference table is a code reference table and else False.
        """

        if len(self.source_tables) > 1:
            return True
        else:
            return False

    @staticmethod
    def from_json(d: dict) -> 'ReferenceTable':
        return ReferenceTable(**d)


class DataVaultSchemaMapping(BaseModel):
    """
    A model which describes a mapping from a source system schema to a Data Vault (raw vault) schema.
    """

    #
    # Other schema mappings where the current one depends on. The current mapping can reference 
    # hubs and links, defined in the imported mapping.
    #
    # The key of the map is the (relative) path to the other file, the value is the parsed mapping.
    #
    imports: Dict[str, 'DataVaultSchemaMapping'] = {}

    #
    # A list of hub definitions.
    #
    hubs: List[Hub] = []

    #
    # A list of satellit definitions.
    #
    satellites: List[Satellite] = []

    #
    # A list of link definitions.
    #
    links: List[Link] = []

    #
    # A list of reference table definitions.
    #
    reference_tables: List[ReferenceTable] = []

    @staticmethod
    def from_yaml(file: str) -> 'DataVaultSchemaMapping':
        """
        Creates a new instance from a YAML file which stores the mapping information.

        :param file - The path of the file to load from.
        """

        instance = DataVaultSchemaMapping()
        instance._read_schema_from_yml(file)
        return instance

    def json(self) -> dict:
        """
        Returns dictionary representation of the class.
        """

        output = {
            "imports": [key for key in self.imports.keys()],
            "hubs": [hub.json() for hub in self.hubs],
            "links": [link.json() for link in self.links],
            "satellites": [sat.json() for sat in self.satellites],
            "reference_tables": [ref.json() for ref in self.reference_tables]
        }

        return output

    def to_yml(self, filepath: str):
        """
        Stores the schema mapping in a YAML file - File is overwritten if it exists already.

        :param filepath - The path of the file where schema mapping should be stored.
        """

        with open(filepath, 'w') as outfile:
            yaml.dump(self.json(), outfile)

    def _read_schema_from_yml(self, filepath: str):
        """
        Enriches the current instance with information read from the source file.
        """
        with open(filepath) as mapping_file:
            parsed_mapping = yaml.load(mapping_file, Loader=yaml.FullLoader)

        if 'imports' in parsed_mapping.keys():
            for file in parsed_mapping['imports']:
                imported = DataVaultSchemaMapping.from_yaml(f'{os.path.dirname(os.path.abspath(filepath))}/{file}')
                self.imports[file] = imported

        if 'hubs' in parsed_mapping.keys():
            for hub in parsed_mapping['hubs']:
                self.hubs.append(Hub.from_json(hub))

        if 'links' in parsed_mapping.keys():
            for link in parsed_mapping['links']:
                self.links.append(Link.from_json(link))

        if 'satellites' in parsed_mapping.keys():
            for satellite in parsed_mapping['satellites']:
                self.satellites.append(Satellite.from_json(satellite))

        if 'reference_tables' in parsed_mapping.keys():
            for reference_table in parsed_mapping['reference_tables']:
                self.reference_tables.append(ReferenceTable.from_json(reference_table))

    def flatten(self, _result: Optional['DataVaultSchemaMapping'] = None) -> 'DataVaultSchemaMapping':
        """
        Returns a "flattened" representation of the schema. All imports will be recursively resolved.

        :param _result - The flattened mapping where information is merged. This parameter is set by the recursive call.
        """

        if _result is None:
            _result = DataVaultSchemaMapping()

        for imported_file in self.imports:
            _result = self.imports[imported_file].flatten(_result)

        _result.hubs = _result.hubs + self.hubs
        _result.links = _result.links + self.links
        _result.satellites = _result.satellites + self.satellites
        _result.reference_tables = _result.reference_tables + self.reference_tables

        return _result

    def get_mapped_entity_types_for_source_table(self, source_table: str) -> Set[RawVaultEntity]:
        """
        Returns a list of entity types a source table is mapped to. The output distinguishes between
        links between two hubs (RawVaultEntity.LINK) and links which connect more than two hubs (RawVaultEntity.MULTILINK).

        :param source_table - The name of the table from the source system.
        """

        result: Set[RawVaultEntity] = set()

        #
        # Get all names of source tables used in reference tables and check if current table name is included.
        #
        ref_tables = [source_table for source_tables in self.reference_tables for source_table in source_tables.source_tables]

        if source_table in ref_tables:
            result.add(RawVaultEntity.REFERENCE_TABLE)

        #
        # Get all names of hubs, satellites and links. Then check whether table has been used there.
        #
        hub_tables = [ hub.source_table for hub in self.hubs ]
        sat_tables = [ sat.source_table for sat in self.satellites ]
        lnk_tables = [ lnk.source_table for lnk in self.links if len(lnk.to) == 1 ]
        multilnk_tables = [ lnk.source_table for lnk in self.links if len(lnk.to) > 1 ]

        if source_table in hub_tables:
            result.add(RawVaultEntity.HUB)

        if source_table in sat_tables:
            result.add(RawVaultEntity.SATELLITE)

        if source_table in lnk_tables:
            result.add(RawVaultEntity.LINK)

        if source_table in multilnk_tables:
            result.add(RawVaultEntity.MULTILINK)

        return result

    def get_source_tables(self) -> List[str]:
        """
        Returns all source tables of a schema mapping.
        """
        source_tables = []
        ref_tables = [source_table for source_tables in self.reference_tables for source_table in source_tables.source_tables]
        hub_tables = [ hub.source_table for hub in self.hubs ]
        sat_tables = [ sat.source_table for sat in self.satellites ]
        lnk_tables = [ lnk.source_table for lnk in self.links if len(lnk.to) == 1 ]
        multilnk_tables = [ lnk.source_table for lnk in self.links if len(lnk.to) > 1 ]
        source_tables = source_tables + ref_tables + hub_tables + sat_tables + lnk_tables + multilnk_tables

        return source_tables


    def get_mapped_hub_definition_for_source_table(self, source_table: str) -> Optional[Hub]:
        """
        Returns a hub definition based on the source table, if present.

        :param source_table - The name of the source table from the source system from which hub is derived.
        """

        return next(filter(lambda hub: hub.source_table == source_table, self.hubs), None)
        
    def get_mapped_link_definitions_for_source_table(self, source_table: str) -> List[Link]:
        """
        Returns a list of all link definitons based on the source table.

        :param source_table - The name of the table from the source system.
        """

        return [link for link in self.links if link.source_table == source_table]

    def get_mapped_multilink_definition_for_source_table(self, source_table: str) -> Optional[Link]:
        """
        Returns a multilink definition based on the source table, if existing.
        """

        return next(iter([link for link in self.links if link.source_table == source_table and len(link.to) > 1]), None)

    def get_mapped_reference_table_definition_for_source_table(self, source_table: str) -> Optional[ReferenceTable]:
        """
        Returns a reference table definition, if present, based on the source table.
        """

        return next(filter(lambda ref: source_table in ref.source_tables, self.reference_tables), None)

    def get_mapped_satellites_for_source_table(self, source_table: str) -> List[Satellite]:
        """
        Returns a list of satellite definitions based on the source table.

        :param source_table - The name of the table from the source system.
        """

        return [sat for sat in self.satellites if sat.source_table == source_table]

    def get_satellites_for_hub_or_link(self, reference: str) -> List[Satellite]:
        """
        Returns a list of satellites based on the reference table (hub or link)

        :param reference - The name of hub or link from the raw layer
        """

        return [sat for sat in self.satellites if sat.hub == reference or sat.link == reference]

    def get_satellites_for_hub(self, hub: Hub) -> List[Satellite]:
        """
        Returns a list of satellites based on the hub

        :param hub - Hub object for which to return satellites
        """

        return self.get_satellites_for_hub_or_link(hub.name)

    def get_entity_type_by_name(self, name: str):
        """
        Returns the type of the entity with the given name.

        :param name: The name of the entity.
        """
        hub_tables = [ hub.name for hub in self.hubs ]
        sat_tables = [ sat.name for sat in self.satellites ]
        lnk_tables = [ lnk.name for lnk in self.links if len(lnk.to) == 1 ]
        multilnk_tables = [ lnk.name for lnk in self.links if len(lnk.to) > 1 ]

        if name in hub_tables:
            return RawVaultEntity.HUB

        if name in sat_tables:
            return RawVaultEntity.SATELLITE

        if name in lnk_tables:
            return RawVaultEntity.LINK

        if name in multilnk_tables:
            return RawVaultEntity.MULTILINK

    def get_link_by_name(self, name: str):
        """
        Returns the link with the given name.

        :param name: The name of the link.
        """

        return next(iter([link for link in self.links if link.name == name]), None)

    def get_hub_by_name(self, name: str):
        """
        Returns the hub with the given name.

        :param name: The name of the hub.
        """

        return next(iter([hub for hub in self.hubs if hub.name == name]), None)

    def get_satellite_by_name(self, name: str):
        """
        Returns the satellite with the given name.

        :param name: The name of the satellite.
        """

        return next(iter([sat for sat in self.satellites if sat.name == name]), None)

    def get_reference_table_by_name(self, name: str):
        """
        Returns the reference table with the given name.

        :param name: The name of the reference table.
        """

        return next(iter([ref for ref in self.reference_tables if ref.name == name]), None)

    def validate_(self) -> None:
        """
        Validates the schema mapping. Errors are printed to output.
        """

        # Flatten schema
        # Then execute validations and collect errors (not raise exceptions)
        # Print errors

        # Checks
        #    /- Valid target names (no spaces, special chars, etc.) - This could also be checked with Pydantic Validators.
        #    /- Valid reference to hus/ links in satellites.
        #    /- Valid references to hubs in links
        #    /- Duplications/ Unique names
        #    /- No overlapping attributes between business keys, satellite definitions.
        #    /- Source tables of reference tables are not used in any other definition.
        #    /- No multiple hubs derived from one source satellite.
        #    /- A source table which is used by a link definition with more than one "to"-relations should not also derive a hub.
        #    /- A source table which is used by a link definition with more than one "to"-relation should not be used by any other link as source table.

        # Error message should always contain meaningful values (e.g. names of entities, source tables, etc...)
        # Optional: Very nice, not easy, but possible: Indicate potential wrong files and lines

        flattened_schema = self.flatten()
        validation_errors = flattened_schema._validate_hubs() + flattened_schema._validate_links() \
                            + flattened_schema._validate_sats() + flattened_schema._validate_reference_table()
        for i in validation_errors:
            print(i)

    def merge_schema_mappings(self, schema_mappings: List['DataVaultSchemaMapping']) -> 'DataVaultSchemaMapping':
        for schema_mapping in schema_mappings:
            self.imports |= schema_mapping.imports
            self.hubs += schema_mapping.hubs
            self.satellites += schema_mapping.satellites
            self.links += schema_mapping.links
            self.reference_tables += schema_mapping.reference_tables
        return self

    def _validate_hubs(self) -> List[str]:
        """
        validates the hubs in the schema. Validation rules are:
        - All hubs should have unique names.
        - Business keys of a hub should not be included as attributes in a corresponding satellite.
        - No multiple hubs derived from one source satellite.
        :return: List of error messages
        """
        err = []

        # All hubs should have unique names
        duplicates = self._validate_names([hub.name for hub in self.hubs])
        if len(duplicates) >= 1:
            for dup in duplicates:
                err.append(f'The hub `{dup}` is defined more than once in the schema mapping.')

        # Business keys of a hub should not be included as attributes in a corresponding satellite.
        for hub in self.hubs:
            err = err + self._validate_hub(hub)

        # No multiple hubs derived from one source satellite.
        hub_sources = [(hub.source_table, hub.name) for hub in self.hubs]
        multiple_sources = [source_table for source_table, count in collections.Counter(hub_sources).items() if
                            count > 1]
        for multiple_source in multiple_sources:
            mapped_hubs = [hub for source, hub in hub_sources if source == multiple_source]
            err.append(f'The source table named `{multiple_source}` is mapped to multiple hubs: `{mapped_hubs}`.')

        return err

    def _validate_hub(self, hub:Hub) -> List[str]:
        """
        - Business keys of a hub should not be included as attributes in a corresponding satellite.

        """
        err = []
        sats = self.get_satellites_for_hub_or_link(hub.name)
        bks = hub.business_keys
        for sat in sats:
            bk_sats = [(bk, sat) for bk in bks if bk in sat.included_attributes]
            for bk_sat in bk_sats:
                err.append(f'The hub `{hub.name}` defines the business key `{bk_sat[0]}`, which also occurs as '
                           f'an attribute in the satellite `{bk_sat[1].name}`.')

        return err

    def _validate_links(self) -> List[str]:
        """
        validates the links in the schema. Validation rules are:
        - All hubs should have unique names.
        - The referenced hubs in the link must exist in the schema.
        - A source table which is used by a link definition with more than one "to"-relations should not also derive a
        hub.
        - A source table which is used by a link definition with more than one "to"-relation should not be used by any
        other link as source table.
        :return: List of error messages
        """
        err = []

        # check for duplicate names
        duplicates = self._validate_names([lnk.name for lnk in self.links])
        if len(duplicates) >= 1:
            for dup in duplicates:
                err.append(f'The link `{dup}` is defined more than once in the schema mapping.')

        for lnk in self.links:
            err = err + self._validate_link(lnk)

        return err

    def _validate_link(self, lnk: Link) -> List[str]:

        err = []

        # Validation of references to hubs in links.
        connected_source_tables = [ref.source_foreign_key.table for ref in lnk.to]
        connected_source_tables.append(lnk.source_table)
        hub_sources = [hub.source_table for hub in self.hubs]
        for tab in connected_source_tables:
            if tab not in hub_sources:
                err.append(
                    f'The link `{lnk.name}`, originated from `{lnk.source_table} ` references to the source table `{tab}`, which cannot be found as a hub in the schema.')

        # A source table which is used by a link definition with more than one "to"-relations should not also derive a
        # hub.
        # A source table which is used by a link definition with more than one "to"-relation should not be used by any
        # other link as source table.
        if len(lnk.to) >= 2:
            mapped_entities = self.get_mapped_entity_types_for_source_table(lnk.source_table)
            if mapped_entities is not None and RawVaultEntity.HUB in mapped_entities:
                err.append(f'The source table `{lnk.source_table}`" is mapped to the link `{lnk.name}` and a '
                           f'hub, although it is already a link-table in the source system.')
            lnks_from_source = [l.name for l in self.links if l.source_table == lnk.source_table]
            if len(lnks_from_source) >= 2:
                err.append(
                    f'The source table `{lnk.source_table}` is mapped to multiple links `{lnks_from_source}`, '
                    f'although it is already a link-table in the source system.')

        return err

    def _validate_sats(self) -> List[str]:
        """
        validates the satellites in the schema. Validation rules are:
        - All satellites should have unique names.
        - The corresponding hubs/links of the satellite must exist in the schema.
        :return: List of error messages
        """
        err = []

        # check for duplicate names
        duplicates = self._validate_names([sat.name for sat in self.satellites])
        if len(duplicates) >= 1:
            for dup in duplicates:
                err.append(f'The satellite `{dup}` is defined more than once in the schema mapping.')

        # check whether all satellites derived from the same source table point to the same hub/ link.

        for sat in self.satellites:
            err = err + self._validate_sat(sat)
        return err

    def _validate_sat(self, sat: Satellite) -> List[str]:
        """
        validates a satellite
        - the hubs/links of the satellite must exist in the schema.
        :return: List of error messages
        """
        err = []
        if not (sat.hub in [hub.name for hub in self.hubs]) or not (sat.link in [link.name for link in self.links]):
            if sat.hub is not None:
                reference = sat.hub
            else:
                reference = sat.link
            err.append(
                f'The satellite `{sat.name}`, originated from `{sat.source_table} ` references to the raw table `{reference}`'
                f', which cannot be found in a schema.')
        return err

    def _validate_reference_table(self) -> List[str]:
        """
        validates the reference_tables in the schema. Validation rules are:
        - All satellites should have unique names.
        - Source tables of reference tables are not used in any other definition.
        :return: List of error messages
        """
        err = []

        # check for duplicate names
        duplicates = self._validate_names([sat.name for sat in self.satellites])
        if len(duplicates) >= 1:
            for dup in duplicates:
                err.append(f'The reference table `{dup}` is defined more than once in the schema mapping.')

        # Source tables of reference tables are not used in any other definition.
        non_rt_entity_sources = [(hub.source_table, hub.name) for hub in self.hubs] + [(lnk.source_table, lnk.name) for
                                                                                       lnk in self.links] + [
                                    (sat.source_table, sat.name) for sat in self.satellites]
        for source, name in non_rt_entity_sources:
            for rt in self.reference_tables:
                if source in rt.source_tables:
                    err.append(f'The source table `{source}` is mapped to the reference table `{rt.name}` and to '
                               f'the raw table `{name}`.')

        # All source tables of one reference table must have same schema
        # TODO implement this check.

        return err

    @staticmethod
    def _validate_names(names: List[str]) -> List[str]:
        return [item for item, count in collections.Counter(names).items() if count > 1]

    def _find_in_yaml(self, key:str, value:str, hub:Hub):
        return []

if __name__ == '__main__':
    mappingschema = DataVaultSchemaMapping()
    test = mappingschema.from_yaml("../../sample_mapping_test.yml")
    test.validate_()
    test = []
    with open("../../sample_mapping_test.yml") as mapping_file:
        counter = 1
        for line in mapping_file:
            test.append((counter, yaml.load(line, Loader=yaml.FullLoader)))
            counter += 1
    key = "business_keys"
    value = "id"


