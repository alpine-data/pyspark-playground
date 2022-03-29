import pytest

from datetime import datetime, timedelta
from os import listdir
from typing import ByteString, List, TypedDict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

from pysparkvault.raw.RawVault import *
from pysparkvault.raw.BusinessVault import *
from pysparkvault.raw.Curated import *
from pysparkvault.raw.DataVaultShared import *


# global variables
SOURCE_TABLE_NAME_MOVIES = "MOVIES"
SOURCE_TABLE_NAME_ACTORS = "ACTORS"
SOURCE_TABLE_NAME_DIRECTORS = "DIRECTORS"
SOURCE_TABLE_NAME_CASTINGS = "CASTINGS"

STAGING_TABLE_NAME_MOVIES = f"STG__{SOURCE_TABLE_NAME_MOVIES}"
STAGING_TABLE_NAME_ACTORS = f"STG__{SOURCE_TABLE_NAME_ACTORS}"
STAGING_TABLE_NAME_DIRECTORS = f"STG__{SOURCE_TABLE_NAME_DIRECTORS}"
STAGING_TABLE_NAME_CASTINGS = f"STG__{SOURCE_TABLE_NAME_CASTINGS}"

TYPELISTS_TABLE_NAME = 'TYPELIST'
TYPELISTS_ACTIVE_TABLE_NAME = 'TYPELIST__ACTIVE'

LINK_TABLE_NAME_MOVIES_DIRECTORS = f"{SOURCE_TABLE_NAME_MOVIES}__{SOURCE_TABLE_NAME_DIRECTORS}"

HKEY_COLUMNS_MOVIES = ["PublicID"]
HKEY_COLUMNS_ACTORS = ["PublicID"]
HKEY_COLUMNS_DIRECTORS = ["PublicID"]
HKEY_COLUMNS_CASTINGS = ["MOVIE_ID", "ACTOR_ID"]

DV_CONV = DataVaultConventions()
SOURCE_SYSTEM_NAME = "Test"
STAGING_BASE_PATH = "./staging"
STAGING_PREPARED_BASE_PATH = "./staging_prepared"
RAW_BASE_PATH = "./raw"

T_0 = datetime.now()
T_1 = T_0 + timedelta(days=1)
T_2 = T_1 + timedelta(hours=3, minutes=17)
T_3 = T_2 + timedelta(minutes=42) 
T_4 = T_3 + timedelta(hours=6)
T_5 = T_1 + timedelta(days=1, minutes=42)


class LoadedTables(TypedDict):
    movies: DataFrame
    actors: DataFrame
    directors: DataFrame
    castings: DataFrame


def create_sample_data(spark: SparkSession) -> List[LoadedTables]:
    """
    Creates some sample test data. The return list is a list of loaded cdc batches from the source tables.

    :param spark - The active spark session.
    """

    # define schemas
    schema_movies = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("PublicID", StringType(), False),
        StructField("NAME", StringType(), False),
        StructField("YEAR", IntegerType(), False),
        StructField("DIRECTOR_ID", IntegerType(), True),
        StructField("RATING", DoubleType(), False),
        StructField("RANK", IntegerType(), False)
    ])

    schema_actors = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("PublicID", StringType(), False),
        StructField("NAME", StringType(), False),
        StructField("COUNTRY", StringType(), False)
    ])

    schema_directors = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("PublicID", StringType(), False),
        StructField("NAME", StringType(), False),
        StructField("COUNTRY", StringType(), False)
    ])

    schema_castings = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("MOVIE_ID", StringType(), False),
        StructField("ACTOR_ID", StringType(), False)
    ])

    # define data
    movies = [
        [
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, "The Shawshank Redemption", 1994, 1, 9.3, 64),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, "The Godfather", 1972, 2, 9.2, 94),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 3, "The Dark Knight", 2008, 3, 9.0, 104),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, "Star Wars: Episode V", 1980, 4, 8.7, 485)
        ],
        [
            (DV_CONV.CDC_OPS.CREATE, T_2, 5, "Pulp Fiction", 1994, 5, 8.9, 138),
            (DV_CONV.CDC_OPS.CREATE, T_2, 6, "Schindler's List", 1993, 6, 8.6, 145),
            (DV_CONV.CDC_OPS.CREATE, T_2, 7, "Inception", 2010, 7, 8.3, 210),
            (DV_CONV.CDC_OPS.DELETE, T_2, 3, "The Dark Knight", 2008, 3, 9.0, 104),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_3, 4, "Star Wars: Episode V", 1980, 4, 8.7, 485),
            (DV_CONV.CDC_OPS.UPDATE, T_3, 4, "Star Wars: Episode V", 1980, 4, 8.4, 500),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_3, 1, "The Shawshank Redemption", 1994, 1, 9.3, 64),
            (DV_CONV.CDC_OPS.UPDATE, T_3, 1, "The Shawshank Redemption", 1994, None, 9.2, 67),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_4, 2, "The Godfather", 1972, 2, 9.2, 94),
            (DV_CONV.CDC_OPS.UPDATE, T_4, 2, "The Godfather", 1972, 2, 9.1, 96),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_4, 6, "Schindler's List", 1993, 6, 8.6, 145),
            (DV_CONV.CDC_OPS.UPDATE, T_4, 6, "Schindler's List", 1993, None, 8.8, 125),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_4, 1, "The Shawshank Redemption", 1994, None, 9.2, 67),
            (DV_CONV.CDC_OPS.UPDATE, T_4, 1, "The Shawshank Redemption", 1994, 1, 9.6, 2),
            (DV_CONV.CDC_OPS.CREATE, T_4, 3, "The Dark Knight", 2008, 3, 9.0, 104),
            (DV_CONV.CDC_OPS.DELETE, T_4, 4, "Star Wars: Episode V", 1980, 4, 8.4, 500)
        ],
        [
            (DV_CONV.CDC_OPS.DELETE, T_5, 3, "The Dark Knight", 2008, 3, 9.0, 104),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_5, 2, "The Godfather", 1972, 2, 9.1, 96),
            (DV_CONV.CDC_OPS.UPDATE, T_5, 2, "The Godfather", 1972, 3, 8.9, 103),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_5, 6, "Schindler's List", 1993, None, 8.8, 125),
            (DV_CONV.CDC_OPS.UPDATE, T_5, 6, "Schindler's List", 1993, 6, 8.3, 210),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_5, 1, "The Shawshank Redemption", 1994, 1, 9.6, 2),
            (DV_CONV.CDC_OPS.UPDATE, T_5, 1, "The Shawshank Redemption", 1994, None, 9.5, 3)
        ]
    ]

    actors = [
        [
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, "Tim Robbins", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, "Morgan Freeman", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 3, "Bob Gunton", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, "William Sadler", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 5, "Marlon Brando", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 6, "Al Pacino", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 7, "James Caan", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 8, "Christian Bale", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 9, "Heath Ledger", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 10, "Mark Hamill", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 11, "Harrison Ford", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 12, "Carrie Fisher", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 13, "Robert Duvall", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 14, "John Marley", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 15, "Gary Oldman", "USA")
        ],
        [
            (DV_CONV.CDC_OPS.CREATE, T_2, 16, "John Travolta", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 17, "Liam Neeson", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 18, "Ralph Fiennes", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 19, "Ben Kingsley", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 20, "Leonardo DiCaprio", "USA"),
            (DV_CONV.CDC_OPS.DELETE, T_4, 13, "Robert Duvall", "USA")
        ],
        [
            (DV_CONV.CDC_OPS.DELETE, T_5, 14, "John Marley", "USA")
        ]
    ]

    directors = [
        [
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, "Frank Darabont", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, "Francis Ford Coppola", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 3, "Christopher Nolan", "USA"),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, "Irvin Kershner", "USA")
        ],
        [
            (DV_CONV.CDC_OPS.CREATE, T_2, 5, "Quentin Terintino", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 6, "Steven Spielberg", "USA"),
            (DV_CONV.CDC_OPS.CREATE, T_2, 7, "Christopher Nolan", "USA"),
        ],
        [
        ]
    ]

    castings = [
        [
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, 1),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, 2),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, 3),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 1, 4),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, 5),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, 6),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 2, 7),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 3, 8),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 3, 9),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, 10),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, 11),
            (DV_CONV.CDC_OPS.SNAPSHOT, T_0, 4, 12)
        ],
        [
            (DV_CONV.CDC_OPS.DELETE, T_1, 1, 1),
            (DV_CONV.CDC_OPS.DELETE, T_1, 1, 2),
            (DV_CONV.CDC_OPS.CREATE, T_2, 5, 16),
            (DV_CONV.CDC_OPS.CREATE, T_2, 6, 17),
            (DV_CONV.CDC_OPS.CREATE, T_2, 6, 18),
            (DV_CONV.CDC_OPS.CREATE, T_2, 6, 19),
            (DV_CONV.CDC_OPS.CREATE, T_2, 7, 20),
            (DV_CONV.CDC_OPS.BEFORE_UPDATE, T_3, 7, 20),
            (DV_CONV.CDC_OPS.UPDATE, T_3, 7, 19),
            (DV_CONV.CDC_OPS.CREATE, T_3, 1, 1),
        ],
        [
            (DV_CONV.CDC_OPS.CREATE, T_5, 7, 19)
        ]
    ]

    # returns a list that contains a LoadedTables object for each batch
    return [
        LoadedTables(
            movies=spark.createDataFrame(movies[i], schema_movies),
            actors=spark.createDataFrame(actors[i], schema_actors),
            directors=spark.createDataFrame(directors[i], schema_directors),
            castings=spark.createDataFrame(castings[i], schema_castings)
        )
        for i in range(len(movies))
    ]


def write_parquet_files(data: List[LoadedTables], staging_base_path: str) -> None:
    """
    Writes a list of LoadedTables objects to .parquet files at the given path.

    :param data - The list of LoadedTables objects.
    :param staging_base_path - The staging path that defines where to store the .parquet files.
    """

    for i, batch in enumerate(data):
        batch["movies"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_MOVIES}_{i}.parquet")
        batch["actors"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_ACTORS}_{i}.parquet")
        batch["directors"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_DIRECTORS}_{i}.parquet")
        batch["castings"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_CASTINGS}_{i}.parquet")


def stage_tables(spark: SparkSession, raw_vault: RawVault, staging_base_path: str) -> None:
    """
    Stages all source tables from the given path.

    :param raw_vault - The RawVault object needed for calling the stage_table(...) function.
    :param staging_base_path - The path of the source tables.
    """

    number_of_batches = 0

    for f in listdir(staging_base_path):
        # remove the file ending and extract the batch number
        i = int(f.replace(".parquet", "").split("_")[-1])
        if i > number_of_batches : number_of_batches = i
        if SOURCE_TABLE_NAME_MOVIES in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_MOVIES}_{i}", f, HKEY_COLUMNS_MOVIES)
        elif SOURCE_TABLE_NAME_ACTORS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_ACTORS}_{i}", f, HKEY_COLUMNS_ACTORS)
        elif SOURCE_TABLE_NAME_DIRECTORS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_DIRECTORS}_{i}", f, HKEY_COLUMNS_DIRECTORS)
        elif SOURCE_TABLE_NAME_CASTINGS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_CASTINGS}_{i}", f, HKEY_COLUMNS_CASTINGS)

    for j in range(int(number_of_batches + 1)):
        movies_df = spark.table(f"{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{j}")
        actors_df = spark.table(f"{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{j}")
        directors_df = spark.table(f"{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_{j}")
        castings_df = spark.table(f"{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{j}")
        movies_df.write.mode('append').saveAsTable(f'{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_FULL')
        actors_df.write.mode('append').saveAsTable(f'{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_FULL')
        directors_df.write.mode('append').saveAsTable(f'{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_FULL')
        castings_df.write.mode('append').saveAsTable(f'{raw_vault.config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_FULL')

            
def create_data_vault_tables(raw_vault: RawVault) -> None:
    """
    Creates empty tables for hubs, links, and satellites.

    :param raw_vault - The RawVault object needed for creating hubs, links, and satellites.
    """

    # create hubs
    raw_vault.create_hub(SOURCE_TABLE_NAME_MOVIES, [
        ColumnDefinition("PublicID", StringType())
    ])

    raw_vault.create_hub(SOURCE_TABLE_NAME_ACTORS, [
        ColumnDefinition("PublicID", StringType())
    ])

    raw_vault.create_hub(SOURCE_TABLE_NAME_DIRECTORS, [
        ColumnDefinition("PublicID", StringType())
    ])

    # create links
    raw_vault.create_link(SOURCE_TABLE_NAME_CASTINGS, ["MOVIES_HKEY", "ACTORS_HKEY"])

    # create links
    raw_vault.create_link(LINK_TABLE_NAME_MOVIES_DIRECTORS, ["MOVIES_HKEY", "DIRECTORS_HKEY"])

    # create satellites
    raw_vault.create_satellite(SOURCE_TABLE_NAME_MOVIES, [
        ColumnDefinition("NAME", StringType()),
        ColumnDefinition("YEAR", IntegerType()),
        ColumnDefinition("DIRECTOR_ID", StringType()),
        ColumnDefinition("RATING", DoubleType()),
        ColumnDefinition("RANK", IntegerType())
    ])

    raw_vault.create_satellite(SOURCE_TABLE_NAME_ACTORS, [
        ColumnDefinition("NAME", StringType()),
        ColumnDefinition("COUNTRY", StringType())
    ])

    raw_vault.create_satellite(SOURCE_TABLE_NAME_DIRECTORS, [
        ColumnDefinition("NAME", StringType()),
        ColumnDefinition("COUNTRY", StringType())
    ])


def load_from_prepared_staging_table(raw_vault: RawVault, batch: int) -> None:
    """
    Loads cdc data into hubs, links, and satellites.

    :param raw_vault - The RawVault object needed for loading cdc data into hubs, links, and satellites.
    :param batch - The number of the batch to be loaded.
    """

    # load hubs and satellites
    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_MOVIES}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES), HKEY_COLUMNS_MOVIES, 
        # [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES), ['PublicID', 'RATING', 'RANK'])])
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES), ['NAME', 'YEAR', 'DIRECTOR_ID', 'RATING', 'RANK'])])

    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_ACTORS}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS), HKEY_COLUMNS_ACTORS, 
        # [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS), ['PublicID', 'COUNTRY'])])
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS), ['NAME', 'COUNTRY'])])

    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_DIRECTORS}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS), HKEY_COLUMNS_DIRECTORS, 
        # [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_DIRECTORS), ['PublicID', 'COUNTRY'])])
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_DIRECTORS), ['NAME', 'COUNTRY'])])

    # load links
    raw_vault.load_link_from_prepared_stage_table(
        f"{STAGING_TABLE_NAME_CASTINGS}_{batch}", 
        [
            LinkedHubDefinition(SOURCE_TABLE_NAME_MOVIES, "MOVIES_HKEY", ForeignKey("MOVIE_ID", ColumnReference(f"{STAGING_TABLE_NAME_MOVIES}_{batch}", "PublicID"))),
            LinkedHubDefinition(SOURCE_TABLE_NAME_ACTORS, "ACTORS_HKEY", ForeignKey("ACTOR_ID", ColumnReference(f"{STAGING_TABLE_NAME_ACTORS}_{batch}", "PublicID")))
        ], DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS), None)
    
    raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables(f"{STAGING_TABLE_NAME_MOVIES}_{batch}",
        ForeignKey("DIRECTOR_ID", ColumnReference(SOURCE_TABLE_NAME_DIRECTORS, "PublicID")), LINK_TABLE_NAME_MOVIES_DIRECTORS, "MOVIES_HKEY", "DIRECTORS_HKEY")


def create_pit_tables(business_vault: RawVault) -> None:
    """
    Creates PIT tables for all satellites.

    :param raw_vault - The RawVault object needed for creating PIT tables.
    """

    business_vault.create_point_in_time_table_for_single_satellite(SOURCE_TABLE_NAME_MOVIES, SOURCE_TABLE_NAME_MOVIES)
    business_vault.create_point_in_time_table_for_single_satellite(SOURCE_TABLE_NAME_ACTORS, SOURCE_TABLE_NAME_ACTORS)
    business_vault.create_point_in_time_table_for_single_satellite(SOURCE_TABLE_NAME_DIRECTORS, SOURCE_TABLE_NAME_DIRECTORS)


def create_reference_tables(spark: SparkSession, business_vault: RawVault) -> None: 
    schema_typelist = StructType([
        StructField(DV_CONV.load_date_column_name(), TimestampType(), False),
        StructField(DV_CONV.ref_group_column_name(), StringType(), False),
        StructField("PublicID", StringType(), False),
        StructField("DESCRIPTION", StringType(), False)
    ])

    data = [
        (T_0, "test_group_1", "1", "A"),
        (T_0, "test_group_1", "2", "B"),
        (T_0, "test_group_1", "3", "C"),
        (T_0, "test_group_1", "4", "D"),
        (T_0, "test_group_1", "5", "E"),
        (T_0, "test_group_1", "6", "F"),
        (T_0, "test_group_1", "7", "G"),
        (T_0, "test_group_1", "8", "H"),
        (T_0, "test_group_2", "1", "A"),
        (T_0, "test_group_2", "2", "B"),
        (T_0, "test_group_2", "3", "C"),
        (T_0, "test_group_2", "4", "D"),
        (T_0, "test_group_2", "5", "E"),
        (T_0, "test_group_2", "6", "F"),
        (T_0, "test_group_2", "7", "G"),
        (T_0, "test_group_2", "8", "H"),
    ]

    df = spark.createDataFrame(data, schema_typelist)
    df.write.mode('overwrite').saveAsTable(f'{business_vault.config.raw_database_name}.{DV_CONV.ref_name(TYPELISTS_TABLE_NAME)}')


def test_datavault_transformatios(spark: SparkSession):
    """
    Executes several test cases for loading cdc data batches into Data Vault tables.

    :param spark - The active spark session.
    """

    # create sample data
    data: List[LoadedTables] = create_sample_data(spark)

    # initialize raw vault
    config = RawVaultConfiguration(
        SOURCE_SYSTEM_NAME, STAGING_BASE_PATH, STAGING_PREPARED_BASE_PATH, RAW_BASE_PATH, 
        DV_CONV.LOAD_DATE, DV_CONV.CDC_OPERATION, "")
    raw_vault = RawVault(spark, config, DV_CONV)
    raw_vault.initialize_database()

    # load staging tables
    write_parquet_files(data, STAGING_BASE_PATH)
    stage_tables(spark, raw_vault, STAGING_BASE_PATH)
    
    # create data vault tables
    create_data_vault_tables(raw_vault)

    # load hubs, satellites, and links from staging batch 0
    batch = 0
    load_from_prepared_staging_table(raw_vault, batch)

    # tests
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_directors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_hub_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_link_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    df_sat_effectivity_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_effectivity_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_sat_effectivity_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_sat_effectivity_casting = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_effectivity_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')

    # Hub for movie "The Shawshank Redemption", 1994 -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The movie {movie.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Hub for movie "The Shawshank Redemption", 1994 -> not deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert not df_sat_effectivity_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The movie {movie.select("NAME").collect()[0][0]} is deleted.'
    
    # Hub for actor "Tim Robbins" -> exists
    actor = df_actors \
        .filter((df_actors.PublicID == 1) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_actors \
        .filter(col(DV_CONV.hkey_column_name()) == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The actor {actor.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Hub for actor "Tim Robbins" -> not deleted
    actor = df_actors \
        .filter((df_actors.PublicID == 1) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert not df_sat_effectivity_actors \
        .filter(col(DV_CONV.hkey_column_name()) == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The actor {actor.select("NAME").collect()[0][0]} is deleted.'

    # Hub for director "Frank Darabont" -> exists
    director = df_directors \
        .filter((df_directors.PublicID == 1) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_directors \
        .filter(col(DV_CONV.hkey_column_name()) == director.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The director {director.select("NAME").collect()[0][0]} was not found or exists multiple times.'
    
    # Hub for director "Frank Darabont" -> not deleted
    director = df_directors \
        .filter((df_directors.PublicID == 1) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert not df_sat_effectivity_directors \
        .filter(col(DV_CONV.hkey_column_name()) == director.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The director {director.select("NAME").collect()[0][0]} is deleted.'

    # Link between movie "The Shawshank Redemption", 1994 and actor "Tim Robbins" -> exists
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 1)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter((df_movies.PublicID == casting.select("MOVIE_ID").collect()[0][0]) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors \
        .filter((df_actors.PublicID == casting.select("ACTOR_ID").collect()[0][0]) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_link_castings \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("ACTORS_HKEY") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0])) \
        .count() == 1, \
        f'{actor.select("NAME").collect()[0][0]} was not casted in {movie.select("NAME").collect()[0][0]} or the link exists multiple times.'

    # Link between movie "The Shawshank Redemption", 1994 and actor "Tim Robbins" -> not deleted
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 1)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter((df_movies.PublicID == casting.select("MOVIE_ID").collect()[0][0]) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors \
        .filter((df_actors.PublicID == casting.select("ACTOR_ID").collect()[0][0]) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_casting = df_link_castings \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("ACTORS_HKEY") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert not df_sat_effectivity_casting \
        .filter(col(DV_CONV.hkey_column_name()) == link_casting.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and actor {actor.select("NAME").collect()[0][0]} is deleted.'
    
    # Link between movie "The Shawshank Redemption", 1994 and director "Frank Darabont" -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors \
        .filter((df_directors.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0])) \
        .count() == 1, \
        f'{director.select("NAME").collect()[0][0]} did not direct movie {movie.select("NAME").collect()[0][0]} or the link exists multiple times.'

    # Link between movie "The Shawshank Redemption", 1994 and director "Frank Darabont" -> not deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors \
        .filter((df_directors.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_movies_directors = df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert not df_sat_effectivity_movies_directors \
        .filter(col(DV_CONV.hkey_column_name()) == link_movies_directors.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and director {director.select("NAME").collect()[0][0]} is deleted.'

    # Rating of movie "The Shawshank Redemption", 1994, is 9,1
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rating = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RATING") \
        .collect()[0][0]
    assert movie.select("RATING").collect()[0][0] == rating, \
        f'The queried rating of movie {movie.select("NAME").collect()[0][0]} is {rating}. Correct would be {movie.select("RATING").collect()[0][0]}.'

    # Rank of movie "The Shawshank Redemption", 1994, is 64
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rank = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RANK") \
        .collect()[0][0]
    assert movie.select("RANK").collect()[0][0] == rank, \
        f'The queried rank of movie {movie.select("NAME").collect()[0][0]} is {rank}. Correct would be {movie.select("RANK").collect()[0][0]}.'

    # Country of actor "Tim Robbins" is "USA"
    actor = df_actors \
        .filter((df_actors.PublicID == 1) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    country = df_sat_actors \
        .filter(col(DV_CONV.hkey_column_name()) == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("COUNTRY") \
        .collect()[0][0] 
    assert actor.select("COUNTRY").collect()[0][0] == country, \
        f'The queried country of actor {actor.select("NAME").collect()[0][0]} is {country}. Correct would be {movie.select("COUNTRY").collect()[0][0]}.'

    # load hubs, satellites, and links from staging batch 1
    batch = 1
    load_from_prepared_staging_table(raw_vault, batch)

    # tests
    df_movies_0 = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch - 1}')
    df_actors_0 = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch - 1}')
    df_directors_0 = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_{batch - 1}')
    df_castings_0 = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch - 1}')

    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_directors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_hub_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_link_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    df_sat_effectivity_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_effectivity_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_sat_effectivity_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_sat_effectivity_casting = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_effectivity_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')

    # Hub for movie "The Dark Knight", 2008 -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 3) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The movie {movie.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Hub for movie "The Dark Knight", 2008 -> not deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 3) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert not df_sat_effectivity_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The movie {movie.select("NAME").collect()[0][0]} is deleted.'

    # Hub for movie "Star Wars: Episode V", 1980 -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 4) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The movie {movie.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Hub for movie "Star Wars: Episode V", 1980 -> deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 4) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_sat_effectivity_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The movie {movie.select("NAME").collect()[0][0]} is not deleted.'

    # Link between movie "The Shawshank Redemption", 1994 and actor "Tim Robbins" -> not deleted
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 1)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter((df_movies.PublicID == casting.select("MOVIE_ID").collect()[0][0]) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors_0 \
        .filter((df_actors_0.PublicID == casting.select("ACTOR_ID").collect()[0][0]) & (df_actors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_casting = df_link_castings \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("ACTORS_HKEY") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert not df_sat_effectivity_casting \
        .filter(col(DV_CONV.hkey_column_name()) == link_casting.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and actor {actor.select("NAME").collect()[0][0]} is deleted.'

    # Link between movie "The Shawshank Redemption", 1994 and actor "Morgan Freeman" -> deleted
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 2)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter((df_movies.PublicID == casting.select("MOVIE_ID").collect()[0][0]) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors_0 \
        .filter((df_actors_0.PublicID == casting.select("ACTOR_ID").collect()[0][0]) & (df_actors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_casting = df_link_castings \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("ACTORS_HKEY") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert df_sat_effectivity_casting \
        .filter(col(DV_CONV.hkey_column_name()) == link_casting.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and actor {actor.select("NAME").collect()[0][0]} is not deleted.'

    # Link between movie "The Shawshank Redemption", 1994 and director "Frank Darabont" -> not deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors_0 \
        .filter((df_directors_0.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_movies_directors = df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert not df_sat_effectivity_movies_directors \
        .filter(col(DV_CONV.hkey_column_name()) == link_movies_directors.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and director {director.select("NAME").collect()[0][0]} is deleted.'

    # Link between movie "Schindler's List", 1993 and director "Steven Spielberg" -> deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 6) & (df_movies[DV_CONV.cdc_operation_column_name()] == DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors \
        .filter((df_directors.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_movies_directors = df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert df_sat_effectivity_movies_directors \
        .filter(col(DV_CONV.hkey_column_name()) == link_movies_directors.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and director {director.select("NAME").collect()[0][0]} is not deleted.'

    # Rating of movie "The Shawshank Redemption", 1994, is 9,6
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rating = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RATING") \
        .collect()[0][0]
    assert movie.select("RATING").collect()[0][0] == rating, \
        f'The queried rating of movie {movie.select("NAME").collect()[0][0]} is {rating}. Correct would be {movie.select("RATING").collect()[0][0]}.'

    # Rank of movie "The Shawshank Redemption", 1994, is 2
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rank = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RANK") \
        .collect()[0][0]
    assert movie.select("RANK").collect()[0][0] == rank, \
        f'The queried rank of movie {movie.select("NAME").collect()[0][0]} is {rank}. Correct would be {movie.select("RANK").collect()[0][0]}.'

    # load hubs, satellites, and links from staging batch 2
    batch = 2
    load_from_prepared_staging_table(raw_vault, batch)

    # tests
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_directors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_hub_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_link_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    df_sat_effectivity_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_effectivity_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_sat_effectivity_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_DIRECTORS)}')
    df_sat_effectivity_casting = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_effectivity_movies_directors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_effectivity_name(LINK_TABLE_NAME_MOVIES_DIRECTORS)}')

    # Link between movie "The Godfather", 1972 and director "Francis Ford Coppola" -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 2) & (df_movies[DV_CONV.cdc_operation_column_name()] == DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors_0 \
        .filter((df_directors_0.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0])) \
        .count() == 1, \
        f'{director.select("NAME").collect()[0][0]} did not direct movie {movie.select("NAME").collect()[0][0]} or the link exists multiple times.'

    # Link between movie "The Godfather", 1972 and director "Francis Ford Coppola" -> deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 2) & (df_movies[DV_CONV.cdc_operation_column_name()] == DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors_0 \
        .filter((df_directors_0.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_movies_directors = df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert df_sat_effectivity_movies_directors \
        .filter(col(DV_CONV.hkey_column_name()) == link_movies_directors.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and director {director.select("NAME").collect()[0][0]} is not deleted.'

    # Link between movie "The Godfather", 1972 and director "Christopher Nolan" -> exists
    movie = df_movies \
        .filter((df_movies.PublicID == 2) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors_0 \
        .filter((df_directors_0.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0])) \
        .count() == 1, \
        f'{director.select("NAME").collect()[0][0]} did not direct movie {movie.select("NAME").collect()[0][0]} or the link exists multiple times.'

    # Link between movie "The Godfather", 1972 and director "Christopher Nolan" -> not deleted
    movie = df_movies \
        .filter((df_movies.PublicID == 2) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors_0 \
        .filter((df_directors_0.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors_0[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    link_movies_directors = df_link_movies_directors \
        .filter( \
            (col("MOVIES_HKEY") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("DIRECTORS_HKEY") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]))
    assert not df_sat_effectivity_movies_directors \
        .filter(col(DV_CONV.hkey_column_name()) == link_movies_directors.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select([DV_CONV.deleted_column_name()]) \
        .collect()[0][0], \
        f'The link between movie {movie.select("NAME").collect()[0][0]} and director {director.select("NAME").collect()[0][0]} is deleted.'

    # Rating of movie "The Shawshank Redemption", 1994, is 9,5
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rating = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RATING") \
        .collect()[0][0]
    assert movie.select("RATING").collect()[0][0] == rating, \
        f'The queried rating of movie {movie.select("NAME").collect()[0][0]} is {rating}. Correct would be {movie.select("RATING").collect()[0][0]}.'

    # Rank of movie "The Shawshank Redemption", 1994, is 3
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rank = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RANK") \
        .collect()[0][0]
    assert movie.select("RANK").collect()[0][0] == rank, \
        f'The queried rank of movie {movie.select("NAME").collect()[0][0]} is {rank}. Correct would be {movie.select("RANK").collect()[0][0]}.'


def test_pit_tables(spark: SparkSession):
    """
    Executes several test cases for PIT table generation.

    :param spark - The active spark session.
    """
    # initialize business vault
    raw_vault_config = RawVaultConfiguration(
        SOURCE_SYSTEM_NAME, STAGING_BASE_PATH, STAGING_PREPARED_BASE_PATH, RAW_BASE_PATH, 
        DV_CONV.LOAD_DATE, DV_CONV.CDC_OPERATION, "")
    config = BusinessVaultConfiguration(SOURCE_SYSTEM_NAME)
    business_vault = BusinessVault(spark, config, DV_CONV)

    # create the PIT tables
    create_pit_tables(business_vault)
    
    # tests
    df_movies = spark.table(f'{raw_vault_config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_FULL')
    df_hub_movies = spark.table(f'{raw_vault_config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_pit = spark.table(f'{raw_vault_config.raw_database_name}.{DV_CONV.pit_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_effectivity_movies = spark.table(f'{raw_vault_config.raw_database_name}.{DV_CONV.sat_effectivity_name(SOURCE_TABLE_NAME_MOVIES)}')

    # Movie "The Dark Knight", 2008: load_end_date == delete_date
    movie = df_movies \
        .filter((df_movies.PublicID == 3) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    
    df_hub_movie = df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0])

    delete_date = df_hub_movie.join(df_sat_effectivity_movies, df_hub_movie[DV_CONV.hkey_column_name()] == df_sat_effectivity_movies[DV_CONV.hkey_column_name()]) \
        .filter(col(DV_CONV.deleted_column_name()) == True) \
        .orderBy(df_sat_effectivity_movies[DV_CONV.load_date_column_name()].desc()) \
        .select(df_sat_effectivity_movies[DV_CONV.load_date_column_name()]).collect()[0][0]

    load_end_date = df_hub_movie.join(df_pit, df_hub_movie[DV_CONV.hkey_column_name()] == df_pit[DV_CONV.hkey_column_name()]) \
        .select(df_pit[DV_CONV.load_end_date_column_name()]) \
        .orderBy([col(DV_CONV.load_end_date_column_name()).desc()]).collect()[0][0]
    
    assert delete_date == load_end_date, \
        f'The load_end_date {load_end_date} is not correct. Correct would be {delete_date}.'

    # Movie "The Shawshank Redemption", 1994: load_end_date == datetime.max
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    
    df_hub_movie = df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0])

    load_end_date = df_hub_movie.join(df_pit, df_hub_movie[DV_CONV.hkey_column_name()] == df_pit[DV_CONV.hkey_column_name()]) \
        .select(df_pit[DV_CONV.load_end_date_column_name()]) \
        .orderBy([col(DV_CONV.load_end_date_column_name()).desc()]).collect()[0][0]
    
    assert load_end_date == datetime.max, \
        f'The load_end_date {load_end_date} is not correct. Correct would be {datetime.max}.'


def test_business_vault(spark: SparkSession):
    """
    Executes several test cases BusinessVault functions.

    :param spark - The active spark session.
    """
    # initialize business vault
    raw_vault_config = RawVaultConfiguration(
        SOURCE_SYSTEM_NAME, STAGING_BASE_PATH, STAGING_PREPARED_BASE_PATH, RAW_BASE_PATH, 
        DV_CONV.LOAD_DATE, DV_CONV.CDC_OPERATION, "")
    config = BusinessVaultConfiguration(SOURCE_SYSTEM_NAME)
    business_vault = BusinessVault(spark, config, DV_CONV)

    # tests
    df_movies = spark.table(f'{raw_vault_config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_FULL')
    df_actors = spark.table(f'{raw_vault_config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_FULL')
    df_directors = spark.table(f'{raw_vault_config.staging_prepared_database_name}.{STAGING_TABLE_NAME_DIRECTORS}_FULL')
    df_castings = spark.table(f'{raw_vault_config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_FULL')
  
    # Test read_data_from_hub(...) with movie "The Shawshank Redemption", 1994
    movie = df_movies \
        .filter((df_movies.PublicID == 1) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    id = movie.select("PublicID").collect()[0][0]
    name = movie.select("NAME").collect()[0][0]
    year = movie.select("YEAR").collect()[0][0]
    rating = movie.select("RATING").collect()[0][0]
    rank = movie.select("RANK").collect()[0][0]

    columns = ["PublicID", "NAME", "YEAR", "RATING", "RANK"]
    df = business_vault.read_data_from_hub(SOURCE_TABLE_NAME_MOVIES, columns, include_hkey=True)

    df = df \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select(columns)
    
    assert df.select(columns[0]).collect()[0][0] == id, \
        f'The attribute {columns[0]} is set to {df.select(columns[0]).collect()[0][0]}. Correct would be {id}.'
    assert df.select(columns[1]).collect()[0][0] == name, \
        f'The attribute {columns[1]} is set to {df.select(columns[1]).collect()[0][0]}. Correct would be {name}.'
    assert df.select(columns[2]).collect()[0][0] == year, \
        f'The attribute {columns[2]} is set to {df.select(columns[2]).collect()[0][0]}. Correct would be {year}.'
    assert df.select(columns[3]).collect()[0][0] == rating, \
        f'The attribute {columns[3]} is set to {df.select(columns[3]).collect()[0][0]}. Correct would be {rating}.'
    assert df.select(columns[4]).collect()[0][0] == rank, \
        f'The attribute {columns[4]} is set to {df.select(columns[4]).collect()[0][0]}. Correct would be {rank}.'

    # Test read_data_from_hub(...) with actor "Morgan Freeman"
    actor = df_actors \
        .filter((df_actors.PublicID == 2) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    id = actor.select("PublicID").collect()[0][0]
    name = actor.select("NAME").collect()[0][0]
    country = actor.select("COUNTRY").collect()[0][0]

    columns = ["PublicID", "NAME", "COUNTRY"]
    df = business_vault.read_data_from_hub(SOURCE_TABLE_NAME_ACTORS, columns, include_hkey=True)

    df = df \
        .filter(col(DV_CONV.hkey_column_name()) == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select(columns)
    
    assert df.select(columns[0]).collect()[0][0] == id, \
        f'The attribute {columns[0]} is set to {df.select(columns[0]).collect()[0][0]}. Correct would be {id}.'
    assert df.select(columns[1]).collect()[0][0] == name, \
        f'The attribute {columns[1]} is set to {df.select(columns[1]).collect()[0][0]}. Correct would be {name}.'
    assert df.select(columns[2]).collect()[0][0] == country, \
        f'The attribute {columns[2]} is set to {df.select(columns[2]).collect()[0][0]}. Correct would be {country}.'

    # Test read_data_from_hub(...) with director "Quentin Terintino"
    director = df_directors \
        .filter((df_directors.PublicID == 5) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    id = director.select("PublicID").collect()[0][0]
    name = director.select("NAME").collect()[0][0]
    country = director.select("COUNTRY").collect()[0][0]

    columns = ["PublicID", "NAME", "COUNTRY"]
    df = business_vault.read_data_from_hub(SOURCE_TABLE_NAME_DIRECTORS, columns, include_hkey=True)

    df = df \
        .filter(col(DV_CONV.hkey_column_name()) == director.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select(columns)
    
    assert df.select(columns[0]).collect()[0][0] == id, \
        f'The attribute {columns[0]} is set to {df.select(columns[0]).collect()[0][0]}. Correct would be {id}.'
    assert df.select(columns[1]).collect()[0][0] == name, \
        f'The attribute {columns[1]} is set to {df.select(columns[1]).collect()[0][0]}. Correct would be {name}.'
    assert df.select(columns[2]).collect()[0][0] == country, \
        f'The attribute {columns[2]} is set to {df.select(columns[2]).collect()[0][0]}. Correct would be {country}.'

    # Test join_linked_hubs(...) with link between movie "The Shawshank Redemption", 1994 and actor "Tim Robbins"
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 1)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter((df_movies.PublicID == casting.select("MOVIE_ID").collect()[0][0]) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors \
        .filter((df_actors.PublicID == casting.select("ACTOR_ID").collect()[0][0]) & (df_actors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    movie_columns = ["PublicID", "NAME", "YEAR", "RATING", "RANK"]
    actor_columns = ["PublicID", "NAME", "COUNTRY"]
    
    df = business_vault.join_linked_hubs(SOURCE_TABLE_NAME_MOVIES, SOURCE_TABLE_NAME_ACTORS, SOURCE_TABLE_NAME_CASTINGS, 
        "MOVIES_HKEY", "ACTORS_HKEY", movie_columns, actor_columns, include_hkeys=True)

    column_prefix_movies_hub = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}"
    column_prefix_actors_hub = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}"
    column_prefix_movies_sat = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}"
    column_prefix_actors_sat = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}"

    df = df \
        .filter(col(f"{column_prefix_movies_hub}.{DV_CONV.hkey_column_name()}") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .filter(col(f"{column_prefix_actors_hub}.{DV_CONV.hkey_column_name()}") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    movie_columns = [
        f"{column_prefix_movies_hub}.{movie_columns[0]}", f"{column_prefix_movies_sat}.{movie_columns[1]}",
        f"{column_prefix_movies_sat}.{movie_columns[2]}", f"{column_prefix_movies_sat}.{movie_columns[3]}",
        f"{column_prefix_movies_sat}.{movie_columns[4]}"
    ]
    actor_columns = [
        f"{column_prefix_actors_hub}.{actor_columns[0]}", f"{column_prefix_actors_sat}.{actor_columns[1]}",
        f"{column_prefix_actors_sat}.{actor_columns[2]}"
    ]

    movie_id = movie.select("PublicID").collect()[0][0]
    movie_name = movie.select("NAME").collect()[0][0]
    year = movie.select("YEAR").collect()[0][0]
    rating = movie.select("RATING").collect()[0][0]
    rank = movie.select("RANK").collect()[0][0]
    actor_id = actor.select("PublicID").collect()[0][0]
    actor_name = actor.select("NAME").collect()[0][0]
    country = actor.select("COUNTRY").collect()[0][0]

    assert df.select(movie_columns[0]).collect()[0][0] == movie_id, \
        f'The attribute {movie_columns[0]} is set to {df.select(movie_columns[0]).collect()[0][0]}. Correct would be {movie_id}.'
    assert df.select(movie_columns[1]).collect()[0][0] == movie_name, \
        f'The attribute {movie_columns[1]} is set to {df.select(movie_columns[1]).collect()[0][0]}. Correct would be {movie_name}.'
    assert df.select(movie_columns[2]).collect()[0][0] == year, \
        f'The attribute {movie_columns[2]} is set to {df.select(movie_columns[2]).collect()[0][0]}. Correct would be {year}.'
    assert df.select(movie_columns[3]).collect()[0][0] == rating, \
        f'The attribute {movie_columns[3]} is set to {df.select(movie_columns[3]).collect()[0][0]}. Correct would be {rating}.'
    assert df.select(movie_columns[4]).collect()[0][0] == rank, \
        f'The attribute {movie_columns[4]} is set to {df.select(movie_columns[4]).collect()[0][0]}. Correct would be {rank}.'
    assert df.select(actor_columns[0]).collect()[0][0] == actor_id, \
        f'The attribute {actor_columns[0]} is set to {df.select(actor_columns[0]).collect()[0][0]}. Correct would be {actor_id}.'
    assert df.select(actor_columns[1]).collect()[0][0] == actor_name, \
        f'The attribute {actor_columns[1]} is set to {df.select(actor_columns[1]).collect()[0][0]}. Correct would be {actor_name}.'
    assert df.select(actor_columns[2]).collect()[0][0] == country, \
        f'The attribute {actor_columns[2]} is set to {df.select(actor_columns[2]).collect()[0][0]}. Correct would be {country}.'

    # Test join_linked_hubs(...) with link between movie "Schindler's List", 1993 and director "Steven Spielberg"
    movie = df_movies \
        .filter((df_movies.PublicID == 6) & (df_movies[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    director = df_directors \
        .filter((df_directors.PublicID == movie.select("DIRECTOR_ID").collect()[0][0]) & (df_directors[DV_CONV.cdc_operation_column_name()] != DV_CONV.CDC_OPS.BEFORE_UPDATE)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    movie_columns = ["PublicID", "NAME", "YEAR", "RATING", "RANK"]
    director_columns = ["PublicID", "NAME", "COUNTRY"]
    
    df = business_vault.join_linked_hubs(SOURCE_TABLE_NAME_MOVIES, SOURCE_TABLE_NAME_DIRECTORS, LINK_TABLE_NAME_MOVIES_DIRECTORS, 
        "MOVIES_HKEY", "DIRECTORS_HKEY", movie_columns, director_columns, include_hkeys=True)

    column_prefix_movies_hub = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}"
    column_prefix_directors_hub = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS)}"
    column_prefix_movies_sat = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}"
    column_prefix_directors_sat = f"spark_catalog.{raw_vault_config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_DIRECTORS)}"

    df = df \
        .filter(col(f"{column_prefix_movies_hub}.{DV_CONV.hkey_column_name()}") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .filter(col(f"{column_prefix_directors_hub}.{DV_CONV.hkey_column_name()}") == director.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())

    movie_columns = [
        f"{column_prefix_movies_hub}.{movie_columns[0]}", f"{column_prefix_movies_sat}.{movie_columns[1]}",
        f"{column_prefix_movies_sat}.{movie_columns[2]}", f"{column_prefix_movies_sat}.{movie_columns[3]}",
        f"{column_prefix_movies_sat}.{movie_columns[4]}"
    ]
    director_columns = [
        f"{column_prefix_directors_hub}.{director_columns[0]}", f"{column_prefix_directors_sat}.{director_columns[1]}",
        f"{column_prefix_directors_sat}.{director_columns[2]}"
    ]

    movie_id = movie.select("PublicID").collect()[0][0]
    movie_name = movie.select("NAME").collect()[0][0]
    year = movie.select("YEAR").collect()[0][0]
    rating = movie.select("RATING").collect()[0][0]
    rank = movie.select("RANK").collect()[0][0]
    director_id = director.select("PublicID").collect()[0][0]
    director_name = director.select("NAME").collect()[0][0]
    country = director.select("COUNTRY").collect()[0][0]

    assert df.select(movie_columns[0]).collect()[0][0] == movie_id, \
        f'The attribute {movie_columns[0]} is set to {df.select(movie_columns[0]).collect()[0][0]}. Correct would be {movie_id}.'
    assert df.select(movie_columns[1]).collect()[0][0] == movie_name, \
        f'The attribute {movie_columns[1]} is set to {df.select(movie_columns[1]).collect()[0][0]}. Correct would be {movie_name}.'
    assert df.select(movie_columns[2]).collect()[0][0] == year, \
        f'The attribute {movie_columns[2]} is set to {df.select(movie_columns[2]).collect()[0][0]}. Correct would be {year}.'
    assert df.select(movie_columns[3]).collect()[0][0] == rating, \
        f'The attribute {movie_columns[3]} is set to {df.select(movie_columns[3]).collect()[0][0]}. Correct would be {rating}.'
    assert df.select(movie_columns[4]).collect()[0][0] == rank, \
        f'The attribute {movie_columns[4]} is set to {df.select(movie_columns[4]).collect()[0][0]}. Correct would be {rank}.'
    assert df.select(director_columns[0]).collect()[0][0] == director_id, \
        f'The attribute {director_columns[0]} is set to {df.select(director_columns[0]).collect()[0][0]}. Correct would be {director_id}.'
    assert df.select(director_columns[1]).collect()[0][0] == director_name, \
        f'The attribute {director_columns[1]} is set to {df.select(director_columns[1]).collect()[0][0]}. Correct would be {director_name}.'
    assert df.select(director_columns[2]).collect()[0][0] == country, \
        f'The attribute {director_columns[2]} is set to {df.select(director_columns[2]).collect()[0][0]}. Correct would be {country}.'


@pytest.mark.skip()
def test_curated(spark: SparkSession):
    """
    Executes several test cases Curated functions.

    :param spark - The active spark session.
    """
    config = CuratedConfiguration(SOURCE_SYSTEM_NAME, RAW_BASE_PATH)
    business_vault = BusinessVault(spark, config)

    create_reference_tables(spark, business_vault)

    typelists_table_name = f'{DV_CONV.ref_name(TYPELISTS_TABLE_NAME)}'
    typelists_active_table_name = f'{DV_CONV.ref_name(TYPELISTS_ACTIVE_TABLE_NAME)}'
    business_vault.create_active_code_reference_table(typelists_table_name, typelists_active_table_name, 'PublicID')
    df_typelists = spark.table(f'{config.raw_database_name}.{typelists_active_table_name}')
    typelists_config = TypelistsConfiguration(df_typelists)
    
    curated_vault = Curated(spark, config, business_vault, typelists_config)
    curated_vault.initialize_database()

    df = curated_vault.map_to_curated([
        FieldDefinition('cc_movies', 'PublicID', to_field_name="MOVIE_ID"),
        FieldDefinition('cc_movies', 'NAME', to_field_name="MOVIE_NAME"),
        FieldDefinition('cc_movies', 'YEAR'),
        FieldDefinition('cc_movies', 'DIRECTOR_ID', to_field_name='PublicID', foreign_key=True, foreign_key_to_table_name='cc_directors'),
        FieldDefinition('cc_movies', 'RATING'),
        FieldDefinition('cc_movies', 'RANK'),
        FieldDefinition('cc_directors', 'PublicID', to_field_name="DIRECTOR_ID"),
        FieldDefinition('cc_directors', 'NAME', to_field_name="DIRECTOR_NAME"),
        FieldDefinition('cc_directors', 'COUNTRY')
    ])

    df.orderBy(["spark_catalog.test__raw.hub__movies.NAME", DV_CONV.load_date_column_name()]).show(100)