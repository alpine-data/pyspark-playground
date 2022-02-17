from datetime import datetime, timedelta
from os import listdir
from typing import List, TypedDict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pysparkvault.raw.RawVault import ColumnDefinition, ColumnReference, DataVaultConfiguration, DataVaultConventions, ForeignKey, LinkedHubDefinition, RawVault, SatelliteDefinition


# global variables
SOURCE_TABLE_NAME_MOVIES = "MOVIES"
SOURCE_TABLE_NAME_ACTORS = "ACTORS"
SOURCE_TABLE_NAME_DIRECTORS = "DIRECTORS"
SOURCE_TABLE_NAME_CASTINGS = "CASTINGS"

STAGING_TABLE_NAME_MOVIES = f"STG__{SOURCE_TABLE_NAME_MOVIES}"
STAGING_TABLE_NAME_ACTORS = f"STG__{SOURCE_TABLE_NAME_ACTORS}"
STAGING_TABLE_NAME_DIRECTORS = f"STG__{SOURCE_TABLE_NAME_DIRECTORS}"
STAGING_TABLE_NAME_CASTINGS = f"STG__{SOURCE_TABLE_NAME_CASTINGS}"

HKEY_COLUMNS_MOVIES = ["NAME", "YEAR"]
HKEY_COLUMNS_ACTORS = ["NAME"]
HKEY_COLUMNS_DIRECTORS = ["NAME"]
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


class Operation:
    SNAPSHOT = 0
    DELETE = 1
    CREATE = 2
    BEFORE_UPDATE = 3
    UPDATE = 4


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
        StructField("ID", StringType(), False),
        StructField("NAME", StringType(), False),
        StructField("YEAR", IntegerType(), False),
        StructField("DIRECTOR_ID", IntegerType(), False),
        StructField("RATING", DoubleType(), False),
        StructField("RANK", IntegerType(), False)
    ])

    schema_actors = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("ID", StringType(), False),
        StructField("NAME", StringType(), False),
        StructField("COUNTRY", StringType(), False)
    ])

    schema_directors = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("ID", StringType(), False),
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
            (Operation.SNAPSHOT, T_0, 1, "The Shawshank Redemption", 1994, 1, 9.3, 64),
            (Operation.SNAPSHOT, T_0, 2, "The Godfather", 1972, 2, 9.2, 94),
            (Operation.SNAPSHOT, T_0, 3, "The Dark Knight", 2008, 3, 9.0, 104),
            (Operation.SNAPSHOT, T_0, 4, "Star Wars: Episode V", 1980, 4, 8.7, 485)
        ],
        [
            (Operation.CREATE, T_2, 5, "Pulp Fiction", 1994, 5, 8.9, 138),
            (Operation.CREATE, T_2, 6, "Schindler's List", 1993, 6, 8.6, 145),
            (Operation.CREATE, T_2, 7, "Inception", 2010, 7, 8.3, 210),
            (Operation.UPDATE, T_2, 3, "The Dark Knight", 2008, 3, 9.1, 97),
            (Operation.UPDATE, T_3, 4, "Star Wars: Episode V", 1980, 4, 8.4, 500),
            (Operation.UPDATE, T_3, 1, "The Shawshank Redemption", 1994, 1, 9.2, 67),
            (Operation.UPDATE, T_4, 2, "The Godfather", 1972, 2, 9.1, 96),
            (Operation.UPDATE, T_4, 6, "Schindler's List", 1993, 6, 8.8, 125),
            (Operation.UPDATE, T_4, 1, "The Shawshank Redemption", 1994, 1, 9.6, 2),
            (Operation.DELETE, T_4, 4, "Star Wars: Episode V", 1980, 4, 8.4, 500),
            (Operation.DELETE, T_4, 3, "The Dark Knight", 2008, 3, 9.1, 97)
        ],
        [
            (Operation.UPDATE, T_5, 2, "The Godfather", 1972, 2, 8.9, 103),
            (Operation.UPDATE, T_5, 6, "Schindler's List", 1993, 6, 8.3, 210),
            (Operation.CREATE, T_5, 4, "Star Wars: Episode V", 1980, 4, 8.4, 500),
            (Operation.UPDATE, T_5, 1, "The Shawshank Redemption", 1994, 1, 9.5, 3)
        ]
    ]

    actors = [
        [
            (Operation.SNAPSHOT, T_0, 1, "Tim Robbins", "USA"),
            (Operation.SNAPSHOT, T_0, 2, "Morgan Freeman", "USA"),
            (Operation.SNAPSHOT, T_0, 3, "Bob Gunton", "USA"),
            (Operation.SNAPSHOT, T_0, 4, "William Sadler", "USA"),
            (Operation.SNAPSHOT, T_0, 5, "Marlon Brando", "USA"),
            (Operation.SNAPSHOT, T_0, 6, "Al Pacino", "USA"),
            (Operation.SNAPSHOT, T_0, 7, "James Caan", "USA"),
            (Operation.SNAPSHOT, T_0, 8, "Christian Bale", "USA"),
            (Operation.SNAPSHOT, T_0, 9, "Heath Ledger", "USA"),
            (Operation.SNAPSHOT, T_0, 10, "Mark Hamill", "USA"),
            (Operation.SNAPSHOT, T_0, 11, "Harrison Ford", "USA"),
            (Operation.SNAPSHOT, T_0, 12, "Carrie Fisher", "USA"),
            (Operation.SNAPSHOT, T_0, 13, "Robert Duvall", "USA"),
            (Operation.SNAPSHOT, T_0, 14, "John Marley", "USA"),
            (Operation.SNAPSHOT, T_0, 15, "Gary Oldman", "USA")
        ],
        [
            (Operation.CREATE, T_2, 16, "John Travolta", "USA"),
            (Operation.CREATE, T_2, 17, "Liam Neeson", "USA"),
            (Operation.CREATE, T_2, 18, "Ralph Fiennes", "USA"),
            (Operation.CREATE, T_2, 19, "Ben Kingsley", "USA"),
            (Operation.CREATE, T_2, 20, "Leonardo DiCaprio", "USA"),
            (Operation.DELETE, T_4, 13, "Robert Duvall", "USA")
        ],
        [
            (Operation.DELETE, T_5, 14, "John Marley", "USA")
        ]
    ]

    directors = [
        [
            (Operation.SNAPSHOT, T_0, 1, "Frank Darabont", "USA"),
            (Operation.SNAPSHOT, T_0, 2, "Francis Ford Coppola", "USA"),
            (Operation.SNAPSHOT, T_0, 3, "Christopher Nolan", "USA"),
            (Operation.SNAPSHOT, T_0, 4, "Irvin Kershner", "USA")
        ],
        [
            (Operation.CREATE, T_2, 5, "Quentin Terintino", "USA"),
            (Operation.CREATE, T_2, 6, "Steven Spielberg", "USA"),
            (Operation.CREATE, T_2, 7, "Christopher Nolan", "USA"),
        ],
        []
    ]

    castings = [
        [
            (Operation.SNAPSHOT, T_0, 1, 1),
            (Operation.SNAPSHOT, T_0, 1, 2),
            (Operation.SNAPSHOT, T_0, 1, 3),
            (Operation.SNAPSHOT, T_0, 1, 4),
            (Operation.SNAPSHOT, T_0, 2, 5),
            (Operation.SNAPSHOT, T_0, 2, 6),
            (Operation.SNAPSHOT, T_0, 2, 7),
            (Operation.SNAPSHOT, T_0, 3, 8),
            (Operation.SNAPSHOT, T_0, 3, 9),
            (Operation.SNAPSHOT, T_0, 4, 10),
            (Operation.SNAPSHOT, T_0, 4, 11),
            (Operation.SNAPSHOT, T_0, 4, 12)
        ],
        [
            (Operation.CREATE, T_2, 5, 16),
            (Operation.CREATE, T_2, 6, 17),
            (Operation.CREATE, T_2, 6, 18),
            (Operation.CREATE, T_2, 6, 19),
            (Operation.CREATE, T_2, 7, 20)
        ],
        [
            (Operation.CREATE, T_5, 7, 19)
        ]
    ]

    # returns a list that contains a LoadedTables object for each batch
    return [
        LoadedTables(
            movies=spark.createDataFrame(movies[i], schema_movies),
            actors=spark.createDataFrame(actors[i], schema_actors),
            directors=spark.createDataFrame(actors[i], schema_directors),
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


def stage_tables(raw_vault: RawVault, staging_base_path: str) -> None:
    """
    Stages all source tables from the given path.

    :param raw_vault - The RawVault object needed for calling the stage_table(...) function.
    :param staging_base_path - The path of the source tables.
    """

    for f in listdir(staging_base_path):
        # remove the file ending and extract the batch number
        i = f.replace(".parquet", "").split("_")[-1]
        if SOURCE_TABLE_NAME_MOVIES in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_MOVIES}_{i}", f, HKEY_COLUMNS_MOVIES)
        elif SOURCE_TABLE_NAME_ACTORS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_ACTORS}_{i}", f, HKEY_COLUMNS_ACTORS)
        elif SOURCE_TABLE_NAME_DIRECTORS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_DIRECTORS}_{i}", f, HKEY_COLUMNS_DIRECTORS)
        elif SOURCE_TABLE_NAME_CASTINGS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_CASTINGS}_{i}", f, HKEY_COLUMNS_CASTINGS)

            
def create_data_vault_tables(raw_vault: RawVault) -> None:
    """
    Creates empty tables for hubs, links, and satellites.

    :param raw_vault - The RawVault object needed for creating hubs, links, and satellites.
    """

    # create hubs
    raw_vault.create_hub(SOURCE_TABLE_NAME_MOVIES, [
        ColumnDefinition("NAME", StringType()),
        ColumnDefinition("YEAR", IntegerType())
    ])

    raw_vault.create_hub(SOURCE_TABLE_NAME_ACTORS, [
        ColumnDefinition("NAME", StringType()),
    ])

    raw_vault.create_hub(SOURCE_TABLE_NAME_DIRECTORS, [
        ColumnDefinition("NAME", StringType()),
    ])

    # create links
    raw_vault.create_link(SOURCE_TABLE_NAME_CASTINGS, ["HKEY_MOVIES", "HKEY_ACTORS"])

    # create links
    raw_vault.create_link("MOVIES_DIRECTORS", ["HKEY_MOVIES", "HKEY_DIRECTORS"])

    # create satellites
    raw_vault.create_satellite(SOURCE_TABLE_NAME_MOVIES, [
        ColumnDefinition("ID", StringType()),
        ColumnDefinition("RATING", DoubleType()),
        ColumnDefinition("RANK", IntegerType())
    ])

    raw_vault.create_satellite(SOURCE_TABLE_NAME_ACTORS, [
        ColumnDefinition("ID", StringType()),
        ColumnDefinition("COUNTRY", StringType())
    ])

    raw_vault.create_satellite(SOURCE_TABLE_NAME_DIRECTORS, [
        ColumnDefinition("ID", StringType()),
        ColumnDefinition("COUNTRY", StringType())
    ])


def load_from_prepared_staging_table(raw_vault: RawVault, batch: int):
    """
    Loads cdc data into hubs, links, and satellites.

    :param raw_vault - The RawVault object needed for loading cdc data into hubs, links, and satellites.
    :param batch - The number of the batch to be loaded.
    """

    # load hubs and satellites
    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_MOVIES}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES), HKEY_COLUMNS_MOVIES, 
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES), ['ID', 'RATING', 'RANK'])])

    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_ACTORS}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS), HKEY_COLUMNS_ACTORS, 
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS), ['ID', 'COUNTRY'])])

    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_DIRECTORS}_{batch}", DV_CONV.hub_name(SOURCE_TABLE_NAME_DIRECTORS), HKEY_COLUMNS_DIRECTORS, 
        [SatelliteDefinition(DV_CONV.sat_name(SOURCE_TABLE_NAME_DIRECTORS), ['ID', 'COUNTRY'])])

    # load links
    raw_vault.load_link_from_prepared_stage_table(
        f"{STAGING_TABLE_NAME_CASTINGS}_{batch}", 
        [
            LinkedHubDefinition("HKEY_MOVIES", ForeignKey("MOVIE_ID", ColumnReference(f"{STAGING_TABLE_NAME_MOVIES}_{batch}", "ID"))),
            LinkedHubDefinition("HKEY_ACTORS", ForeignKey("ACTOR_ID", ColumnReference(f"{STAGING_TABLE_NAME_ACTORS}_{batch}", "ID")))
        ], DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS), None)
    
    raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables(f"{STAGING_TABLE_NAME_MOVIES}_{batch}",
        ForeignKey("DIRECTOR_ID", ColumnReference(f"{STAGING_TABLE_NAME_DIRECTORS}_{batch}", "ID")), "MOVIES_DIRECTORS", "HKEY_MOVIES", "HKEY_DIRECTORS")


def test_datavault_transformatios(spark: SparkSession):
    """
    Executes several test cases for loading cdc data batches into Data Vault tables.

    :param spark - The active spark session.
    """

    # create sample data
    data: List[LoadedTables] = create_sample_data(spark)

    # initialize raw vault
    config = DataVaultConfiguration(
        SOURCE_SYSTEM_NAME, STAGING_BASE_PATH, STAGING_PREPARED_BASE_PATH, RAW_BASE_PATH, 
        DV_CONV.LOAD_DATE, DV_CONV.CDC_OPERATION, "")
    raw_vault = RawVault(spark, config, DV_CONV)
    raw_vault.initialize_database()

    # load staging tables
    write_parquet_files(data, STAGING_BASE_PATH)
    stage_tables(raw_vault, STAGING_BASE_PATH)
    
    # create data vault tables
    create_data_vault_tables(raw_vault)

    # load hubs, satellites, and links from staging batch 0
    batch = 0
    load_from_prepared_staging_table(raw_vault, batch)

    sat_effectivity_table_name = DV_CONV.link_name("MOVIES_DIRECTORS")
    sat_effectivity_table_name = f'{config.raw_database_name}.{sat_effectivity_table_name}'
    spark.table(sat_effectivity_table_name).show()

    # tests
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    # Hub for movie "The Shawshank Redemption", 1994 -> exists
    movie = df_movies \
        .filter(df_movies.ID == 1) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The movie {movie.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Hub for actor "Tim Robbins" -> exists
    actor = df_actors \
        .filter(df_actors.ID == 1) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_hub_actors \
        .filter(col(DV_CONV.hkey_column_name()) == actor.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .count() == 1, \
        f'The actor {actor.select("NAME").collect()[0][0]} was not found or exists multiple times.'

    # Link between movie "The Shawshank Redemption", 1994 and actor "Tim Robbins" -> exists
    casting = df_castings \
        .filter((col("MOVIE_ID") == 1) & (col("ACTOR_ID") == 1)) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    movie = df_movies \
        .filter(df_movies.ID == casting.select("MOVIE_ID").collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    actor = df_actors \
        .filter(df_actors.ID == casting.select("ACTOR_ID").collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    assert df_link_castings \
        .filter( \
            (col("HKEY_MOVIES") == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) & \
            (col("HKEY_ACTORS") == actor.select(DV_CONV.hkey_column_name()).collect()[0][0])) \
        .count() == 1, \
        f'{actor.select("NAME").collect()[0][0]} was not casted in {movie.select("NAME").collect()[0][0]} or the link exists multiple times.'

    # Rating of movie "The Shawshank Redemption", 1994, is 9,1
    movie = df_movies \
        .filter(df_movies.ID == 1) \
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
        .filter(df_movies.ID == 1) \
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
        .filter(df_actors.ID == 1) \
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
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    # # Rating of movie "The Shawshank Redemption", 1994, is 9,6
    movie = df_movies \
        .filter(df_movies.ID == 1) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rating = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RATING") \
        .collect()[0][0]
    assert movie.select("RATING").collect()[0][0] == rating, \
        f'The queried rating of movie {movie.select("NAME").collect()[0][0]} is {rating}. Correct would be {movie.select("RATING").collect()[0][0]}.'

    # # Rank of movie "The Shawshank Redemption", 1994, is 2
    movie = df_movies \
        .filter(df_movies.ID == 1) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rank = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RANK") \
        .collect()[0][0]
    assert movie.select("RANK").collect()[0][0] == rank, \
        f'The queried rank of movie {movie.select("NAME").collect()[0][0]} is {rank}. Correct would be {movie.select("RANK").collect()[0][0]}.'

    # load hubs, satellites, and links from staging batch 1
    batch = 2
    load_from_prepared_staging_table(raw_vault, batch)

    # tests
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}_{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}_{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}_{batch}')

    df_hub_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_hub_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.hub_name(SOURCE_TABLE_NAME_ACTORS)}')
    df_link_castings = spark.table(f'{config.raw_database_name}.{DV_CONV.link_name(SOURCE_TABLE_NAME_CASTINGS)}')
    df_sat_movies = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_MOVIES)}')
    df_sat_actors = spark.table(f'{config.raw_database_name}.{DV_CONV.sat_name(SOURCE_TABLE_NAME_ACTORS)}')

    # Rating of movie "The Shawshank Redemption", 1994, is 9,5
    movie = df_movies \
        .filter(df_movies.ID == 1) \
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
        .filter(df_movies.ID == 1) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc())
    rank = df_sat_movies \
        .filter(col(DV_CONV.hkey_column_name()) == movie.select(DV_CONV.hkey_column_name()).collect()[0][0]) \
        .orderBy(col(DV_CONV.load_date_column_name()).desc()) \
        .select("RANK") \
        .collect()[0][0]
    assert movie.select("RANK").collect()[0][0] == rank, \
        f'The queried rank of movie {movie.select("NAME").collect()[0][0]} is {rank}. Correct would be {movie.select("RANK").collect()[0][0]}.'

