from pyspark.sql.dataframe import DataFrame

from datetime import datetime, timedelta
from os import listdir
from typing import List, TypedDict

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pysparkvault.raw.RawVault import ColumnDefinition, ColumnReference, DataVaultConfiguration, DataVaultConventions, ForeignKey, LinkedHubDefinition, RawVault, SatelliteDefinition


# global variables
SOURCE_TABLE_NAME_MOVIES = "SRC__MOVIES"
SOURCE_TABLE_NAME_ACTORS = "SRC__ACTORS"
SOURCE_TABLE_NAME_CASTINGS = "SRC__CASTINGS"

STAGING_TABLE_NAME_MOVIES = f"STAGE__{SOURCE_TABLE_NAME_MOVIES}"
STAGING_TABLE_NAME_ACTORS = f"STAGE__{SOURCE_TABLE_NAME_ACTORS}"
STAGING_TABLE_NAME_CASTINGS = f"STAGE__{SOURCE_TABLE_NAME_CASTINGS}"

HUB_TABLE_NAME_MOVIES = "HUB__MOVIES"
HUB_TABLE_NAME_ACTORS = "HUB__ACTORS"
LINK_TABLE_NAME_CASTINGS = "LNK__CASTINGS"
SAT_TABLE_NAME_MOVIES = "SAT__MOVIES"
SAT_TABLE_NAME_ACTORS = "SAT__ACTORS"

HKEY_COLUMNS_MOVIES = ["NAME", "YEAR"]
HKEY_COLUMNS_ACTORS = ["NAME"]
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
    castings: DataFrame


def create_sample_data(spark: SparkSession) -> List[LoadedTables]:
    """
    Creates some sample test data. The return list is a list of loaded cdc batches from the source tables.
    """

    schema_movies = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("director", StringType(), False),
        StructField("rating", DoubleType(), False),
        StructField("rank", IntegerType(), False)
    ])

    schema_actors = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("country", StringType(), False),
    ])

    schema_castings = StructType([
        StructField(DV_CONV.CDC_OPERATION, StringType(), False),
        StructField(DV_CONV.LOAD_DATE, TimestampType(), False),
        StructField("movie_id", StringType(), False),
        StructField("actor_id", StringType(), False),
    ])

    movies = [
        [
            (Operation.SNAPSHOT, T_0, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
            (Operation.SNAPSHOT, T_0, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.2, 94),
            (Operation.SNAPSHOT, T_0, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 104),
            (Operation.SNAPSHOT, T_0, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.7, 485)
        ],
        [
            (Operation.CREATE, T_2, 5, "Pulp Fiction", 1994, "Quentin Terintino", 8.9, 138),
            (Operation.CREATE, T_2, 6, "Schindler's List", 1993, "Steven Spielberg", 8.6, 145),
            (Operation.CREATE, T_2, 7, "Inception", 2010, "Christopher Nolan", 8.3, 210),
            (Operation.UPDATE, T_2, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97),
            (Operation.UPDATE, T_3, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.UPDATE, T_3, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.2, 67),
            (Operation.UPDATE, T_4, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.1, 96),
            (Operation.UPDATE, T_4, 6, "Schindler's List", 1993, "Steven Spielberg", 8.8, 125),
            (Operation.DELETE, T_4, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.DELETE, T_4, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97)
        ],
        [
            (Operation.UPDATE, T_5, 2, "The Godfather", 1972, "Francis Ford Coppola", 8.9, 103),
            (Operation.UPDATE, T_5, 6, "Schindler's List", 1993, "Steven Spielberg", 8.3, 210),
            (Operation.CREATE, T_5, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500)
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
            (Operation.SNAPSHOT, T_0, 15, "Gary Oldman", "USA"),
        ],
        [
            (Operation.CREATE, T_2, 16, "John Travolta", "USA"),
            (Operation.CREATE, T_2, 17, "Liam Neeson", "USA"),
            (Operation.CREATE, T_2, 18, "Ralph Fiennes", "USA"),
            (Operation.CREATE, T_2, 19, "Ben Kingsley", "USA"),
            (Operation.CREATE, T_2, 20, "Leonardo DiCaprio", "USA"),
            (Operation.DELETE, T_4, 13, "Robert Duvall", "USA"),
        ],
        [
            (Operation.DELETE, T_5, 14, "John Marley", "USA"),
        ]
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
            (Operation.CREATE, T_2, 7, 20),
        ],
        [
            (Operation.CREATE, T_5, 7, 19)
        ]
    ]

    return [
        LoadedTables(
            movies=spark.createDataFrame(movies[i], schema_movies),
            actors=spark.createDataFrame(actors[i], schema_actors),
            castings=spark.createDataFrame(castings[i], schema_castings)
        )
        for i in range(len(movies))
    ]


def write_parquet_files(data: List[LoadedTables], staging_base_path: str) -> None:
    for i, batch in enumerate(data):
        batch["movies"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_MOVIES}__{i}.parquet")
        batch["actors"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_ACTORS}__{i}.parquet")
        batch["castings"].write.mode('overwrite').parquet(f"{staging_base_path}/{STAGING_TABLE_NAME_CASTINGS}__{i}.parquet")


def stage_tables(raw_vault: RawVault, staging_base_path: str) -> None:
    for f in listdir(staging_base_path):
        i = f.replace(".parquet", "").split("_")[-1]
        if SOURCE_TABLE_NAME_MOVIES in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_MOVIES}__{i}", f, HKEY_COLUMNS_MOVIES)
        elif SOURCE_TABLE_NAME_ACTORS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_ACTORS}__{i}", f, HKEY_COLUMNS_ACTORS)
        elif SOURCE_TABLE_NAME_CASTINGS in f:
            raw_vault.stage_table(f"{STAGING_TABLE_NAME_CASTINGS}__{i}", f, HKEY_COLUMNS_CASTINGS)

            
def create_data_vault_tables(raw_vault: RawVault) -> None:
    # create hubs
    raw_vault.create_hub(HUB_TABLE_NAME_MOVIES, [
        ColumnDefinition("NAME", StringType()),
        ColumnDefinition("YEAR", IntegerType())
    ])

    raw_vault.create_hub(HUB_TABLE_NAME_ACTORS, [
        ColumnDefinition("NAME", StringType()),
    ])

    # create links
    raw_vault.create_link(LINK_TABLE_NAME_CASTINGS, ["HKEY_MOVIES", "HKEY_ACTORS"])

    # create satellites
    raw_vault.create_satellite(SAT_TABLE_NAME_MOVIES, [
        ColumnDefinition("ID", StringType()),
        ColumnDefinition("DIRECTOR", StringType()),
        ColumnDefinition("RATING", DoubleType()),
        ColumnDefinition("RANK", IntegerType())
    ])

    raw_vault.create_satellite(SAT_TABLE_NAME_ACTORS, [
        ColumnDefinition("ID", StringType()),
        ColumnDefinition("COUNTRY", StringType())
    ])


def load_from_prepared_staging_table(raw_vault: RawVault, batch: int):
    # load hubs and satellites
    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_MOVIES}__{batch}", HUB_TABLE_NAME_MOVIES, HKEY_COLUMNS_MOVIES, 
        [SatelliteDefinition(SAT_TABLE_NAME_MOVIES, ['ID', 'DIRECTOR', 'RATING', 'RANK'])])

    raw_vault.load_hub_from_prepared_staging_table(
        f"{STAGING_TABLE_NAME_ACTORS}__{batch}", HUB_TABLE_NAME_ACTORS, HKEY_COLUMNS_ACTORS, 
        [SatelliteDefinition(SAT_TABLE_NAME_ACTORS, ['ID', 'COUNTRY'])])

    # load links
    raw_vault.load_link_from_prepared_stage_table(
        f"{STAGING_TABLE_NAME_CASTINGS}__{batch}", 
        [
            LinkedHubDefinition("HKEY_MOVIES", ForeignKey("MOVIE_ID", ColumnReference(f"{STAGING_TABLE_NAME_MOVIES}__{batch}", "ID"))),
            LinkedHubDefinition("HKEY_ACTORS", ForeignKey("ACTOR_ID", ColumnReference(f"{STAGING_TABLE_NAME_ACTORS}__{batch}", "ID")))
        ], LINK_TABLE_NAME_CASTINGS, None)


def test_datavault_transformatios(spark: SparkSession):
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

    # load hubs, satellites, and links from staging batch 1
    batch = 0
    load_from_prepared_staging_table(raw_vault, batch)

    # tests
    df_movies = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_MOVIES}__{batch}')
    df_actors = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_ACTORS}__{batch}')
    df_castings = spark.table(f'{config.staging_prepared_database_name}.{STAGING_TABLE_NAME_CASTINGS}__{batch}')
    print(df_movies.show())
    df_movies.filter(df_movies.id == 1).show()
    # movie = df_movies[0].iloc[0]
    # spark.sql(f'SELECT * FROM test__raw.hub__movies').show()
    # spark.table(sat_table_name)
#     assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 1, \
#         f'The movie {movie["name"]}, directed by {movie["director"]} was not found or exists multiple times.'

#     # HUB_movies "Pulp Fiction", 1994 -> Not exists
#     movie = df_movies[1].iloc[0]
#     assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 0, \
#         f'The movie {movie["name"]} was found, although it does not exist yet.'

#     # HUB_actors "Tim Robbins" -> Exists
#     actor = df_actors[0].iloc[0]
#     assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 1, \
#         f'The actor {actor["name"]} was not found or exists multiple times.'
    
#     # HUB_actors "John Travolta" -> Not exists
#     actor = df_actors[1].iloc[0]
#     assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 0, \
#         f'The actor {actor["name"]} was found, although he/she does not exist yet.'

#     # LINK "The Shawshank Redemption", 1994 -> "Tim Robbins" -> Exists
#     casting = df_castings[0].iloc[0]
#     movie = df_movies[0].query(f'id == {int(casting["movie_id"])}')
#     actor = df_actors[0].query(f'id == {int(casting["actor_id"])}')
#     assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 1, \
#         f'{actor["name"][0]} was not casted in {movie["name"][0]} or the link exists multiple times.'

#     # LINK "Pulp Fiction", 1994, "John Travolta" -> Not exists yet
#     casting = df_castings[1].iloc[0]
#     movie = df_movies[1].query(f'id == {int(casting["movie_id"])}')
#     actor = df_actors[1].query(f'id == {int(casting["actor_id"])}')
#     assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 0, \
#         f'{actor["name"][0]} was not casted in {movie["name"][0]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 9,1
#     movie = df_movies[0].iloc[0]
#     rating = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
#     assert rating == movie["rating"], \
#         f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 64
#     movie = df_movies[0].iloc[0]
#     rank = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
#     assert rank == movie["rank"], \
#         f'The queried rating of {movie["name"]} was {rank}. Correct would be {movie["rating"]}.'

#     # SAT_actors "Tim Robbins", "USA"
#     actor = df_actors[0].iloc[0]
#     country = df_sat_actors[0].query(f'hkey == "{actor["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["country"][0]
#     assert country == actor["country"], \
#         f'The queried country of {actor["name"]} was {country}. Correct would be {actor["country"]}.'



# def load_df_hub_movies():
#     columns = [Columns.LOAD_DATE, "name", "year"]
#     hash_columns = ["name", "year"]
#     data = [
#         np.array([
#             (t0, "The Shawshank Redemption", 1994)
#         ]),
#         np.array([
#             (t0, "The Shawshank Redemption", 1994)
#         ]),
#         np.array([
#             (t0, "The Shawshank Redemption", 1994)
#         ]),
#     ]
#     return create_dataframe(data, columns, hash_columns)

# def load_df_hub_actors():
#     columns = [Columns.LOAD_DATE, "name"]
#     hash_columns = ["name"]
#     data = [
#         np.array([
#             (t0, "Tim Robbins"),
#             (t0, "Morgan Freeman"),
#             (t0, "Bob Gunton")
#         ]),
#         np.array([
#             (t0, "Tim Robbins"),
#             (t0, "Morgan Freeman"),
#             (t0, "Bob Gunton")
#         ]),
#         np.array([
#             (t0, "Tim Robbins"),
#             (t0, "Morgan Freeman"),
#             (t0, "Bob Gunton")
#         ])
#     ]
#     return create_dataframe(data, columns, hash_columns)

# def load_df_link_castings():
#     columns = [Columns.LOAD_DATE, "hkey_movie", "hkey_actor"]
#     hash_columns = ["hkey_movie", "hkey_actor"]
#     data = [
#         np.array([
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
#         ]),
#         np.array([
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
#         ]),
#         np.array([
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
#             (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
#         ])
#     ]
#     return create_dataframe(data, columns, hash_columns)
    

# def load_df_sat_movies():
#     columns = [Columns.HKEY, Columns.LOAD_DATE, "id", "director", "rating", "rank"]
#     hash_columns = columns
#     data = [
#         np.array([
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64)
#         ]),
#         np.array([
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64),
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t3, 1, "Frank Darabont", 9.2, 67),
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t4, 1, "Frank Darabont", 9.6, 2)
#         ]),
#         np.array([
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64),
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t3, 1, "Frank Darabont", 9.2, 67),
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t4, 1, "Frank Darabont", 9.6, 2),
#             ("53c3e1b92b41b1d9bc78de9e48c37a14", t5, 1, "Frank Darabont", 9.5, 3)
#         ])
#     ]
#     return create_dataframe(data, columns, hash_columns, name=Columns.HDIFF)

# def load_df_sat_actors():
#     columns = [Columns.HKEY, Columns.LOAD_DATE, "id", "country"]
#     hash_columns = columns
#     data = [
#         np.array([
#             ("81715b3264ddc5350ac93b11042fae9c", t0, 1, "USA"),
#             ("956823024e15c3127dea8150feb4512d", t0, 2, "USA"),
#             ("95396cf2dcf8a21b7d1d232f6c38daea", t0, 3, "USA")
#         ]),
#         np.array([
#             ("81715b3264ddc5350ac93b11042fae9c", t0, 1, "USA"),
#             ("956823024e15c3127dea8150feb4512d", t0, 2, "USA"),
#             ("95396cf2dcf8a21b7d1d232f6c38daea", t0, 3, "USA")
#         ]),
#         np.array([
#             ("81715b3264ddc5350ac93b11042fae9c", t0, 1, "USA"),
#             ("956823024e15c3127dea8150feb4512d", t0, 2, "USA"),
#             ("95396cf2dcf8a21b7d1d232f6c38daea", t0, 3, "USA")
#         ])
#     ]
#     return create_dataframe(data, columns, hash_columns, name=Columns.HDIFF)


# def prepare_hub(staging: DataFrame, business_key_columns: List[str]) -> DataFrame:
#     # Löschen getrennt behandeln 

#     return staging \
#         .alias("l") \
#         .groupBy("l.hkey", "l.name", "l.year") \
#         .agg(F.min(F.col("l.load_date")).alias("load_date"), F.max(F.col("l.load_date")).alias("last_seen_date")) \


# def test_datavault_transformatios():
#     df_movies: List[DataFrame] = create_movies()
#     df_actors: List[DataFrame] = create_actors()
#     df_castings: List[DataFrame] = create_castings()

#     # Load sample data
#     df_hub_movies = load_df_hub_movies()
#     df_hub_actors = load_df_hub_actors()
#     df_link_castings = load_df_link_castings()
#     df_sat_movies = load_df_sat_movies()
#     df_sat_actors = load_df_sat_actors()

#     # Initial Snapshot
#     # HUB_movies "The Shawshank Redemption", 1994 -> Exists
#     movie = df_movies[0].iloc[0]
#     assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 1, \
#         f'The movie {movie["name"]}, directed by {movie["director"]} was not found or exists multiple times.'

#     # HUB_movies "Pulp Fiction", 1994 -> Not exists
#     movie = df_movies[1].iloc[0]
#     assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 0, \
#         f'The movie {movie["name"]} was found, although it does not exist yet.'

#     # HUB_actors "Tim Robbins" -> Exists
#     actor = df_actors[0].iloc[0]
#     assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 1, \
#         f'The actor {actor["name"]} was not found or exists multiple times.'
    
#     # HUB_actors "John Travolta" -> Not exists
#     actor = df_actors[1].iloc[0]
#     assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 0, \
#         f'The actor {actor["name"]} was found, although he/she does not exist yet.'

#     # LINK "The Shawshank Redemption", 1994 -> "Tim Robbins" -> Exists
#     casting = df_castings[0].iloc[0]
#     movie = df_movies[0].query(f'id == {int(casting["movie_id"])}')
#     actor = df_actors[0].query(f'id == {int(casting["actor_id"])}')
#     assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 1, \
#         f'{actor["name"][0]} was not casted in {movie["name"][0]} or the link exists multiple times.'

#     # LINK "Pulp Fiction", 1994, "John Travolta" -> Not exists yet
#     casting = df_castings[1].iloc[0]
#     movie = df_movies[1].query(f'id == {int(casting["movie_id"])}')
#     actor = df_actors[1].query(f'id == {int(casting["actor_id"])}')
#     assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 0, \
#         f'{actor["name"][0]} was not casted in {movie["name"][0]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 9,1
#     movie = df_movies[0].iloc[0]
#     rating = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
#     assert rating == movie["rating"], \
#         f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 64
#     movie = df_movies[0].iloc[0]
#     rank = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
#     assert rank == movie["rank"], \
#         f'The queried rating of {movie["name"]} was {rank}. Correct would be {movie["rating"]}.'

#     # SAT_actors "Tim Robbins", "USA"
#     actor = df_actors[0].iloc[0]
#     country = df_sat_actors[0].query(f'hkey == "{actor["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["country"][0]
#     assert country == actor["country"], \
#         f'The queried country of {actor["name"]} was {country}. Correct would be {actor["country"]}.'

#     # Day 1
#     # SAT_movies "The Shawshank Redemption", 1994, 9,6
#     movie = df_movies[1].iloc[9]
#     prev_movie = df_movies[1].iloc[5]
#     rating = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
#     prev_rating = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][1]
#     assert rating == movie["rating"], \
#         f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'
#     assert prev_rating == prev_movie["rating"], \
#         f'The queried previous rating of {prev_movie["name"]} was {prev_rating}. Correct would be {prev_movie["rating"]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 2
#     movie = df_movies[1].iloc[9]
#     prev_movie = df_movies[1].iloc[5]
#     rank = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
#     prev_rank = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][1]
#     assert rank == movie["rank"], \
#         f'The queried rank of {movie["name"]} was {rank}. Correct would be {movie["rank"]}.'
#     assert prev_rank == prev_movie["rank"], \
#         f'The queried previous rank of {prev_movie["name"]} was {prev_rank}. Correct would be {prev_movie["rank"]}.'
    
#     # # Day 2
#     # SAT_movies "The Shawshank Redemption", 1994, 9,5
#     movie = df_movies[2].iloc[3]
#     prev_movie = df_movies[1].iloc[9]
#     rating = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
#     prev_rating = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][1]
#     assert rating == movie["rating"], \
#         f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'
#     assert prev_rating == prev_movie["rating"], \
#         f'The queried previous rating of {prev_movie["name"]} was {prev_rating}. Correct would be {prev_movie["rating"]}.'

#     # SAT_movies "The Shawshank Redemption", 1994, 3
#     movie = df_movies[2].iloc[3]
#     prev_movie = df_movies[1].iloc[9]
#     rank = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
#     prev_rank = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][1]
#     assert rank == movie["rank"], \
#         f'The queried rank of {movie["name"]} was {rank}. Correct would be {movie["rank"]}.'
#     assert prev_rank == prev_movie["rank"], \
#         f'The queried previous rank of {prev_movie["name"]} was {prev_rank}. Correct would be {prev_movie["rank"]}.'


# def add_hash_column(df: DataFrame, columns: List, name=Columns.HKEY):
#     h_key = df[columns].apply(lambda x: hashlib.md5(str(tuple(x)).encode()).hexdigest(), axis=1)
#     df[name] = h_key
#     return df


# def create_dataframe(data: List, columns: List, hash_columns: List, name=Columns.HKEY):
#     return [
#         add_hash_column(pd.DataFrame(b, columns=columns), hash_columns, name) \
#         if b.size != 0 \
#         else add_hash_column(pd.DataFrame(columns=columns), hash_columns, name) \
#         for b in data
#     ]

# class RawVault(TypedDict):
#     hub__movies: DataFrame
#     hub__actors: DataFrame

#     sat__movies: DataFrame
#     sat__actors: DataFrame

#     lnk__castings: DataFrame

# class Columns:
#     HKEY = "$__HKEY"
#     HDIFF = "$__HDIFF"
#     RECORD_SOURCE = "$__RS"
#     LOAD_DATE = "$__LOAD_DATE"
#     OPERATION = "operation"

# def create_empty_vault(spark: SparkSession) -> RawVault:
#     """
#     Creates empty data frames which act as the persisted raw vault.
#     """

#     hub__movies__schema = StructType([
#         StructField(Columns.HKEY, StringType(), False),
#         StructField(Columns.LOAD_DATE, TimestampType(), False),
#         StructField("name", StringType(), False),
#         StructField("year", IntegerType(), False)
#     ])

#     hub__actors__schema = StructType([
#         StructField(Columns.HKEY, StringType(), False),
#         StructField(Columns.LOAD_DATE, TimestampType(), False),
#         StructField("name", StringType(), False)
#     ])

#     sat__movies__schema = StructType([
#         StructField(Columns.HKEY, StringType(), False),
#         StructField(Columns.LOAD_DATE, TimestampType(), False),
#         StructField(Columns.HDIFF, StringType(), False),
#         StructField("id", StringType(), True),
#         StructField("director", StringType(), True),
#         StructField("rating", DoubleType(), True),
#         StructField("rank", IntegerType(), True)
#     ])

#     sat__actors__schema = StructType([
#         StructField(Columns.HKEY, StringType(), False),
#         StructField(Columns.LOAD_DATE, TimestampType(), False),
#         StructField(Columns.HDIFF, StringType(), False),
#         StructField("id", StringType(), True),
#         StructField("country", StringType(), True)
#     ])

#     lnk__castings__schema = StructType([
#         StructField(Columns.HKEY, StringType(), False),
#         StructField(Columns.LOAD_DATE, TimestampType(), False),
#         StructField(f"{Columns.HKEY}__MOVIES", StringType(), False),
#         StructField(f"{Columns.HKEY}__ACTORS", StringType(), False)
#     ])

#     return RawVault(
#         hub__movies=spark.createDataFrame([], hub__movies__schema),
#         hub__actors=spark.createDataFrame([], hub__actors__schema),

#         sat__movies=spark.createDataFrame([], sat__movies__schema),
#         sat__actors=spark.createDataFrame([], sat__actors__schema),

#         lnk__castings=spark.createDataFrame([], lnk__castings__schema)
#     )