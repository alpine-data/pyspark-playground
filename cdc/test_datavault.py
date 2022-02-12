from datetime import datetime, timedelta
from functools import reduce
from select import select
from time import time
from typing import TypedDict

from chispa import *
from delta.tables import *
from pyspark.sql import session

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, NumericType, IntegerType, DoubleType
from pysparkvault.raw.LoadRaw import DataVaultFunctions

import pyspark.sql.functions as F


class Columns:
    HKEY = "$__HKEY"
    HDIFF = "$__HDIFF"
    RECORD_SOURCE = "$__RS"
    LOAD_DATE = "$__LOAD_DATE"

    OPERATION = "operation"


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


class RawVault(TypedDict):
    hub__movies: DataFrame
    hub__actors: DataFrame

    sat__movies: DataFrame
    sat__actors: DataFrame

    lnk__castings: DataFrame


t0 = datetime.now()
t1 = t0 + timedelta(days=1)
t2 = t1 + timedelta(hours=3, minutes=17)
t3 = t2 + timedelta(minutes=42) 
t4 = t3 + timedelta(hours=6)
t5 = t1 + timedelta(days=1, minutes=42)


def create_empty_vault(spark: SparkSession) -> RawVault:
    """
    Creates empty data frames which act as the persisted raw vault.
    """

    hub__movies__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField("name", StringType(), False),
        StructField("year", IntegerType(), False)
    ])

    hub__actors__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField("name", StringType(), False)
    ])

    sat__movies__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField(Columns.HDIFF, StringType(), False),
        StructField("id", StringType(), True),
        StructField("director", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("rank", IntegerType(), True)
    ])

    sat__actors__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField(Columns.HDIFF, StringType(), False),
        StructField("id", StringType(), True),
        StructField("country", StringType(), True)
    ])

    sat__actors__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField(Columns.HDIFF, StringType(), False),
        StructField("id", StringType(), True),
        StructField("country", StringType(), True)
    ])

    lnk__castings__schema = StructType([
        StructField(Columns.HKEY, StringType(), False),
        StructField(Columns.LOAD_DATE, TimestampType(), False),
        StructField(f"{Columns.HKEY}__MOVIES", StringType(), False),
        StructField(f"{Columns.HKEY}__ACTORS", StringType(), False)
    ])

    return RawVault(
        hub__movies=spark.createDataFrame([], hub__movies__schema),
        hub__actors=spark.createDataFrame([], hub__actors__schema),

        sat__movies=spark.createDataFrame([], sat__movies__schema),
        sat__actors=spark.createDataFrame([], sat__actors__schema),

        lnk__castings=spark.createDataFrame([], lnk__castings__schema)
    )


def create_sample_data(spark: SparkSession) -> List[LoadedTables]:
    """
    Creates some sample test data. The return list is a list of loaded cdc batches from the source tables.
    """

    movies = [
        [
            # $operation, $load_date, id, name, year, director, rating, rank
            (Operation.SNAPSHOT, t0, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
            (Operation.SNAPSHOT, t0, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.2, 94),
            (Operation.SNAPSHOT, t0, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 104),
            (Operation.SNAPSHOT, t0, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.7, 485)
        ],
        [
            (Operation.CREATE, t2, 5, "Pulp Fiction", 1994, "Quentin Terintino", 8.9, 138),
            (Operation.CREATE, t2, 6, "Schindler's List", 1993, "Steven Spielberg", 8.6, 145),
            (Operation.CREATE, t2, 7, "Inception", 2010, "Christopher Nolan", 8.3, 210),
            (Operation.UPDATE, t2, 2, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97),
            (Operation.UPDATE, t3, 3, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.UPDATE, t3, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.2, 67),
            (Operation.UPDATE, t4, 3, "The Godfather", 1972, "Francis Ford Coppola", 9.1, 96),
            (Operation.UPDATE, t4, 5, "Schindler's List", 1993, "Steven Spielberg", 8.8, 125),
            (Operation.DELETE, t4, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.DELETE, t4, 1, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97)
        ],
        [
            (Operation.UPDATE, t5, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.1, 69),
            (Operation.UPDATE, t5, 3, "The Godfather", 1972, "Francis Ford Coppola", 8.9, 103),
            (Operation.UPDATE, t5, 5, "Schindler's List", 1993, "Steven Spielberg", 8.3, 210),
            (Operation.CREATE, t5, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500)
        ]
    ]

    actors = [
        [
            # $operation, $load_date, id, name
            (Operation.SNAPSHOT, t0, 1, "Tim Robbins"),
            (Operation.SNAPSHOT, t0, 2, "Morgan Freeman"),
            (Operation.SNAPSHOT, t0, 3, "Bob Gunton"),
            (Operation.SNAPSHOT, t0, 4, "William Sadler"),
            (Operation.SNAPSHOT, t0, 5, "Marlon Brando"),
            (Operation.SNAPSHOT, t0, 6, "Al Pacino"),
            (Operation.SNAPSHOT, t0, 7, "James Caan"),
            (Operation.SNAPSHOT, t0, 8, "Christian Bale"),
            (Operation.SNAPSHOT, t0, 9, "Heath Ledger"),
            (Operation.SNAPSHOT, t0, 10, "Mark Hamill"),
            (Operation.SNAPSHOT, t0, 11, "Harrison Ford"),
            (Operation.SNAPSHOT, t0, 12, "Carrie Fisher"),
            (Operation.SNAPSHOT, t0, 13, "Robert Duvall"),
            (Operation.SNAPSHOT, t0, 14, "John Marley"),
            (Operation.SNAPSHOT, t0, 15, "Gary Oldman"),
        ],
        [
            (Operation.CREATE, t2, 16, "John Travolta"),
            (Operation.CREATE, t2, 17, "Liam Neeson"),
            (Operation.CREATE, t2, 18, "Ralph Fiennes"),
            (Operation.CREATE, t2, 19, "Ben Kingsley"),
            (Operation.CREATE, t2, 20, "Leonardo DiCaprio"),
            (Operation.DELETE, t4, 13, "Robert Duvall"),
        ],
        [
            (Operation.DELETE, t5, 14, "John Marley"),
        ]
    ]

    castings = [
        [
            # $operation, $load_date, movie_id, actor_id
            (Operation.SNAPSHOT, t0, 1, 1),
            (Operation.SNAPSHOT, t0, 1, 2),
            (Operation.SNAPSHOT, t0, 1, 3),
            (Operation.SNAPSHOT, t0, 1, 4),
            (Operation.SNAPSHOT, t0, 2, 5),
            (Operation.SNAPSHOT, t0, 2, 6),
            (Operation.SNAPSHOT, t0, 2, 7),
            (Operation.SNAPSHOT, t0, 3, 8),
            (Operation.SNAPSHOT, t0, 3, 9),
            (Operation.SNAPSHOT, t0, 4, 10),
            (Operation.SNAPSHOT, t0, 4, 11),
            (Operation.SNAPSHOT, t0, 4, 12),
        ],
        [
            (Operation.CREATE, t2, 5, 16),
            (Operation.CREATE, t2, 6, 17),
            (Operation.CREATE, t2, 6, 18),
            (Operation.CREATE, t2, 6, 19),
            (Operation.CREATE, t2, 7, 20),
        ],
        [
            (Operation.CREATE, t5, 7, 19)
        ]
    ]

    movies = [ spark.createDataFrame(m, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "year", "director", "rating", "rank"]) for m in movies ]
    actors = [ spark.createDataFrame(a, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name"]) for a in actors ]
    castings = [ spark.createDataFrame(c, [Columns.OPERATION, Columns.LOAD_DATE, "movie_id", "actor_id"]) for c in castings ]

    return [ LoadedTables(movies=movies[i], actors=actors[i], castings=castings[i]) for i in range (0, len(movies)) ]


def stage_hub(staging_df: DataFrame, business_key_column_names: List[str]) -> DataFrame:
    return staging_df.withColumn(Columns.HKEY, DataVaultFunctions.hash(business_key_column_names))


def load_hub(hub_df: DataFrame, staging_df: DataFrame, business_key_column_names: List[str]) -> DataFrame:
    staged_df = stage_hub(staging_df, business_key_column_names)

    return staged_df \
        .withColumn(Columns.LOAD_DATE, F.current_timestamp()) \
        .select(Columns.HKEY, Columns.LOAD_DATE, *business_key_column_names) \
        .distinct() \
        .join(hub_df, staged_df[Columns.HKEY] == hub_df[Columns.HKEY], how="left_anti") \
        .union(hub_df)


def load_link(lnk_df: DataFrame, staging_df: DataFrame, link_to: List[Tuple[DataFrame, str, str, str]]) -> DataFrame:
    """
    :param lnk_df The link table in the data vault.
    :param staging_df The staged source data frame.
    :param link_to List of linked tables.
        :0 The staging table of the linked hub.
        :1 The name of the FK column in staging_df.
        :2 The name of the FK column in linked staging table.
        :3 The name of the HKEY column in the link table.
    """

    joined = staging_df




def test_datavault_transformatios(spark: SparkSession):
    vault: RawVault = create_empty_vault(spark)
    data: List[LoadedTables] = create_sample_data(spark)

    vault["hub__movies"] = load_hub(vault["hub__movies"], data[0]["movies"], ["name", "year"])
    print(vault["hub__movies"].show())

    print("---")

    vault["hub__movies"] = load_hub(vault["hub__movies"], data[1]["movies"], ["name", "year"])
    print(vault["hub__movies"].show())
