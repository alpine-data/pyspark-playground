from datetime import datetime, timedelta
from select import select
from time import time

from chispa import *
from delta.tables import *

import pyspark.sql.functions as F
from pyspark.sql.types import LongType

from pysparkvault.raw.LoadRaw import DataVaultFunctions


class Columns:
    HKEY = "hkey"
    LAST_SEEN_DATE = "last_seen_date"
    LOAD_DATE = "load_date"
    OPERATION = "operation"


class Operation:
    SNAPSHOT = 0
    DELETE = 1
    CREATE = 2
    BEFORE_UPDATE = 3
    UPDATE = 4


def create_movies(spark: SparkSession) -> DataFrame:
    t1 = datetime.now()
    t0 = t1 - timedelta(days=1)
    t2 = t1 + timedelta(hours=3, minutes=17)
    t3 = t2 + timedelta(minutes=42) 
    t4 = t3 + timedelta(hours=6)
    t5 = t1 + timedelta(days=1, minutes=42)

    data = [
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
            (Operation.UPDATE, t3, 3, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500)
            (Operation.UPDATE, t3, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.2, 67),
            (Operation.UPDATE, t4, 3, "The Godfather", 1972, "Francis Ford Coppola", 9.1, 96),
            (Operation.UPDATE, t4, 5, "Schindler's List", 1993, "Steven Spielberg", 8.8, 125),
            (Operation.DELETE, t4, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500)
            (Operation.DELETE, t4, 1, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97)
        ],
        [
            (Operation.UPDATE, t5, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.1, 69),
            (Operation.UPDATE, t5, 3, "The Godfather", 1972, "Francis Ford Coppola", 8.9, 103),
            (Operation.UPDATE, t5, 5, "Schindler's List", 1993, "Steven Spielberg", 8.3, 210),
            (Operation.CREATE, t5, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500)
        ]
    ]

    return [
        spark.createDataFrame(b, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "year", "director", "rating", "rank"]) \
        .withColumn(Columns.HKEY, DataVaultFunctions.hash(["name", "year"])) for b in data
    ]


def create_actors(spark: SparkSession) -> DataFrame:
    t1 = datetime.now()
    t0 = t1 - timedelta(days=1)
    t2 = t1 + timedelta(hours=3, minutes=17)
    t3 = t2 + timedelta(minutes=42) 
    t4 = t3 + timedelta(hours=6)
    t5 = t1 + timedelta(days=1, minutes=42)

    data = [
        [
            # $operation, $load_date, id, name, country
            (Operation.SNAPSHOT, t0, 1, "Tim Robbins", "USA"),
            (Operation.SNAPSHOT, t0, 2, "Morgan Freeman", "USA"),
            (Operation.SNAPSHOT, t0, 3, "Bob Gunton", "USA"),
            (Operation.SNAPSHOT, t0, 4, "William Sadler", "USA"),
            (Operation.SNAPSHOT, t0, 5, "Marlon Brando", "USA"),
            (Operation.SNAPSHOT, t0, 6, "Al Pacino", "USA"),
            (Operation.SNAPSHOT, t0, 7, "James Caan", "USA"),
            (Operation.SNAPSHOT, t0, 8, "Christian Bale", "USA"),
            (Operation.SNAPSHOT, t0, 9, "Heath Ledger", "Australia"),
            (Operation.SNAPSHOT, t0, 10, "Mark Hamill", "USA"),
            (Operation.SNAPSHOT, t0, 11, "Harrison Ford", "USA"),
            (Operation.SNAPSHOT, t0, 12, "Carrie Fisher", "USA"),
            (Operation.SNAPSHOT, t0, 13, "Robert Duvall", "USA"),
            (Operation.SNAPSHOT, t0, 14, "John Marley", "USA"),
            (Operation.SNAPSHOT, t0, 15, "Gary Oldman", "UK"),
        ],
        [
            (Operation.CREATE, t2, 16, "John Travolta", "USA"),
            (Operation.CREATE, t2, 17, "Liam Neeson", "Ireland"),
            (Operation.CREATE, t2, 18, "Ralph Fiennes", "UK"),
            (Operation.CREATE, t2, 19, "Ben Kingsley", "UK"),
            (Operation.CREATE, t2, 20, "Leonardo DiCaprio", "USA")
            (Operation.DELETE, t4, 13, "Robert Duvall", "USA"),
        ],
        [
            (Operation.DELETE, t5, 14, "John Marley", "USA"),
        ]
    ]

    return [
        spark.createDataFrame(b, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "country"]) \
        .withColumn(Columns.HKEY, DataVaultFunctions.hash(["name", "year"])) for b in data
    ]


def create_castings(spark: SparkSession) -> DataFrame:
    t1 = datetime.now()
    t0 = t1 - timedelta(days=1)
    t2 = t1 + timedelta(hours=3, minutes=17)
    t3 = t2 + timedelta(minutes=42) 
    t4 = t3 + timedelta(hours=6)
    t5 = t1 + timedelta(days=1, minutes=42)

    data = [
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
        []
    ]

    return [
        spark.createDataFrame(b, [Columns.OPERATION, Columns.LOAD_DATE, "movie_id", "actor_id"]) \
        .withColumn(Columns.HKEY, DataVaultFunctions.hash(["name", "year"])) for b in data
    ]


def prepare_hub(staging: DataFrame, business_key_columns: List[str]) -> DataFrame:
    # Löschen getrennt behandeln 

    return staging \
        .alias("l") \
        .groupBy("l.hkey", "l.name", "l.year") \
        .agg(F.min(F.col("l.load_date")).alias("load_date"), F.max(F.col("l.load_date")).alias("last_seen_date")) \


def test_datavault_transformatios(spark: SparkSession):
    # Tabellen: Movies, Actors, Casts
    df: List[Tuple[DataFrame, DataFrame, DataFrame]] = create_movies(spark)

    # Day 1
    day01 = df[0]
    df_hub_movies = load_hub("HUB__movies", day01[0], business_key_columns=["name", "year"])
    df_hub_actors = load_hub("HUB__actor", day01[0], business_key_columns=["firstname", "lastname", "dateOfBirth"])

    df_link = load_link("LINK_casts", link_table = day01[2], linked_entities = [day01[0], day01[1]])

    df_sat_movies = load_sat("HUB__movies", day01[0], business_key_columns=["name", "year"])
    df_sat_actors = load_sat("HUB__movies", day01[0], business_key_columns=["name", "year"])

    # Assert content of df_hub... and df_link and df_sat...

    # Day 2
    day02 = df[1]
    df_hub_movies = load_hub("HUB__movies", day01[0], business_key_columns=["name", "year"], current = df_hub_movies)
    df_hub_actors = load_hub("HUB__actor", day01[0], business_key_columns=["firstname", "lastname", "dateOfBirth"], current = df_hub_actors)

    df_link = load_link("LINK_casts", link_table = day01[2], linked_entities = [day01[0], day01[1]], current = df_link)

    df_sat_movies = load_sat("HUB__movies", day01[0], business_key_columns=["name", "year"], current = df_sat_movies)
    df_sat_actors = load_sat("HUB__movies", day01[0], business_key_columns=["name", "year"], current = df_sat_actors)

    # Assert updated content ...


def test_transform(spark: SparkSession):
    df = create_movies(spark)
    df = prepare_hub(df, ["name", "year"])
    df.show()

    print(df.dtypes)
