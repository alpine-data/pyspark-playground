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
    t0 = t1 - datetime.now(days=1)
    t2 = t1 + timedelta(hours=3, minutes=17)
    t3 = t2 + timedelta(minutes=42) 
    t4 = t3 + timedelta(hours=6)
    t5 = t1 + timedelta(days=1, minutes=42)


    data = [
        [
            (Operation.SNAPSHOT, t0, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
            (Operation.SNAPSHOT, t0, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.2, 94),
        ],
    [
        # $operation, $load_date, id, name, year, director, rating, rank
        (Operation.CREATE, t1, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
        (Operation.CREATE, t1, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.2, 94),
        (Operation.CREATE, t1, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 104),
        (Operation.CREATE, t1, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.7, 485),

        (Operation.UPDATE, t2, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.1, 91),
        (Operation.UPDATE, t2, 3, "The Dark Knight", 2008, "Christopher Nolan", 8.9, 104),
        (Operation.CREATE, t2, 5, "Pulp Fiction", 1994, "Quentin Terintino", 8.9, 138),

        (Operation.DELETE, t3, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
        (Operation.UPDATE, t4, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 101)
    ],
    [
        (Operation.UPDATE, t5, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 101),
        (Operation.DELETE, t3, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64)
    ]]

    return spark \
        .createDataFrame(data, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "year", "director", "rating", "rank"]) \
        .withColumn(Columns.HKEY, DataVaultFunctions.hash(["name", "year"]))


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