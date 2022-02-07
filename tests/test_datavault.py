from datetime import datetime
from chispa import *

from delta.tables import *

import pyspark.sql.functions as F

from pysparkvault import pysparkvault

"""
def test_remove_non_word_characters(spark: SparkSession):
    load_date_1 = '2021-12-31T22:00:00Z'
    load_date_2 = '2022-01-01T22:00:00Z'

    date = datetime.now()

    data_load_1 = [
        ("johnny", "John", "Halingto", date, "Founder"),
        ("mike", "Mike", "Chemili", date, "Founder"),
        ("emil", "Emil", "Edison", date, "Assistant")
    ]

    data_load_2 = [
        ("kai", "Kai", "Granuly", date, "Accountant"),
        ("mike", "Mike", "Chemili", date, "Founder"),
        ("emil", "Emil", "Edison", date, "CTO")
    ]

    df_1 = spark.createDataFrame(data_load_1, ["id", "firstname", "name", "date_of_birth", "position"])
    df_2 = spark.createDataFrame(data_load_2, ["id", "firstname", "name", "date_of_birth", "position"])

    DeltaTable.createIfNotExists(spark) \
        .tableName("default.hub__people") \
        .addColumn("hkey", "STRING", nullable=False) \
        .addColumn("load_date", "TIMESTAMP", nullable=False) \
        .addColumn("last_seen_date", "TIMESTAMP", nullable=False) \
        .addColumn("record_source", "STRING", nullable=False) \
        .addColumn("id", "STRING") \
        .addColumn("firstname", "STRING") \
        .execute()

    DeltaTable.createIfNotExists(spark) \
        .tableName("default.sat__people") \
        .addColumn("hkey", "STRING", nullable=False) \
        .addColumn("hdiff", "STRING", nullable=False) \
        .addColumn("load_date", "TIMESTAMP", nullable=False) \
        .addColumn("load_end_date", "TIMESTAMP", nullable=True) \
        .addColumn("name", "STRING") \
        .addColumn("date_of_birth", "TIMESTAMP") \
        .addColumn("position", "STRING") \
        .execute()


    pysparkvault.load_hub(spark, 'default.hub__people', df_1, load_date_1, ['id', 'firstname'], 'some source')
    pysparkvault.load_hub(spark, 'default.hub__people', df_2, load_date_2, ['id', 'firstname'], 'some source')

    pysparkvault.load_satellite(spark, 'default.sat__people', df_1, load_date_1, ['id', 'firstname'])
    pysparkvault.load_satellite(spark, 'default.sat__people', df_2, load_date_2, ['id', 'firstname'])
"""

def create_movies(spark: SparkSession) -> DataFrame:
    data = [
        (1, "Die Vertuteilten", 1994, "Frank Darabont", 9.3, 64),
        (2, "Der Pate", 1972, "Francis Ford Coppola", 9.2, 94),
        (3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 104),
        (4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.7, 485)
    ]

    return spark.createDataFrame(data, ["id", "title", "yr", "director", "rating", "popularity"])

def create_actors(spark: SparkSession) -> DataFrame:
    data = [
        (1, "Tim Robbins"),
        (2, "Morgan Freeman"),
        (3, "Bob Gunton"),
        (4, "William Sadler"),
        (5, "Clancy Brown"),
        (6, "Marlon Brando"),
        (7, "Al Pacino"),
        (8, "James Caan"),
        (9, "Robert Duvall"),
        (10, "John Marley"),
        (11, "Christian Bayle"),
        (12, "Heath Ledger"),
        (13, "Gary Oldman"),
        (14, "Mark Hamill"),
        (15, "Harrison Ford"),
        (16, "Carrie Fisher")
    ]

    return spark.createDataFrame(data, ["id", "name"])

def create_castings(spark: SparkSession) -> DataFrame:
    data = [
        (1, 1), (1, 2), (1, 3), (1, 4), (1, 5),
        (2, 6), (2, 7), (2, 8), (2, 9), (2, 10),
        (3, 11), (3, 12), (3, 13), (3, 2),
        (4, 14), (4, 15), (4, 16)
    ]

    return spark.createDataFrame(data, ["movie_id", "actor_id"])

def test_transform(spark: SparkSession):

    movies = create_movies(spark)
    actors = create_actors(spark)
    castings = create_castings(spark)

    df: DataFrame = movies \
        .join(castings, movies["id"] == castings["movie_id"]) \
        .join(actors, castings["actor_id"] == actors["id"]) \
        .withColumnRenamed("yr", "year") \
        .select(actors["id"], "title", "year", "director", "rating", "popularity", "name")

    print("MOVIES -----..")
    movies.explain("formatted")

    print("ACTORS -----..")
    actors.explain("formatted")

    print("JOINED ------")
    df.explain()

    print("TEST ----")
    spark.sql(f'SELECT * FROM foo').select("a", "b", "c").explain()
