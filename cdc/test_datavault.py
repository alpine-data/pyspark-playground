import hashlib

from datetime import datetime, timedelta
from select import select
from time import time
from typing import List

from chispa import *
from delta.tables import *

import numpy as np
import pandas as pd
from pandas import DataFrame

from pysparkvault.raw.LoadRaw import DataVaultFunctions
from pysparkvault import pysparkvault


class Columns:
    HKEY = "hkey"
    HDIFF = "hdiff"
    LAST_SEEN_DATE = "last_seen_date"
    LOAD_DATE = "load_date"
    OPERATION = "operation"


class Operation:
    SNAPSHOT = 0
    DELETE = 1
    CREATE = 2
    BEFORE_UPDATE = 3
    UPDATE = 4


t1 = datetime.now()
t0 = t1 - timedelta(days=1)
t2 = t1 + timedelta(hours=3, minutes=17)
t3 = t2 + timedelta(minutes=42) 
t4 = t3 + timedelta(hours=6)
t5 = t1 + timedelta(days=1, minutes=42)


def add_hash_column(df: DataFrame, columns: List, name=Columns.HKEY):
    h_key = df[columns].apply(lambda x: hashlib.md5(str(tuple(x)).encode()).hexdigest(), axis=1)
    df[name] = h_key
    return df


def create_dataframe(data: List, columns: List, hash_columns: List, name=Columns.HKEY):
    return [
        add_hash_column(pd.DataFrame(b, columns=columns), hash_columns, name) \
        if b.size != 0 \
        else add_hash_column(pd.DataFrame(columns=columns), hash_columns, name) \
        for b in data
    ]


def create_movies() -> List[DataFrame]:
    columns = [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "year", "director", "rating", "rank"]
    hash_columns = ["name", "year"]
    data = [
        np.array([
            (Operation.SNAPSHOT, t0, 1, "The Shawshank Redemption", 1994, "Frank Darabont", 9.3, 64),
            (Operation.SNAPSHOT, t0, 2, "The Godfather", 1972, "Francis Ford Coppola", 9.2, 94),
            (Operation.SNAPSHOT, t0, 3, "The Dark Knight", 2008, "Christopher Nolan", 9.0, 104),
            (Operation.SNAPSHOT, t0, 4, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.7, 485)
        ]),
        np.array([
            (Operation.CREATE, t2, 5, "Pulp Fiction", 1994, "Quentin Terintino", 8.9, 138),
            (Operation.CREATE, t2, 6, "Schindler's List", 1993, "Steven Spielberg", 8.6, 145),
            (Operation.CREATE, t2, 7, "Inception", 2010, "Christopher Nolan", 8.3, 210),
            (Operation.UPDATE, t2, 2, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97),
            (Operation.UPDATE, t3, 3, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.UPDATE, t3, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.2, 67),
            (Operation.UPDATE, t4, 3, "The Godfather", 1972, "Francis Ford Coppola", 9.1, 96),
            (Operation.UPDATE, t4, 5, "Schindler's List", 1993, "Steven Spielberg", 8.8, 125),
            (Operation.DELETE, t4, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.UPDATE, t4, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.6, 2),
            (Operation.DELETE, t4, 1, "The Dark Knight", 2008, "Christopher Nolan", 9.1, 97)
        ]),
        np.array([
            (Operation.UPDATE, t5, 3, "The Godfather", 1972, "Francis Ford Coppola", 8.9, 103),
            (Operation.UPDATE, t5, 5, "Schindler's List", 1993, "Steven Spielberg", 8.3, 210),
            (Operation.CREATE, t5, 1, "Star Wars: Episode V", 1980, "Irvin Kershner", 8.4, 500),
            (Operation.UPDATE, t5, 5, "The Shawshank Redemption", 1994, "Frank Darabont", 9.5, 3)
        ])
    ]
    return create_dataframe(data, columns, hash_columns)


def create_actors() -> List[DataFrame]:
    columns = [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "country"]
    hash_columns = ["name"]
    data = [
        np.array([
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
            (Operation.SNAPSHOT, t0, 15, "Gary Oldman", "UK")
        ]),
        np.array([
            (Operation.CREATE, t2, 16, "John Travolta", "USA"),
            (Operation.CREATE, t2, 17, "Liam Neeson", "Ireland"),
            (Operation.CREATE, t2, 18, "Ralph Fiennes", "UK"),
            (Operation.CREATE, t2, 19, "Ben Kingsley", "UK"),
            (Operation.CREATE, t2, 20, "Leonardo DiCaprio", "USA"),
            (Operation.DELETE, t4, 13, "Robert Duvall", "USA")
        ]),
        np.array([
            (Operation.DELETE, t5, 14, "John Marley", "USA")
        ])
    ]
    return create_dataframe(data, columns, hash_columns)


def create_castings() -> List[DataFrame]:
    columns = [Columns.OPERATION, Columns.LOAD_DATE, "movie_id", "actor_id"]
    hash_columns = ["movie_id", "actor_id"]
    data = [
        np.array([
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
            (Operation.SNAPSHOT, t0, 4, 12)
        ]),
        np.array([
            (Operation.CREATE, t2, 5, 16),
            (Operation.CREATE, t2, 6, 17),
            (Operation.CREATE, t2, 6, 18),
            (Operation.CREATE, t2, 6, 19),
            (Operation.CREATE, t2, 7, 20)
        ]),
        np.array([])
    ]
    return create_dataframe(data, columns, hash_columns)


def load_df_hub_movies():
    columns = [Columns.LOAD_DATE, "name", "year"]
    hash_columns = ["name", "year"]
    data = [
        np.array([
            (t0, "The Shawshank Redemption", 1994)
        ]),
        np.array([
            (t0, "The Shawshank Redemption", 1994)
        ]),
        np.array([
            (t0, "The Shawshank Redemption", 1994)
        ]),
    ]
    return create_dataframe(data, columns, hash_columns)

def load_df_hub_actors():
    columns = [Columns.LOAD_DATE, "name"]
    hash_columns = ["name"]
    data = [
        np.array([
            (t0, "Tim Robbins"),
            (t0, "Morgan Freeman"),
            (t0, "Bob Gunton")
        ]),
        np.array([
            (t0, "Tim Robbins"),
            (t0, "Morgan Freeman"),
            (t0, "Bob Gunton")
        ]),
        np.array([
            (t0, "Tim Robbins"),
            (t0, "Morgan Freeman"),
            (t0, "Bob Gunton")
        ])
    ]
    return create_dataframe(data, columns, hash_columns)

def load_df_link_castings():
    columns = [Columns.LOAD_DATE, "hkey_movie", "hkey_actor"]
    hash_columns = ["hkey_movie", "hkey_actor"]
    data = [
        np.array([
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
        ]),
        np.array([
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
        ]),
        np.array([
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "81715b3264ddc5350ac93b11042fae9c"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "956823024e15c3127dea8150feb4512d"),
            (t0, "53c3e1b92b41b1d9bc78de9e48c37a14", "95396cf2dcf8a21b7d1d232f6c38daea")
        ])
    ]
    return create_dataframe(data, columns, hash_columns)
    

def load_df_sat_movies():
    columns = [Columns.HKEY, Columns.LOAD_DATE, "id", "director", "rating", "rank"]
    hash_columns = columns
    data = [
        np.array([
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64)
        ]),
        np.array([
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64),
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t3, 1, "Frank Darabont", 9.2, 67),
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t4, 1, "Frank Darabont", 9.6, 2)
        ]),
        np.array([
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t0, 1, "Frank Darabont", 9.3, 64),
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t3, 1, "Frank Darabont", 9.2, 67),
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t4, 1, "Frank Darabont", 9.6, 2),
            ("53c3e1b92b41b1d9bc78de9e48c37a14", t5, 1, "Frank Darabont", 9.5, 3)
        ])
    ]
    return create_dataframe(data, columns, hash_columns, name=Columns.HDIFF)

def load_df_sat_actors():
    columns = [Columns.HKEY, Columns.LOAD_DATE, "country"]
    hash_columns = columns
    data = [
        np.array([
            ("81715b3264ddc5350ac93b11042fae9c", t0, "USA"),
            ("956823024e15c3127dea8150feb4512d", t0, "USA"),
            ("95396cf2dcf8a21b7d1d232f6c38daea", t0, "USA")
        ]),
        np.array([
            ("81715b3264ddc5350ac93b11042fae9c", t0, "USA"),
            ("956823024e15c3127dea8150feb4512d", t0, "USA"),
            ("95396cf2dcf8a21b7d1d232f6c38daea", t0, "USA")
        ]),
        np.array([
            ("81715b3264ddc5350ac93b11042fae9c", t0, "USA"),
            ("956823024e15c3127dea8150feb4512d", t0, "USA"),
            ("95396cf2dcf8a21b7d1d232f6c38daea", t0, "USA")
        ])
    ]
    return create_dataframe(data, columns, hash_columns, name=Columns.HDIFF)


# def prepare_hub(staging: DataFrame, business_key_columns: List[str]) -> DataFrame:
#     # Löschen getrennt behandeln 

#     return staging \
#         .alias("l") \
#         .groupBy("l.hkey", "l.name", "l.year") \
#         .agg(F.min(F.col("l.load_date")).alias("load_date"), F.max(F.col("l.load_date")).alias("last_seen_date")) \


def test_datavault_transformatios():
    df_movies: List[DataFrame] = create_movies()
    df_actors: List[DataFrame] = create_actors()
    df_castings: List[DataFrame] = create_castings()

    # Load sample data
    df_hub_movies = load_df_hub_movies()
    df_hub_actors = load_df_hub_actors()
    df_link_castings = load_df_link_castings()
    df_sat_movies = load_df_sat_movies()
    df_sat_actors = load_df_sat_actors()

    # Initial Snapshot
    # HUB_movies "The Shawshank Redemption", 1994 -> Exists
    movie = df_movies[0].iloc[0]
    assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 1, \
        f'The movie {movie["name"]}, directed by {movie["director"]} was not found or exists multiple times.'

    # HUB_movies "Pulp Fiction", 1994 -> Not exists
    movie = df_movies[1].iloc[0]
    assert df_hub_movies[0].query(f'{Columns.HKEY} == "{movie["hkey"]}"').shape[0] == 0, \
        f'The movie {movie["name"]} was found, although it does not exist yet.'

    # HUB_actors "Tim Robbins" -> Exists
    actor = df_actors[0].iloc[0]
    assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 1, \
        f'The actor {actor["name"]} was not found or exists multiple times.'
    
    # HUB_actors "John Travolta" -> Not exists
    actor = df_actors[1].iloc[0]
    assert df_hub_actors[0].query(f'{Columns.HKEY} == "{actor["hkey"]}"').shape[0] == 0, \
        f'The actor {actor["name"]} was found, although he/she does not exist yet.'

    # LINK "The Shawshank Redemption", 1994 -> "Tim Robbins" -> Exists
    casting = df_castings[0].iloc[0]
    movie = df_movies[0].query(f'id == {int(casting["movie_id"])}')
    actor = df_actors[0].query(f'id == {int(casting["actor_id"])}')
    assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 1, \
        f'{actor["name"][0]} was not casted in {movie["name"][0]} or the link exists multiple times.'

    # LINK "Pulp Fiction", 1994, "John Travolta" -> Not exists yet
    casting = df_castings[1].iloc[0]
    movie = df_movies[1].query(f'id == {int(casting["movie_id"])}')
    actor = df_actors[1].query(f'id == {int(casting["actor_id"])}')
    assert df_link_castings[0].query(f'hkey_movie == "{movie["hkey"][0]}" and hkey_actor == "{actor["hkey"][0]}"').shape[0] == 0, \
        f'{actor["name"][0]} was not casted in {movie["name"][0]}.'

    # SAT_movies "The Shawshank Redemption", 1994, 9,1
    movie = df_movies[0].iloc[0]
    rating = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
    assert rating == movie["rating"], \
        f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'

    # SAT_movies "The Shawshank Redemption", 1994, 64
    movie = df_movies[0].iloc[0]
    rank = df_sat_movies[0].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
    assert rank == movie["rank"], \
        f'The queried rating of {movie["name"]} was {rank}. Correct would be {movie["rating"]}.'

    # SAT_actors "Tim Robbins", "USA"
    actor = df_actors[0].iloc[0]
    country = df_sat_actors[0].query(f'hkey == "{actor["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["country"][0]
    assert country == actor["country"], \
        f'The queried country of {actor["name"]} was {country}. Correct would be {actor["country"]}.'

    # Day 1
    # SAT_movies "The Shawshank Redemption", 1994, 9,6
    movie = df_movies[1].iloc[9]
    prev_movie = df_movies[1].iloc[5]
    rating = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
    prev_rating = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][1]
    assert rating == movie["rating"], \
        f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'
    assert prev_rating == prev_movie["rating"], \
        f'The queried previous rating of {prev_movie["name"]} was {prev_rating}. Correct would be {prev_movie["rating"]}.'

    # SAT_movies "The Shawshank Redemption", 1994, 2
    movie = df_movies[1].iloc[9]
    prev_movie = df_movies[1].iloc[5]
    rank = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
    prev_rank = df_sat_movies[1].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][1]
    assert rank == movie["rank"], \
        f'The queried rank of {movie["name"]} was {rank}. Correct would be {movie["rank"]}.'
    assert prev_rank == prev_movie["rank"], \
        f'The queried previous rank of {prev_movie["name"]} was {prev_rank}. Correct would be {prev_movie["rank"]}.'
    
    # # Day 2
    # SAT_movies "The Shawshank Redemption", 1994, 9,5
    movie = df_movies[2].iloc[3]
    prev_movie = df_movies[1].iloc[9]
    rating = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][0]
    prev_rating = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rating"][1]
    print(rating)
    assert rating == movie["rating"], \
        f'The queried rating of {movie["name"]} was {rating}. Correct would be {movie["rating"]}.'
    assert prev_rating == prev_movie["rating"], \
        f'The queried previous rating of {prev_movie["name"]} was {prev_rating}. Correct would be {prev_movie["rating"]}.'

    # SAT_movies "The Shawshank Redemption", 1994, 3
    movie = df_movies[2].iloc[3]
    prev_movie = df_movies[1].iloc[9]
    rank = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][0]
    prev_rank = df_sat_movies[2].query(f'hkey == "{movie["hkey"]}"').sort_values(by=["load_date"], ascending=False, ignore_index=True)["rank"][1]
    assert rank == movie["rank"], \
        f'The queried rank of {movie["name"]} was {rank}. Correct would be {movie["rank"]}.'
    assert prev_rank == prev_movie["rank"], \
        f'The queried previous rank of {prev_movie["name"]} was {prev_rank}. Correct would be {prev_movie["rank"]}.'


# def test_transform(spark: SparkSession):
#     df = create_movies(spark)
#     df = prepare_hub(df, ["name", "year"])
#     df.show()

#     print(df.dtypes)
