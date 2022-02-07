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
    DELETE = 1
    CREATE = 2
    BEFORE_UPDATE = 3
    UPDATE = 4

def create_movies(spark: SparkSession) -> DataFrame:
    t1 = datetime.now()
    t2 = t1 + timedelta(hours=3, minutes=17)
    t3 = t2 + timedelta(minutes=42) 
    t4 = t3 + timedelta(days=1)


    data = [
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
    ]

    return spark \
        .createDataFrame(data, [Columns.OPERATION, Columns.LOAD_DATE, "id", "name", "year", "director", "rating", "rank"]) \
        .withColumn(Columns.HKEY, DataVaultFunctions.hash(["name", "year"]))


def prepare_hub(staging: DataFrame, business_key_columns: List[str]) -> DataFrame:
    return staging \
        .withColumn(Columns.LAST_SEEN_DATE, staging[Columns.LOAD_DATE]) \
        .alias("l") \
        .join(staging.alias("r"), Columns.HKEY) \
        .where("l.load_date <= r.load_date AND l.operation != 1") \
        .withColumn("diff", F.col("r.load_date").cast("long") - F.col("l.load_date").cast("long")) \
        .withColumn("test", F.abs(F.hash("l.hkey"))) \
        .groupBy("l.hkey", "test", "l.name", "l.year") \
        .agg(F.min(F.col("l.load_date")).alias("load_date"), F.max(F.col("r.load_date")).alias("last_seen_date")) \

def test_transform(spark: SparkSession):
    df = create_movies(spark)
    df = prepare_hub(df, ["name", "year"])
    df.show()

    print(df.dtypes)