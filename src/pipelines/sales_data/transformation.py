from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType
)
from utils.bronze_helper import ingest_raw_csv

SALES_SCHEMA = StructType([
    StructField("date", StringType(), True),

    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("business_district", StringType(), True),
    StructField("community", StringType(), True),

    StructField("layout", StringType(), True),
    StructField("orientation", StringType(), True),

    StructField("floor", StringType(), True),

    StructField("area_sqm", DoubleType(), True),

    StructField("listing_price_wan", DoubleType(), True),
    StructField("deal_price_wan", DoubleType(), True),
    StructField("deal_price_yuan", DoubleType(), True),

    StructField("deal_cycle", StringType(), True),
])

def ingest_raw_sales(spark, path) -> DataFrame:
    return ingest_raw_csv(spark, path, SALES_SCHEMA).withColumn(
                "transaction_year",
                F.split(F.col("date"), "\\.")[0]
            )

def process_sales_data(df1: DataFrame, df2: DataFrame) -> DataFrame:

    df = df1.unionByName(df2, allowMissingColumns=True)

    return (
        df
        .filter(F.col("layout") != "Parking space")
        .withColumn(
            "transaction_date",
            F.to_date(
                F.col("date"),
                "yyyy.MM.dd"
            )
        )
        .withColumn(
            "transaction_year",
            F.year("transaction_date")
        )
        .withColumn(
            "deal_cycle",
            F.when(
                F.col("deal_cycle") == "暂无",
                F.lit(None).cast("integer")
            ).otherwise(
                F.regexp_extract(
                    F.col("deal_cycle"),
                    r"(\d+)",
                    1
                ).cast("integer")
            )
        )
        .withColumn(
            "price_per_sqm_yuan",
            F.when(
                F.col("area_sqm") > 0,
                F.col("deal_price_wan") * 10000 / F.col("area_sqm")
            )
        )
        .withColumn(
            "orientation",
            F.from_json(
                F.col("orientation"),
                ArrayType(StringType())
            )
        )
        .withColumn(
            "floor",
            F.from_json(
                "floor",
                ArrayType(StringType())
            )
        )
        .withColumn(
            "floor_position",
            F.col("floor")[0]
        )
        .withColumn(
            "floor_number",
            F.col("floor")[1].cast("integer")
        )
        .withColumn(
            "room_num",
            F.regexp_extract(
                F.col("layout"),
                r"(\d+)\s*room",
                1
            ).cast("integer")
        )
        .withColumn(
            "hall_num",
            F.regexp_extract(
                F.col("layout"),
                r"(\d+)\s*hall",
                1
            ).cast("integer")
        )
        .drop("layout")
        .drop("floor")
        .drop("date")
        .drop("deal_price_yuan")
    )