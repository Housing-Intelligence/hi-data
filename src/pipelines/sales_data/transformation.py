from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
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