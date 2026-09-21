"""
Basic transformations for POI data
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

GRID_SIZE = 0.01



def prepare_poi(spark, loc: str) -> DataFrame:

    POI_SCHEMA = StructType([
        StructField("ID", StringType(), True),
        StructField("name", StringType(), True),
        StructField("col", StringType(), True),
        StructField("major_category", StringType(), True),
        StructField("middle_category", StringType(), True),
        StructField("minor_category", StringType(), True),
        StructField("province", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),

        # Keep coordinates as strings in raw layer.
        StructField("_GCJ02", StringType(), True),
        StructField("_GCJ02_1", StringType(), True),
        StructField("_WGS84", StringType(), True),
        StructField("_WGS84_1", StringType(), True),

        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
    ])

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .schema(POI_SCHEMA)
            .load(loc)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingest_at", F.current_timestamp())
        )
    


def transform_poi(df: DataFrame) -> DataFrame:
    return (
        df
        # Basic cleaning
        .withColumn(
            "source_poi_id",
            F.trim(F.col("ID"))
        )
        .withColumn(
            "poi_name",
            F.trim(F.col("name"))
        )
        .withColumn(
            "district",
            F.trim(F.col("district"))
        )
        .withColumn(
            "address",
            F.trim(F.col("address"))
        )

        # Coordinates
        .withColumn(
            "longitude",
            F.col("_WGS84").cast("double")
        )
        .withColumn(
            "latitude",
            F.col("_WGS84_1").cast("double")
        )

        # Coordinate validation
        .filter(
            F.col("longitude").between(-180, 180)
            & F.col("latitude").between(-90, 90)
        )

        # Geo Grid
        .withColumn(
            "grid_lat",
            F.floor(
                F.col("latitude") / GRID_SIZE
            ).cast("long")
        )
        .withColumn(
            "grid_lng",
            F.floor(
                F.col("longitude") / GRID_SIZE
            ).cast("long")
        )
        .withColumn(
            "geo_grid_id",
            F.concat_ws(
                "_",
                F.col("grid_lat"),
                F.col("grid_lng")
            )
        )

        .select(
            "source_poi_id",
            "poi_name",

            "major_category",
            "middle_category",
            "minor_category",

            "province",
            "city",
            "district",

            "longitude",
            "latitude",
            "geo_grid_id",

            "phone",
            "address",

            "_source_file",
            "_ingest_at",
        )
    )
