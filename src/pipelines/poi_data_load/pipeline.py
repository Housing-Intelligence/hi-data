"""

Load POI Data from the public folder

"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

POI_PATH = "s3://housing-intelligence-data/raw_data/public_data/poi_data/"

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

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



@dp.table
def poi_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(POI_SCHEMA)
        .load(POI_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_at", F.current_timestamp())
    )