"""
Load community data from public folder
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformation import (
    AQICN_EXPECTATIONS,
    OPEN_METEO_EXPECTATIONS,
    build_valid_condition,
    transform_aqicn,
    transform_open_meteo,
)

AIR_QUALITY_PATH = "s3://housing-intelligence-data/raw_data/public_data/air_quality/"

@dp.table(
    name="air_quality_raw",
    private=True
)
def air_quality_raw():

    return (
        spark.readStream
        .format("cloudFiles")
        .option( "cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaEvolutionMode", "rescue")
        .load(AIR_QUALITY_PATH)
        .withColumn("_source_file", F.input_file_name()
        )
        .withColumn("_ingest_timestamp", F.current_timestamp()
        )
    )

# Explode records
@dp.table(
    name="air_quality_records",
    private=True
)
def air_quality_records():
    return (
        spark.readStream.table(
            "air_quality_raw"
        )
        .withColumn(
            "record",
            F.explode("records")
        )
        .select(
            "source",
            "record_count",
            "record",
            "_source_file",
            "_ingest_timestamp",
            "_rescued_data"
        )
    )

# AQICN Bronze
@dp.table(
    name="aqicn_bronze"
)
@dp.expect_all_or_drop(
    AQICN_EXPECTATIONS
)
def aqicn_bronze():
    df = (
        spark.readStream.table(
            "air_quality_records"
        )
        .filter(
            F.col("source") == "AQICN"
        )
    )
    return transform_aqicn(df)

# AQICN Quarantine
@dp.table(
    name="aqicn_quarantine"
)
def aqicn_quarantine():

    df = (
        spark.readStream.table(
            "air_quality_records"
        )
        .filter(
            F.col("source") == "AQICN"
        )
    )

    transformed = transform_aqicn(df)

    valid_condition = build_valid_condition(
        AQICN_EXPECTATIONS
    )
    return (
        transformed
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_timestamp",
            F.current_timestamp()
        )
    )


# Open-Meteo Bronze
@dp.table(
    name="open_meteo_bronze"
)
@dp.expect_all_or_drop(
    OPEN_METEO_EXPECTATIONS
)
def open_meteo_bronze():

    df = (
        spark.readStream.table(
            "air_quality_records"
        )
        .filter(
            F.col("source") == "OPEN_METEO"
        )
    )
    return transform_open_meteo(df)

# 6. Open-Meteo Quarantine
@dp.table(
    name="open_meteo_quarantine"
)
def open_meteo_quarantine():

    df = (
        spark.readStream.table(
            "air_quality_records"
        )
        .filter(
            F.col("source") == "OPEN_METEO"
        )
    )
    transformed = transform_open_meteo(df)
    valid_condition = build_valid_condition(
        OPEN_METEO_EXPECTATIONS
    )
    return (
        transformed
        .filter(
            ~valid_condition
        )
        .withColumn(
            "_quarantine_timestamp",
            F.current_timestamp()
        )
    )