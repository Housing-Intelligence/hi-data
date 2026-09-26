from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

OPEN_METEO_RECORD_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("model_grid_latitude", DoubleType(), True),
    StructField("model_grid_longitude", DoubleType(), True),
    StructField("measurement_time", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("co", DoubleType(), True),
])


AQICN_RECORD_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("source_url", StringType(), True),

    StructField("station_id", IntegerType(), True),
    StructField("station_name", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("measurement_time", StringType(), True),

    StructField("aqi", IntegerType(), True),
    StructField("dominant_pollutant", StringType(), True),

    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("co", DoubleType(), True),

    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("dew_point", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_gust", DoubleType(), True),
])

AQICN_EXPECTATIONS = {

    "source_not_null":
        "source IS NOT NULL",

    "station_id_valid":
        "station_id IS NOT NULL",

    "station_name_valid":
        "station_name IS NOT NULL AND TRIM(station_name) != ''",

    "latitude_valid":
        "latitude IS NOT NULL",

    "longitude_valid":
        "longitude IS NOT NULL",

    "measurement_time_valid":
        "measurement_time IS NOT NULL",

    "aqi_valid":
        "aqi IS NULL OR aqi >= 0",

    "pm25_valid":
        "pm25 IS NULL OR pm25 >= 0",

    "pm10_valid":
        "pm10 IS NULL OR pm10 >= 0",

    "no2_valid":
        "no2 IS NULL OR no2 >= 0",

    "o3_valid":
        "o3 IS NULL OR o3 >= 0",

    "so2_valid":
        "so2 IS NULL OR so2 >= 0",

    "co_valid":
        "co IS NULL OR co >= 0",

    "humidity_valid":
        "humidity IS NULL OR "
        "(humidity >= 0 AND humidity <= 100)",
}


OPEN_METEO_EXPECTATIONS = {

    "source_not_null":
        "source IS NOT NULL",

    "source_valid":
        "source = 'OPEN_METEO'",

    "latitude_valid":
        "model_grid_latitude IS NOT NULL",

    "longitude_valid":
        "model_grid_longitude IS NOT NULL",

    "measurement_time_valid":
        "measurement_time IS NOT NULL",

    "pm25_valid":
        "pm25 IS NULL OR pm25 >= 0",

    "pm10_valid":
        "pm10 IS NULL OR pm10 >= 0",

    "no2_valid":
        "no2 IS NULL OR no2 >= 0",

    "o3_valid":
        "o3 IS NULL OR o3 >= 0",

    "so2_valid":
        "so2 IS NULL OR so2 >= 0",

    "co_valid":
        "co IS NULL OR co >= 0",
}

def transform_aqicn(df: DataFrame) -> DataFrame:

    return (
        df
        .select(
            F.col("record.source").alias("source"),
            F.col("record.source_url").alias("source_url"),
            F.col("record.station_id").alias("station_id"),
            F.col("record.station_name").alias("station_name"),
            F.col("record.latitude").alias("latitude"),
            F.col("record.longitude").alias("longitude"),
            F.to_timestamp(
                F.col("record.measurement_time")
            ).alias("measurement_time"),
            F.col("record.aqi").alias("aqi"),
            F.col(
                "record.dominant_pollutant"
            ).alias("dominant_pollutant"),
            F.col("record.pm25").alias("pm25"),
            F.col("record.pm10").alias("pm10"),
            F.col("record.no2").alias("no2"),
            F.col("record.o3").alias("o3"),
            F.col("record.so2").alias("so2"),
            F.col("record.co").alias("co"),
            F.col("record.temperature").alias(
                "temperature"
            ),
            F.col("record.humidity").alias(
                "humidity"
            ),
            F.col("record.pressure").alias(
                "pressure"
            ),
            F.col("record.dew_point").alias(
                "dew_point"
            ),
            F.col("record.wind_speed").alias(
                "wind_speed"
            ),
            F.col("record.wind_gust").alias(
                "wind_gust"
            ),
            F.col("_source_file"),
            F.col("_ingest_timestamp"),
        )
    )


# Transform Open-Meteo
def transform_open_meteo(df: DataFrame) -> DataFrame:

    return (
        df
        .select(
            F.col("record.source").alias("source"),
            F.col(
                "record.model_grid_latitude"
            ).alias("model_grid_latitude"),
            F.col(
                "record.model_grid_longitude"
            ).alias("model_grid_longitude"),
            F.to_timestamp(
                F.col("record.measurement_time")
            ).alias("measurement_time"),
            F.col("record.pm25").alias("pm25"),
            F.col("record.pm10").alias("pm10"),
            F.col("record.no2").alias("no2"),
            F.col("record.o3").alias("o3"),
            F.col("record.so2").alias("so2"),
            F.col("record.co").alias("co"),
            F.col("_source_file"),
            F.col("_ingest_timestamp"),
        )
    )


# Build validity condition
def build_valid_condition(expectations: dict):

    condition = F.lit(True)

    for expression in expectations.values():

        condition = (
            condition
            & F.expr(expression)
        )

    return condition