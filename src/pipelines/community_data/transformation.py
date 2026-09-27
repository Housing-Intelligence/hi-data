from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

GRID_SIZE = 0.01

COMMUNITY_SCHEMA = StructType([
    StructField("ID", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("city_name_month", StringType(), True),
    StructField("community_id", StringType(), True),
    StructField("community_name", StringType(), True),
    StructField("subdistrict_id", StringType(), True),
    StructField("subdistrict", StringType(), True),
    StructField("biz_area_id", StringType(), True),
    StructField("business_area", StringType(), True),
    StructField("address", StringType(), True),

    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),

    StructField("cover_image", StringType(), True),
    StructField("building_type", StringType(), True),
    StructField("ownership_type", StringType(), True),
    StructField("completion_year", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("col", StringType(), True),

    StructField("near_metro", StringType(), True),
    StructField("near_school", StringType(), True),
    StructField("panorama", StringType(), True),
    StructField("has_elevator", StringType(), True),

    StructField("for_sale_units", StringType(), True),
    StructField("for_rent_units", StringType(), True),

    StructField("pinyin", StringType(), True),
    StructField("col_1", StringType(), True),

    StructField("green_rate", StringType(), True),
    StructField("total_households", StringType(), True),

    StructField("1", StringType(), True),
    StructField("description", StringType(), True),

    StructField("total_area", StringType(), True),
    StructField("heating", StringType(), True),
    StructField("water_supply", StringType(), True),

    StructField("col_2", StringType(), True),
    StructField("col_3", StringType(), True),

    StructField("property_service", StringType(), True),
    StructField("pros", StringType(), True),
    StructField("cons", StringType(), True),

    StructField("avg_price", StringType(), True),

    StructField("building_quality_score", StringType(), True),
    StructField("environment_score", StringType(), True),
    StructField("commerce_score", StringType(), True),
    StructField("education_score", StringType(), True),
    StructField("popularity_score", StringType(), True),
    StructField("user_score", StringType(), True),
    StructField("transport_score", StringType(), True),

    StructField("col_4", StringType(), True),

    StructField("living_score", StringType(), True),
    StructField("living_quality_score", StringType(), True),

    StructField("building_quality_tags", StringType(), True),
    StructField("environment_tags", StringType(), True),
    StructField("commerce_tags", StringType(), True),
    StructField("education_tags", StringType(), True),
    StructField("popularity_tags", StringType(), True),
    StructField("user_score_tags", StringType(), True),
    StructField("transport_tags", StringType(), True),

    StructField("search_name", StringType(), True),
    StructField("search_name_group", StringType(), True),
])

COMMUNITY_TRANSLATION_SCHEMA = StructType([
    StructField("community_id", StringType(), True),
    StructField("label", StringType(), True),
    StructField("community_introduction", StringType(), True),
    StructField("property_services", StringType(), True),
    StructField("community_advantages", StringType(), True),
    StructField("community_disadvantages", StringType(), True),
    StructField("commercial_supporting_labels", StringType(), True),
    StructField("education_quality_labels", StringType(), True),
    StructField("transportation_convenience_label", StringType(), True),
])

def prepare_community(spark, location:str) -> DataFrame:
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .schema(COMMUNITY_SCHEMA)
            .load(location)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )

def prepare_translation(spark, location:str) -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(COMMUNITY_TRANSLATION_SCHEMA)
        .load(location)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

def transform_community(
        community_df: DataFrame,
        translation_df: DataFrame,
    ) -> DataFrame:

    community = (community_df
                .drop(
                    "col",
                    "search_name",
                    "search_name_group"
                )
                .withColumn(
                    "longitude",
                    F.col("longitude").cast("double")
                )
                .withColumn(
                    "latitude",
                    F.col("latitude").cast("double")
                )
                .withColumn(
                    "completion_year",
                    F.col("completion_year").cast("int")
                )
                .withColumn(
                    "green_rate",
                    F.col("green_rate").cast("double")
                )
                .withColumn(
                    "total_households",
                    F.col("total_households").cast("int")
                )
                .withColumn(
                    "total_area",
                    F.col("total_area").cast("double")
                )
                .withColumn(
                    "for_sale_units",
                    F.col("for_sale_units").cast("int")
                )
                .withColumn(
                    "for_rent_units",
                    F.col("for_rent_units").cast("int")
                )
                .withColumn(
                    "avg_price",
                    F.col("avg_price").cast("double")
                )
                .withColumn(
                    "building_quality_score",
                    F.col("building_quality_score").cast("double")
                )
                .withColumn(
                    "environment_score",
                    F.col("environment_score").cast("double")
                )
                .withColumn(
                    "commerce_score",
                    F.col("commerce_score").cast("double")
                )
                .withColumn(
                    "education_score",
                    F.col("education_score").cast("double")
                )
                .withColumn(
                    "popularity_score",
                    F.col("popularity_score").cast("double")
                )
                .withColumn(
                    "user_score",
                    F.col("user_score").cast("double")
                )
                .withColumn(
                    "transport_score",
                    F.col("transport_score").cast("double")
                )
                .withColumn(
                    "living_score",
                    F.col("living_score").cast("double")
                )
                .withColumn(
                    "living_quality_score",
                    F.col("living_quality_score").cast("double")
                )
                # Add geo grid 
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
                .filter(
                    F.col("longitude").isNotNull()
                    & F.col("latitude").isNotNull()
                    & F.col("longitude").between(-180, 180)
                    & F.col("latitude").between(-90, 90)
                )
    )
    result = (
        community.alias("c")
        .join(
            translation_df.alias("t"),
            F.col("c.community_id") == F.col("t.community_id"),
            "left"
        )
        .select(
            *[
                F.col(f"c.{col}")
                for col in community.columns
                if col not in [
                    "description",
                    "property_service",
                    "pros",
                    "cons",
                    "transport_tags",
                    "education_tags",
                    "commerce_tags"
                ]
            ],

            F.coalesce(
                F.col("t.community_introduction"),
                F.col("c.description")
            ).alias("description"),

            F.coalesce(
                F.col("t.property_services"),
                F.col("c.property_service")
            ).alias("property_service"),

            F.coalesce(
                F.col("t.community_advantages"),
                F.col("c.pros")
            ).alias("pros"),

            F.coalesce(
                F.col("t.community_disadvantages"),
                F.col("c.cons")
            ).alias("cons"),

            F.coalesce(
                F.col("t.commercial_supporting_labels"),
                F.col("c.commerce_tags")
            ).alias("commerce_tags"),

            F.coalesce(
                F.col("t.education_quality_labels"),
                F.col("c.education_tags")
            ).alias("education_tags"),

            F.coalesce(
                F.col("t.transportation_convenience_label"),
                F.col("c.transport_tags")
            ).alias("transport_tags")
        )
    )

    return result
