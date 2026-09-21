"""
Load POI Data from the public folder
"""

from pyspark import pipelines as dp
from transformation import transform_poi, prepare_poi

POI_PATH = "s3://housing-intelligence-data/raw_data/public_data/poi_data/"

@dp.table
def poi_raw():
    return prepare_poi(spark, POI_PATH)


@dp.table
def dim_poi():

    df = spark.readStream.table(
        "housing_intelligence.processed.poi_raw"
    )

    return transform_poi(df)