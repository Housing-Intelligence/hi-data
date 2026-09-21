"""
Load community data from public folder
"""

from pyspark import pipelines as dp
from transformation import prepare_community, prepare_translation, transform_community

COMMINITY_PATH = "s3://housing-intelligence-data/raw_data/public_data/community_data/"
TRANSLATION_PATH = "s3://housing-intelligence-data/raw_data/public_data/community_translation/"

@dp.table
def community_raw():
    return prepare_community(spark, COMMINITY_PATH)

@dp.table
def translation_raw():
    return prepare_translation(spark, TRANSLATION_PATH)

@dp.table
def community_transform():
    df_c = spark.readStream.table(
            "housing_intelligence.processed.community_raw"
        )

    df_t = spark.table(
            "housing_intelligence.processed.translation_raw"
        )
    
    return transform_community(df_c, df_t)
