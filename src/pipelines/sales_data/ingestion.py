"""
Load sales data from public folder
"""

from pyspark import pipelines as dp
from transformation import ingest_raw_sales, process_sales_data

SALES_PATH = "s3://housing-intelligence-data/raw_data/sales_data/shell_data/new"

@dp.table(
    partition_cols=["transaction_year"]
)
def sales_data_incremental():
    return ingest_raw_sales(spark,SALES_PATH)

@dp.table(
    partition_cols=["transaction_year"]
)
def sales_data_silver():
    df_history = spark.readStream.table("housing_intelligence.raw.sales_data")
    df_new = spark.readStream.table("housing_intelligence.raw.sales_data_increment")
    return process_sales_data(spark, df_history, df_new)