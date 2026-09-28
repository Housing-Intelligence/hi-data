"""
Load sales data from public folder
"""

from pyspark import pipelines as dp
from transformation import process_sales_data

@dp.table(
    partition_cols=["transaction_year"]
)
def sales_data_silver():
    df_history = spark.readStream.table("housing_intelligence.raw.sales_data")
    df_new = spark.readStream.table("housing_intelligence.raw.sales_data_incremental")
    return process_sales_data(df_history, df_new)