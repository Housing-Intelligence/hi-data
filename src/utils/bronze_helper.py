from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def ingest_raw_csv(spark, path, schema):
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .schema(schema)
            .load(path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )