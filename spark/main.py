from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def main():
    spark = (
        SparkSession.builder
        .appName("ApacheLakehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    raw_path = os.path.expanduser("~/apache_lakehouse/data/lake/raw")
    processed_path = os.path.expanduser("~/apache_lakehouse/data/lake/processed")

    df = spark.read.option("header", "true").csv(raw_path)

    df = df.select([col(c) for c in df.columns])

    df.write.mode("overwrite").parquet(processed_path)

    spark.stop()

if __name__ == "__main__":
    main()
