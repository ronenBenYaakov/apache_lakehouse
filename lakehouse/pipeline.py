from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.types import IntegerType, StringType, BooleanType
from config import spark, s3_base, log_pipeline_run

raw_path = s3_base + "raw/*.csv"
processed_path = s3_base + "processed/data"
analytics_path = s3_base + "analytics/summary"

def ingest(path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(path)
    return df

def clean(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates().na.drop()
    for c in df.columns:
        if df.schema[c].dataType == StringType():
            df = df.withColumn(c, col(c).cast(StringType()))
    if "id" in df.columns:
        df = df.withColumn("id", col("id").cast(IntegerType()))
    return df

def transform(df: DataFrame) -> DataFrame:
    if "some_column" in df.columns:
        df = df.withColumn("processed_flag", col("some_column").isNotNull().cast(BooleanType()))
    return df

def validate(df: DataFrame):
    if "id" in df.columns and df.filter(col("id").isNull()).count() > 0:
        raise ValueError("Null IDs found in data!")

def optimize(df: DataFrame, path: str):
    partition_cols = ["id"] if "id" in df.columns else []
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)

def analytics_features(df: DataFrame, path: str):
    if "id" in df.columns:
        agg_df = df.groupBy("id").agg(count("*").alias("count_rows"))
        agg_df.write.mode("overwrite").parquet(path)
        return agg_df
    return None

def cataloging(processed_path: str, analytics_path: str):
    spark.sql("CREATE DATABASE IF NOT EXISTS lake_demo")
    spark.sql("DROP TABLE IF EXISTS lake_demo.processed_data")
    spark.sql(f"CREATE TABLE lake_demo.processed_data USING PARQUET LOCATION '{processed_path}'")
    spark.sql("DROP TABLE IF EXISTS lake_demo.analytics_summary")
    spark.sql(f"CREATE TABLE lake_demo.analytics_summary USING PARQUET LOCATION '{analytics_path}'")

def run_pipeline():
    df = ingest(raw_path)
    df = clean(df)
    validate(df)
    df = transform(df)
    optimize(df, processed_path)
    analytics_features(df, analytics_path)
    cataloging(processed_path, analytics_path)
    log_pipeline_run({
        "input_files": raw_path,
        "processed_path": processed_path,
        "analytics_path": analytics_path,
        "row_count": df.count()
    })
    spark.stop()

