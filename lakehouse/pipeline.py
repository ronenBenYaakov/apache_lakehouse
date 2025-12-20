from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.types import IntegerType, StringType, BooleanType
from config import spark, s3_base, log_pipeline_run

raw_path = s3_base + "raw/*.csv"
processed_path = s3_base + "processed/data"
analytics_path = s3_base + "analytics/summary"

def ingest(path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(path)
    print(f"[INGEST] Rows ingested from {path}: {df.count()}")
    df.show(5, truncate=50)
    return df

def clean(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates().na.drop(how="all")  # drop rows where all columns are null
    for c in df.columns:
        if df.schema[c].dataType == StringType():
            df = df.withColumn(c, col(c).cast(StringType()))
        elif df.schema[c].dataType == IntegerType():
            df = df.withColumn(c, col(c).cast(IntegerType()))
    print(f"[CLEAN] Rows after cleaning: {df.count()}")
    return df

def transform(df: DataFrame) -> DataFrame:
    if "some_column" in df.columns:
        df = df.withColumn("processed_flag", col("some_column").isNotNull().cast(BooleanType()))
    print(f"[TRANSFORM] Columns after transform: {df.columns}")
    return df

def validate(df: DataFrame):
    if "id" in df.columns:
        null_count = df.filter(col("id").isNull()).count()
        if null_count > 0:
            raise ValueError(f"Validation failed: {null_count} null IDs found!")
    print("[VALIDATE] Validation passed")

def optimize(df: DataFrame, path: str):
    partition_cols = ["id"] if "id" in df.columns else []
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    print(f"[OPTIMIZE] Data written to {path}, rows: {df.count()}")

def analytics_features(df: DataFrame, path: str):
    if "id" in df.columns:
        agg_df = df.groupBy("id").agg(count("*").alias("count_rows"))
        agg_df.write.mode("overwrite").parquet(path)
        print(f"[ANALYTICS] Analytics written to {path}, rows: {agg_df.count()}")
        return agg_df
    return None

def cataloging(processed_path: str, analytics_path: str):
    spark.sql("CREATE DATABASE IF NOT EXISTS lake_demo")
    spark.sql("DROP TABLE IF EXISTS lake_demo.processed_data")
    spark.sql(f"CREATE TABLE lake_demo.processed_data USING PARQUET LOCATION '{processed_path}'")
    spark.sql("DROP TABLE IF EXISTS lake_demo.analytics_summary")
    spark.sql(f"CREATE TABLE lake_demo.analytics_summary USING PARQUET LOCATION '{analytics_path}'")
    print("[CATALOG] Tables created in Spark catalog")

def run_pipeline():
    try:
        df = ingest(raw_path)
        if df.count() == 0:
            print("[RUN] No data to process, exiting pipeline")
            return

        df = clean(df)
        if df.count() == 0:
            print("[RUN] All rows dropped during cleaning, exiting pipeline")
            return

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
        print(f"[RUN] Pipeline finished successfully, total rows processed: {df.count()}")
    finally:
        spark.stop()
