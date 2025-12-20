from pyspark.sql import SparkSession
from datetime import datetime
import boto3
import json

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

DEFAULT_BUCKET = "ronen"

RAW_FOLDER = "raw/"
PROCESSED_FOLDER = "processed/"
ANALYTICS_FOLDER = "analytics/"
METADATA_FOLDER = "metadata/"

LAKE_FOLDERS = [
    RAW_FOLDER,
    PROCESSED_FOLDER,
    ANALYTICS_FOLDER,
    METADATA_FOLDER,
]

def create_spark():
    return (
        SparkSession.builder
        .appName("DataLakePipeline")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

spark = create_spark()

s3_base = f"s3a://{DEFAULT_BUCKET}/"

def create_s3():
    return boto3.resource(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

s3 = create_s3()

def log_pipeline_run(log_data):
    ts = int(datetime.utcnow().timestamp())
    key = f"{METADATA_FOLDER}run_{ts}.json"
    s3.Object(DEFAULT_BUCKET, key).put(
        Body=json.dumps(log_data).encode("utf-8")
    )
