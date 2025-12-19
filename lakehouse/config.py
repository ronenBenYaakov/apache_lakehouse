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

LAKE_FOLDERS = ["raw/", "processed/", "analytics/", "metadata/"]

spark = SparkSession.builder \
    .appName("DataLakePipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

s3_base = f"s3a://{DEFAULT_BUCKET}/"

s3 = boto3.resource(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def log_pipeline_run(log_data):
    ts = datetime.now().timestamp()
    key = f"metadata/run_{int(ts)}.json"
    s3.Object(DEFAULT_BUCKET, key).put(Body=json.dumps(log_data))
