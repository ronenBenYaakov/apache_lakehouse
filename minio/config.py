import boto3

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

DEFAULT_BUCKET = "ronen"

LAKE_FOLDERS = [
    "raw/",
    "processed/",
    "analytics/",
    "metadata/",
]

def create_s3():
    return boto3.resource(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_bucket(s3, bucket=DEFAULT_BUCKET):
    buckets = [b.name for b in s3.buckets.all()]
    if bucket not in buckets:
        s3.create_bucket(Bucket=bucket)
    for folder in LAKE_FOLDERS:
        s3.Object(bucket, folder).put()
