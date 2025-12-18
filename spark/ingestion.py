from datetime import datetime
from spark.utils import convert_to_parquet
from spark.config import create_spark
from spark.utils import shutdown_spark
from minio.config import create_s3, ensure_bucket
import os
import json


def ingest_file(spark, s3, input_file, bucket="ronen"):
    dataset = os.path.splitext(os.path.basename(input_file))[0]
    raw_path = f"s3a://{bucket}/raw/{dataset}/"

    df = convert_to_parquet(spark, input_file, raw_path)
    rows = df.count()

    metadata = {
        "dataset": dataset,
        "source": input_file,
        "rows": rows,
        "columns": df.columns,
        "ingested_at": datetime.utcnow().isoformat()
    }

    meta_key = f"metadata/{dataset}_{int(datetime.utcnow().timestamp())}.json"
    s3.Object(bucket, meta_key).put(Body=json.dumps(metadata))


def ingestion_flow(input_file, bucket="ronen"):
    spark = create_spark()
    s3 = create_s3()
    ensure_bucket(s3, bucket)
    ingest_file(spark, s3, input_file, bucket)
    shutdown_spark(spark)
