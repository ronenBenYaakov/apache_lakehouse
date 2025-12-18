from spark.ingestion import ingestion_flow
from spark.utils import convert_to_parquet
from spark.config import create_spark
from minio.config import create_s3, ensure_bucket
import os

# Example input file
input_file = '/home/ronen/apache_lakehouse/src/missing_info_questions_1.csv'  # Replace with an actual local file path

# Ensure the file exists
if not os.path.exists(input_file):
    print(f"Input file does not exist: {input_file}")
    exit(1)

# Run Prefect ingestion flow
ingestion_flow(
    input_file=input_file,
    bucket="ronen"
)

print("Test run complete. Check MinIO for raw data and metadata.")
