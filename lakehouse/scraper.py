import os
from config import create_s3, DEFAULT_BUCKET, RAW_FOLDER

LOCAL_DIR = "pages"

s3 = create_s3()
bucket = s3.Bucket(DEFAULT_BUCKET)

if not os.path.isdir(LOCAL_DIR):
    raise RuntimeError(f"{LOCAL_DIR} does not exist")

files = [f for f in os.listdir(LOCAL_DIR) if f.endswith(".csv")]

if not files:
    print("No CSV files found")
    exit(0)

for fname in files:
    local_path = os.path.join(LOCAL_DIR, fname)
    s3_key = RAW_FOLDER + fname

    bucket.upload_file(local_path, s3_key)
    print(f"Uploaded {fname} → s3://{DEFAULT_BUCKET}/{s3_key}")
