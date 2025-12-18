from pyspark.sql import SparkSession

SPARK_APP_NAME = "DataLakeIngestion"

S3A_CONFIG = {
    "fs.s3a.access.key": "admin",
    "fs.s3a.secret.key": "admin123",
    "fs.s3a.endpoint": "http://localhost:9000",
    "fs.s3a.path.style.access": "true",
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

HADOOP_AWS_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

def create_spark():
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[1]")
        .config("spark.jars.packages", ",".join(HADOOP_AWS_PACKAGES))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    hconf = spark._jsc.hadoopConfiguration()
    for k, v in S3A_CONFIG.items():
        hconf.set(k, v)

    return spark
