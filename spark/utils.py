from pathlib import Path
from bs4 import BeautifulSoup
from pyspark.sql import Row

def convert_to_parquet(spark, input_path, output_path):
    ext = Path(input_path).suffix.lower()

    readers = {
        ".csv": lambda: spark.read.csv(input_path, header=True, inferSchema=True),
        ".json": lambda: spark.read.json(input_path),
        ".parquet": lambda: spark.read.parquet(input_path),
        ".avro": lambda: spark.read.format("avro").load(input_path),
        ".orc": lambda: spark.read.orc(input_path),
        ".html": lambda: spark.createDataFrame(
            [Row(
                text=BeautifulSoup(
                    Path(input_path).read_text(encoding="utf-8"),
                    "html.parser"
                ).get_text()
            )]
        )
    }

    if ext not in readers:
        raise ValueError(f"Unsupported format: {ext}")

    df = readers[ext]()
    df.write.mode("overwrite").parquet(output_path)
    return df

def shutdown_spark(spark):
    spark.stop()
