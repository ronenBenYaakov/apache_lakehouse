#!/usr/bin/env bash

SPARK_HOME="$HOME/apache_lakehouse/src/spark-3.3.4-bin-hadoop3"

"$SPARK_HOME/bin/spark-submit" \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  test.py
