import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from .minio_settings import *

conf = (
    SparkConf()
    .set("spark.executor.memory", "20g")
    .set("spark.driver.memory", "40g")
    .set("spark.executor.cores", "1")
    .set(
        "spark.jars.packages",
        "com.databricks:spark-xml_2.12:0.18.0,org.apache.hadoop:hadoop-aws:3.3.0"
    )
)

if minio_username is not None and minio_password is not None and minio_host is not None:
    conf = (
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        )
        .set("fs.s3a.access.key", minio_username)
        .set("fs.s3a.secret.key", minio_password)
        .set("fs.s3a.endpoint", minio_host)
        .set("fs.s3a.path.style.access", "true")
        .set("fs.s3a.connection.ssl.enabled", "false")
        .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

spark = (
    SparkSession.builder.master(
        os.getenv("MASTER", os.getenv("SPARK_MASTER", "local[15]"))
    )
    .appName("RAGIndex")
    .config(conf=conf)
    .getOrCreate()
)
